/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.execution.datasources.lance

import org.apache.hudi.SparkAdapterSupport.sparkAdapter
import org.apache.hudi.common.util
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.io.memory.HoodieArrowAllocator
import org.apache.hudi.io.storage.{HoodieSparkLanceReader, LanceRecordIterator}
import org.apache.hudi.storage.StorageConfiguration

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.MessageType
import org.apache.spark.TaskContext
import org.apache.spark.sql.avro.BlobLanceSchemaSupport
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkColumnarFileReader, SparkSchemaTransformUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}
import org.apache.spark.sql.util.LanceArrowUtils
import org.lance.file.{BlobReadMode, FileReadOptions, LanceFileReader}

import java.io.IOException

import scala.collection.JavaConverters._

/**
 * Reader for Lance files in Spark datasource.
 * Implements vectorized reading using LanceArrowColumnVector.
 *
 * @param enableVectorizedReader whether to use vectorized reading (currently always true for Lance)
 */
class SparkLanceReaderBase(enableVectorizedReader: Boolean) extends SparkColumnarFileReader {

  // Batch size for reading Lance files (number of rows per batch)
  private val DEFAULT_BATCH_SIZE = 512

  /**
   * Read a Lance file with schema projection and partition column support.
   *
   * @param file              Lance file to read
   * @param requiredSchema    desired output schema of the data (columns to read)
   * @param partitionSchema   schema of the partition columns. Partition values will be appended to the end of every row
   * @param internalSchemaOpt option of internal schema for schema.on.read (not currently used for Lance)
   * @param filters           filters for data skipping. Not guaranteed to be used; the spark plan will also apply the filters.
   * @param storageConf       the hadoop conf
   * @return iterator of rows read from the file output type says [[InternalRow]] but could be [[ColumnarBatch]]
   */
  override def read(file: PartitionedFile,
                    requiredSchema: StructType,
                    partitionSchema: StructType,
                    internalSchemaOpt: util.Option[InternalSchema],
                    filters: scala.Seq[Filter],
                    storageConf: StorageConfiguration[Configuration],
                    tableSchemaOpt: util.Option[MessageType] = util.Option.empty()): Iterator[InternalRow] = {

    val filePath = file.filePath.toString

    if (requiredSchema.isEmpty && partitionSchema.isEmpty) {
      // No columns requested - return empty iterator
      Iterator.empty
    } else {
      // Track iterator for cleanup
      var lanceIterator: LanceRecordIterator = null

      // Create child allocator for reading
      val allocator = HoodieArrowAllocator.newChildAllocator(getClass.getSimpleName + "-data-" + filePath,
        HoodieSparkLanceReader.LANCE_DATA_ALLOCATOR_SIZE);

      try {
        // Open Lance file reader
        val lanceReader = LanceFileReader.open(filePath, allocator)

        // Get schema from Lance file
        val arrowSchema = lanceReader.schema()
        val fileSchema = LanceArrowUtils.fromArrowSchema(arrowSchema)

        // Build type change info for schema evolution
        val (implicitTypeChangeInfo, sparkRequestSchema) =
          SparkSchemaTransformUtils.buildImplicitSchemaChangeInfo(fileSchema, requiredSchema)

        // Filter schema to only fields that exist in file (Lance can only read columns present in file).
        val requestSchema =
          SparkSchemaTransformUtils.filterSchemaByFileSchema(sparkRequestSchema, fileSchema)

        // Identify blob column names. When present we open Lance in DESCRIPTOR mode and let
        // LanceRecordIterator synthesize the Hudi OUT_OF_LINE reference rows directly from
        // lance-spark's BlobStructAccessor (public API). No descriptor-shape schema is exposed
        // to the projection.
        val hasBlobColumns = requestSchema.fields.exists(BlobLanceSchemaSupport.isBlobField)
        val blobFieldNameSet: java.util.Set[String] = if (hasBlobColumns) {
          val names = new java.util.HashSet[String]()
          requestSchema.fields.foreach { f =>
            if (BlobLanceSchemaSupport.isBlobField(f)) names.add(f.name)
          }
          names
        } else {
          java.util.Collections.emptySet[String]()
        }

        // Widen nullability: Lance can return non-null parent structs whose leaf children
        // are null (e.g. a BLOB's `reference` struct is non-null with all-null members).
        val iteratorSchema = forceAllFieldsNullable(requestSchema)

        val columnNames = if (iteratorSchema.nonEmpty) {
          iteratorSchema.fieldNames.toList.asJava
        } else {
          // If only partition columns requested, read minimal data
          null
        }

        // Read data with column projection. Use DESCRIPTOR mode for blob columns so the
        // iterator can read {position, size} via BlobStructAccessor without materializing
        // the bytes; the iterator maps these into a Hudi OUT_OF_LINE reference row.
        val readOpts = if (hasBlobColumns) {
          FileReadOptions.builder().blobReadMode(BlobReadMode.DESCRIPTOR).build()
        } else {
          null
        }
        val arrowReader = if (readOpts != null) {
          lanceReader.readAll(columnNames, null, DEFAULT_BATCH_SIZE, readOpts)
        } else {
          lanceReader.readAll(columnNames, null, DEFAULT_BATCH_SIZE)
        }

        lanceIterator = new LanceRecordIterator(
          allocator,
          lanceReader,
          arrowReader,
          iteratorSchema,
          filePath,
          blobFieldNameSet
        )

        // Register cleanup listener
        Option(TaskContext.get()).foreach { ctx =>
          ctx.addTaskCompletionListener[Unit](_ => lanceIterator.close())
        }

        // Iterator already emits rows in the Hudi OUT_OF_LINE shape; feed them straight into
        // the padding + cast projections below.
        val baseIter: Iterator[InternalRow] = lanceIterator.asScala

        // iteratorSchema already matches forceAllFieldsNullable(requestSchema); reuse it
        // for the padding/cast projections.
        val nullableRequestSchema = iteratorSchema

        // Create the following projections for schema evolution:
        // 1. Padding projection: add NULL for missing columns
        // 2. Casting projection: handle type conversions
        val schemaUtils = sparkAdapter.getSchemaUtils
        val paddingProj = SparkSchemaTransformUtils.generateNullPaddingProjection(nullableRequestSchema, requiredSchema)
        val castProj = SparkSchemaTransformUtils.generateUnsafeProjection(
          schemaUtils.toAttributes(requiredSchema),
          Some(SQLConf.get.sessionLocalTimeZone),
          implicitTypeChangeInfo,
          requiredSchema,
          new StructType(),
          schemaUtils
        )

        // Unify projections by applying padding and then casting for each row
        val projection: UnsafeProjection = new UnsafeProjection {
          def apply(row: InternalRow): UnsafeRow =
            castProj(paddingProj(row))
        }
        val projectedIter = baseIter.map(projection.apply)

        // Handle partition columns
        if (partitionSchema.length == 0) {
          // No partition columns - return rows directly
          projectedIter
        } else {
          // Create UnsafeProjection to convert JoinedRow to UnsafeRow
          val fullSchema = (requiredSchema.fields ++ partitionSchema.fields).map(f =>
            AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
          val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)

          // Append partition values to each row using JoinedRow, then convert to UnsafeRow
          val joinedRow = new JoinedRow()
          projectedIter.map(row => unsafeProjection(joinedRow(row, file.partitionValues)))
        }

      } catch {
        case e: Exception =>
          if (lanceIterator != null) {
            lanceIterator.close()  // Close iterator which handles lifecycle for all objects
          } else {
            allocator.close()      // Close allocator directly
          }
          throw new IOException(s"Failed to read Lance file: $filePath", e)
      }
    }
  }

  /**
   * Recursively widens nullability to true on every field of a Spark schema,
   * including children of nested structs, arrays, and maps.
   */
  private def forceAllFieldsNullable(schema: StructType): StructType = {
    StructType(schema.fields.map(forceFieldNullable))
  }

  private def forceFieldNullable(field: StructField): StructField =
    field.copy(nullable = true, dataType = forceTypeNullable(field.dataType))

  private def forceTypeNullable(dt: DataType): DataType = dt match {
    case s: StructType => StructType(s.fields.map(forceFieldNullable))
    case a: ArrayType => a.copy(elementType = forceTypeNullable(a.elementType), containsNull = true)
    case m: MapType => m.copy(
      keyType = forceTypeNullable(m.keyType),
      valueType = forceTypeNullable(m.valueType),
      valueContainsNull = true)
    case other => other
  }
}
