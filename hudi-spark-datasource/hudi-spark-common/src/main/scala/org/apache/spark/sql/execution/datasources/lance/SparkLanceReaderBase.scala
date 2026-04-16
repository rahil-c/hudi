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
import org.apache.hudi.common.schema.HoodieSchema
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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericInternalRow, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkColumnarFileReader, SparkSchemaTransformUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, DataType, LongType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.util.LanceArrowUtils
import org.apache.spark.unsafe.types.UTF8String
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

  /** Lance blob descriptor schema: Struct<position: Long, size: Long>. */
  private val BLOB_DESCRIPTOR_SCHEMA = StructType(Seq(
    StructField("position", LongType, nullable = false),
    StructField("size", LongType, nullable = false)
  ))

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

        // Identify blob column indices and build the descriptor-mode schema.
        // In DESCRIPTOR mode, blob `data: Binary` becomes `data: Struct<position, size>`.
        val blobColIndices = requestSchema.fields.zipWithIndex.collect {
          case (field, idx) if BlobLanceSchemaSupport.isBlobField(field) => idx
        }
        val hasBlobColumns = blobColIndices.nonEmpty

        // Build the schema that matches what Lance ACTUALLY returns in DESCRIPTOR mode.
        val descriptorSchema = if (hasBlobColumns) {
          StructType(requestSchema.fields.zipWithIndex.map { case (field, idx) =>
            if (blobColIndices.contains(idx)) {
              toBlobDescriptorField(field)
            } else {
              field
            }
          })
        } else {
          requestSchema
        }

        // Widen nullability: Lance can return non-null parent structs whose leaf children
        // are null (e.g. a BLOB's `reference` struct is non-null with all-null members).
        val iteratorSchema = forceAllFieldsNullable(descriptorSchema)

        val columnNames = if (iteratorSchema.nonEmpty) {
          iteratorSchema.fieldNames.toList.asJava
        } else {
          // If only partition columns requested, read minimal data
          null
        }

        // Read data with column projection. Use DESCRIPTOR mode so blob columns
        // come back as Struct<position, size> — we map these into Hudi's
        // OUT_OF_LINE reference struct before returning rows to Spark.
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

        // Create iterator. For blob fields, pass their names so the iterator
        // wraps those vectors with Spark's standard ArrowColumnVector instead
        // of LanceArrowColumnVector (whose BlobStructAccessor breaks struct access).
        val blobFieldNameSet: java.util.Set[String] = if (hasBlobColumns) {
          val names = new java.util.HashSet[String]()
          blobColIndices.foreach(idx => names.add(iteratorSchema.fields(idx).name))
          names
        } else {
          java.util.Collections.emptySet[String]()
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

        // If there are blob columns, transform descriptor rows → Hudi OUT_OF_LINE
        // structs before the main projection.
        val baseIter: Iterator[InternalRow] = if (hasBlobColumns) {
          val blobTransform = buildBlobDescriptorTransform(
            iteratorSchema, requestSchema, blobColIndices, filePath)
          lanceIterator.asScala.map(blobTransform)
        } else {
          lanceIterator.asScala
        }

        // Widen the requestSchema nullability for the padding/casting projections
        // (same reason as iteratorSchema widening above).
        val nullableRequestSchema = forceAllFieldsNullable(requestSchema)

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
   * Replace the `data` child of a blob StructField with the Lance descriptor
   * schema (Struct<position: Long, size: Long>), keeping all other children.
   */
  private def toBlobDescriptorField(blobField: StructField): StructField = {
    val blobStruct = blobField.dataType.asInstanceOf[StructType]
    val newFields = blobStruct.fields.map { child =>
      if (child.name == HoodieSchema.Blob.INLINE_DATA_FIELD) {
        child.copy(dataType = BLOB_DESCRIPTOR_SCHEMA)
      } else {
        child
      }
    }
    blobField.copy(dataType = StructType(newFields))
  }

  /**
   * Build a row-level transformation function that converts Lance DESCRIPTOR
   * blob columns (with `data: Struct<position, size>`) into Hudi OUT_OF_LINE
   * reference structs (with `data: null, reference: {external_path, offset, length, managed}`).
   *
   * Non-blob columns are passed through unchanged.
   *
   * Scope: the OUT_OF_LINE branch is exercised end-to-end via
   * [[org.apache.hudi.functional.TestLanceDataSource#testBlobOutline]] (bytes
   * round-tripped through read_blob). The INLINE branch rewrites Lance's
   * {position, size} into an OUT_OF_LINE reference pointing at the Lance file
   * itself so downstream BatchedBlobReader can pread it, but it is not yet
   * covered by a byte-level round-trip test — INLINE isn't validated
   * end-to-end through Parquet either in the current BLOB PR.
   */
  private def buildBlobDescriptorTransform(
      descriptorSchema: StructType,
      outputSchema: StructType,
      blobColIndices: Array[Int],
      lancePath: String): InternalRow => InternalRow = {

    val numFields = descriptorSchema.fields.length
    val blobSet = blobColIndices.toSet
    val lancePathUtf8 = UTF8String.fromString(lancePath)

    // Pre-compute child field counts for blob structs (always 3: type, data, reference)
    val blobStructSize = HoodieSchema.Blob.getFieldCount

    // Pre-compute the reference struct child indices
    val outputBlobStructs = blobColIndices.map { idx =>
      outputSchema.fields(idx).dataType.asInstanceOf[StructType]
    }

    (inputRow: InternalRow) => {
      val result = new Array[Any](numFields)
      var blobIdx = 0
      for (i <- 0 until numFields) {
        if (blobSet.contains(i)) {
          val descriptorStruct = descriptorSchema.fields(i).dataType.asInstanceOf[StructType]
          val descriptorChildCount = descriptorStruct.fields.length

          if (inputRow.isNullAt(i)) {
            result(i) = null
          } else {
            val payloadRow = inputRow.getStruct(i, descriptorChildCount)
            val typeChildIdx = descriptorStruct.fieldIndex(HoodieSchema.Blob.TYPE)
            val blobType = if (payloadRow.isNullAt(typeChildIdx)) null
                           else payloadRow.getUTF8String(typeChildIdx)

            if (blobType != null && blobType.toString == HoodieSchema.Blob.OUT_OF_LINE) {
              // OUT_OF_LINE blob: the original reference struct is preserved by Lance.
              // Rebuild the struct with the correct output schema (data: Binary = null)
              // and the original reference sub-struct.
              val refChildIdx = descriptorStruct.fieldIndex(HoodieSchema.Blob.EXTERNAL_REFERENCE)
              val refChildCount = descriptorStruct.fields(refChildIdx).dataType.asInstanceOf[StructType].fields.length
              val originalRef = if (payloadRow.isNullAt(refChildIdx)) null
                                else payloadRow.getStruct(refChildIdx, refChildCount)
              val blobRow = new GenericInternalRow(Array[Any](
                blobType,           // type (OUT_OF_LINE)
                null,               // data (not materialized)
                originalRef         // original reference
              ))
              result(i) = blobRow
            } else {
              // INLINE blob: Lance stored the bytes as a blob; DESCRIPTOR mode returns
              // {data: Struct<position, size>}. Map this to Hudi's OUT_OF_LINE reference
              // so BatchedBlobReader can read the bytes from the Lance file.
              val dataChildIdx = descriptorStruct.fieldIndex(HoodieSchema.Blob.INLINE_DATA_FIELD)
              val (position, size) = if (payloadRow.isNullAt(dataChildIdx)) {
                (0L, 0L)
              } else {
                val dataDescriptor = payloadRow.getStruct(dataChildIdx, 2)
                (dataDescriptor.getLong(0), dataDescriptor.getLong(1))
              }

              val referenceRow = new GenericInternalRow(Array[Any](
                lancePathUtf8,      // external_path
                position,           // offset
                size,               // length
                true: java.lang.Boolean  // managed
              ))

              val blobRow = new GenericInternalRow(Array[Any](
                UTF8String.fromString(HoodieSchema.Blob.OUT_OF_LINE),
                null,               // data (not materialized)
                referenceRow
              ))
              result(i) = blobRow
            }
          }
          blobIdx += 1
        } else {
          result(i) = if (inputRow.isNullAt(i)) null else inputRow.get(i, descriptorSchema.fields(i).dataType)
        }
      }
      new GenericInternalRow(result)
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
