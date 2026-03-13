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

package org.apache.hudi

import org.apache.hadoop.conf.Configuration
import org.apache.hudi.SparkFileFormatInternalRowReaderContext.{filterIsSafeForBootstrap, filterIsSafeForPrimaryKey, getAppliedRequiredSchema}
import org.apache.hudi.avro.{AvroSchemaUtils, HoodieAvroUtils}
import org.apache.hudi.common.engine.HoodieReaderContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType, HoodieSchemaUtils}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.read.buffer.PositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.collection.{CachingIterator, ClosableIterator, Pair => HPair}
import org.apache.hudi.io.storage.{HoodieSparkFileReaderFactory, HoodieSparkParquetReader}
import org.apache.hudi.storage.{HoodieStorage, StorageConfiguration, StoragePath}
import org.apache.hudi.util.CloseableInternalRowIterator
import org.apache.parquet.avro.HoodieAvroParquetSchemaConverter.getAvroSchemaConverter
import org.apache.spark.sql.HoodieInternalRowUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkColumnarFileReader}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.hudi.SparkAdapter
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{BinaryType, LongType, Metadata, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import scala.collection.JavaConverters._

/**
 * Implementation of [[HoodieReaderContext]] to read [[InternalRow]]s with
 * [[ParquetFileFormat]] on Spark.
 *
 * This uses Spark parquet reader to read parquet data files or parquet log blocks.
 *
 * @param parquetFileReader A reader that transforms a [[PartitionedFile]] to an iterator of
 *                          [[InternalRow]]. This is required for reading the base file and
 *                          not required for reading a file group with only log files.
 * @param filters           spark filters that might be pushed down into the reader
 * @param requiredFilters   filters that are required and should always be used, even in merging situations
 */
class SparkFileFormatInternalRowReaderContext(baseFileReader: SparkColumnarFileReader,
                                              filters: Seq[Filter],
                                              requiredFilters: Seq[Filter],
                                              storageConfiguration: StorageConfiguration[_],
                                              tableConfig: HoodieTableConfig)
  extends BaseSparkInternalRowReaderContext(storageConfiguration, tableConfig, SparkFileFormatInternalRecordContext.apply(tableConfig)) {
  lazy val sparkAdapter: SparkAdapter = SparkAdapterSupport.sparkAdapter
  private lazy val recordKeyFields = Option(tableConfig.getRecordKeyFields.orElse(null)).map(_.map(_.toLowerCase).toSet).getOrElse(Set.empty)
  private lazy val bootstrapSafeFilters: Seq[Filter] = filters.filter(filterIsSafeForBootstrap) ++ requiredFilters
  private lazy val morFilters = filters.filter(filterIsSafeForPrimaryKey(_, recordKeyFields)) ++ requiredFilters
  private lazy val allFilters = filters ++ requiredFilters

  override def getFileRecordIterator(filePath: StoragePath,
                                     start: Long,
                                     length: Long,
                                     dataSchema: HoodieSchema, // dataSchema refers to table schema in most cases(non log file reads).
                                     requiredSchema: HoodieSchema,
                                     storage: HoodieStorage): ClosableIterator[InternalRow] = {
    val hasRowIndexField = requiredSchema.getField(ROW_INDEX_TEMPORARY_COLUMN_NAME).isPresent
    if (hasRowIndexField) {
      assert(getRecordContext.supportsParquetRowIndex())
    }
    val structType = HoodieInternalRowUtils.getCachedSchema(requiredSchema)

    // Detect VECTOR columns and replace with BinaryType for the Parquet reader
    // (Parquet stores VECTOR as FIXED_LEN_BYTE_ARRAY which Spark maps to BinaryType)
    val vectorColumnInfo = SparkFileFormatInternalRowReaderContext.detectVectorColumns(requiredSchema)
    val parquetReadStructType = if (vectorColumnInfo.nonEmpty) {
      SparkFileFormatInternalRowReaderContext.replaceVectorColumnsWithBinary(structType, vectorColumnInfo)
    } else {
      structType
    }

    val (readSchema, readFilters) = getSchemaAndFiltersForRead(parquetReadStructType, hasRowIndexField)
    if (FSUtils.isLogFile(filePath)) {
      // NOTE: now only primary key based filtering is supported for log files
      new HoodieSparkFileReaderFactory(storage).newParquetFileReader(filePath)
        .asInstanceOf[HoodieSparkParquetReader].getUnsafeRowIterator(requiredSchema, readFilters.asJava).asInstanceOf[ClosableIterator[InternalRow]]
    } else {
      // partition value is empty because the spark parquet reader will append the partition columns to
      // each row if they are given. That is the only usage of the partition values in the reader.
      val fileInfo = sparkAdapter.getSparkPartitionedFileUtils
        .createPartitionedFile(InternalRow.empty, filePath, start, length)

      // Convert Avro dataSchema to Parquet MessageType for timestamp precision conversion
      val tableSchemaOpt = if (dataSchema != null) {
        val hadoopConf = storage.getConf.unwrapAs(classOf[Configuration])
        val parquetSchema = getAvroSchemaConverter(hadoopConf).convert(dataSchema)
        org.apache.hudi.common.util.Option.of(parquetSchema)
      } else {
        org.apache.hudi.common.util.Option.empty[org.apache.parquet.schema.MessageType]()
      }
      val rawIterator = new CloseableInternalRowIterator(baseFileReader.read(fileInfo,
        readSchema, StructType(Seq.empty), getSchemaHandler.getInternalSchemaOpt,
        readFilters, storage.getConf.asInstanceOf[StorageConfiguration[Configuration]], tableSchemaOpt))

      // Post-process: convert binary VECTOR columns back to typed arrays
      if (vectorColumnInfo.nonEmpty) {
        SparkFileFormatInternalRowReaderContext.wrapWithVectorConversion(rawIterator, vectorColumnInfo, readSchema)
      } else {
        rawIterator
      }
    }
  }

  private def getSchemaAndFiltersForRead(structType: StructType, hasRowIndexField: Boolean): (StructType, Seq[Filter]) = {
    val schemaForRead = getAppliedRequiredSchema(structType, hasRowIndexField)
    if (!getHasLogFiles && !getNeedsBootstrapMerge) {
      (schemaForRead, allFilters)
    } else if (!getHasLogFiles && hasRowIndexField) {
      (schemaForRead, bootstrapSafeFilters)
    } else if (!getNeedsBootstrapMerge) {
      (schemaForRead, morFilters)
    } else {
      (schemaForRead, requiredFilters)
    }
  }

  /**
   * Merge the skeleton file and data file iterators into a single iterator that will produce rows that contain all columns from the
   * skeleton file iterator, followed by all columns in the data file iterator
   *
   * @param skeletonFileIterator iterator over bootstrap skeleton files that contain hudi metadata columns
   * @param dataFileIterator     iterator over data files that were bootstrapped into the hudi table
   * @return iterator that concatenates the skeletonFileIterator and dataFileIterator
   */
  override def mergeBootstrapReaders(skeletonFileIterator: ClosableIterator[InternalRow],
                                     skeletonRequiredSchema: HoodieSchema,
                                     dataFileIterator: ClosableIterator[InternalRow],
                                     dataRequiredSchema: HoodieSchema,
                                     partitionFieldAndValues: java.util.List[HPair[String, Object]]): ClosableIterator[InternalRow] = {
    doBootstrapMerge(skeletonFileIterator.asInstanceOf[ClosableIterator[Any]], skeletonRequiredSchema,
      dataFileIterator.asInstanceOf[ClosableIterator[Any]], dataRequiredSchema, partitionFieldAndValues)
  }

  private def doBootstrapMerge(skeletonFileIterator: ClosableIterator[Any],
                               skeletonRequiredSchema: HoodieSchema,
                               dataFileIterator: ClosableIterator[Any],
                               dataRequiredSchema: HoodieSchema,
                               partitionFieldAndValues: java.util.List[HPair[String, Object]]): ClosableIterator[InternalRow] = {
    if (getRecordContext.supportsParquetRowIndex()) {
      assert(skeletonRequiredSchema.getField(ROW_INDEX_TEMPORARY_COLUMN_NAME).isPresent)
      assert(dataRequiredSchema.getField(ROW_INDEX_TEMPORARY_COLUMN_NAME).isPresent)
      val rowIndexColumn = new java.util.HashSet[String]()
      rowIndexColumn.add(ROW_INDEX_TEMPORARY_COLUMN_NAME)
      //always remove the row index column from the skeleton because the data file will also have the same column
      val skeletonProjection = recordContext.projectRecord(skeletonRequiredSchema,
        HoodieSchemaUtils.removeFields(skeletonRequiredSchema, rowIndexColumn))

      //If we need to do position based merging with log files we will leave the row index column at the end
      val dataProjection = if (getShouldMergeUseRecordPosition) {
        getBootstrapProjection(dataRequiredSchema, dataRequiredSchema, partitionFieldAndValues)
      } else {
        getBootstrapProjection(dataRequiredSchema,
          HoodieSchemaUtils.removeFields(dataRequiredSchema, rowIndexColumn), partitionFieldAndValues)
      }

      //row index will always be the last column
      val skeletonRowIndex = skeletonRequiredSchema.getFields.size() - 1
      val dataRowIndex = dataRequiredSchema.getFields.size() - 1

      //Always use internal row for positional merge because
      //we need to iterate row by row when merging
      new CachingIterator[InternalRow] {
        val combinedRow = new JoinedRow()

        private def getNextSkeleton: (InternalRow, Long) = {
          val nextSkeletonRow = skeletonFileIterator.next().asInstanceOf[InternalRow]
          (nextSkeletonRow, nextSkeletonRow.getLong(skeletonRowIndex))
        }

        private def getNextData: (InternalRow, Long) = {
          val nextDataRow = dataFileIterator.next().asInstanceOf[InternalRow]
          (nextDataRow,  nextDataRow.getLong(dataRowIndex))
        }

        override def close(): Unit = {
          skeletonFileIterator.close()
          dataFileIterator.close()
        }

        override protected def doHasNext(): Boolean = {
          if (!dataFileIterator.hasNext || !skeletonFileIterator.hasNext) {
            false
          } else {
            var nextSkeleton = getNextSkeleton
            var nextData = getNextData
            while (nextSkeleton._2 != nextData._2) {
              if (nextSkeleton._2 > nextData._2) {
                if (!dataFileIterator.hasNext) {
                  return false
                } else {
                  nextData = getNextData
                }
              } else {
                if (!skeletonFileIterator.hasNext) {
                  return false
                } else {
                  nextSkeleton = getNextSkeleton
                }
              }
            }
            nextRecord = combinedRow(skeletonProjection.apply(nextSkeleton._1), dataProjection.apply(nextData._1))
            true
          }
        }
      }
    } else {
      val dataProjection = getBootstrapProjection(dataRequiredSchema, dataRequiredSchema, partitionFieldAndValues)
      new ClosableIterator[Any] {
        val combinedRow = new JoinedRow()

        override def hasNext: Boolean = {
          //If the iterators are out of sync it is probably due to filter pushdown
          checkState(dataFileIterator.hasNext == skeletonFileIterator.hasNext,
            "Bootstrap data-file iterator and skeleton-file iterator have to be in-sync!")
          dataFileIterator.hasNext && skeletonFileIterator.hasNext
        }

        override def next(): Any = {
          (skeletonFileIterator.next(), dataFileIterator.next()) match {
            case (s: ColumnarBatch, d: ColumnarBatch) =>
              //This will not be used until [HUDI-7693] is implemented
              val numCols = s.numCols() + d.numCols()
              val vecs: Array[ColumnVector] = new Array[ColumnVector](numCols)
              for (i <- 0 until numCols) {
                if (i < s.numCols()) {
                  vecs(i) = s.column(i)
                } else {
                  vecs(i) = d.column(i - s.numCols())
                }
              }
              assert(s.numRows() == d.numRows())
              sparkAdapter.makeColumnarBatch(vecs, s.numRows())
            case (_: ColumnarBatch, _: InternalRow) => throw new IllegalStateException("InternalRow ColumnVector mismatch")
            case (_: InternalRow, _: ColumnarBatch) => throw new IllegalStateException("InternalRow ColumnVector mismatch")
            case (s: InternalRow, d: InternalRow) => combinedRow(s, dataProjection.apply(d))
          }
        }

        override def close(): Unit = {
          skeletonFileIterator.close()
          dataFileIterator.close()
        }
      }.asInstanceOf[ClosableIterator[InternalRow]]
    }
  }
}

object SparkFileFormatInternalRowReaderContext {
  // From "namedExpressions.scala": Used to construct to record position field metadata.
  private val FILE_SOURCE_GENERATED_METADATA_COL_ATTR_KEY = "__file_source_generated_metadata_col"
  private val FILE_SOURCE_METADATA_COL_ATTR_KEY = "__file_source_metadata_col"
  private val METADATA_COL_ATTR_KEY = "__metadata_col"

  def getAppliedRequiredSchema(requiredSchema: StructType, shouldAddRecordPosition: Boolean): StructType = {
    if (shouldAddRecordPosition) {
      val metadata = new MetadataBuilder()
        .putString(METADATA_COL_ATTR_KEY, ROW_INDEX_TEMPORARY_COLUMN_NAME)
        .putBoolean(FILE_SOURCE_METADATA_COL_ATTR_KEY, value = true)
        .putString(FILE_SOURCE_GENERATED_METADATA_COL_ATTR_KEY, ROW_INDEX_TEMPORARY_COLUMN_NAME)
        .build()
      val rowIndexField = StructField(ROW_INDEX_TEMPORARY_COLUMN_NAME, LongType, nullable = false, metadata)
      StructType(requiredSchema.fields.filterNot(isIndexTempColumn) :+ rowIndexField)
    } else {
      requiredSchema
    }
  }

  /**
   * Only valid if there is support for RowIndexField and no log files
   * Filters are safe for bootstrap if meta col filters are independent from data col filters.
   */
  def filterIsSafeForBootstrap(filter: Filter): Boolean = {
    val metaRefCount = filter.references.count(c => HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION.contains(c.toLowerCase))
    metaRefCount == filter.references.length || metaRefCount == 0
  }

  /**
   * Only valid if the filter's references only include primary key columns or {@link HoodieRecord.RECORD_KEY_METADATA_FIELD}
   */
  def filterIsSafeForPrimaryKey(filter: Filter, recordKeyFields: Set[String]): Boolean = {
    filter.references.forall(c => recordKeyFields.contains(c.toLowerCase) || c.equalsIgnoreCase(HoodieRecord.RECORD_KEY_METADATA_FIELD))
  }

  private def isIndexTempColumn(field: StructField): Boolean = {
    field.name.equals(ROW_INDEX_TEMPORARY_COLUMN_NAME)
  }

  /**
   * Detects VECTOR columns from HoodieSchema.
   * @return Map of ordinal to (dimension, elementType) for VECTOR fields.
   */
  private[hudi] def detectVectorColumns(schema: HoodieSchema): Map[Int, (Int, HoodieSchema.Vector.VectorElementType)] = {
    if (schema == null) return Map.empty
    schema.getFields.asScala.zipWithIndex.flatMap { case (field, idx) =>
      val fieldSchema = field.schema().getNonNullType
      if (fieldSchema.getType == HoodieSchemaType.VECTOR) {
        val v = fieldSchema.asInstanceOf[HoodieSchema.Vector]
        Some(idx -> (v.getDimension, v.getVectorElementType))
      } else {
        None
      }
    }.toMap
  }

  /**
   * Detects VECTOR columns from Spark StructType metadata.
   * @return Map of ordinal to (dimension, elementType) for VECTOR fields.
   */
  def detectVectorColumnsFromMetadata(schema: StructType): Map[Int, (Int, HoodieSchema.Vector.VectorElementType)] = {
    schema.fields.zipWithIndex.flatMap { case (field, idx) =>
      if (field.metadata.contains(HoodieSchema.TYPE_METADATA_FIELD)) {
        val typeStr = field.metadata.getString(HoodieSchema.TYPE_METADATA_FIELD)
        if (typeStr.startsWith("VECTOR")) {
          val parsed = HoodieSchema.parseTypeDescriptor(typeStr)
          if (parsed.getType == HoodieSchemaType.VECTOR) {
            val vectorSchema = parsed.asInstanceOf[HoodieSchema.Vector]
            Some(idx -> (vectorSchema.getDimension, vectorSchema.getVectorElementType))
          } else None
        } else None
      } else None
    }.toMap
  }

  /**
   * Replaces ArrayType with BinaryType for VECTOR columns so the Parquet reader
   * can read FIXED_LEN_BYTE_ARRAY data without type mismatch.
   */
  def replaceVectorColumnsWithBinary(structType: StructType, vectorColumns: Map[Int, _]): StructType = {
    StructType(structType.fields.zipWithIndex.map { case (field, idx) =>
      if (vectorColumns.contains(idx)) {
        StructField(field.name, BinaryType, field.nullable, Metadata.empty)
      } else {
        field
      }
    })
  }

  /**
   * Converts binary bytes from a FIXED_LEN_BYTE_ARRAY parquet column back to a typed array
   * based on the vector's element type and dimension.
   */
  def convertBinaryToVectorArray(bytes: Array[Byte], dim: Int, elemType: HoodieSchema.Vector.VectorElementType): GenericArrayData = {
    val expectedSize = dim * elemType.getElementSize
    require(bytes.length == expectedSize,
      s"Vector byte array length mismatch: expected $expectedSize but got ${bytes.length}")
    val buffer = java.nio.ByteBuffer.wrap(bytes).order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER)
    elemType match {
      case HoodieSchema.Vector.VectorElementType.FLOAT =>
        val arr = new Array[Float](dim)
        var j = 0
        while (j < dim) { arr(j) = buffer.getFloat(); j += 1 }
        new GenericArrayData(arr)
      case HoodieSchema.Vector.VectorElementType.DOUBLE =>
        val arr = new Array[Double](dim)
        var j = 0
        while (j < dim) { arr(j) = buffer.getDouble(); j += 1 }
        new GenericArrayData(arr)
      case HoodieSchema.Vector.VectorElementType.INT8 =>
        val arr = new Array[Byte](dim)
        buffer.get(arr)
        new GenericArrayData(arr)
    }
  }

  /**
   * Wraps an iterator to convert binary VECTOR columns back to typed arrays.
   * Unpacks bytes from FIXED_LEN_BYTE_ARRAY into GenericArrayData using the canonical vector byte order.
   */
  private[hudi] def wrapWithVectorConversion(
      iterator: ClosableIterator[InternalRow],
      vectorColumns: Map[Int, (Int, HoodieSchema.Vector.VectorElementType)],
      readSchema: StructType): ClosableIterator[InternalRow] = {
    val numFields = readSchema.fields.length
    new ClosableIterator[InternalRow] {
      override def hasNext: Boolean = iterator.hasNext
      override def next(): InternalRow = {
        val row = iterator.next()
        val result = new GenericInternalRow(numFields)
        var i = 0
        while (i < numFields) {
          if (row.isNullAt(i)) {
            result.setNullAt(i)
          } else if (vectorColumns.contains(i)) {
            val (dim, elemType) = vectorColumns(i)
            result.update(i, convertBinaryToVectorArray(row.getBinary(i), dim, elemType))
          } else {
            result.update(i, row.get(i, readSchema(i).dataType))
          }
          i += 1
        }
        result
      }
      override def close(): Unit = iterator.close()
    }
  }

}
