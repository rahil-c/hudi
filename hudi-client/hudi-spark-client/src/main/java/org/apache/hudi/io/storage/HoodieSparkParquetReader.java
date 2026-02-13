/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io.storage;

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetReaderIterator;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.SchemaRepair;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.avro.HoodieSparkSchemaConverters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.spark.sql.execution.datasources.parquet.HoodieParquetReadSupport;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport;
import org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter;
import org.apache.spark.sql.execution.datasources.parquet.SparkBasicSchemaEvolution;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import scala.Option$;

import static org.apache.hudi.common.util.TypeUtils.unsafeCast;
import static org.apache.parquet.avro.AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS;
import static org.apache.parquet.avro.HoodieAvroParquetSchemaConverter.getAvroSchemaConverter;
import static org.apache.parquet.hadoop.ParquetInputFormat.RECORD_FILTERING_ENABLED;

public class HoodieSparkParquetReader implements HoodieSparkFileReader {

  public static final String ENABLE_LOGICAL_TIMESTAMP_REPAIR = "spark.hudi.logicalTimestampField.repair.enable";
  private final StoragePath path;
  private final HoodieStorage storage;
  private final FileFormatUtils parquetUtils;
  private final List<ClosableIterator> readerIterators = new ArrayList<>();
  private Option<MessageType> fileSchemaOption = Option.empty();
  private Option<StructType> structTypeOption = Option.empty();
  private Option<HoodieSchema> schemaOption = Option.empty();

  public HoodieSparkParquetReader(HoodieStorage storage, StoragePath path) {
    this.path = path;
    this.storage = storage.newInstance(path, storage.getConf().newInstance());
    // Avoid adding record in list element when convert parquet schema to avro schema
    this.storage.getConf().set(ADD_LIST_ELEMENT_RECORDS, "false");
    this.parquetUtils = HoodieIOFactory.getIOFactory(storage)
        .getFileFormatUtils(HoodieFileFormat.PARQUET);
  }

  @Override
  public String[] readMinMaxRecordKeys() {
    return parquetUtils.readMinMaxRecordKeys(storage, path);
  }

  @Override
  public BloomFilter readBloomFilter() {
    return parquetUtils.readBloomFilterFromMetadata(storage, path);
  }

  @Override
  public Set<Pair<String, Long>> filterRowKeys(Set<String> candidateRowKeys) {
    return parquetUtils.filterRowKeys(storage, path, candidateRowKeys);
  }

  @Override
  public ClosableIterator<HoodieRecord<InternalRow>> getRecordIterator(HoodieSchema readerSchema, HoodieSchema requestedSchema) throws IOException {
    return getRecordIterator(requestedSchema);
  }

  @Override
  public ClosableIterator<HoodieRecord<InternalRow>> getRecordIterator(HoodieSchema schema) throws IOException {
    ClosableIterator<UnsafeRow> iterator = getUnsafeRowIterator(schema);
    return new CloseableMappingIterator<>(iterator, data -> unsafeCast(new HoodieSparkRecord(data)));
  }

  @Override
  public ClosableIterator<String> getRecordKeyIterator() throws IOException {
    HoodieSchema schema = HoodieSchemaUtils.getRecordKeySchema();
    ClosableIterator<UnsafeRow> iterator = getUnsafeRowIterator(schema);
    return new CloseableMappingIterator<>(iterator, data -> {
      HoodieSparkRecord record = unsafeCast(new HoodieSparkRecord(data));
      return record.getRecordKey();
    });
  }

  public ClosableIterator<UnsafeRow> getUnsafeRowIterator(HoodieSchema requestedSchema) throws IOException {
    return getUnsafeRowIterator(requestedSchema, Collections.emptyList());
  }

  /**
   * Read parquet with requested schema and filters.
   * WARN:
   * Currently, the filter must only contain field references related to the primary key, as the primary key does not involve schema evolution.
   * If it is necessary to expand to push down more fields in the future, please consider the issue of schema evolution carefully
   */
  public ClosableIterator<UnsafeRow> getUnsafeRowIterator(HoodieSchema requestedSchema, List<Filter> readFilters) throws IOException {
    HoodieSchema nonNullSchema = requestedSchema.getNonNullType();
    StructType structSchema = HoodieInternalRowUtils.getCachedSchema(nonNullSchema);

    // Detect vector columns: ordinal → dimension
    Map<Integer, Integer> vectorColumnInfo = detectVectorColumns(nonNullSchema);

    // For vector columns, replace ArrayType(FloatType) with BinaryType in the read schema
    // so SparkBasicSchemaEvolution sees matching types (file has FIXED_LEN_BYTE_ARRAY → BinaryType)
    StructType readStructSchema = structSchema;
    if (!vectorColumnInfo.isEmpty()) {
      StructField[] fields = structSchema.fields().clone();
      for (Map.Entry<Integer, Integer> entry : vectorColumnInfo.entrySet()) {
        int idx = entry.getKey();
        StructField orig = fields[idx];
        fields[idx] = new StructField(orig.name(), DataTypes.BinaryType, orig.nullable(), Metadata.empty());
      }
      readStructSchema = new StructType(fields);
    }

    Option<MessageType> messageSchema = Option.of(getAvroSchemaConverter(storage.getConf().unwrapAs(Configuration.class)).convert(nonNullSchema));
    boolean enableTimestampFieldRepair = storage.getConf().getBoolean(ENABLE_LOGICAL_TIMESTAMP_REPAIR, true);
    StructType dataStructType = convertToStruct(enableTimestampFieldRepair ? SchemaRepair.repairLogicalTypes(getFileSchema(), messageSchema) : getFileSchema());
    SparkBasicSchemaEvolution evolution = new SparkBasicSchemaEvolution(dataStructType, readStructSchema, SQLConf.get().sessionLocalTimeZone());
    String readSchemaJson = evolution.getRequestSchema().json();
    SQLConf sqlConf = SQLConf.get();
    storage.getConf().set(ParquetReadSupport.PARQUET_READ_SCHEMA, readSchemaJson);
    storage.getConf().set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA(), readSchemaJson);
    storage.getConf().set(SQLConf.PARQUET_BINARY_AS_STRING().key(), sqlConf.getConf(SQLConf.PARQUET_BINARY_AS_STRING()).toString());
    storage.getConf().set(SQLConf.PARQUET_INT96_AS_TIMESTAMP().key(), sqlConf.getConf(SQLConf.PARQUET_INT96_AS_TIMESTAMP()).toString());
    RebaseDateTime.RebaseSpec rebaseDateSpec = SparkAdapterSupport$.MODULE$.sparkAdapter().getRebaseSpec("CORRECTED");
    boolean parquetFilterPushDown = storage.getConf().getBoolean(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED().key(), sqlConf.parquetFilterPushDown());
    if (parquetFilterPushDown && readFilters != null && !readFilters.isEmpty()) {
      ParquetFilters parquetFilters = SparkAdapterSupport$.MODULE$.sparkAdapter().createParquetFilters(
          getFileSchema(),
          storage.getConf(),
          sqlConf
      );
      Option<FilterPredicate> predicateOpt = Option.fromJavaOptional(readFilters
          .stream()
          .map(filter -> parquetFilters.createFilter(filter))
          .filter(opt -> opt.isDefined())
          .map(opt -> opt.get())
          .reduce(FilterApi::and));
      predicateOpt
          .ifPresent(predicate -> {
            // set the filter predicate, it will be used to filter row groups and records(may be)
            ParquetInputFormat.setFilterPredicate(storage.getConf().unwrapAs(Configuration.class), predicate);
            // explicitly specify whether to filter records
            storage.getConf().set(RECORD_FILTERING_ENABLED.toString(),
                String.valueOf(storage.getConf().getBoolean(SQLConf.PARQUET_RECORD_FILTER_ENABLED().key(), sqlConf.parquetRecordFilterEnabled())));
          });
    }
    ParquetReader<InternalRow> reader = ParquetReader.builder(new HoodieParquetReadSupport(Option$.MODULE$.empty(), true, true,
                rebaseDateSpec,
                SparkAdapterSupport$.MODULE$.sparkAdapter().getRebaseSpec("LEGACY"), messageSchema),
            new Path(path.toUri()))
        .withConf(storage.getConf().unwrapAs(Configuration.class))
        .build();
    UnsafeProjection projection = evolution.generateUnsafeProjection();
    ParquetReaderIterator<InternalRow> parquetReaderIterator = new ParquetReaderIterator<>(reader);
    CloseableMappingIterator<InternalRow, UnsafeRow> projectedIterator = new CloseableMappingIterator<>(parquetReaderIterator, projection::apply);

    if (!vectorColumnInfo.isEmpty()) {
      // Post-process: convert binary VECTOR columns back to float arrays
      UnsafeProjection vectorProjection = UnsafeProjection.create(structSchema);
      int numFields = readStructSchema.fields().length;
      StructType finalReadSchema = readStructSchema;
      CloseableMappingIterator<UnsafeRow, UnsafeRow> vectorIterator =
          new CloseableMappingIterator<>(projectedIterator, row -> {
            GenericInternalRow converted = new GenericInternalRow(numFields);
            for (int i = 0; i < numFields; i++) {
              if (row.isNullAt(i)) {
                converted.setNullAt(i);
              } else if (vectorColumnInfo.containsKey(i)) {
                byte[] bytes = row.getBinary(i);
                int dim = vectorColumnInfo.get(i);
                ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
                float[] floats = new float[dim];
                for (int j = 0; j < dim; j++) {
                  floats[j] = buffer.getFloat();
                }
                converted.update(i, new GenericArrayData(floats));
              } else {
                converted.update(i, row.get(i, finalReadSchema.apply(i).dataType()));
              }
            }
            return vectorProjection.apply(converted);
          });
      readerIterators.add(vectorIterator);
      return vectorIterator;
    }

    readerIterators.add(projectedIterator);
    return projectedIterator;
  }

  /**
   * Detects vector columns in the schema and returns a map of ordinal to dimension.
   */
  private static Map<Integer, Integer> detectVectorColumns(HoodieSchema schema) {
    Map<Integer, Integer> vectorColumnInfo = new HashMap<>();
    if (schema == null) {
      return vectorColumnInfo;
    }
    List<HoodieSchemaField> fields = schema.getFields();
    for (int i = 0; i < fields.size(); i++) {
      HoodieSchema fieldSchema = fields.get(i).schema().getNonNullType();
      if (fieldSchema.getType() == HoodieSchemaType.VECTOR) {
        vectorColumnInfo.put(i, ((HoodieSchema.Vector) fieldSchema).getDimension());
      }
    }
    return vectorColumnInfo;
  }

  private MessageType getFileSchema() {
    if (fileSchemaOption.isEmpty()) {
      MessageType messageType = ((ParquetUtils) parquetUtils).readMessageType(storage, path);
      fileSchemaOption = Option.of(messageType);
    }
    return fileSchemaOption.get();
  }

  @Override
  public HoodieSchema getSchema() {
    if (schemaOption.isEmpty()) {
      // Some types in avro are not compatible with parquet.
      // Avro only supports representing Decimals as fixed byte array
      // and therefore if we convert to Avro directly we'll lose logical type-info.
      MessageType messageType = getFileSchema();
      StructType structType = getStructSchema();
      schemaOption = Option.of(HoodieSparkSchemaConverters.toHoodieType(
          structType, true, messageType.getName(), StringUtils.EMPTY_STRING, Metadata.empty()));
    }
    return schemaOption.get();
  }

  protected StructType getStructSchema() {
    if (structTypeOption.isEmpty()) {
      MessageType messageType = getFileSchema();
      structTypeOption = Option.of(convertToStruct(messageType));
    }
    return structTypeOption.get();
  }

  private StructType convertToStruct(MessageType messageType) {
    return new ParquetToSparkSchemaConverter(storage.getConf().unwrapAs(Configuration.class)).convert(messageType);
  }

  @Override
  public void close() {
    readerIterators.forEach(ClosableIterator::close);
  }

  @Override
  public long getTotalRecords() {
    return parquetUtils.getRowCount(storage, path);
  }
}
