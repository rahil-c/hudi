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

package org.apache.hudi.avro;

import org.apache.hudi.common.config.HoodieParquetConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.io.storage.hadoop.HoodieAvroParquetWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that BLOB and VECTOR logical-type columns are written with PLAIN encoding (dictionary
 * disabled). Exercises the per-column override wired through
 * {@link HoodieParquetConfig#getBlobVectorColumnPaths()}.
 *
 * <p>Per-column statistics disable (also requested for blob/vector workloads) is not asserted
 * here because parquet-mr 1.13.1 does not expose per-column statistics setters; that becomes
 * possible on parquet 1.14+.
 */
public class TestHoodieAvroParquetWriterBlobVectorTuning {

  @TempDir
  java.nio.file.Path tmpDir;

  private static final int VECTOR_DIM = 8;

  @Test
  public void testVectorColumnUsesPlainEncodingAndNoStats() throws IOException {
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());

    // Schema: scalar string + nullable vector. Top-level fields only.
    HoodieSchema vectorSchema = HoodieSchema.createVector("embedding_vec", VECTOR_DIM);
    HoodieSchema recordSchema = HoodieSchema.createRecord("VectorRecord", null, null, false, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("embedding", HoodieSchema.createNullable(vectorSchema), null, null)));

    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(
        new AvroSchemaConverter().convert(recordSchema.toAvroSchema()),
        recordSchema, Option.empty(), new Properties());

    List<String> blobVectorPaths = HoodieSchema.collectBlobAndVectorColumnPaths(recordSchema);
    assertEquals(Collections.singletonList("embedding"), blobVectorPaths);

    HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig = new HoodieParquetConfig<>(
        writeSupport, CompressionCodecName.UNCOMPRESSED,
        ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE,
        1024L * 1024L * 1024L, storage.getConf(), 0.1, true,
        blobVectorPaths);

    StoragePath filePath = new StoragePath(tmpDir.resolve("vector.parquet").toAbsolutePath().toString());

    // Write enough records that the string column will dictionary-encode if eligible.
    List<GenericRecord> records = new ArrayList<>();
    int n = 32;
    for (int i = 0; i < n; i++) {
      GenericRecord rec = new GenericData.Record(recordSchema.toAvroSchema());
      rec.put("id", "row-" + (i % 4));
      rec.put("embedding", new GenericData.Fixed(vectorSchema.toAvroSchema(), floatVectorBytes(i, VECTOR_DIM)));
      records.add(rec);
    }

    try (HoodieAvroParquetWriter writer = new HoodieAvroParquetWriter(
        filePath, parquetConfig, "001", new LocalTaskContextSupplier(), false)) {
      for (GenericRecord rec : records) {
        writer.writeAvro((String) rec.get("id"), rec);
      }
    }

    ParquetMetadata metadata = ParquetUtils.readMetadata(storage, filePath);
    ColumnChunkMetaData embeddingChunk = findColumn(metadata, "embedding");
    ColumnChunkMetaData idChunk = findColumn(metadata, "id");

    // Vector column: PLAIN, no dictionary.
    Set<Encoding> embeddingEncodings = embeddingChunk.getEncodings();
    assertTrue(embeddingEncodings.contains(Encoding.PLAIN),
        "Expected vector column to use PLAIN encoding, got " + embeddingEncodings);
    assertTrue(Collections.disjoint(embeddingEncodings,
            EnumSet.of(Encoding.PLAIN_DICTIONARY, Encoding.RLE_DICTIONARY)),
        "Expected vector column to have no dictionary encoding, got " + embeddingEncodings);

    // String column: dictionary should still be eligible.
    Set<Encoding> idEncodings = idChunk.getEncodings();
    assertTrue(idEncodings.contains(Encoding.PLAIN_DICTIONARY) || idEncodings.contains(Encoding.RLE_DICTIONARY),
        "Expected scalar string column to remain dictionary-encoded, got " + idEncodings);
  }

  @Test
  public void testScopedPageSizeIsAppliedOnHoodieParquetConfig() {
    // The factory resolves the scoped page/row-group sizes before construction; this test
    // verifies that values passed into HoodieParquetConfig are surfaced unchanged via the
    // getters that drive HoodieBaseParquetWriter.
    HoodieParquetConfig<Object> cfg = new HoodieParquetConfig<>(
        null, CompressionCodecName.UNCOMPRESSED,
        /* blockSize */ 8 * 1024 * 1024,
        /* pageSize */ 64 * 1024,
        /* maxFileSize */ 1024L * 1024L * 1024L,
        /* storageConf */ null, 0.1, true,
        Arrays.asList("embedding", "payload.data"));
    assertEquals(64 * 1024, cfg.getPageSize());
    assertEquals(8 * 1024 * 1024, cfg.getBlockSize());
    assertEquals(Arrays.asList("embedding", "payload.data"), cfg.getBlobVectorColumnPaths());
  }

  @Test
  public void testBlobColumnDataLeafUsesPlainEncoding() throws IOException {
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());

    HoodieSchema blobSchema = HoodieSchema.createBlob();
    HoodieSchema recordSchema = HoodieSchema.createRecord("BlobRecord", null, null, false, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("payload", HoodieSchema.createNullable(blobSchema), null, null)));

    // Sanity check the helper before writing.
    assertEquals(Collections.singletonList("payload.data"),
        HoodieSchema.collectBlobAndVectorColumnPaths(recordSchema));

    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(
        new AvroSchemaConverter().convert(recordSchema.toAvroSchema()),
        recordSchema, Option.empty(), new Properties());

    HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig = new HoodieParquetConfig<>(
        writeSupport, CompressionCodecName.UNCOMPRESSED,
        ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE,
        1024L * 1024L * 1024L, storage.getConf(), 0.1, true,
        HoodieSchema.collectBlobAndVectorColumnPaths(recordSchema));

    StoragePath filePath = new StoragePath(tmpDir.resolve("blob.parquet").toAbsolutePath().toString());
    Schema blobAvroSchema = blobSchema.toAvroSchema();
    Schema typeEnumSchema = blobAvroSchema.getField(HoodieSchema.Blob.TYPE).schema();

    List<GenericRecord> records = new ArrayList<>();
    int n = 32;
    for (int i = 0; i < n; i++) {
      GenericRecord blob = new GenericData.Record(blobAvroSchema);
      blob.put(HoodieSchema.Blob.TYPE,
          new GenericData.EnumSymbol(typeEnumSchema, HoodieSchema.Blob.INLINE));
      // INLINE mode: data carries the payload, reference is null.
      blob.put(HoodieSchema.Blob.INLINE_DATA_FIELD,
          ByteBuffer.wrap(blobPayload(i)));
      blob.put(HoodieSchema.Blob.EXTERNAL_REFERENCE, null);

      GenericRecord rec = new GenericData.Record(recordSchema.toAvroSchema());
      rec.put("id", "row-" + (i % 4));
      rec.put("payload", blob);
      records.add(rec);
    }

    try (HoodieAvroParquetWriter writer = new HoodieAvroParquetWriter(
        filePath, parquetConfig, "001", new LocalTaskContextSupplier(), false)) {
      for (GenericRecord rec : records) {
        writer.writeAvro((String) rec.get("id"), rec);
      }
    }

    ParquetMetadata metadata = ParquetUtils.readMetadata(storage, filePath);
    ColumnChunkMetaData payloadDataChunk = findColumn(metadata, "payload.data");
    ColumnChunkMetaData idChunk = findColumn(metadata, "id");

    // BLOB binary leaf: PLAIN only, no dictionary.
    Set<Encoding> payloadEncodings = payloadDataChunk.getEncodings();
    assertTrue(payloadEncodings.contains(Encoding.PLAIN),
        "Expected blob data leaf to use PLAIN encoding, got " + payloadEncodings);
    assertTrue(Collections.disjoint(payloadEncodings,
            EnumSet.of(Encoding.PLAIN_DICTIONARY, Encoding.RLE_DICTIONARY)),
        "Expected blob data leaf to have no dictionary encoding, got " + payloadEncodings);

    // Sibling scalar string column: dictionary still eligible.
    Set<Encoding> idEncodings = idChunk.getEncodings();
    assertTrue(idEncodings.contains(Encoding.PLAIN_DICTIONARY) || idEncodings.contains(Encoding.RLE_DICTIONARY),
        "Expected scalar string column to remain dictionary-encoded, got " + idEncodings);
  }

  @Test
  public void testHoodieParquetConfigBackwardsCompatibleConstructor() {
    // The 8-arg delegate constructor (pre-blob/vector) must default to an empty paths list.
    HoodieParquetConfig<Object> cfg = new HoodieParquetConfig<>(
        null, CompressionCodecName.GZIP,
        ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE,
        1024L * 1024L * 1024L, null, 0.1, true);
    assertNotNull(cfg.getBlobVectorColumnPaths());
    assertTrue(cfg.getBlobVectorColumnPaths().isEmpty());
  }

  private static byte[] floatVectorBytes(int seed, int dim) {
    ByteBuffer buf = ByteBuffer.allocate(dim * Float.BYTES);
    for (int i = 0; i < dim; i++) {
      buf.putFloat((float) (seed + i) * 0.125f);
    }
    return buf.array();
  }

  private static byte[] blobPayload(int seed) {
    // 256-byte payloads ensure the column is non-trivial and dictionary would have a chance
    // to engage if it weren't explicitly disabled.
    byte[] payload = new byte[256];
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) ((seed * 31 + i) & 0xFF);
    }
    return payload;
  }

  private static ColumnChunkMetaData findColumn(ParquetMetadata metadata, String path) {
    for (BlockMetaData block : metadata.getBlocks()) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        if (column.getPath().toDotString().equals(path)) {
          return column;
        }
      }
    }
    throw new AssertionError("Column not found in parquet metadata: " + path);
  }
}
