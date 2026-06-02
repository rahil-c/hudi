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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.avro.model.HoodieVectorIndexInfo;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.stats.ValueMetadata;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.hudi.common.util.CollectionUtils.createImmutableMap;
import static org.apache.hudi.metadata.HoodieIndexVersion.V1;
import static org.apache.hudi.metadata.HoodieMetadataPayload.SECONDARY_INDEX_RECORD_KEY_SEPARATOR;
import static org.apache.hudi.metadata.HoodieMetadataPayload.VECTOR_INDEX_ENCODING_RAW_FLOAT32;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieMetadataPayload}.
 */
public class TestHoodieMetadataPayload extends HoodieCommonTestHarness {
  public static final String PARTITION_NAME = "2022/10/01";
  public static final String PARTITION_NAME2 = "2023/10/01";
  public static final String PARTITION_NAME3 = "2024/10/01";

  @Test
  public void testFileSystemMetadataPayloadMerging() {
    Map<String, Long> firstCommitAddedFiles = createImmutableMap(
        Pair.of("file1.parquet", 1000L),
        Pair.of("file2.parquet", 2000L),
        Pair.of("file3.parquet", 3000L)
    );

    HoodieRecord<HoodieMetadataPayload> firstPartitionFilesRecord =
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME, firstCommitAddedFiles, Collections.emptyList());

    Map<String, Long> secondCommitAddedFiles = createImmutableMap(
        // NOTE: This is an append
        Pair.of("file3.parquet", 3333L),
        Pair.of("file4.parquet", 4000L),
        Pair.of("file5.parquet", 5000L)
    );

    List<String> secondCommitDeletedFiles = Collections.singletonList("file1.parquet");

    HoodieRecord<HoodieMetadataPayload> secondPartitionFilesRecord =
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME, secondCommitAddedFiles, secondCommitDeletedFiles);

    HoodieMetadataPayload combinedPartitionFilesRecordPayload =
        secondPartitionFilesRecord.getData().preCombine(firstPartitionFilesRecord.getData());

    HoodieMetadataPayload expectedCombinedPartitionedFilesRecordPayload =
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME,
            createImmutableMap(
                Pair.of("file2.parquet", 2000L),
                Pair.of("file3.parquet", 3333L),
                Pair.of("file4.parquet", 4000L),
                Pair.of("file5.parquet", 5000L)
            ),
            Collections.emptyList()
        ).getData();

    assertEquals(expectedCombinedPartitionedFilesRecordPayload, combinedPartitionFilesRecordPayload);
  }

  @Test
  public void testFileSystemMetadataPayloadMergingWithDeletions() {
    Map<String, Long> addedFileMap = createImmutableMap(
        Pair.of("file1.parquet", 1000L),
        Pair.of("file2.parquet", 2000L),
        Pair.of("file3.parquet", 3000L),
        Pair.of("file4.parquet", 4000L)
    );
    HoodieRecord<HoodieMetadataPayload> additionRecord =
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME, addedFileMap, Collections.emptyList());

    List<String> deletedFileList1 = new ArrayList<>();
    deletedFileList1.add("file1.parquet");
    deletedFileList1.add("file3.parquet");
    HoodieRecord<HoodieMetadataPayload> deletionRecord1 =
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME, Collections.emptyMap(), deletedFileList1);

    List<String> deletedFileList2 = new ArrayList<>();
    deletedFileList2.add("file1.parquet");
    deletedFileList2.add("file4.parquet");
    HoodieRecord<HoodieMetadataPayload> deletionRecord2 =
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME, Collections.emptyMap(), deletedFileList2);

    assertEquals(
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME,
            createImmutableMap(
                Pair.of("file2.parquet", 2000L),
                Pair.of("file4.parquet", 4000L)
            ),
            Collections.emptyList()
        ).getData(),
        deletionRecord1.getData().preCombine(additionRecord.getData())
    );

    List<String> expectedDeleteFileList = new ArrayList<>();
    expectedDeleteFileList.add("file1.parquet");
    expectedDeleteFileList.add("file3.parquet");
    expectedDeleteFileList.add("file4.parquet");

    assertEquals(
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME,
            Collections.emptyMap(),
            expectedDeleteFileList
        ).getData(),
        deletionRecord2.getData().preCombine(deletionRecord1.getData())
    );

    assertEquals(
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME,
            createImmutableMap(
                Pair.of("file2.parquet", 2000L)
            ),
            Collections.emptyList()
        ).getData(),
        deletionRecord2.getData().preCombine(deletionRecord1.getData()).preCombine(additionRecord.getData())
    );

    assertEquals(
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME,
            createImmutableMap(
                Pair.of("file2.parquet", 2000L)
            ),
            Collections.singletonList("file1.parquet")
        ).getData(),
        deletionRecord2.getData().preCombine(deletionRecord1.getData().preCombine(additionRecord.getData()))
    );

    // lets delete all files
    List<String> allDeletedFileList = new ArrayList<>();
    allDeletedFileList.add("file1.parquet");
    allDeletedFileList.add("file2.parquet");
    allDeletedFileList.add("file3.parquet");
    allDeletedFileList.add("file4.parquet");
    HoodieRecord<HoodieMetadataPayload> allDeletionRecord =
        HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME, Collections.emptyMap(), allDeletedFileList);

    HoodieMetadataPayload combinedPayload = allDeletionRecord.getData().preCombine(additionRecord.getData());
    assertEquals(HoodieMetadataPayload.createPartitionFilesRecord(PARTITION_NAME, Collections.emptyMap(), Collections.emptyList()).getData(), combinedPayload);
    assertTrue(combinedPayload.filesystemMetadata.isEmpty());

    // test all partition record
    HoodieRecord<HoodieMetadataPayload> allPartitionsRecord = HoodieMetadataPayload.createPartitionListRecord(Arrays.asList(PARTITION_NAME, PARTITION_NAME2, PARTITION_NAME3), false);
    HoodieRecord<HoodieMetadataPayload> partitionDeletedRecord = HoodieMetadataPayload.createPartitionListRecord(Collections.singletonList(PARTITION_NAME), true);
    // combine to ensure the deleted partitions is not seen
    HoodieMetadataPayload payload = partitionDeletedRecord.getData().preCombine(allPartitionsRecord.getData());
    assertEquals(HoodieMetadataPayload.createPartitionListRecord(Arrays.asList(PARTITION_NAME2, PARTITION_NAME3), false).getData(),
        payload);
  }

  @Test
  public void testColumnStatsPayloadMerging() throws IOException {
    String fileName = "file.parquet";
    String targetColName = "c1";

    HoodieColumnRangeMetadata<Comparable> c1Metadata =
        HoodieColumnRangeMetadata.<Comparable>create(fileName, targetColName, 100, 1000, 5, 1000, 123456, 123456, ValueMetadata.V1EmptyMetadata.get());

    HoodieRecord<HoodieMetadataPayload> columnStatsRecord =
        HoodieMetadataPayload.createColumnStatsRecords(PARTITION_NAME, Collections.singletonList(c1Metadata), false)
            .findFirst().get();

    ////////////////////////////////////////////////////////////////////////
    // Case 1: Combining proper (non-deleted) records
    ////////////////////////////////////////////////////////////////////////

    // NOTE: Column Stats record will only be merged in case existing file will be modified,
    //       which could only happen on storages schemes supporting appends
    HoodieColumnRangeMetadata<Comparable> c1AppendedBlockMetadata =
        HoodieColumnRangeMetadata.<Comparable>create(fileName, targetColName, 0, 500, 0, 100, 12345, 12345, ValueMetadata.V1EmptyMetadata.get());

    HoodieRecord<HoodieMetadataPayload> updatedColumnStatsRecord =
        HoodieMetadataPayload.createColumnStatsRecords(PARTITION_NAME, Collections.singletonList(c1AppendedBlockMetadata), false)
            .findFirst().get();

    HoodieMetadataPayload combinedMetadataPayload =
        columnStatsRecord.getData().preCombine(updatedColumnStatsRecord.getData());

    HoodieColumnRangeMetadata<Comparable> expectedColumnRangeMetadata =
        HoodieColumnRangeMetadata.<Comparable>create(fileName, targetColName, 0, 1000, 5, 1100, 135801, 135801, ValueMetadata.V1EmptyMetadata.get());

    HoodieRecord<HoodieMetadataPayload> expectedColumnStatsRecord =
        HoodieMetadataPayload.createColumnStatsRecords(PARTITION_NAME, Collections.singletonList(expectedColumnRangeMetadata), false)
            .findFirst().get();

    // Assert combined payload
    assertEquals(combinedMetadataPayload, expectedColumnStatsRecord.getData());

    Option<IndexedRecord> alternativelyCombinedMetadataPayloadAvro =
        columnStatsRecord.getData().combineAndGetUpdateValue(updatedColumnStatsRecord.getData().getInsertValue(null).get(), null);

    // Assert that using legacy API yields the same value
    assertEquals(combinedMetadataPayload.getInsertValue(null), alternativelyCombinedMetadataPayloadAvro);

    ////////////////////////////////////////////////////////////////////////
    // Case 2: Combining w/ deleted records
    ////////////////////////////////////////////////////////////////////////

    HoodieColumnRangeMetadata<Comparable> c1StubbedMetadata =
        HoodieColumnRangeMetadata.<Comparable>stub(fileName, targetColName, V1);

    HoodieRecord<HoodieMetadataPayload> deletedColumnStatsRecord =
        HoodieMetadataPayload.createColumnStatsRecords(PARTITION_NAME, Collections.singletonList(c1StubbedMetadata), true)
            .findFirst().get();

    // NOTE: In this case, deleted (or tombstone) record will be therefore deleting
    //       previous state of the record
    HoodieMetadataPayload deletedCombinedMetadataPayload =
        deletedColumnStatsRecord.getData().preCombine(columnStatsRecord.getData());

    assertEquals(deletedColumnStatsRecord.getData(), deletedCombinedMetadataPayload);
    assertFalse(deletedCombinedMetadataPayload.getInsertValue(null).isPresent());
    assertTrue(deletedCombinedMetadataPayload.isDeleted());

    // NOTE: In this case, proper incoming record will be overwriting previously deleted
    //       record
    HoodieMetadataPayload overwrittenCombinedMetadataPayload =
        columnStatsRecord.getData().preCombine(deletedColumnStatsRecord.getData());

    assertEquals(columnStatsRecord.getData(), overwrittenCombinedMetadataPayload);
  }

  @Test
  public void testPartitionStatsPayloadMerging() {
    HoodieColumnRangeMetadata<Comparable> fileColumnRange1 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file", "columnName", 1, 5, 0, 10, 100, 200, ValueMetadata.V1EmptyMetadata.get());
    HoodieRecord<HoodieMetadataPayload> firstPartitionStatsRecord =
        HoodieMetadataPayload.createPartitionStatsRecords(PARTITION_NAME, Collections.singletonList(fileColumnRange1), false, false, Option.empty()).findFirst().get();
    HoodieColumnRangeMetadata<Comparable> fileColumnRange2 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file", "columnName", 3, 8, 1, 15, 120, 250, ValueMetadata.V1EmptyMetadata.get());
    HoodieRecord<HoodieMetadataPayload> updatedPartitionStatsRecord =
        HoodieMetadataPayload.createPartitionStatsRecords(PARTITION_NAME, Collections.singletonList(fileColumnRange2), false, false, Option.empty()).findFirst().get();
    HoodieMetadataPayload combinedPartitionStatsRecordPayload =
        updatedPartitionStatsRecord.getData().preCombine(firstPartitionStatsRecord.getData());
    HoodieColumnRangeMetadata<Comparable> expectedColumnRange = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file", "columnName", 1, 8, 1, 25, 220, 450, ValueMetadata.V1EmptyMetadata.get());
    HoodieMetadataPayload expectedColumnRangeMetadata = (HoodieMetadataPayload) HoodieMetadataPayload.createPartitionStatsRecords(
        PARTITION_NAME, Collections.singletonList(expectedColumnRange), false, false, Option.empty()).findFirst().get().getData();
    assertEquals(expectedColumnRangeMetadata, combinedPartitionStatsRecordPayload);
  }

  @Test
  public void testPartitionStatsPayloadMergingWithDelete() {
    HoodieColumnRangeMetadata<Comparable> fileColumnRange1 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file", "columnName", 1, 5, 0, 10, 100, 200, ValueMetadata.V1EmptyMetadata.get());
    HoodieRecord<HoodieMetadataPayload> firstPartitionStatsRecord =
        HoodieMetadataPayload.createPartitionStatsRecords(PARTITION_NAME, Collections.singletonList(fileColumnRange1), false, false, Option.empty()).findFirst().get();
    HoodieColumnRangeMetadata<Comparable> fileColumnRange2 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file", "columnName", 3, 8, 1, 15, 120, 250, ValueMetadata.V1EmptyMetadata.get());
    // create delete payload
    HoodieRecord<HoodieMetadataPayload> deletedPartitionStatsRecord =
        HoodieMetadataPayload.createPartitionStatsRecords(PARTITION_NAME, Collections.singletonList(fileColumnRange2), true, false, Option.empty()).findFirst().get();
    // deleted (or tombstone) record will be therefore deleting previous state of the record
    HoodieMetadataPayload combinedPartitionStatsRecordPayload =
        deletedPartitionStatsRecord.getData().preCombine(firstPartitionStatsRecord.getData());
    HoodieColumnRangeMetadata<Comparable> expectedColumnRange = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file", "columnName", 3, 8, 1, 15, 120, 250, ValueMetadata.V1EmptyMetadata.get());
    HoodieMetadataPayload expectedColumnRangeMetadata = (HoodieMetadataPayload) HoodieMetadataPayload.createPartitionStatsRecords(
        PARTITION_NAME, Collections.singletonList(expectedColumnRange), true, false, Option.empty()).findFirst().get().getData();
    assertEquals(expectedColumnRangeMetadata, combinedPartitionStatsRecordPayload);

    // another update for the same key should overwrite the delete record
    HoodieMetadataPayload overwrittenCombinedPartitionStatsRecordPayload =
        firstPartitionStatsRecord.getData().preCombine(deletedPartitionStatsRecord.getData());
    assertEquals(firstPartitionStatsRecord.getData(), overwrittenCombinedPartitionStatsRecordPayload);
  }

  @Test
  public void testSecondaryIndexPayloadMerging() {
    // test delete and combine
    String secondaryIndexPartition = MetadataPartitionType.SECONDARY_INDEX.getPartitionPath() + "secondaryCol";
    String recordKey = "rk1";
    String initialSecondaryKey = "sk1";
    String updatedSecondaryKey = "sk2";
    // test creation
    String expectedPayloadKey = initialSecondaryKey + SECONDARY_INDEX_RECORD_KEY_SEPARATOR + recordKey;
    HoodieRecord<HoodieMetadataPayload> oldSecondaryIndexRecord =
        HoodieMetadataPayload.createSecondaryIndexRecord(recordKey, initialSecondaryKey, secondaryIndexPartition, false);
    assertEquals(expectedPayloadKey, oldSecondaryIndexRecord.getRecordKey());
    // test delete and combine
    HoodieRecord<HoodieMetadataPayload> oldSecondaryIndexRecordDeleted =
        HoodieMetadataPayload.createSecondaryIndexRecord(recordKey, initialSecondaryKey, secondaryIndexPartition, true);
    Option<HoodieRecord<HoodieMetadataPayload>> combinedSecondaryIndexRecord =
        HoodieMetadataPayload.combineSecondaryIndexRecord(oldSecondaryIndexRecord, oldSecondaryIndexRecordDeleted);
    assertFalse(combinedSecondaryIndexRecord.isPresent());
    // test update and combine
    HoodieRecord<HoodieMetadataPayload> newSecondaryIndexRecord =
        HoodieMetadataPayload.createSecondaryIndexRecord(recordKey, updatedSecondaryKey, secondaryIndexPartition, false);
    expectedPayloadKey = updatedSecondaryKey + SECONDARY_INDEX_RECORD_KEY_SEPARATOR + recordKey;
    assertEquals(expectedPayloadKey, newSecondaryIndexRecord.getRecordKey());
    combinedSecondaryIndexRecord = HoodieMetadataPayload.combineSecondaryIndexRecord(oldSecondaryIndexRecord, newSecondaryIndexRecord);
    assertTrue(combinedSecondaryIndexRecord.isPresent());
    assertEquals(newSecondaryIndexRecord.getData(), combinedSecondaryIndexRecord.get().getData());
  }

  @Test
  public void testConstructSecondaryIndexKey() {
    // Simple case
    String secondaryKey = "part1";
    String recordKey = "key1";
    String constructedKey = SecondaryIndexKeyUtils.constructSecondaryIndexKey(secondaryKey, recordKey);
    assertEquals("part1$key1", constructedKey);
    assertEquals(secondaryKey, SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey));
    assertEquals(recordKey, SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey));

    // Case with escape characters
    secondaryKey = "part\\one";
    recordKey = "key$two";
    constructedKey = SecondaryIndexKeyUtils.constructSecondaryIndexKey(secondaryKey, recordKey);
    assertEquals("part\\\\one$key\\$two", constructedKey);
    assertEquals(secondaryKey, SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey));
    assertEquals(recordKey, SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey));

    // Complex case with multiple `$` and `\` characters
    secondaryKey = "comp\\lex$sec";
    recordKey = "prim\\ary$k\\ey";
    constructedKey = SecondaryIndexKeyUtils.constructSecondaryIndexKey(secondaryKey, recordKey);
    assertEquals("comp\\\\lex\\$sec$prim\\\\ary\\$k\\\\ey", constructedKey);

    // Verify correct extraction
    String extractedSecondaryKey = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(constructedKey);
    String extractedPrimaryKey = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(constructedKey);
    assertEquals(secondaryKey, extractedSecondaryKey);
    assertEquals(recordKey, extractedPrimaryKey);

    // Edge case: only secondary key with no primary key
    String key = "secondaryOnly$";
    recordKey = SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(key);
    assertEquals("", recordKey);

    // Edge case: only primary key with no secondary key
    key = "$primaryOnly";
    secondaryKey = SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(key);
    assertEquals("", secondaryKey);

    // Edge case: empty string, invalid key format
    assertThrows(IllegalStateException.class, () -> SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey(""));
    assertThrows(IllegalStateException.class, () -> SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(""));

    // Case with no separator
    assertThrows(IllegalStateException.class, () -> SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey("invalidKey"));
    assertThrows(IllegalStateException.class, () -> SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey("invalidKey"));

    // Case with only escape characters but no actual separator
    assertThrows(IllegalStateException.class, () -> SecondaryIndexKeyUtils.getSecondaryKeyFromSecondaryIndexKey("part\\one"));
    assertThrows(IllegalStateException.class, () -> SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey("part\\one"));
  }

  private static final String VECTOR_INDEX_PARTITION = MetadataPartitionType.VECTOR_INDEX.getPartitionPath() + "embedding";
  private static final String VECTOR_INDEX_INSTANT_TIME = "20240101120000";
  private static final int VECTOR_INDEX_DIMENSION = 8;

  private static ByteBuffer createVectorPayload(int dimension, float seed) {
    ByteBuffer buffer = ByteBuffer.allocate(dimension * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < dimension; i++) {
      buffer.putFloat(seed + i);
    }
    buffer.flip();
    return buffer;
  }

  private static byte[] vectorBytes(ByteBuffer buffer) {
    ByteBuffer dup = buffer.duplicate();
    byte[] out = new byte[dup.remaining()];
    dup.get(out);
    return out;
  }

  @Test
  public void testVectorIndexPayloadAvroRoundtrip() throws IOException {
    String recordKey = "rk1";
    ByteBuffer vector = createVectorPayload(VECTOR_INDEX_DIMENSION, 0.5f);
    String dataPartition = "2024/01/01";
    String fileId = UUID.randomUUID().toString() + "-0";
    int clusterId = 42;
    long position = 17L;

    HoodieRecord<HoodieMetadataPayload> record = HoodieMetadataPayload.createVectorIndexRecord(
        recordKey, VECTOR_INDEX_PARTITION, clusterId, vector,
        dataPartition, fileId, VECTOR_INDEX_INSTANT_TIME, position, false);

    assertEquals(recordKey, record.getRecordKey());
    assertEquals(VECTOR_INDEX_PARTITION, record.getPartitionPath());

    // Serialize through getInsertValue (same-schema fast path) and then re-construct the payload from the IndexedRecord.
    Option<IndexedRecord> serialized = record.getData().getInsertValue(null);
    assertTrue(serialized.isPresent());
    HoodieMetadataPayload roundTripped = new HoodieMetadataPayload(Option.of((GenericRecord) serialized.get()));

    Option<HoodieVectorIndexInfo> info = roundTripped.getVectorIndexMetadata();
    assertTrue(info.isPresent());
    assertEquals(clusterId, info.get().getClusterId());
    assertEquals(VECTOR_INDEX_ENCODING_RAW_FLOAT32, info.get().getEncoding());
    assertFalse(info.get().getIsDeleted());
    assertArrayEquals(vectorBytes(vector), vectorBytes(info.get().getVectorPayload()));

    // Flat location fields round-trip and resolve to a HoodieRecordGlobalLocation in one hop.
    assertEquals(dataPartition, info.get().getDataPartition());
    assertEquals(fileId, info.get().getFileId());
    assertEquals(Long.valueOf(position), info.get().getPosition());
    Option<HoodieRecordGlobalLocation> loc = roundTripped.getVectorIndexRecordLocation();
    assertTrue(loc.isPresent());
    assertEquals(fileId, loc.get().getFileId());
    assertEquals(dataPartition, loc.get().getPartitionPath());
    assertEquals(VECTOR_INDEX_INSTANT_TIME, loc.get().getInstantTime());
  }

  @Test
  public void testVectorIndexPayloadLatestWinsMerge() {
    String recordKey = "rk-merge";
    String fileId = UUID.randomUUID().toString() + "-0";

    // Older entry: cluster A, vector v_old, same row location.
    HoodieRecord<HoodieMetadataPayload> older = HoodieMetadataPayload.createVectorIndexRecord(
        recordKey, VECTOR_INDEX_PARTITION, 1, createVectorPayload(VECTOR_INDEX_DIMENSION, 0.0f),
        "2024/01/01", fileId, VECTOR_INDEX_INSTANT_TIME, 0L, false);

    // Newer entry simulating a re-embed: cluster B, vector v_new.
    HoodieRecord<HoodieMetadataPayload> newer = HoodieMetadataPayload.createVectorIndexRecord(
        recordKey, VECTOR_INDEX_PARTITION, 2, createVectorPayload(VECTOR_INDEX_DIMENSION, 100.0f),
        "2024/01/01", fileId, VECTOR_INDEX_INSTANT_TIME, 0L, false);

    HoodieMetadataPayload merged = newer.getData().preCombine(older.getData());
    assertEquals(2, merged.getVectorIndexMetadata().get().getClusterId());
  }

  /**
   * Round-trips a tombstone through Avro serialization. Catches the bug where the deserialization
   * path forgets to set {@code isDeletedRecord} from the inner {@code isDeleted} flag, which would
   * cause {@code preCombine} to no longer short-circuit on tombstones read from storage.
   */
  @Test
  public void testVectorIndexPayloadTombstoneRoundtrip() {
    String recordKey = "rk-tombstone";
    // Build the GenericRecord directly so we exercise the deserialization path - the factory
    // method short-circuits getInsertValue() for tombstones, so it cannot drive this test.
    HoodieVectorIndexInfo deletedInfo = new HoodieVectorIndexInfo(
        /*isDeleted=*/true, /*clusterId=*/3, VECTOR_INDEX_ENCODING_RAW_FLOAT32,
        createVectorPayload(VECTOR_INDEX_DIMENSION, 2.0f),
        "2024/01/01", UUID.randomUUID().toString() + "-0", parseInstant(VECTOR_INDEX_INSTANT_TIME), 5L);
    HoodieMetadataRecord onDisk = new HoodieMetadataRecord(
        recordKey, MetadataPartitionType.VECTOR_INDEX.getRecordType(),
        null, null, null, null, null, deletedInfo);

    HoodieMetadataPayload roundTripped = new HoodieMetadataPayload(Option.of(onDisk));

    assertTrue(roundTripped.isDeleted(), "deserialized tombstone must report isDeleted()=true so preCombine short-circuits");
    assertTrue(roundTripped.getVectorIndexMetadata().get().getIsDeleted());
  }

  /**
   * The same-schema fast path in getInsertValue() skips the per-field projected-schema decode
   * branch in MetadataPartitionType.VECTOR_INDEX.constructMetadataPayload. This test drives the
   * slow path by passing the canonical Avro schema explicitly, exercising the readOptionalField /
   * readOptionalStringField helpers.
   */
  @Test
  public void testVectorIndexPayloadProjectedSchemaRoundtrip() throws IOException {
    String recordKey = "rk-projected";
    ByteBuffer vector = createVectorPayload(VECTOR_INDEX_DIMENSION, 7.0f);
    String dataPartition = "2024/02/02";
    String fileId = "raw-fileid-projected";
    HoodieRecord<HoodieMetadataPayload> record = HoodieMetadataPayload.createVectorIndexRecord(
        recordKey, VECTOR_INDEX_PARTITION, 11, vector,
        dataPartition, fileId, VECTOR_INDEX_INSTANT_TIME, 99L, false);

    IndexedRecord serialized = record.getData().getInsertValue(HoodieMetadataRecord.SCHEMA$).get();
    HoodieMetadataPayload roundTripped = new HoodieMetadataPayload(Option.of((GenericRecord) serialized));

    HoodieVectorIndexInfo info = roundTripped.getVectorIndexMetadata().get();
    assertEquals(11, info.getClusterId());
    assertEquals(dataPartition, info.getDataPartition());
    assertEquals(fileId, info.getFileId());
    assertEquals(Long.valueOf(99L), info.getPosition());
    assertArrayEquals(vectorBytes(vector), vectorBytes(info.getVectorPayload()));
  }

  private static long parseInstant(String instant) {
    try {
      return org.apache.hudi.common.table.timeline.TimelineUtils.parseDateFromInstantTime(instant).getTime();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
