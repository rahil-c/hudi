/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType}
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._

/**
 * End-to-end tests for vector column support in Hudi.
 * Tests round-trip data correctness through Spark DataFrames.
 */
class TestVectorDataSource extends HoodieSparkClientTestBase {

  var spark: SparkSession = null

  @BeforeEach override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  @Test
  def testVectorRoundTrip(): Unit = {
    // 1. Create schema with vector metadata
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(128)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata),
      StructField("label", StringType, nullable = true)
    ))

    // 2. Generate test data (128-dim float vectors)
    val random = new scala.util.Random(42)
    val data = (0 until 100).map { i =>
      val embedding = Array.fill(128)(random.nextFloat())
      Row(s"key_$i", embedding.toSeq, s"label_$i")
    }

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    // 3. Write as COW Hudi table
    df.write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "id")
      .option(TABLE_NAME.key, "vector_test_table")
      .option(TABLE_TYPE.key, "COPY_ON_WRITE")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // 4. Read back
    val readDf = spark.read.format("hudi").load(basePath)

    // 5. Verify row count
    assertEquals(100, readDf.count())

    // 6. Verify schema preserved
    val embeddingField = readDf.schema("embedding")
    assertTrue(embeddingField.dataType.isInstanceOf[ArrayType])
    val arrayType = embeddingField.dataType.asInstanceOf[ArrayType]
    assertEquals(FloatType, arrayType.elementType)
    assertFalse(arrayType.containsNull)

    // 7. Verify vector metadata preserved
    val readMetadata = embeddingField.metadata
    assertTrue(readMetadata.contains(HoodieSchema.TYPE_METADATA_FIELD))
    val parsedSchema = HoodieSchema.parseTypeDescriptor(
      readMetadata.getString(HoodieSchema.TYPE_METADATA_FIELD))
    assertEquals(HoodieSchemaType.VECTOR, parsedSchema.getType)
    val vectorSchema = parsedSchema.asInstanceOf[HoodieSchema.Vector]
    assertEquals(128, vectorSchema.getDimension)

    // 8. Verify float values match exactly
    val originalRows = df.select("id", "embedding").collect()
      .map(r => (r.getString(0), r.getSeq[Float](1)))
      .toMap

    val readRows = readDf.select("id", "embedding").collect()
      .map(r => (r.getString(0), r.getSeq[Float](1)))
      .toMap

    originalRows.foreach { case (id, origEmbedding) =>
      val readEmbedding = readRows(id)
      assertEquals(128, readEmbedding.size, s"Vector size mismatch for $id")

      origEmbedding.zip(readEmbedding).zipWithIndex.foreach {
        case ((orig, read), idx) =>
          assertEquals(orig, read, 1e-9f,
            s"Vector mismatch at $id index $idx: orig=$orig read=$read")
      }
    }
  }


  @Test
  def testNullableVectorField(): Unit = {
    // Vector column itself nullable (entire array can be null)
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(32)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = true, metadata) // nullable = true
    ))

    val data = Seq(
      Row("key_1", Array.fill(32)(0.5f).toSeq),
      Row("key_2", null), // null vector
      Row("key_3", Array.fill(32)(1.0f).toSeq)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    df.write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "id")
      .option(TABLE_NAME.key, "nullable_vector_test")
      .option(TABLE_TYPE.key, "COPY_ON_WRITE")
      .mode(SaveMode.Overwrite)
      .save(basePath + "/nullable")

    val readDf = spark.read.format("hudi").load(basePath + "/nullable")
    val readRows = readDf.select("id", "embedding").collect()

    // Verify null handling
    val key2Row = readRows.find(_.getString(0) == "key_2").get
    assertTrue(key2Row.isNullAt(1), "Null vector not preserved")

    // Verify non-null vectors preserved correctly
    val key1Row = readRows.find(_.getString(0) == "key_1").get
    assertFalse(key1Row.isNullAt(1))
    val key1Embedding = key1Row.getSeq[Float](1)
    assertEquals(32, key1Embedding.size)
    assertTrue(key1Embedding.forall(_ == 0.5f))

    val key3Row = readRows.find(_.getString(0) == "key_3").get
    assertFalse(key3Row.isNullAt(1))
    val key3Embedding = key3Row.getSeq[Float](1)
    assertEquals(32, key3Embedding.size)
    assertTrue(key3Embedding.forall(_ == 1.0f))
  }

  @Test
  def testColumnProjectionWithVector(): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(16)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata),
      StructField("label", StringType, nullable = true),
      StructField("score", IntegerType, nullable = true)
    ))

    val data = (0 until 10).map { i =>
      Row(s"key_$i", Array.fill(16)(i.toFloat).toSeq, s"label_$i", i * 10)
    }

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    df.write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "id")
      .option(TABLE_NAME.key, "projection_vector_test")
      .option(TABLE_TYPE.key, "COPY_ON_WRITE")
      .mode(SaveMode.Overwrite)
      .save(basePath + "/projection")

    // Read only non-vector columns (vector column excluded)
    val nonVectorDf = spark.read.format("hudi").load(basePath + "/projection")
      .select("id", "label", "score")
    assertEquals(10, nonVectorDf.count())
    val row0 = nonVectorDf.filter("id = 'key_0'").collect()(0)
    assertEquals("label_0", row0.getString(1))
    assertEquals(0, row0.getInt(2))

    // Read only the vector column with id
    val vectorOnlyDf = spark.read.format("hudi").load(basePath + "/projection")
      .select("id", "embedding")
    assertEquals(10, vectorOnlyDf.count())
    val vecRow = vectorOnlyDf.filter("id = 'key_5'").collect()(0)
    val embedding = vecRow.getSeq[Float](1)
    assertEquals(16, embedding.size)
    assertTrue(embedding.forall(_ == 5.0f))

    // Read all columns including vector
    val allDf = spark.read.format("hudi").load(basePath + "/projection")
      .select("id", "embedding", "label", "score")
    assertEquals(10, allDf.count())
    val allRow = allDf.filter("id = 'key_3'").collect()(0)
    assertEquals("label_3", allRow.getString(2))
    assertEquals(30, allRow.getInt(3))
    val allEmbedding = allRow.getSeq[Float](1)
    assertEquals(16, allEmbedding.size)
    assertTrue(allEmbedding.forall(_ == 3.0f))
  }

  @Test
  def testVectorMorInsertAndRead(): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(128)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata),
      StructField("label", StringType, nullable = true)
    ))

    val random = new scala.util.Random(42)
    val data = (0 until 100).map { i =>
      val embedding = Array.fill(128)(random.nextFloat())
      Row(s"key_$i", embedding.toSeq, s"label_$i")
    }

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    df.write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "id")
      .option(TABLE_NAME.key, "vector_mor_test")
      .option(TABLE_TYPE.key, "MERGE_ON_READ")
      .mode(SaveMode.Overwrite)
      .save(basePath + "/mor_insert")

    val readDf = spark.read.format("hudi").load(basePath + "/mor_insert")

    assertEquals(100, readDf.count())

    val embeddingField = readDf.schema("embedding")
    assertTrue(embeddingField.dataType.isInstanceOf[ArrayType])
    val arrayType = embeddingField.dataType.asInstanceOf[ArrayType]
    assertEquals(FloatType, arrayType.elementType)
    assertFalse(arrayType.containsNull)

    val readMetadata = embeddingField.metadata
    assertTrue(readMetadata.contains(HoodieSchema.TYPE_METADATA_FIELD))
    val parsedSchema = HoodieSchema.parseTypeDescriptor(
      readMetadata.getString(HoodieSchema.TYPE_METADATA_FIELD))
    assertEquals(HoodieSchemaType.VECTOR, parsedSchema.getType)
    val vectorSchema = parsedSchema.asInstanceOf[HoodieSchema.Vector]
    assertEquals(128, vectorSchema.getDimension)

    val originalRows = df.select("id", "embedding").collect()
      .map(r => (r.getString(0), r.getSeq[Float](1)))
      .toMap

    val readRows = readDf.select("id", "embedding").collect()
      .map(r => (r.getString(0), r.getSeq[Float](1)))
      .toMap

    originalRows.foreach { case (id, origEmbedding) =>
      val readEmbedding = readRows(id)
      assertEquals(128, readEmbedding.size, s"Vector size mismatch for $id")
      origEmbedding.zip(readEmbedding).zipWithIndex.foreach {
        case ((orig, read), idx) =>
          assertEquals(orig, read, 1e-9f,
            s"Vector mismatch at $id index $idx: orig=$orig read=$read")
      }
    }
  }

  @Test
  def testVectorMorUpsert(): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(64)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata),
      StructField("label", StringType, nullable = true)
    ))

    // Insert 50 records to create base Parquet files
    val random = new scala.util.Random(123)
    val insertData = (0 until 50).map { i =>
      Row(s"key_$i", Array.fill(64)(random.nextFloat()).toSeq, s"label_v1_$i")
    }

    val insertDf = spark.createDataFrame(
      spark.sparkContext.parallelize(insertData),
      schema
    )

    val tablePath = basePath + "/mor_upsert"

    insertDf.write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "id")
      .option(TABLE_NAME.key, "vector_mor_upsert_test")
      .option(TABLE_TYPE.key, "MERGE_ON_READ")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Upsert same 50 keys with new vector values → generates Avro log files
    val random2 = new scala.util.Random(456)
    val upsertData = (0 until 50).map { i =>
      Row(s"key_$i", Array.fill(64)(random2.nextFloat()).toSeq, s"label_v2_$i")
    }

    val upsertDf = spark.createDataFrame(
      spark.sparkContext.parallelize(upsertData),
      schema
    )

    upsertDf.write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "id")
      .option(TABLE_NAME.key, "vector_mor_upsert_test")
      .option(TABLE_TYPE.key, "MERGE_ON_READ")
      .mode(SaveMode.Append)
      .save(tablePath)

    // Read snapshot and verify
    val readDf = spark.read.format("hudi").load(tablePath)

    // Count is 50 (no duplicates)
    assertEquals(50, readDf.count())

    // Vector values match the upserted values (not originals)
    val upsertRows = upsertDf.select("id", "embedding", "label").collect()
      .map(r => (r.getString(0), r.getSeq[Float](1), r.getString(2)))
      .map { case (id, emb, lbl) => id -> (emb, lbl) }
      .toMap

    val readRows = readDf.select("id", "embedding", "label").collect()
      .map(r => (r.getString(0), r.getSeq[Float](1), r.getString(2)))

    readRows.foreach { case (id, readEmbedding, readLabel) =>
      val (expectedEmbedding, expectedLabel) = upsertRows(id)
      assertEquals(expectedLabel, readLabel, s"Label mismatch for $id")
      assertEquals(64, readEmbedding.size, s"Vector size mismatch for $id")
      expectedEmbedding.zip(readEmbedding).zipWithIndex.foreach {
        case ((expected, read), idx) =>
          assertEquals(expected, read, 1e-9f,
            s"Vector mismatch at $id index $idx: expected=$expected read=$read")
      }
    }
  }

  @Test
  def testVectorMorNullableUpsert(): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(32)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = true, metadata)
    ))

    val tablePath = basePath + "/mor_nullable"

    // Insert: keys 0-4 with vectors, keys 5-9 with null vectors
    val insertData = (0 until 10).map { i =>
      if (i < 5) {
        Row(s"key_$i", Array.fill(32)(i.toFloat).toSeq)
      } else {
        Row(s"key_$i", null)
      }
    }

    val insertDf = spark.createDataFrame(
      spark.sparkContext.parallelize(insertData),
      schema
    )

    insertDf.write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "id")
      .option(TABLE_NAME.key, "vector_mor_nullable_test")
      .option(TABLE_TYPE.key, "MERGE_ON_READ")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Upsert: flip some null→non-null and non-null→null
    // keys 0-2: set to null (was non-null)
    // keys 5-7: set to non-null (was null)
    // keys 3-4, 8-9: keep as-is
    val upsertData = (0 until 10).map { i =>
      if (i < 3) {
        Row(s"key_$i", null) // was non-null, now null
      } else if (i >= 5 && i < 8) {
        Row(s"key_$i", Array.fill(32)(i.toFloat * 10).toSeq) // was null, now non-null
      } else if (i < 5) {
        Row(s"key_$i", Array.fill(32)(i.toFloat).toSeq) // keep non-null
      } else {
        Row(s"key_$i", null) // keep null
      }
    }

    val upsertDf = spark.createDataFrame(
      spark.sparkContext.parallelize(upsertData),
      schema
    )

    upsertDf.write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "id")
      .option(TABLE_NAME.key, "vector_mor_nullable_test")
      .option(TABLE_TYPE.key, "MERGE_ON_READ")
      .mode(SaveMode.Append)
      .save(tablePath)

    val readDf = spark.read.format("hudi").load(tablePath)
    assertEquals(10, readDf.count())

    val readRows = readDf.select("id", "embedding").collect()
      .map(r => (r.getString(0), r))
      .toMap

    // keys 0-2: should be null now
    (0 until 3).foreach { i =>
      assertTrue(readRows(s"key_$i").isNullAt(1),
        s"key_$i should be null after upsert")
    }

    // keys 3-4: should still be non-null with original values
    (3 until 5).foreach { i =>
      assertFalse(readRows(s"key_$i").isNullAt(1),
        s"key_$i should be non-null")
      val emb = readRows(s"key_$i").getSeq[Float](1)
      assertEquals(32, emb.size)
      assertTrue(emb.forall(_ == i.toFloat),
        s"key_$i vector values should be $i.0f")
    }

    // keys 5-7: should be non-null with new values
    (5 until 8).foreach { i =>
      assertFalse(readRows(s"key_$i").isNullAt(1),
        s"key_$i should be non-null after upsert")
      val emb = readRows(s"key_$i").getSeq[Float](1)
      assertEquals(32, emb.size)
      assertTrue(emb.forall(_ == i.toFloat * 10),
        s"key_$i vector values should be ${i.toFloat * 10}f")
    }

    // keys 8-9: should still be null
    (8 until 10).foreach { i =>
      assertTrue(readRows(s"key_$i").isNullAt(1),
        s"key_$i should still be null")
    }
  }

  @Test
  def testVectorMorColumnProjection(): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(16)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata),
      StructField("label", StringType, nullable = true),
      StructField("score", IntegerType, nullable = true)
    ))

    val tablePath = basePath + "/mor_projection"

    // Insert
    val insertData = (0 until 10).map { i =>
      Row(s"key_$i", Array.fill(16)(i.toFloat).toSeq, s"label_v1_$i", i * 10)
    }

    val insertDf = spark.createDataFrame(
      spark.sparkContext.parallelize(insertData),
      schema
    )

    insertDf.write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "id")
      .option(TABLE_NAME.key, "vector_mor_projection_test")
      .option(TABLE_TYPE.key, "MERGE_ON_READ")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    // Upsert to generate log files
    val upsertData = (0 until 10).map { i =>
      Row(s"key_$i", Array.fill(16)(i.toFloat + 100).toSeq, s"label_v2_$i", i * 10 + 1)
    }

    val upsertDf = spark.createDataFrame(
      spark.sparkContext.parallelize(upsertData),
      schema
    )

    upsertDf.write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "id")
      .option(TABLE_NAME.key, "vector_mor_projection_test")
      .option(TABLE_TYPE.key, "MERGE_ON_READ")
      .mode(SaveMode.Append)
      .save(tablePath)

    // (a) Read scalar columns only (no vector)
    val scalarDf = spark.read.format("hudi").load(tablePath)
      .select("id", "label", "score")
    assertEquals(10, scalarDf.count())
    val scalarRow = scalarDf.filter("id = 'key_3'").collect()(0)
    assertEquals("label_v2_3", scalarRow.getString(1))
    assertEquals(31, scalarRow.getInt(2))

    // (b) Read vector + id only
    val vectorIdDf = spark.read.format("hudi").load(tablePath)
      .select("id", "embedding")
    assertEquals(10, vectorIdDf.count())
    val vecRow = vectorIdDf.filter("id = 'key_5'").collect()(0)
    val embedding = vecRow.getSeq[Float](1)
    assertEquals(16, embedding.size)
    assertTrue(embedding.forall(_ == 105.0f))

    // (c) Read all columns
    val allDf = spark.read.format("hudi").load(tablePath)
      .select("id", "embedding", "label", "score")
    assertEquals(10, allDf.count())
    val allRow = allDf.filter("id = 'key_7'").collect()(0)
    assertEquals("label_v2_7", allRow.getString(2))
    assertEquals(71, allRow.getInt(3))
    val allEmbedding = allRow.getSeq[Float](1)
    assertEquals(16, allEmbedding.size)
    assertTrue(allEmbedding.forall(_ == 107.0f))
  }

}
