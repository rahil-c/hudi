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
}
