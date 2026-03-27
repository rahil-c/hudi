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
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._

/**
 * End-to-end tests for the hudi_vector_search table-valued function.
 * Tests both single-query and batch-query modes with Spark SQL and DataFrame API.
 */
class TestHoodieVectorSearchFunction extends HoodieSparkClientTestBase {

  var spark: SparkSession = null
  private val corpusPath = "corpus"
  private val corpusViewName = "corpus_view"

  // Test corpus: 5 unit-ish vectors in 3D for easy manual verification
  // doc_1: [1, 0, 0] - x-axis
  // doc_2: [0, 1, 0] - y-axis
  // doc_3: [0, 0, 1] - z-axis
  // doc_4: [0.707, 0.707, 0] - 45 degrees in xy-plane (normalized)
  // doc_5: [0.577, 0.577, 0.577] - equal in all 3 dims (normalized)
  private val corpusData = Seq(
    ("doc_1", Seq(1.0f, 0.0f, 0.0f), "x-axis"),
    ("doc_2", Seq(0.0f, 1.0f, 0.0f), "y-axis"),
    ("doc_3", Seq(0.0f, 0.0f, 1.0f), "z-axis"),
    ("doc_4", Seq(0.70710678f, 0.70710678f, 0.0f), "xy-diagonal"),
    ("doc_5", Seq(0.57735027f, 0.57735027f, 0.57735027f), "xyz-diagonal")
  )

  @BeforeEach override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
    createCorpusTable()
  }

  @AfterEach override def tearDown(): Unit = {
    spark.catalog.dropTempView(corpusViewName)
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  private def createCorpusTable(): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(3)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata),
      StructField("label", StringType, nullable = true)
    ))

    val rows = corpusData.map { case (id, emb, label) =>
      Row(id, emb, label)
    }

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      schema
    )

    df.write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "id")
      .option(TABLE_NAME.key, "vector_search_corpus")
      .option(TABLE_TYPE.key, "COPY_ON_WRITE")
      .mode(SaveMode.Overwrite)
      .save(basePath + "/" + corpusPath)

    spark.read.format("hudi").load(basePath + "/" + corpusPath)
      .createOrReplaceTempView(corpusViewName)
  }

  // ==================== Single Query Mode: Spark SQL ====================

  @Test
  def testSingleQueryCosineDistance(): Unit = {
    // Query vector [1, 0, 0] should be closest to doc_1, then doc_4, then doc_5
    val result = spark.sql(
      s"""
         |SELECT id, label, _distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  3,
         |  'cosine'
         |)
         |ORDER BY _distance
         |""".stripMargin
    ).collect()

    assertEquals(3, result.length)

    // doc_1 [1,0,0]: cosine distance to [1,0,0] = 1 - 1.0 = 0.0
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_distance"), 1e-5)

    // doc_4 [0.707,0.707,0]: cosine distance to [1,0,0] = 1 - 0.707 ~= 0.293
    assertEquals("doc_4", result(1).getAs[String]("id"))
    assertEquals(1.0 - 0.70710678, result(1).getAs[Double]("_distance"), 1e-4)

    // doc_5 [0.577,0.577,0.577]: cosine distance to [1,0,0] = 1 - 0.577 ~= 0.423
    assertEquals("doc_5", result(2).getAs[String]("id"))
    assertEquals(1.0 - 0.57735027, result(2).getAs[Double]("_distance"), 1e-4)
  }

  @Test
  def testSingleQueryL2Distance(): Unit = {
    // Query [1, 0, 0] with L2
    val result = spark.sql(
      s"""
         |SELECT id, _distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  3,
         |  'l2'
         |)
         |ORDER BY _distance
         |""".stripMargin
    ).collect()

    assertEquals(3, result.length)

    // doc_1: L2 = 0.0
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_distance"), 1e-5)

    // doc_4: L2 = sqrt((1-0.707)^2 + (0-0.707)^2 + 0) = sqrt(0.086 + 0.5) ~= 0.765
    assertEquals("doc_4", result(1).getAs[String]("id"))
    val expectedL2Doc4 = math.sqrt(
      math.pow(1.0 - 0.70710678, 2) + math.pow(0.70710678, 2))
    assertEquals(expectedL2Doc4, result(1).getAs[Double]("_distance"), 1e-4)
  }

  @Test
  def testSingleQueryDotProduct(): Unit = {
    // Query [1, 0, 0] with dot_product (negated: lower = more similar)
    val result = spark.sql(
      s"""
         |SELECT id, _distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  3,
         |  'dot_product'
         |)
         |ORDER BY _distance
         |""".stripMargin
    ).collect()

    assertEquals(3, result.length)

    // doc_1: -dot = -1.0 (most similar)
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals(-1.0, result(0).getAs[Double]("_distance"), 1e-5)

    // doc_4: -dot = -0.707
    assertEquals("doc_4", result(1).getAs[String]("id"))
    assertEquals(-0.70710678, result(1).getAs[Double]("_distance"), 1e-4)
  }

  @Test
  def testSingleQueryDefaultMetric(): Unit = {
    // Omit metric arg, should default to cosine
    val result = spark.sql(
      s"""
         |SELECT id, _distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  3
         |)
         |ORDER BY _distance
         |""".stripMargin
    ).collect()

    assertEquals(3, result.length)
    // Should match cosine: doc_1 first with distance ~0
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_distance"), 1e-5)
  }

  @Test
  def testSingleQueryReturnsAllCorpusColumns(): Unit = {
    val result = spark.sql(
      s"""
         |SELECT *
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  2
         |)
         |""".stripMargin
    )

    // Should have the _distance column plus original corpus columns
    assertTrue(result.columns.contains("_distance"))
    assertTrue(result.columns.contains("id"))
    assertTrue(result.columns.contains("label"))
    assertTrue(result.columns.contains("embedding"))
    assertEquals(2, result.count())
  }

  @Test
  def testKGreaterThanCorpus(): Unit = {
    // k=100, corpus has 5 rows -> should return all 5
    val result = spark.sql(
      s"""
         |SELECT id, _distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  100
         |)
         |""".stripMargin
    ).collect()

    assertEquals(5, result.length)
  }

  // ==================== Single Query Mode: Composability ====================

  @Test
  def testVectorSearchWithWhereClause(): Unit = {
    val result = spark.sql(
      s"""
         |SELECT id, _distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  5,
         |  'cosine'
         |)
         |WHERE _distance < 0.5
         |ORDER BY _distance
         |""".stripMargin
    ).collect()

    // doc_1 (distance ~0) and doc_4 (distance ~0.29) should pass; doc_5 (~0.42) should pass too
    // doc_2 and doc_3 have distance = 1.0 and should be filtered out
    assertTrue(result.length >= 2)
    assertTrue(result.forall(_.getAs[Double]("_distance") < 0.5))
  }

  @Test
  def testVectorSearchAsSubquery(): Unit = {
    val result = spark.sql(
      s"""
         |SELECT sub.id, sub.label, sub._distance
         |FROM (
         |  SELECT *
         |  FROM hudi_vector_search(
         |    '$corpusViewName',
         |    'embedding',
         |    ARRAY(0.0, 1.0, 0.0),
         |    3
         |  )
         |) sub
         |WHERE sub.label != 'y-axis'
         |ORDER BY sub._distance
         |""".stripMargin
    ).collect()

    // doc_2 (y-axis) is filtered out
    assertTrue(result.forall(_.getAs[String]("id") != "doc_2"))
  }

  // ==================== Batch Query Mode ====================

  @Test
  def testBatchQueryMode(): Unit = {
    // Create a query table with 2 queries
    val querySchema = StructType(Seq(
      StructField("query_name", StringType, nullable = false),
      StructField("query_embedding", ArrayType(FloatType, containsNull = false), nullable = false)
    ))

    val queryRows = Seq(
      Row("q_x", Seq(1.0f, 0.0f, 0.0f)),     // should find doc_1 closest
      Row("q_y", Seq(0.0f, 1.0f, 0.0f))      // should find doc_2 closest
    )

    spark.createDataFrame(
      spark.sparkContext.parallelize(queryRows), querySchema
    ).createOrReplaceTempView("queries_view")

    val result = spark.sql(
      s"""
         |SELECT *
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  'queries_view',
         |  'query_embedding',
         |  2,
         |  'cosine'
         |)
         |""".stripMargin
    ).collect()

    // 2 queries x 2 results each = 4 rows
    assertEquals(4, result.length)

    // Check that _distance column exists
    val columns = result.head.schema.fieldNames
    assertTrue(columns.contains("_distance"))
    assertTrue(columns.contains("_query_id"))

    spark.catalog.dropTempView("queries_view")
  }

  @Test
  def testBatchQueryResultsPerQuery(): Unit = {
    val querySchema = StructType(Seq(
      StructField("qid", StringType, nullable = false),
      StructField("qvec", ArrayType(FloatType, containsNull = false), nullable = false)
    ))

    val queryRows = Seq(
      Row("q1", Seq(1.0f, 0.0f, 0.0f)),
      Row("q2", Seq(0.0f, 0.0f, 1.0f))
    )

    spark.createDataFrame(
      spark.sparkContext.parallelize(queryRows), querySchema
    ).createOrReplaceTempView("batch_queries")

    val resultDf = spark.sql(
      s"""
         |SELECT *
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  'batch_queries',
         |  'qvec',
         |  2,
         |  'cosine'
         |)
         |""".stripMargin
    )

    // Each query should get 2 results
    val resultsByQuery = resultDf.groupBy("_query_id").count().collect()
    assertEquals(2, resultsByQuery.length)
    resultsByQuery.foreach { row =>
      assertEquals(2, row.getLong(1))
    }

    spark.catalog.dropTempView("batch_queries")
  }

  @Test
  def testBatchQuerySameEmbeddingColumnName(): Unit = {
    // Both corpus and query use the column name "embedding" — previously caused ambiguity error
    val querySchema = StructType(Seq(
      StructField("query_name", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false)
    ))

    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("q_x", Seq(1.0f, 0.0f, 0.0f)),
        Row("q_y", Seq(0.0f, 1.0f, 0.0f))
      )), querySchema
    ).createOrReplaceTempView("same_col_queries")

    val result = spark.sql(
      s"""
         |SELECT *
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  'same_col_queries',
         |  'embedding',
         |  2,
         |  'cosine'
         |)
         |""".stripMargin
    ).collect()

    // 2 queries x 2 results each = 4 rows; should not throw AnalysisException
    assertEquals(4, result.length)
    assertTrue(result.head.schema.fieldNames.contains("_distance"))

    spark.catalog.dropTempView("same_col_queries")
  }

  // ==================== DataFrame API ====================

  @Test
  def testSingleQueryViaDataFrameApi(): Unit = {
    val resultDf = spark.sql(
      s"""
         |SELECT id, label, _distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(0.0, 0.0, 1.0),
         |  2,
         |  'cosine'
         |)
         |""".stripMargin
    )

    val results = resultDf.collect()
    assertEquals(2, results.length)

    // doc_3 [0,0,1] should be the closest to query [0,0,1]
    assertEquals("doc_3", results(0).getAs[String]("id"))
    assertEquals(0.0, results(0).getAs[Double]("_distance"), 1e-5)

    // Verify we can do DataFrame operations on the result
    val filtered = resultDf.filter("_distance < 0.5")
    assertTrue(filtered.count() >= 1)
  }

  @Test
  def testBatchQueryViaDataFrameApi(): Unit = {
    val querySchema = StructType(Seq(
      StructField("query_name", StringType, nullable = false),
      StructField("query_vec", ArrayType(FloatType, containsNull = false), nullable = false)
    ))

    val queries = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("q1", Seq(1.0f, 0.0f, 0.0f)),
        Row("q2", Seq(0.0f, 1.0f, 0.0f))
      )), querySchema)

    queries.createOrReplaceTempView("df_queries")

    val resultDf = spark.sql(
      s"""
         |SELECT *
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  'df_queries',
         |  'query_vec',
         |  3
         |)
         |""".stripMargin
    )

    // 2 queries x 3 results each = 6 rows
    assertEquals(6, resultDf.count())

    // Can apply DataFrame operations
    val topResults = resultDf
      .filter("_distance < 0.5")
      .select("id", "_distance", "_query_id")
    assertTrue(topResults.count() > 0)

    spark.catalog.dropTempView("df_queries")
  }

  // ==================== Path-based resolution ====================

  @Test
  def testTableByPath(): Unit = {
    val tablePath = basePath + "/" + corpusPath
    val result = spark.sql(
      s"""
         |SELECT id, _distance
         |FROM hudi_vector_search(
         |  '$tablePath',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  2
         |)
         |ORDER BY _distance
         |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    assertEquals("doc_1", result(0).getAs[String]("id"))
  }

  // ==================== Different embedding types ====================

  @Test
  def testDoubleVectorEmbeddings(): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(3, DOUBLE)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(DoubleType, containsNull = false),
        nullable = false, metadata)
    ))

    val data = Seq(
      Row("d1", Seq(1.0, 0.0, 0.0)),
      Row("d2", Seq(0.0, 1.0, 0.0)),
      Row("d3", Seq(0.0, 0.0, 1.0))
    )

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      .write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "id")
      .option(TABLE_NAME.key, "double_vec_search")
      .option(TABLE_TYPE.key, "COPY_ON_WRITE")
      .mode(SaveMode.Overwrite)
      .save(basePath + "/double_search")

    spark.read.format("hudi").load(basePath + "/double_search")
      .createOrReplaceTempView("double_corpus")

    val result = spark.sql(
      """
        |SELECT id, _distance
        |FROM hudi_vector_search(
        |  'double_corpus',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  2
        |)
        |ORDER BY _distance
        |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    assertEquals("d1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_distance"), 1e-10)

    spark.catalog.dropTempView("double_corpus")
  }

  // ==================== Error handling ====================

  @Test
  def testInvalidEmbeddingColumn(): Unit = {
    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search(
           |  '$corpusViewName',
           |  'nonexistent_col',
           |  ARRAY(1.0, 0.0, 0.0),
           |  3
           |)
           |""".stripMargin
      ).collect()
    })
    assertTrue(ex.getMessage.contains("nonexistent_col") ||
      ex.getCause.getMessage.contains("nonexistent_col"))
  }

  @Test
  def testInvalidDistanceMetric(): Unit = {
    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search(
           |  '$corpusViewName',
           |  'embedding',
           |  ARRAY(1.0, 0.0, 0.0),
           |  3,
           |  'invalid_metric'
           |)
           |""".stripMargin
      ).collect()
    })
    assertTrue(ex.getMessage.contains("Unsupported distance metric") ||
      ex.getCause.getMessage.contains("Unsupported distance metric"))
  }

  @Test
  def testTooFewArguments(): Unit = {
    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search(
           |  '$corpusViewName',
           |  'embedding'
           |)
           |""".stripMargin
      ).collect()
    })
    assertTrue(ex.getMessage.contains("expects 4-6 arguments") ||
      ex.getCause.getMessage.contains("expects 4-6 arguments"))
  }

  // ==================== Correctness: manual distance verification ====================

  @Test
  def testCosineDistanceExactValues(): Unit = {
    // Query [0, 1, 0], verify all distances against manually computed values
    val result = spark.sql(
      s"""
         |SELECT id, _distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(0.0, 1.0, 0.0),
         |  5,
         |  'cosine'
         |)
         |ORDER BY _distance
         |""".stripMargin
    ).collect()

    assertEquals(5, result.length)
    val distanceMap = result.map(r => r.getAs[String]("id") -> r.getAs[Double]("_distance")).toMap

    // doc_2 [0,1,0]: cos_dist = 1 - 1.0 = 0.0
    assertEquals(0.0, distanceMap("doc_2"), 1e-5)

    // doc_1 [1,0,0]: cos_dist = 1 - 0.0 = 1.0
    assertEquals(1.0, distanceMap("doc_1"), 1e-5)

    // doc_3 [0,0,1]: cos_dist = 1 - 0.0 = 1.0
    assertEquals(1.0, distanceMap("doc_3"), 1e-5)

    // doc_4 [0.707,0.707,0]: cos_dist = 1 - 0.707 ~= 0.293
    assertEquals(1.0 - 0.70710678, distanceMap("doc_4"), 1e-4)

    // doc_5 [0.577,0.577,0.577]: cos_dist = 1 - 0.577 ~= 0.423
    assertEquals(1.0 - 0.57735027, distanceMap("doc_5"), 1e-4)
  }

  @Test
  def testL2DistanceExactValues(): Unit = {
    // Query [0, 0, 0] with L2 - distance is just the norm of each vector
    val result = spark.sql(
      s"""
         |SELECT id, _distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(0.0, 0.0, 0.0),
         |  5,
         |  'l2'
         |)
         |ORDER BY _distance
         |""".stripMargin
    ).collect()

    assertEquals(5, result.length)
    val distanceMap = result.map(r => r.getAs[String]("id") -> r.getAs[Double]("_distance")).toMap

    // All corpus vectors are unit vectors, so L2 from origin = 1.0 for each
    assertEquals(1.0, distanceMap("doc_1"), 1e-4)
    assertEquals(1.0, distanceMap("doc_2"), 1e-4)
    assertEquals(1.0, distanceMap("doc_3"), 1e-4)
    assertEquals(1.0, distanceMap("doc_4"), 1e-4)
    assertEquals(1.0, distanceMap("doc_5"), 1e-4)
  }

  @Test
  def testNullEmbeddingsAreFiltered(): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(3)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = true, metadata),
      StructField("label", StringType, nullable = true)
    ))

    val data = Seq(
      Row("n1", Seq(1.0f, 0.0f, 0.0f), "has-vector"),
      Row("n2", null, "null-vector"),
      Row("n3", Seq(0.0f, 1.0f, 0.0f), "has-vector")
    )

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      .write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "id")
      .option(TABLE_NAME.key, "null_vec_search")
      .option(TABLE_TYPE.key, "COPY_ON_WRITE")
      .mode(SaveMode.Overwrite)
      .save(basePath + "/null_search")

    spark.read.format("hudi").load(basePath + "/null_search")
      .createOrReplaceTempView("null_corpus")

    // Should not throw NPE — null rows are filtered out
    val result = spark.sql(
      """
        |SELECT id, _distance
        |FROM hudi_vector_search(
        |  'null_corpus',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  5,
        |  'cosine'
        |)
        |ORDER BY _distance
        |""".stripMargin
    ).collect()

    // Only non-null rows returned
    assertEquals(2, result.length)
    assertEquals("n1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_distance"), 1e-5)

    spark.catalog.dropTempView("null_corpus")
  }

  @Test
  def testEmptyCorpus(): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(3)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata)
    ))

    // Create an empty DataFrame and write it — we need an actual Hudi table,
    // so write one row then filter it out in the view
    val data = Seq(Row("temp", Seq(1.0f, 0.0f, 0.0f)))
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      .write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "id")
      .option(TABLE_NAME.key, "empty_vec_search")
      .option(TABLE_TYPE.key, "COPY_ON_WRITE")
      .mode(SaveMode.Overwrite)
      .save(basePath + "/empty_search")

    spark.read.format("hudi").load(basePath + "/empty_search")
      .filter("id = 'nonexistent'")
      .createOrReplaceTempView("empty_corpus")

    val result = spark.sql(
      """
        |SELECT id, _distance
        |FROM hudi_vector_search(
        |  'empty_corpus',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  3
        |)
        |""".stripMargin
    ).collect()

    assertEquals(0, result.length)

    spark.catalog.dropTempView("empty_corpus")
  }

  @Test
  def testDimensionMismatch(): Unit = {
    // Query vector has 5 dims but corpus has 3-dim embeddings with VECTOR(3) metadata
    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search(
           |  '$corpusViewName',
           |  'embedding',
           |  ARRAY(1.0, 0.0, 0.0, 0.0, 0.0),
           |  3
           |)
           |""".stripMargin
      ).collect()
    })
    assertTrue(ex.getMessage.contains("dimension") ||
      (ex.getCause != null && ex.getCause.getMessage.contains("dimension")))
  }

  @Test
  def testZeroK(): Unit = {
    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search(
           |  '$corpusViewName',
           |  'embedding',
           |  ARRAY(1.0, 0.0, 0.0),
           |  0
           |)
           |""".stripMargin
      ).collect()
    })
    assertTrue(ex.getMessage.contains("positive integer") ||
      (ex.getCause != null && ex.getCause.getMessage.contains("positive integer")))
  }

  @Test
  def testNegativeK(): Unit = {
    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search(
           |  '$corpusViewName',
           |  'embedding',
           |  ARRAY(1.0, 0.0, 0.0),
           |  -5
           |)
           |""".stripMargin
      ).collect()
    })
    assertTrue(ex.getMessage.contains("positive integer") ||
      (ex.getCause != null && ex.getCause.getMessage.contains("positive integer")))
  }

  @Test
  def testMorTableVectorSearch(): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(3)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata),
      StructField("ts", LongType, nullable = false)
    ))

    val data = Seq(
      Row("m1", Seq(1.0f, 0.0f, 0.0f), 1L),
      Row("m2", Seq(0.0f, 1.0f, 0.0f), 1L),
      Row("m3", Seq(0.0f, 0.0f, 1.0f), 1L)
    )

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      .write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "ts")
      .option(TABLE_NAME.key, "mor_search_test")
      .option(TABLE_TYPE.key, "MERGE_ON_READ")
      .mode(SaveMode.Overwrite)
      .save(basePath + "/mor_search")

    spark.read.format("hudi").load(basePath + "/mor_search")
      .createOrReplaceTempView("mor_corpus")

    val result = spark.sql(
      """
        |SELECT id, _distance
        |FROM hudi_vector_search(
        |  'mor_corpus',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  2,
        |  'cosine'
        |)
        |ORDER BY _distance
        |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    assertEquals("m1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_distance"), 1e-5)

    spark.catalog.dropTempView("mor_corpus")
  }
}
