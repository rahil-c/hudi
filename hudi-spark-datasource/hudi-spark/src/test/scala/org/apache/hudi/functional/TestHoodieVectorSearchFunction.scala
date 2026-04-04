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

  /**
   * Creates an in-memory Float corpus temp view (no Hudi write).
   * Schema: id (String), embedding (Array[Float]).
   */
  private def createFloatInMemoryView(viewName: String, data: Seq[(String, Seq[Float])]): Unit = {
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false)
    ))
    val rows = data.map { case (id, emb) => Row(id, emb) }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .createOrReplaceTempView(viewName)
  }

  /**
   * Creates an in-memory Byte corpus temp view (no Hudi write).
   * Schema: id (String), embedding (Array[Byte]).
   */
  private def createByteCorpusView(viewName: String, data: Seq[(String, Seq[Byte])]): Unit = {
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(ByteType, containsNull = false), nullable = false)
    ))
    val rows = data.map { case (id, emb) => Row(id, emb) }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .createOrReplaceTempView(viewName)
  }

  /**
   * Creates a Float query temp view with configurable id and vector column names.
   * Used by batch-query tests to avoid repeating StructType + createDataFrame boilerplate.
   */
  private def createFloatQueryView(viewName: String, idCol: String, vecCol: String,
                                    data: Seq[(String, Seq[Float])]): Unit = {
    val schema = StructType(Seq(
      StructField(idCol, StringType, nullable = false),
      StructField(vecCol, ArrayType(FloatType, containsNull = false), nullable = false)
    ))
    val rows = data.map { case (id, vec) => Row(id, vec) }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .createOrReplaceTempView(viewName)
  }

  /**
   * Writes rows to a Hudi table and registers the result as a Spark temp view.
   * The supplied schema must include an "id" column used as the record key.
   */
  private def writeHudiAndCreateView(schema: StructType, data: Seq[Row], tableName: String,
                                      subPath: String, viewName: String,
                                      tableType: String = "COPY_ON_WRITE",
                                      precombineField: String = "id"): Unit = {
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      .write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, precombineField)
      .option(TABLE_NAME.key, tableName)
      .option(TABLE_TYPE.key, tableType)
      .mode(SaveMode.Overwrite)
      .save(basePath + "/" + subPath)
    spark.read.format("hudi").load(basePath + "/" + subPath)
      .createOrReplaceTempView(viewName)
  }

  @Test
  def testSingleQueryCosineDistance(): Unit = {
    // Query vector [1, 0, 0] should be closest to doc_1, then doc_4, then doc_5
    val result = spark.sql(
      s"""
         |SELECT id, label, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  3,
         |  'cosine'
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    assertEquals(3, result.length)

    // doc_1 [1,0,0]: cosine distance to [1,0,0] = 1 - 1.0 = 0.0
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)

    // doc_4 [0.707,0.707,0]: cosine distance to [1,0,0] = 1 - 0.707 ~= 0.293
    assertEquals("doc_4", result(1).getAs[String]("id"))
    assertEquals(1.0 - 0.70710678, result(1).getAs[Double]("_hudi_distance"), 1e-4)

    // doc_5 [0.577,0.577,0.577]: cosine distance to [1,0,0] = 1 - 0.577 ~= 0.423
    assertEquals("doc_5", result(2).getAs[String]("id"))
    assertEquals(1.0 - 0.57735027, result(2).getAs[Double]("_hudi_distance"), 1e-4)
  }

  @Test
  def testSingleQueryL2Distance(): Unit = {
    // Query [1, 0, 0] with L2
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  3,
         |  'l2'
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    assertEquals(3, result.length)

    // doc_1: L2 = 0.0
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)

    // doc_4: L2 = sqrt((1-0.707)^2 + (0-0.707)^2 + 0) = sqrt(0.086 + 0.5) ~= 0.765
    assertEquals("doc_4", result(1).getAs[String]("id"))
    val expectedL2Doc4 = math.sqrt(
      math.pow(1.0 - 0.70710678, 2) + math.pow(0.70710678, 2))
    assertEquals(expectedL2Doc4, result(1).getAs[Double]("_hudi_distance"), 1e-4)

    // doc_5: L2 = sqrt((1-0.577)^2 + 0.577^2 + 0.577^2)
    assertEquals("doc_5", result(2).getAs[String]("id"))
    val expectedL2Doc5 = math.sqrt(
      math.pow(1.0 - 0.57735027, 2) + math.pow(0.57735027, 2) + math.pow(0.57735027, 2))
    assertEquals(expectedL2Doc5, result(2).getAs[Double]("_hudi_distance"), 1e-4)
  }

  @Test
  def testSingleQueryDotProduct(): Unit = {
    // Query [1, 0, 0] with dot_product (negated: lower = more similar)
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  3,
         |  'dot_product'
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    assertEquals(3, result.length)

    // doc_1: -dot = -1.0 (most similar)
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals(-1.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)

    // doc_4: -dot = -0.707
    assertEquals("doc_4", result(1).getAs[String]("id"))
    assertEquals(-0.70710678, result(1).getAs[Double]("_hudi_distance"), 1e-4)
  }

  @Test
  def testSingleQueryDefaultMetric(): Unit = {
    // Omit metric arg, should default to cosine
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  3
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    assertEquals(3, result.length)
    // Should match cosine: doc_1 first with distance ~0
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)
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

    // Should have the _hudi_distance column plus original corpus columns (embedding is dropped)
    assertTrue(result.columns.contains("_hudi_distance"))
    assertTrue(result.columns.contains("id"))
    assertTrue(result.columns.contains("label"))
    assertFalse(result.columns.contains("embedding"))
    assertEquals(2, result.count())
  }

  @Test
  def testKGreaterThanCorpus(): Unit = {
    // k=100, corpus has 5 rows -> should return all 5
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
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

  @Test
  def testVectorSearchWithWhereClause(): Unit = {
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  5,
         |  'cosine'
         |)
         |WHERE _hudi_distance < 0.5
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    // doc_1 (distance ~0), doc_4 (distance ~0.29), doc_5 (~0.42) should pass
    // doc_2 and doc_3 have distance = 1.0 and should be filtered out
    assertEquals(3, result.length)
    assertTrue(result.forall(_.getAs[Double]("_hudi_distance") < 0.5))
  }

  @Test
  def testVectorSearchAsSubquery(): Unit = {
    val result = spark.sql(
      s"""
         |SELECT sub.id, sub.label, sub._hudi_distance
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
         |ORDER BY sub._hudi_distance
         |""".stripMargin
    ).collect()

    // doc_2 (y-axis) is filtered out
    assertTrue(result.forall(_.getAs[String]("id") != "doc_2"))
  }

  @Test
  def testBatchQueryResultsPerQuery(): Unit = {
    createFloatQueryView("batch_queries", "qid", "qvec", Seq(
      ("q1", Seq(1.0f, 0.0f, 0.0f)),
      ("q2", Seq(0.0f, 0.0f, 1.0f))
    ))

    val resultDf = spark.sql(
      s"""
         |SELECT *
         |FROM hudi_vector_search_batch(
         |  '$corpusViewName',
         |  'embedding',
         |  'batch_queries',
         |  'qvec',
         |  2,
         |  'cosine'
         |)
         |""".stripMargin
    )

    // Verify output columns
    val columns = resultDf.columns
    assertTrue(columns.contains("_hudi_distance"))
    assertTrue(columns.contains("_hudi_query_index"))

    // Each query should get exactly 2 results
    val resultsByQuery = resultDf.groupBy("_hudi_query_index").count().collect()
    assertEquals(2, resultsByQuery.length)
    resultsByQuery.foreach { row =>
      assertEquals(2, row.getLong(1))
    }

    // Validate that _hudi_query_index has two distinct values
    val queryIndexValues = resultDf.select("_hudi_query_index").distinct().collect()
      .map(_.getLong(0)).sorted
    assertEquals(2, queryIndexValues.length)
    assertTrue(queryIndexValues(0) != queryIndexValues(1))

    spark.catalog.dropTempView("batch_queries")
  }

  @Test
  def testBatchQuerySameEmbeddingColumnName(): Unit = {
    // Both corpus and query use the column name "embedding" — previously caused ambiguity error
    createFloatQueryView("same_col_queries", "query_name", "embedding", Seq(
      ("q_x", Seq(1.0f, 0.0f, 0.0f)),
      ("q_y", Seq(0.0f, 1.0f, 0.0f))
    ))

    val result = spark.sql(
      s"""
         |SELECT *
         |FROM hudi_vector_search_batch(
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
    assertTrue(result.head.schema.fieldNames.contains("_hudi_distance"))

    spark.catalog.dropTempView("same_col_queries")
  }

  @Test
  def testBatchQueryViaDataFrameApi(): Unit = {
    createFloatQueryView("df_queries", "query_name", "query_vec", Seq(
      ("q1", Seq(1.0f, 0.0f, 0.0f)),
      ("q2", Seq(0.0f, 1.0f, 0.0f))
    ))

    val resultDf = spark.sql(
      s"""
         |SELECT *
         |FROM hudi_vector_search_batch(
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
      .filter("_hudi_distance < 0.5")
      .select("id", "_hudi_distance", "_hudi_query_index")
    assertTrue(topResults.count() > 0)

    spark.catalog.dropTempView("df_queries")
  }

  @Test
  def testTableByPath(): Unit = {
    val tablePath = basePath + "/" + corpusPath
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$tablePath',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  2
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    assertEquals("doc_1", result(0).getAs[String]("id"))
  }

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

    writeHudiAndCreateView(schema, Seq(
      Row("d1", Seq(1.0, 0.0, 0.0)),
      Row("d2", Seq(0.0, 1.0, 0.0)),
      Row("d3", Seq(0.0, 0.0, 1.0))
    ), "double_vec_search", "double_search", "double_corpus")

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'double_corpus',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  2
        |)
        |ORDER BY _hudi_distance
        |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    assertEquals("d1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-10)

    spark.catalog.dropTempView("double_corpus")
  }

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

  @Test
  def testCosineDistanceExactValues(): Unit = {
    // Query [0, 1, 0], verify all distances against manually computed values
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(0.0, 1.0, 0.0),
         |  5,
         |  'cosine'
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    assertEquals(5, result.length)
    val distanceMap = result.map(r => r.getAs[String]("id") -> r.getAs[Double]("_hudi_distance")).toMap

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
    // Query [1, 0, 0] with L2 - distances differ so we can validate ordering
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  5,
         |  'l2'
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    assertEquals(5, result.length)

    // doc_1 [1,0,0]: L2 = 0.0 (exact match)
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-4)

    // doc_4 [0.707,0.707,0]: L2 = sqrt((1-0.707)^2 + 0.707^2) ~= 0.765
    assertEquals("doc_4", result(1).getAs[String]("id"))
    val expectedDoc4 = math.sqrt(math.pow(1.0 - 0.70710678, 2) + math.pow(0.70710678, 2))
    assertEquals(expectedDoc4, result(1).getAs[Double]("_hudi_distance"), 1e-4)

    // doc_5 [0.577,0.577,0.577]: L2 ~= 0.816
    assertEquals("doc_5", result(2).getAs[String]("id"))
    val expectedDoc5 = math.sqrt(
      math.pow(1.0 - 0.57735027, 2) + math.pow(0.57735027, 2) + math.pow(0.57735027, 2))
    assertEquals(expectedDoc5, result(2).getAs[Double]("_hudi_distance"), 1e-4)

    // doc_2 [0,1,0] and doc_3 [0,0,1]: L2 = sqrt(2) ~= 1.414
    assertEquals(math.sqrt(2.0), result(3).getAs[Double]("_hudi_distance"), 1e-4)
    assertEquals(math.sqrt(2.0), result(4).getAs[Double]("_hudi_distance"), 1e-4)

    // Verify ordering: distances are ascending
    for (i <- 0 until result.length - 1) {
      assertTrue(result(i).getAs[Double]("_hudi_distance") <= result(i + 1).getAs[Double]("_hudi_distance"))
    }
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

    writeHudiAndCreateView(schema, Seq(
      Row("n1", Seq(1.0f, 0.0f, 0.0f), "has-vector"),
      Row("n2", null, "null-vector"),
      Row("n3", Seq(0.0f, 1.0f, 0.0f), "has-vector")
    ), "null_vec_search", "null_search", "null_corpus")

    // Should not throw NPE — null rows are filtered out
    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'null_corpus',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  5,
        |  'cosine'
        |)
        |ORDER BY _hudi_distance
        |""".stripMargin
    ).collect()

    // Only non-null rows returned
    assertEquals(2, result.length)
    assertEquals("n1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)

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
        |SELECT id, _hudi_distance
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
  def testInvalidK(): Unit = {
    // k=0
    val exZero = assertThrows(classOf[Exception], () => {
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
    assertTrue(exZero.getMessage.contains("positive integer") ||
      (exZero.getCause != null && exZero.getCause.getMessage.contains("positive integer")))

    // k=-5
    val exNeg = assertThrows(classOf[Exception], () => {
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
    assertTrue(exNeg.getMessage.contains("positive integer") ||
      (exNeg.getCause != null && exNeg.getCause.getMessage.contains("positive integer")))
  }

  @Test
  def testByteVectorCosineDistance(): Unit = {
    createByteCorpusView("byte_corpus", Seq(
      ("b1", Seq(127.toByte, 0.toByte, 0.toByte)),
      ("b2", Seq(0.toByte, 127.toByte, 0.toByte)),
      ("b3", Seq(0.toByte, 0.toByte, 127.toByte))
    ))

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'byte_corpus',
        |  'embedding',
        |  ARRAY(127.0, 0.0, 0.0),
        |  2,
        |  'cosine'
        |)
        |ORDER BY _hudi_distance
        |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    assertEquals("b1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)

    spark.catalog.dropTempView("byte_corpus")
  }

  @Test
  def testByteVectorL2Distance(): Unit = {
    createByteCorpusView("byte_l2_corpus", Seq(
      ("b1", Seq(10.toByte, 0.toByte, 0.toByte)),
      ("b2", Seq(0.toByte, 10.toByte, 0.toByte))
    ))

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'byte_l2_corpus',
        |  'embedding',
        |  ARRAY(10.0, 0.0, 0.0),
        |  2,
        |  'l2'
        |)
        |ORDER BY _hudi_distance
        |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    assertEquals("b1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)
    // b2: sqrt(10^2 + 10^2) = sqrt(200) ~= 14.14
    assertEquals(math.sqrt(200.0), result(1).getAs[Double]("_hudi_distance"), 1e-4)

    spark.catalog.dropTempView("byte_l2_corpus")
  }

  @Test
  def testByteVectorDotProductDistance(): Unit = {
    createByteCorpusView("byte_dot_corpus", Seq(
      ("b1", Seq(10.toByte, 5.toByte, 0.toByte)),
      ("b2", Seq(0.toByte, 5.toByte, 0.toByte))
    ))

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'byte_dot_corpus',
        |  'embedding',
        |  ARRAY(10.0, 0.0, 0.0),
        |  2,
        |  'dot_product'
        |)
        |ORDER BY _hudi_distance
        |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    // b1: -dot = -(10*10 + 5*0 + 0*0) = -100
    assertEquals("b1", result(0).getAs[String]("id"))
    assertEquals(-100.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)
    // b2: -dot = -(0*10 + 5*0 + 0*0) = 0
    assertEquals("b2", result(1).getAs[String]("id"))
    assertEquals(0.0, result(1).getAs[Double]("_hudi_distance"), 1e-5)

    spark.catalog.dropTempView("byte_dot_corpus")
  }

  @Test
  def testBatchQueryCorrectnessVerification(): Unit = {
    createFloatQueryView("correctness_queries", "query_name", "query_vec", Seq(
      ("q_x", Seq(1.0f, 0.0f, 0.0f)),
      ("q_y", Seq(0.0f, 1.0f, 0.0f))
    ))

    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance, query_name
         |FROM hudi_vector_search_batch(
         |  '$corpusViewName',
         |  'embedding',
         |  'correctness_queries',
         |  'query_vec',
         |  1,
         |  'cosine'
         |)
         |""".stripMargin
    ).collect()

    assertEquals(2, result.length)

    // Verify which corpus doc is nearest to each query
    val resultMap = result.map(r =>
      r.getAs[String]("query_name") -> r.getAs[String]("id")).toMap

    assertEquals("doc_1", resultMap("q_x"))
    assertEquals("doc_2", resultMap("q_y"))

    spark.catalog.dropTempView("correctness_queries")
  }

  @Test
  def testBatchQueryMismatchedElementTypes(): Unit = {
    // Query table uses array<double>, corpus uses array<float>
    val querySchema = StructType(Seq(
      StructField("qid", StringType, nullable = false),
      StructField("qvec", ArrayType(DoubleType, containsNull = false), nullable = false)
    ))

    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("q1", Seq(1.0, 0.0, 0.0)))),
      querySchema
    ).createOrReplaceTempView("mismatched_type_queries")

    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search_batch(
           |  '$corpusViewName',
           |  'embedding',
           |  'mismatched_type_queries',
           |  'qvec',
           |  2,
           |  'cosine'
           |)
           |""".stripMargin
      ).collect()
    })
    assertTrue(ex.getMessage.contains("element type") ||
      (ex.getCause != null && ex.getCause.getMessage.contains("element type")))

    spark.catalog.dropTempView("mismatched_type_queries")
  }

  @Test
  def testBatchQueryDimensionMismatch(): Unit = {
    val metadata5 = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(5)")
      .build()

    val querySchema = StructType(Seq(
      StructField("qid", StringType, nullable = false),
      StructField("qvec", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata5)
    ))

    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("q1", Seq(1.0f, 0.0f, 0.0f, 0.0f, 0.0f)))),
      querySchema
    ).createOrReplaceTempView("dim_mismatch_queries")

    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search_batch(
           |  '$corpusViewName',
           |  'embedding',
           |  'dim_mismatch_queries',
           |  'qvec',
           |  2,
           |  'cosine'
           |)
           |""".stripMargin
      ).collect()
    })
    assertTrue(ex.getMessage.contains("dimension") ||
      (ex.getCause != null && ex.getCause.getMessage.contains("dimension")))

    spark.catalog.dropTempView("dim_mismatch_queries")
  }

  @Test
  def testBatchQueryOverlappingNonEmbeddingColumns(): Unit = {
    // Query table also has "id" and "label" columns, same as corpus
    val querySchema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("label", StringType, nullable = true),
      StructField("query_vec", ArrayType(FloatType, containsNull = false), nullable = false)
    ))

    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("q1", "query_label_1", Seq(1.0f, 0.0f, 0.0f)),
        Row("q2", "query_label_2", Seq(0.0f, 1.0f, 0.0f))
      )), querySchema
    ).createOrReplaceTempView("overlapping_queries")

    val result = spark.sql(
      s"""
         |SELECT *
         |FROM hudi_vector_search_batch(
         |  '$corpusViewName',
         |  'embedding',
         |  'overlapping_queries',
         |  'query_vec',
         |  1,
         |  'cosine'
         |)
         |""".stripMargin
    )

    // Query columns that clash with corpus columns get the _hudi_query_ prefix.
    // "id" -> "_hudi_query_id", "label" -> "_hudi_query_label"
    val columns = result.columns
    assertTrue(columns.contains("id"))                // corpus id (unchanged)
    assertTrue(columns.contains("label"))             // corpus label (unchanged)
    assertTrue(columns.contains("_hudi_query_id"))    // query id renamed
    assertTrue(columns.contains("_hudi_query_label")) // query label renamed
    assertTrue(columns.contains("_hudi_distance"))

    val rows = result.collect()
    assertEquals(2, rows.length)

    spark.catalog.dropTempView("overlapping_queries")
  }

  @Test
  def testNonFoldableQueryVectorError(): Unit = {
    // A non-constant expression (column reference) as the query vector must fail.
    // We use a subquery that returns an array — this is foldable=false.
    assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search(
           |  '$corpusViewName',
           |  'embedding',
           |  embedding,
           |  3
           |)
           |""".stripMargin
      ).collect()
    })
  }

  @Test
  def testIntegerQueryVector(): Unit = {
    // ARRAY(1, 0, 0) is inferred as decimal by Spark, should still work
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1, 0, 0),
         |  2
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)
  }

  @Test
  def testNonHudiTableAsCorpus(): Unit = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(3)")
      .build()

    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false),
        nullable = false, metadata)
    ))

    // Create a plain DataFrame (no Hudi write) and register as temp view
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("plain_1", Seq(1.0f, 0.0f, 0.0f)),
        Row("plain_2", Seq(0.0f, 1.0f, 0.0f))
      )), schema
    ).createOrReplaceTempView("plain_corpus")

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'plain_corpus',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  2,
        |  'cosine'
        |)
        |ORDER BY _hudi_distance
        |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    assertEquals("plain_1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)

    spark.catalog.dropTempView("plain_corpus")
  }

  @Test
  def testZeroVectorInCorpus(): Unit = {
    createFloatInMemoryView("zero_corpus", Seq(
      ("normal", Seq(1.0f, 0.0f, 0.0f)),
      ("zero", Seq(0.0f, 0.0f, 0.0f))
    ))

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'zero_corpus',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  2,
        |  'cosine'
        |)
        |ORDER BY _hudi_distance
        |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    // Normal vector should be closest (distance = 0)
    assertEquals("normal", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)
    // Zero vector should have maximal cosine distance (1.0)
    assertEquals("zero", result(1).getAs[String]("id"))
    assertEquals(1.0, result(1).getAs[Double]("_hudi_distance"), 1e-5)

    spark.catalog.dropTempView("zero_corpus")
  }

  @Test
  def testAllIdenticalVectors(): Unit = {
    createFloatInMemoryView("same_corpus", Seq(
      ("same_1", Seq(1.0f, 0.0f, 0.0f)),
      ("same_2", Seq(1.0f, 0.0f, 0.0f)),
      ("same_3", Seq(1.0f, 0.0f, 0.0f))
    ))

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'same_corpus',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  3,
        |  'cosine'
        |)
        |""".stripMargin
    ).collect()

    assertEquals(3, result.length)
    // All distances should be 0.0
    result.foreach { row =>
      assertEquals(0.0, row.getAs[Double]("_hudi_distance"), 1e-5)
    }

    spark.catalog.dropTempView("same_corpus")
  }

  @Test
  def testBatchQueryLargeK(): Unit = {
    createFloatQueryView("large_k_queries", "qid", "qvec", Seq(
      ("q1", Seq(1.0f, 0.0f, 0.0f)),
      ("q2", Seq(0.0f, 1.0f, 0.0f))
    ))

    val result = spark.sql(
      s"""
         |SELECT *
         |FROM hudi_vector_search_batch(
         |  '$corpusViewName',
         |  'embedding',
         |  'large_k_queries',
         |  'qvec',
         |  100,
         |  'cosine'
         |)
         |""".stripMargin
    )

    // k=100 but corpus has 5 rows, so each query gets 5 results = 10 total
    val resultsByQuery = result.groupBy("_hudi_query_index").count().collect()
    assertEquals(2, resultsByQuery.length)
    resultsByQuery.foreach { row =>
      assertEquals(5, row.getLong(1))
    }

    spark.catalog.dropTempView("large_k_queries")
  }

  @Test
  def testUnsupportedAlgorithmError(): Unit = {
    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search(
           |  '$corpusViewName',
           |  'embedding',
           |  ARRAY(1.0, 0.0, 0.0),
           |  3,
           |  'cosine',
           |  'hnsw'
           |)
           |""".stripMargin
      ).collect()
    })
    assertTrue(ex.getMessage.contains("Unsupported search algorithm") ||
      (ex.getCause != null && ex.getCause.getMessage.contains("Unsupported search algorithm")))
  }

  @Test
  def testExplicitBruteForceAlgorithm(): Unit = {
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  2,
         |  'cosine',
         |  'brute_force'
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)
  }

  @Test
  def testCosineDistanceAntiparallelVectors(): Unit = {
    // [1,0,0] vs [-1,0,0] are exactly antiparallel; cosine distance should be 2.0.
    // FP rounding can push dot/denom slightly below -1.0, making 1 - x > 2.0.
    // Verify the upper clamp keeps the result at exactly 2.0.
    createFloatInMemoryView("antiparallel_corpus", Seq(("anti", Seq(-1.0f, 0.0f, 0.0f))))

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search('antiparallel_corpus', 'embedding', ARRAY(1.0, 0.0, 0.0), 1, 'cosine')
        |""".stripMargin
    ).collect()

    assertEquals(1, result.length)
    val dist = result(0).getAs[Double]("_hudi_distance")
    assertTrue(dist <= 2.0, s"Cosine distance should be <= 2.0 but was $dist")
    assertEquals(2.0, dist, 1e-5)

    spark.catalog.dropTempView("antiparallel_corpus")
  }

  @Test
  def testByteCorpusOutOfRangeQueryVector(): Unit = {
    createByteCorpusView("byte_range_corpus", Seq(("b1", Seq(10.toByte, 0.toByte, 0.toByte))))

    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        """
          |SELECT *
          |FROM hudi_vector_search('byte_range_corpus', 'embedding', ARRAY(200.0, 0.0, 0.0), 1, 'cosine')
          |""".stripMargin
      ).collect()
    })
    val msg = if (ex.getCause != null) ex.getCause.getMessage else ex.getMessage
    assertTrue(msg.contains("Query vector value 200.0 is out of range for byte corpus"),
      s"Expected out-of-range error, got: $msg")

    spark.catalog.dropTempView("byte_range_corpus")
  }

  @Test
  def testTooManyArguments(): Unit = {
    // Passing 9 arguments (max is 8) should trigger an argument-count error
    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search(
           |  '$corpusViewName', 'embedding', ARRAY(1.0, 0.0, 0.0), 3,
           |  'cosine', 'brute_force', NULL, 0.5, 'extra_arg'
           |)
           |""".stripMargin
      ).collect()
    })
    val msg = if (ex.getCause != null) ex.getCause.getMessage else ex.getMessage
    assertTrue(msg.contains("4-8 arguments"), s"Expected arg-count error, got: $msg")
  }

  @Test
  def testNullQueryVector(): Unit = {
    // ARRAY(1.0, null, 0.0) — the null element should produce a useful error, not an NPE
    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        s"""
           |SELECT *
           |FROM hudi_vector_search('$corpusViewName', 'embedding', ARRAY(1.0, null, 0.0), 3)
           |""".stripMargin
      ).collect()
    })
    val rootCause = Option(ex.getCause).getOrElse(ex)
    assertFalse(rootCause.isInstanceOf[NullPointerException],
      s"Expected a meaningful error, not NPE: $rootCause")
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

    writeHudiAndCreateView(schema, Seq(
      Row("m1", Seq(1.0f, 0.0f, 0.0f), 1L),
      Row("m2", Seq(0.0f, 1.0f, 0.0f), 1L),
      Row("m3", Seq(0.0f, 0.0f, 1.0f), 1L)
    ), "mor_search_test", "mor_search", "mor_corpus", "MERGE_ON_READ", "ts")

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'mor_corpus',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  2,
        |  'cosine'
        |)
        |ORDER BY _hudi_distance
        |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    assertEquals("m1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)

    spark.catalog.dropTempView("mor_corpus")
  }

  @Test
  def testDimensionMismatchWithoutVectorMetadata(): Unit = {
    // Corpus WITHOUT VECTOR(dim) metadata — dimension mismatch is caught at UDF runtime
    createFloatInMemoryView("no_meta_corpus", Seq(
      ("n1", Seq(1.0f, 0.0f, 0.0f)),
      ("n2", Seq(0.0f, 1.0f, 0.0f))
    ))

    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        """
          |SELECT *
          |FROM hudi_vector_search(
          |  'no_meta_corpus',
          |  'embedding',
          |  ARRAY(1.0, 0.0, 0.0, 0.0, 0.0),
          |  2,
          |  'cosine'
          |)
          |""".stripMargin
      ).collect()
    })
    def rootMessage(e: Throwable): String = if (e.getCause != null) rootMessage(e.getCause) else e.getMessage
    val msg = rootMessage(ex)
    assertTrue(msg.contains("dimension mismatch") || msg.contains("mismatch"),
      s"Expected dimension mismatch error, got: $msg")

    spark.catalog.dropTempView("no_meta_corpus")
  }

  @Test
  def testBatchQueryExactDistanceValues(): Unit = {
    createFloatQueryView("exact_dist_queries", "query_name", "query_vec", Seq(
      ("q_x", Seq(1.0f, 0.0f, 0.0f)),
      ("q_z", Seq(0.0f, 0.0f, 1.0f))
    ))

    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance, query_name
         |FROM hudi_vector_search_batch(
         |  '$corpusViewName',
         |  'embedding',
         |  'exact_dist_queries',
         |  'query_vec',
         |  5,
         |  'cosine'
         |)
         |""".stripMargin
    ).collect()

    // 2 queries x 5 results each = 10 rows
    assertEquals(10, result.length)

    // Verify exact distances for q_x query [1,0,0]
    val qxResults = result.filter(_.getAs[String]("query_name") == "q_x")
      .map(r => r.getAs[String]("id") -> r.getAs[Double]("_hudi_distance")).toMap

    // doc_1 [1,0,0]: cosine distance = 0.0
    assertEquals(0.0, qxResults("doc_1"), 1e-5)
    // doc_2 [0,1,0]: cosine distance = 1.0
    assertEquals(1.0, qxResults("doc_2"), 1e-5)
    // doc_3 [0,0,1]: cosine distance = 1.0
    assertEquals(1.0, qxResults("doc_3"), 1e-5)
    // doc_4 [0.707,0.707,0]: cosine distance = 1 - 0.707 ~= 0.293
    assertEquals(1.0 - 0.70710678, qxResults("doc_4"), 1e-4)

    // Verify exact distances for q_z query [0,0,1]
    val qzResults = result.filter(_.getAs[String]("query_name") == "q_z")
      .map(r => r.getAs[String]("id") -> r.getAs[Double]("_hudi_distance")).toMap

    // doc_3 [0,0,1]: cosine distance = 0.0
    assertEquals(0.0, qzResults("doc_3"), 1e-5)
    // doc_1 [1,0,0]: cosine distance = 1.0
    assertEquals(1.0, qzResults("doc_1"), 1e-5)

    spark.catalog.dropTempView("exact_dist_queries")
  }

  @Test
  def testNaNInCorpusVectors(): Unit = {
    // Corpus with a NaN element — should not crash, NaN distances sort last
    createFloatInMemoryView("nan_corpus", Seq(
      ("normal", Seq(1.0f, 0.0f, 0.0f)),
      ("has_nan", Seq(Float.NaN, 0.0f, 0.0f))
    ))

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'nan_corpus',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  2,
        |  'cosine'
        |)
        |ORDER BY _hudi_distance
        |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    // Normal vector should be first (distance = 0)
    assertEquals("normal", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)
    // NaN vector sorts last in Spark's orderBy (NaN > all other values)
    assertEquals("has_nan", result(1).getAs[String]("id"))
    assertTrue(result(1).getAs[Double]("_hudi_distance").isNaN)

    spark.catalog.dropTempView("nan_corpus")
  }

  @Test
  def testZeroQueryVectorCosineDistance(): Unit = {
    // Zero query vector with cosine distance: all corpus vectors should have distance 1.0
    createFloatInMemoryView("zero_q_corpus", Seq(
      ("a", Seq(1.0f, 0.0f, 0.0f)),
      ("b", Seq(0.0f, 1.0f, 0.0f)),
      ("c", Seq(0.0f, 0.0f, 1.0f))
    ))

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'zero_q_corpus',
        |  'embedding',
        |  ARRAY(0.0, 0.0, 0.0),
        |  3,
        |  'cosine'
        |)
        |""".stripMargin
    ).collect()

    assertEquals(3, result.length)
    // All distances should be 1.0 (convention for zero query vector)
    result.foreach { row =>
      assertEquals(1.0, row.getAs[Double]("_hudi_distance"), 1e-5)
    }

    spark.catalog.dropTempView("zero_q_corpus")
  }

  @Test
  def testByteCorpusFractionalQueryVector(): Unit = {
    // Fractional query values against a byte corpus should fail with a clear error
    createByteCorpusView("byte_frac_corpus", Seq(("b1", Seq(10.toByte, 0.toByte, 0.toByte))))

    val ex = assertThrows(classOf[Exception], () => {
      spark.sql(
        """
          |SELECT *
          |FROM hudi_vector_search('byte_frac_corpus', 'embedding', ARRAY(10.5, 0.0, 0.0), 1, 'cosine')
          |""".stripMargin
      ).collect()
    })
    val msg = if (ex.getCause != null) ex.getCause.getMessage else ex.getMessage
    assertTrue(msg.contains("not a whole number"),
      s"Expected integrality error, got: $msg")

    spark.catalog.dropTempView("byte_frac_corpus")
  }

  // ─── Pre-filter tests ───

  @Test
  def testSingleQueryWithPreFilter(): Unit = {
    // The corpus has labels: x-axis, y-axis, z-axis, xy-diagonal, xyz-diagonal.
    // Pre-filter to only "x-axis" or "xy-diagonal" labels, then search for [1,0,0].
    // Without filter: top 3 = doc_1, doc_4, doc_5
    // With filter (label IN ('x-axis','xy-diagonal')): only doc_1 and doc_4 are candidates
    val result = spark.sql(
      s"""
         |SELECT id, label, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  5,
         |  'cosine',
         |  'brute_force',
         |  'label IN ("x-axis", "xy-diagonal")'
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    // Only 2 rows should be returned (filter removed doc_2, doc_3, doc_5)
    assertEquals(2, result.length)
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)
    assertEquals("doc_4", result(1).getAs[String]("id"))
    assertEquals(1.0 - 0.70710678, result(1).getAs[Double]("_hudi_distance"), 1e-4)
  }

  @Test
  def testSingleQueryWithPreFilterNarrowResult(): Unit = {
    // Filter to only z-axis, then search for [1,0,0] (orthogonal).
    // Should return exactly 1 result with cosine distance = 1.0.
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  5,
         |  'cosine',
         |  'brute_force',
         |  'label = "z-axis"'
         |)
         |""".stripMargin
    ).collect()

    assertEquals(1, result.length)
    assertEquals("doc_3", result(0).getAs[String]("id"))
    assertEquals(1.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)
  }

  @Test
  def testSingleQueryWithNullFilter(): Unit = {
    // NULL filter should behave the same as no filter (all rows considered)
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  3,
         |  'cosine',
         |  'brute_force',
         |  NULL
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    assertEquals(3, result.length)
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals("doc_4", result(1).getAs[String]("id"))
    assertEquals("doc_5", result(2).getAs[String]("id"))
  }

  // ─── Max distance tests ───

  @Test
  def testSingleQueryWithMaxDistance(): Unit = {
    // Search for [1,0,0] with cosine metric and max_distance = 0.3.
    // doc_1: distance 0.0 (included)
    // doc_4: distance ~0.293 (included, barely under 0.3)
    // doc_5: distance ~0.423 (excluded)
    // doc_2: distance 1.0 (excluded)
    // doc_3: distance 1.0 (excluded)
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  5,
         |  'cosine',
         |  'brute_force',
         |  NULL,
         |  0.3
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    assertEquals(2, result.length)
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)
    assertEquals("doc_4", result(1).getAs[String]("id"))
    assertEquals(1.0 - 0.70710678, result(1).getAs[Double]("_hudi_distance"), 1e-4)
  }

  @Test
  def testSingleQueryMaxDistanceExcludesAll(): Unit = {
    // max_distance = 0 should exclude everything except exact matches.
    // Only doc_1 has distance 0.0 to [1,0,0].
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  5,
         |  'cosine',
         |  'brute_force',
         |  NULL,
         |  0.0
         |)
         |""".stripMargin
    ).collect()

    assertEquals(1, result.length)
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)
  }

  @Test
  def testSingleQueryMaxDistanceWithNullThreshold(): Unit = {
    // NULL max_distance should behave like no threshold (all results returned up to k)
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  3,
         |  'cosine',
         |  'brute_force',
         |  NULL,
         |  NULL
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    assertEquals(3, result.length)
    assertEquals("doc_1", result(0).getAs[String]("id"))
  }

  // ─── Combined filter + max distance tests ───

  @Test
  def testSingleQueryFilterAndMaxDistanceCombined(): Unit = {
    // Filter to exclude z-axis, max_distance = 0.5 with cosine and query [1,0,0].
    // After filter: doc_1 (0.0), doc_2 (1.0), doc_4 (~0.293), doc_5 (~0.423)
    // After max_distance 0.5: doc_1 (0.0), doc_4 (~0.293), doc_5 (~0.423)
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  5,
         |  'cosine',
         |  'brute_force',
         |  'label != "z-axis"',
         |  0.5
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    assertEquals(3, result.length)
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals("doc_4", result(1).getAs[String]("id"))
    assertEquals("doc_5", result(2).getAs[String]("id"))
    // Verify doc_2 (distance 1.0) is excluded by max_distance
    assertFalse(result.map(_.getAs[String]("id")).contains("doc_2"))
    // Verify doc_3 (z-axis) is excluded by filter
    assertFalse(result.map(_.getAs[String]("id")).contains("doc_3"))
  }

  @Test
  def testSingleQueryFilterWithL2AndMaxDistance(): Unit = {
    // L2 distance with filter and max_distance. Query [1,0,0].
    // doc_1: L2 = 0.0
    // doc_4: L2 = sqrt((1-0.707)^2 + 0.707^2) ~= 0.765
    // doc_5: L2 = sqrt((1-0.577)^2 + 0.577^2 + 0.577^2) ~= 0.816
    // Only include label containing 'axis', max_distance = 1.0 (L2)
    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$corpusViewName',
         |  'embedding',
         |  ARRAY(1.0, 0.0, 0.0),
         |  5,
         |  'l2',
         |  'brute_force',
         |  'label LIKE "%axis%"',
         |  1.0
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    // label LIKE '%axis%' matches doc_1 (x-axis), doc_2 (y-axis), doc_3 (z-axis)
    // L2 distances: doc_1=0.0, doc_2=sqrt(2)~=1.414, doc_3=sqrt(2)~=1.414
    // max_distance = 1.0 excludes doc_2 and doc_3
    assertEquals(1, result.length)
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)
  }

  // ─── Batch mode filter + max distance tests ───

  @Test
  def testBatchQueryWithPreFilter(): Unit = {
    // Batch search with filter — only search within "x-axis" and "y-axis" labels
    createFloatQueryView("batch_filter_queries", "qid", "qvec", Seq(
      ("q_x", Seq(1.0f, 0.0f, 0.0f)),
      ("q_y", Seq(0.0f, 1.0f, 0.0f))
    ))

    val result = spark.sql(
      s"""
         |SELECT *
         |FROM hudi_vector_search_batch(
         |  '$corpusViewName',
         |  'embedding',
         |  'batch_filter_queries',
         |  'qvec',
         |  5,
         |  'cosine',
         |  'brute_force',
         |  'label IN ("x-axis", "y-axis")'
         |)
         |""".stripMargin
    )

    val rows = result.collect()
    // Filter narrows corpus to 2 docs (doc_1, doc_2), 2 queries, k=5 but only 2 corpus rows
    // So each query gets at most 2 results = 4 total rows
    assertEquals(4, rows.length)

    // Verify only doc_1 and doc_2 appear
    val ids = rows.map(_.getAs[String]("id")).toSet
    assertEquals(Set("doc_1", "doc_2"), ids)

    spark.catalog.dropTempView("batch_filter_queries")
  }

  @Test
  def testBatchQueryWithMaxDistance(): Unit = {
    // Batch search with max_distance threshold
    createFloatQueryView("batch_maxd_queries", "qid", "qvec", Seq(
      ("q_x", Seq(1.0f, 0.0f, 0.0f)),
      ("q_z", Seq(0.0f, 0.0f, 1.0f))
    ))

    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance, _hudi_qid
         |FROM hudi_vector_search_batch(
         |  '$corpusViewName',
         |  'embedding',
         |  'batch_maxd_queries',
         |  'qvec',
         |  5,
         |  'cosine',
         |  'brute_force',
         |  NULL,
         |  0.5
         |)
         |ORDER BY _hudi_qid, _hudi_distance
         |""".stripMargin
    ).collect()

    // For q_x [1,0,0] with max_distance 0.5:
    //   doc_1: 0.0, doc_4: ~0.293, doc_5: ~0.423 → 3 results
    // For q_z [0,0,1] with max_distance 0.5:
    //   doc_3: 0.0, doc_5: ~0.423 → 2 results
    // Total: 5 results
    assertEquals(5, result.length)

    // Verify no result exceeds the threshold
    result.foreach { row =>
      assertTrue(row.getAs[Double]("_hudi_distance") <= 0.5 + 1e-5,
        s"Distance ${row.getAs[Double]("_hudi_distance")} exceeds max_distance 0.5")
    }

    spark.catalog.dropTempView("batch_maxd_queries")
  }

  @Test
  def testBatchQueryFilterAndMaxDistanceCombined(): Unit = {
    // Combine filter and max_distance in batch mode
    createFloatQueryView("batch_combo_queries", "qid", "qvec", Seq(
      ("q_x", Seq(1.0f, 0.0f, 0.0f))
    ))

    val result = spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search_batch(
         |  '$corpusViewName',
         |  'embedding',
         |  'batch_combo_queries',
         |  'qvec',
         |  5,
         |  'cosine',
         |  'brute_force',
         |  'label != "z-axis"',
         |  0.3
         |)
         |ORDER BY _hudi_distance
         |""".stripMargin
    ).collect()

    // Corpus after filter (no z-axis): doc_1, doc_2, doc_4, doc_5
    // Distances to [1,0,0]: doc_1=0.0, doc_4=~0.293, doc_5=~0.423, doc_2=1.0
    // After max_distance 0.3: doc_1=0.0, doc_4=~0.293
    assertEquals(2, result.length)
    assertEquals("doc_1", result(0).getAs[String]("id"))
    assertEquals("doc_4", result(1).getAs[String]("id"))

    spark.catalog.dropTempView("batch_combo_queries")
  }

  // ─── Pre-filter on in-memory views (non-Hudi tables) ───

  @Test
  def testSingleQueryPreFilterOnInMemoryView(): Unit = {
    // Create a view with a category column to filter on
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false),
      StructField("category", StringType, nullable = true)
    ))
    val rows = Seq(
      Row("a", Seq(1.0f, 0.0f, 0.0f), "science"),
      Row("b", Seq(0.0f, 1.0f, 0.0f), "art"),
      Row("c", Seq(0.0f, 0.0f, 1.0f), "science"),
      Row("d", Seq(0.5f, 0.5f, 0.0f), "art")
    )
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .createOrReplaceTempView("filter_test_view")

    val result = spark.sql(
      """
        |SELECT id, _hudi_distance
        |FROM hudi_vector_search(
        |  'filter_test_view',
        |  'embedding',
        |  ARRAY(1.0, 0.0, 0.0),
        |  5,
        |  'l2',
        |  'brute_force',
        |  'category = "science"'
        |)
        |ORDER BY _hudi_distance
        |""".stripMargin
    ).collect()

    // Only "science" rows: a=[1,0,0] and c=[0,0,1]
    assertEquals(2, result.length)
    assertEquals("a", result(0).getAs[String]("id"))
    assertEquals(0.0, result(0).getAs[Double]("_hudi_distance"), 1e-5)
    assertEquals("c", result(1).getAs[String]("id"))
    // L2 of [1,0,0] to [0,0,1] = sqrt(2)
    assertEquals(math.sqrt(2.0), result(1).getAs[Double]("_hudi_distance"), 1e-4)

    spark.catalog.dropTempView("filter_test_view")
  }
}
