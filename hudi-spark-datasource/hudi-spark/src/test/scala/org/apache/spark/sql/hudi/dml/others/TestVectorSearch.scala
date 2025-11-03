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

package org.apache.spark.sql.hudi.dml.others

import org.apache.hudi.{DataSourceWriteOptions, DefaultSparkRecordMerger}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource, ValueSource}

class TestVectorSearch extends HoodieSparkSqlTestBase {

  @ParameterizedTest
  @MethodSource(Array("testParams"))
  def testBasicVectorSearchWithL2Distance(baseFileFormat: String, useTableName: Boolean): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName + "_" + baseFileFormat.toLowerCase
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      // Create test data with embeddings
      val records = Seq(
        (1, "Product A", Array(1.0f, 2.0f, 3.0f), 10.0),
        (2, "Product B", Array(2.0f, 3.0f, 4.0f), 20.0),
        (3, "Product C", Array(10.0f, 11.0f, 12.0f), 30.0),
        (4, "Product D", Array(1.5f, 2.5f, 3.5f), 15.0)
      )
      val df = spark.createDataFrame(records).toDF("id", "name", "embedding", "price")

      // Write to Hudi table
      if (useTableName) {
        // Register table in catalog using saveAsTable
        df.write
          .format("hudi")
          .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFileFormat)
          .option(DataSourceWriteOptions.TABLE_TYPE.key(), "COPY_ON_WRITE")
          .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
          .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "id")
          .option(DataSourceWriteOptions.TABLE_NAME.key(), tableName)
          .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
          .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
          .option("path", tablePath)
          .mode(SaveMode.Overwrite)
          .saveAsTable(tableName)
      } else {
        // Write to path without catalog registration
        df.write
          .format("hudi")
          .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFileFormat)
          .option(DataSourceWriteOptions.TABLE_TYPE.key(), "COPY_ON_WRITE")
          .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
          .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "id")
          .option(DataSourceWriteOptions.TABLE_NAME.key(), tableName)
          .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
          .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
          .mode(SaveMode.Overwrite)
          .save(tablePath)
      }

      // Use appropriate identifier in queries
      val identifier = if (useTableName) tableName else tablePath

      // Test vector search (query for Product D's embedding)
      val result1 = spark.sql(
        s"""
           |SELECT id, name, price, _distance
           |FROM hudi_vector_search(
           |  '$identifier',
           |  'embedding',
           |  ARRAY(1.5, 2.5, 3.5),
           |  2
           |)
           |ORDER BY _distance
           |""".stripMargin
      ).collect()

      // Verify results - Product D should be closest (distance = 0)
      assert(result1.length == 2)
      assert(result1(0).getInt(0) == 4) // Product D
      assert(result1(0).getDouble(3) < 0.01) // Distance should be very close to 0

      // Test vector search with different query vector (query for Product A's embedding)
      val result2 = spark.sql(
        s"""
           |SELECT id, name, price, _distance
           |FROM hudi_vector_search(
           |  '$identifier',
           |  'embedding',
           |  ARRAY(1.0, 2.0, 3.0),
           |  3
           |)
           |ORDER BY _distance
           |""".stripMargin
      ).collect()

      // Product A should be closest
      assert(result2.length == 3)
      assert(result2(0).getInt(0) == 1) // Product A
      assert(result2(0).getDouble(3) < 0.01)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("PARQUET", "LANCE"))
  def testVectorSearchWithCosineDistance(baseFileFormat: String): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName + "_" + baseFileFormat.toLowerCase
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      // Create test data
      val records = Seq(
        (1, "hello world", Array(0.1f, 0.2f, 0.3f)),
        (2, "goodbye world", Array(0.15f, 0.25f, 0.35f)),
        (3, "different text", Array(0.9f, 0.8f, 0.7f))
      )
      val df = spark.createDataFrame(records).toDF("id", "text", "embedding")

      // Write to Hudi table
      df.write
        .format("hudi")
        .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFileFormat)
        .option(DataSourceWriteOptions.TABLE_TYPE.key(), "COPY_ON_WRITE")
        .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "id")
        .option(DataSourceWriteOptions.TABLE_NAME.key(), tableName)
        .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
        .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Test cosine distance
      val result = spark.sql(
        s"""
           |SELECT id, text, _distance
           |FROM hudi_vector_search(
           |  '$tablePath',
           |  'embedding',
           |  ARRAY(0.12, 0.22, 0.32),
           |  2,
           |  'cosine'
           |)
           |ORDER BY _distance
           |""".stripMargin
      ).collect()

      assert(result.length == 2)
      // First result should have smaller cosine distance
      assert(result(0).getDouble(2) < result(1).getDouble(2))
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("PARQUET", "LANCE"))
  def testVectorSearchWithFiltering(baseFileFormat: String): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName + "_" + baseFileFormat.toLowerCase
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      // Create test data
      val records = Seq(
        (1, "Product A", Array(1.0f, 2.0f, 3.0f), 50.0, "electronics"),
        (2, "Product B", Array(1.1f, 2.1f, 3.1f), 150.0, "electronics"),
        (3, "Product C", Array(1.2f, 2.2f, 3.2f), 80.0, "furniture"),
        (4, "Product D", Array(1.3f, 2.3f, 3.3f), 30.0, "electronics")
      )
      val df = spark.createDataFrame(records).toDF("id", "name", "embedding", "price", "category")

      // Write to Hudi table
      df.write
        .format("hudi")
        .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFileFormat)
        .option(DataSourceWriteOptions.TABLE_TYPE.key(), "COPY_ON_WRITE")
        .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "id")
        .option(DataSourceWriteOptions.TABLE_NAME.key(), tableName)
        .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
        .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Vector search with WHERE clause filtering
      val result = spark.sql(
        s"""
           |SELECT id, name, price, category, _distance
           |FROM hudi_vector_search(
           |  '$tablePath',
           |  'embedding',
           |  ARRAY(1.0, 2.0, 3.0),
           |  10
           |)
           |WHERE category = 'electronics' AND price < 100
           |ORDER BY _distance
           |""".stripMargin
      ).collect()

      assert(result.length == 2) // Should return Products A and D
      assert(result(0).getInt(0) == 1) // Product A closest
      assert(result(1).getInt(0) == 4) // Product D second
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("PARQUET", "LANCE"))
  def testVectorSearchWithJoin(baseFileFormat: String): Unit = {
    withTempDir { tmp =>
      val productsTable = generateTableName + "_products_" + baseFileFormat.toLowerCase
      val categoriesTable = generateTableName + "_categories_" + baseFileFormat.toLowerCase
      val productsPath = s"${tmp.getCanonicalPath}/$productsTable"
      val categoriesPath = s"${tmp.getCanonicalPath}/$categoriesTable"

      // Create products test data
      val productsRecords = Seq(
        (1, "Product A", Array(1.0f, 2.0f), 1),
        (2, "Product B", Array(2.0f, 3.0f), 2),
        (3, "Product C", Array(3.0f, 4.0f), 1)
      )
      val productsDf = spark.createDataFrame(productsRecords).toDF("id", "name", "embedding", "category_id")

      // Write products to Hudi table
      productsDf.write
        .format("hudi")
        .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFileFormat)
        .option(DataSourceWriteOptions.TABLE_TYPE.key(), "COPY_ON_WRITE")
        .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "id")
        .option(DataSourceWriteOptions.TABLE_NAME.key(), productsTable)
        .option(HoodieWriteConfig.TBL_NAME.key(), productsTable)
        .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
        .mode(SaveMode.Overwrite)
        .save(productsPath)

      // Create categories test data
      val categoriesRecords = Seq(
        (1, "Electronics"),
        (2, "Furniture")
      )
      val categoriesDf = spark.createDataFrame(categoriesRecords).toDF("category_id", "category_name")

      // Write categories to Hudi table
      categoriesDf.write
        .format("hudi")
        .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFileFormat)
        .option(DataSourceWriteOptions.TABLE_TYPE.key(), "COPY_ON_WRITE")
        .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "category_id")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "category_id")
        .option(DataSourceWriteOptions.TABLE_NAME.key(), categoriesTable)
        .option(HoodieWriteConfig.TBL_NAME.key(), categoriesTable)
        .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
        .mode(SaveMode.Overwrite)
        .save(categoriesPath)

      // Register tables for SQL queries
      spark.read.format("hudi").load(categoriesPath).createOrReplaceTempView(categoriesTable)

      // Vector search with JOIN
      val result = spark.sql(
        s"""
           |SELECT vs.id, vs.name, c.category_name, vs._distance
           |FROM hudi_vector_search(
           |  '$productsPath',
           |  'embedding',
           |  ARRAY(1.5, 2.5),
           |  3
           |) vs
           |JOIN $categoriesTable c ON vs.category_id = c.category_id
           |ORDER BY vs._distance
           |""".stripMargin
      ).collect()

      assert(result.length == 3)
      assert(result(0).getString(2) != null) // Has category name
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("PARQUET", "LANCE"))
  def testVectorSearchWithDotProductDistance(baseFileFormat: String): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName + "_" + baseFileFormat.toLowerCase
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      // Create test data
      val records = Seq(
        (1, Array(1.0f, 0.0f, 0.0f)),
        (2, Array(0.0f, 1.0f, 0.0f)),
        (3, Array(0.0f, 0.0f, 1.0f))
      )
      val df = spark.createDataFrame(records).toDF("id", "embedding")

      // Write to Hudi table
      df.write
        .format("hudi")
        .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFileFormat)
        .option(DataSourceWriteOptions.TABLE_TYPE.key(), "COPY_ON_WRITE")
        .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "id")
        .option(DataSourceWriteOptions.TABLE_NAME.key(), tableName)
        .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
        .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Test dot product distance
      val result = spark.sql(
        s"""
           |SELECT id, _distance
           |FROM hudi_vector_search(
           |  '$tablePath',
           |  'embedding',
           |  ARRAY(1.0, 0.0, 0.0),
           |  2,
           |  'dot_product'
           |)
           |ORDER BY _distance
           |""".stripMargin
      ).collect()

      assert(result.length == 2)
      // First result should be id=1 with distance = -1.0 (highest dot product)
      assert(result(0).getInt(0) == 1)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("PARQUET", "LANCE"))
  def testVectorSearchTopKLimit(baseFileFormat: String): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName + "_" + baseFileFormat.toLowerCase
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      // Create test data - 10 records
      val records = (1 to 10).map { id =>
        (id, Array(id.toFloat, (id * 2).toFloat))
      }
      val df = spark.createDataFrame(records).toDF("id", "embedding")

      // Write to Hudi table
      df.write
        .format("hudi")
        .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFileFormat)
        .option(DataSourceWriteOptions.TABLE_TYPE.key(), "COPY_ON_WRITE")
        .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "id")
        .option(DataSourceWriteOptions.TABLE_NAME.key(), tableName)
        .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
        .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Request top 3
      val result = spark.sql(
        s"""
           |SELECT id
           |FROM hudi_vector_search(
           |  '$tablePath',
           |  'embedding',
           |  ARRAY(5.0, 10.0),
           |  3
           |)
           |""".stripMargin
      ).collect()

      assert(result.length == 3) // Should return exactly 3 results
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("PARQUET", "LANCE"))
  def testVectorSearchWithScalarSubquery(baseFileFormat: String): Unit = {
    withTempDir { tmp =>
      val tableName = generateTableName + "_" + baseFileFormat.toLowerCase
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      // Create test data with embeddings
      val records = Seq(
        (1, "Product A", Array(1.0f, 2.0f, 3.0f), 10.0),
        (2, "Product B", Array(2.0f, 3.0f, 4.0f), 20.0),
        (3, "Product C", Array(10.0f, 11.0f, 12.0f), 30.0),
        (4, "Product D", Array(1.5f, 2.5f, 3.5f), 15.0)
      )
      val df = spark.createDataFrame(records).toDF("id", "name", "embedding", "price")

      // Write to Hudi table
      df.write
        .format("hudi")
        .option(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFileFormat)
        .option(DataSourceWriteOptions.TABLE_TYPE.key(), "COPY_ON_WRITE")
        .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "id")
        .option(DataSourceWriteOptions.TABLE_NAME.key(), tableName)
        .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
        .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      // Test 1: Create single-row temp view with query embedding (Python script pattern)
      // This mimics the Python code:
      //   query_schema = StructType([StructField("query_embedding", ArrayType(FloatType()), False)])
      //   query_df = spark.createDataFrame([(query_embedding,)], schema=query_schema)
      //   query_df.createOrReplaceTempView("query_vector")
      import org.apache.spark.sql.Row
      import org.apache.spark.sql.types._
      import scala.collection.JavaConverters._

      // Define explicit schema (matches Python StructType definition)
      val querySchema = StructType(Seq(
        StructField("query_embedding", ArrayType(FloatType), nullable = false)
      ))

      // Create DataFrame with explicit schema (matches Python createDataFrame with schema)
      val queryData = Seq(Row(Array(1.5f, 2.5f, 3.5f)))
      val queryDf = spark.createDataFrame(queryData.asJava, querySchema)
      queryDf.createOrReplaceTempView("query_vector")

      // Use scalar subquery to fetch the query vector (exact pattern from Python script)
      val result1 = spark.sql(
        s"""
           |SELECT id, name, price, _distance
           |FROM hudi_vector_search(
           |  '$tablePath',
           |  'embedding',
           |  (SELECT query_embedding FROM query_vector),
           |  2
           |)
           |ORDER BY _distance
           |""".stripMargin
      ).collect()

      // Verify results - Product D should be closest (distance ~0)
      assert(result1.length == 2)
      assert(result1(0).getInt(0) == 4) // Product D
      assert(result1(0).getDouble(3) < 0.01) // Distance should be very close to 0

      // Test 2: Update the temp view with a different query vector
      // Reuse the same schema, create new data
      val queryData2 = Seq(Row(Array(1.0f, 2.0f, 3.0f)))
      val queryDf2 = spark.createDataFrame(queryData2.asJava, querySchema)
      queryDf2.createOrReplaceTempView("query_vector")

      // Use the same scalar subquery pattern with updated view
      val result2 = spark.sql(
        s"""
           |SELECT id, name, price, _distance
           |FROM hudi_vector_search(
           |  '$tablePath',
           |  'embedding',
           |  (SELECT query_embedding FROM query_vector),
           |  3
           |)
           |ORDER BY _distance
           |""".stripMargin
      ).collect()

      // Verify results - Product A should be closest now
      assert(result2.length == 3)
      assert(result2(0).getInt(0) == 1) // Product A
      assert(result2(0).getDouble(3) < 0.01) // Distance should be very close to 0

      // Test 3: Verify scalar subquery results match ARRAY literal results
      val literalResult = spark.sql(
        s"""
           |SELECT id, name, price, _distance
           |FROM hudi_vector_search(
           |  '$tablePath',
           |  'embedding',
           |  ARRAY(1.0, 2.0, 3.0),
           |  3
           |)
           |ORDER BY _distance
           |""".stripMargin
      ).collect()

      // Results should be identical
      assert(result2.length == literalResult.length)
      assert(result2(0).getInt(0) == literalResult(0).getInt(0))
      assert(math.abs(result2(0).getDouble(3) - literalResult(0).getDouble(3)) < 0.0001)
    }
  }
}

object TestVectorSearch {
  def testParams(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      Arguments.of("PARQUET", java.lang.Boolean.FALSE),
      Arguments.of("PARQUET", java.lang.Boolean.TRUE),
      Arguments.of("LANCE", java.lang.Boolean.FALSE),
      // not supported yet Arguments.of("LANCE", java.lang.Boolean.TRUE)
    )
  }
}
