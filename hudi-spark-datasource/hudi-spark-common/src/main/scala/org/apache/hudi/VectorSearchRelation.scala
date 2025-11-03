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

package org.apache.hudi

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{DoubleType, StructType}

/**
 * Relation for executing vector similarity search on Hudi tables.
 *
 * Performs k-NN search by:
 * 1. Reading data from the Hudi table
 * 2. Computing distance between each embedding and the query vector
 * 3. Returning the top-k closest matches
 *
 * @param spark SparkSession
 * @param tablePath Path to the Hudi table
 * @param embeddingColumn Column containing vector embeddings
 * @param queryVector Query vector for similarity search
 * @param topK Number of nearest neighbors to return
 * @param distanceMetric Distance metric: "l2", "cosine", or "dot_product"
 */
class VectorSearchRelation(
  @transient val spark: SparkSession,
  val tablePath: String,
  val embeddingColumn: String,
  val queryVector: Array[Float],
  val topK: Int,
  val distanceMetric: String
) extends BaseRelation with TableScan with Serializable {
  //TODO pass filters in the function

  @transient override val sqlContext: SQLContext = spark.sqlContext

  // Get schema from Hudi table and add _distance column
  override def schema: StructType = {
    val hoodieDataSource = new DefaultSource
    val baseRelation = hoodieDataSource.createRelation(sqlContext, Map("path" -> tablePath))
    val baseSchema = baseRelation.schema
    baseSchema.add("_distance", DoubleType, nullable = false)
  }

  override def buildScan(): RDD[Row] = {
    // Read Hudi table using DataFrame API
    val df = spark.read.format("hudi").load(tablePath)

    // Get schema column names (excluding _distance which we'll add later)
    val schemaColumnNames = schema.fieldNames.dropRight(1)

    // Reorder DataFrame columns to match schema order
    // This is critical because Hudi may return columns in a different order
    // (e.g., alphabetical or storage order), but we need them in schema order
    // for Row.fromSeq() to work correctly
    import org.apache.spark.sql.functions.col
    val reorderedDf = df.select(schemaColumnNames.map(col): _*)

    // Get RDD for our custom distance computation
    val baseData = reorderedDf.rdd

    // Compute distances for each row
    val withDistances = computeDistances(baseData, embeddingColumn, queryVector, distanceMetric)

    // Get top-k results
    val topKResults = selectTopK(withDistances, topK)

    // Append distance column to each row
    val rowsWithDistance = topKResults.map { case (row, distance) =>
      Row.fromSeq(row.toSeq :+ distance)
    }

    // Convert to DataFrame with explicit schema to preserve type information, then back to RDD.
    // This is necessary because Row.fromSeq() loses schema-aware type information, which can
    // cause Integer columns to be misinterpreted as Strings during collect() operations.
    // By creating a DataFrame with the explicit schema, we ensure types are preserved correctly.
    import scala.collection.JavaConverters._
    val resultDF = sqlContext.createDataFrame(rowsWithDistance.toJavaRDD(), schema)

    resultDF.rdd
  }

  private def computeDistances(
    data: RDD[Row],
    embeddingCol: String,
    queryVec: Array[Float],
    metric: String
  ): RDD[(Row, Double)] = {

    // Broadcast query vector and parameters for efficiency
    val queryVectorBc = spark.sparkContext.broadcast(queryVec)
    val embeddingColBc = spark.sparkContext.broadcast(embeddingCol)
    val metricBc = spark.sparkContext.broadcast(metric)

    data.mapPartitions { partition =>
      val query = queryVectorBc.value
      val colName = embeddingColBc.value
      val distMetric = metricBc.value

      partition.map { row =>
        try {
          val embedding = extractEmbedding(row, colName)
          val distance = computeDistance(embedding, query, distMetric)
          (row, distance)
        } catch {
          case e: Exception =>
            throw new RuntimeException(
              s"Failed to compute distance for row. Ensure '$colName' contains an array of floats. Error: ${e.getMessage}",
              e
            )
        }
      }
    }
  }

  private def selectTopK(
    data: RDD[(Row, Double)],
    k: Int
  ): RDD[(Row, Double)] = {
    // Use takeOrdered for distributed top-k
    // Orders by distance (ascending - lower distance = more similar)
    val topK = data.takeOrdered(k)(Ordering.by[(Row, Double), Double](_._2))
    spark.sparkContext.parallelize(topK)
  }

  private def extractEmbedding(row: Row, columnName: String): Array[Float] = {
    val schema = row.schema
    val idx = schema.fieldIndex(columnName)

    // Handle both Seq and Array types
    row.get(idx) match {
      case seq: Seq[_] => seq.map {
        case f: Float => f
        case d: Double => d.toFloat
        case dec: org.apache.spark.sql.types.Decimal => dec.toFloat
        case bd: java.math.BigDecimal => bd.floatValue()
        case other => throw new IllegalArgumentException(
          s"Embedding must contain numeric values (Float, Double, or Decimal), got ${other.getClass.getSimpleName}"
        )
      }.toArray
      case arr: Array[_] => arr.map {
        case f: Float => f
        case d: Double => d.toFloat
        case dec: org.apache.spark.sql.types.Decimal => dec.toFloat
        case bd: java.math.BigDecimal => bd.floatValue()
        case other => throw new IllegalArgumentException(
          s"Embedding must contain numeric values (Float, Double, or Decimal), got ${other.getClass.getSimpleName}"
        )
      }.asInstanceOf[Array[Float]]
      case other => throw new IllegalArgumentException(
        s"Column '$columnName' must be an array type, got ${other.getClass.getSimpleName}"
      )
    }
  }

  private def computeDistance(
    embedding: Array[Float],
    query: Array[Float],
    metric: String
  ): Double = {
    if (embedding.length != query.length) {
      throw new IllegalArgumentException(
        s"Embedding dimension mismatch: got ${embedding.length}, expected ${query.length}"
      )
    }

    metric match {
      case "l2" => euclideanDistance(embedding, query)
      case "cosine" => cosineDistance(embedding, query)
      case "dot_product" => -dotProduct(embedding, query) // negative for ascending sort
      case _ => throw new IllegalArgumentException(s"Unknown distance metric: $metric")
    }
  }

  private def euclideanDistance(a: Array[Float], b: Array[Float]): Double = {
    math.sqrt(a.zip(b).map { case (x, y) =>
      val diff = x - y
      diff * diff
    }.sum)
  }

  private def cosineDistance(a: Array[Float], b: Array[Float]): Double = {
    val dotProd = a.zip(b).map { case (x, y) => x * y }.sum
    val normA = math.sqrt(a.map(x => x * x).sum)
    val normB = math.sqrt(b.map(x => x * x).sum)

    if (normA == 0.0 || normB == 0.0) {
      1.0 // Maximum distance if either vector is zero
    } else {
      1.0 - (dotProd / (normA * normB))
    }
  }

  private def dotProduct(a: Array[Float], b: Array[Float]): Double = {
    a.zip(b).map { case (x, y) => x * y }.sum
  }
}
