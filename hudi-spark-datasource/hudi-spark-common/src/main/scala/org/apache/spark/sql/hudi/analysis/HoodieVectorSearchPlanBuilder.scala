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

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.common.schema.HoodieSchema

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.HoodieVectorSearchTableValuedFunction.DistanceMetric
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{broadcast, col, lit, monotonically_increasing_id, row_number, udf}
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.types.{ArrayType, ByteType, DoubleType, FloatType}

/**
 * Builds brute force KNN vector search plans for the hudi_vector_search TVF.
 *
 * For single-query mode, computes distance between each corpus row and one query vector,
 * then returns the top-K closest rows ordered by distance.
 *
 * For batch-query mode, cross-joins the corpus with broadcast query vectors, computes
 * pairwise distances, and returns top-K per query using a window function.
 *
 * <b>Scalability note for batch mode:</b> The batch query performs a cross-join between
 * the corpus and the (broadcast) query table, producing O(|corpus| * |queries|) intermediate
 * rows. This is suitable for small-to-medium query sets (tens to low hundreds of queries)
 * against moderate corpora.
 */
object HoodieVectorSearchPlanBuilder {

  val DISTANCE_COL = "_distance"
  private val QUERY_ID_COL = "_query_id"
  private val QUERY_EMB_ALIAS = "_query_emb_internal"
  private val RANK_COL = "_rank"
  private val QUERY_COL_PREFIX = "_query_"

  /**
   * Builds the brute force KNN plan for single-query mode.
   *
   * @param spark the SparkSession
   * @param corpusDf the resolved corpus DataFrame
   * @param embeddingCol the embedding column name in the corpus
   * @param queryVector the query vector as Array[Double]
   * @param k number of nearest neighbors to return
   * @param metric the distance metric to use
   * @return the analyzed LogicalPlan
   */
  def buildSingleQueryPlan(
      spark: SparkSession,
      corpusDf: DataFrame,
      embeddingCol: String,
      queryVector: Array[Double],
      k: Int,
      metric: DistanceMetric.Value): LogicalPlan = {
    validateEmbeddingColumn(corpusDf, embeddingCol)
    validateQueryVectorDimension(corpusDf, embeddingCol, queryVector.length)

    val distanceUdf = createDistanceUdf(metric)
    val castedDf = castEmbeddingToDouble(corpusDf, embeddingCol)
      .filter(col(embeddingCol).isNotNull)

    // Create a literal column from the query vector
    val queryLit = lit(queryVector)

    val result = castedDf
      .withColumn(DISTANCE_COL, distanceUdf(col(embeddingCol), queryLit))
      .orderBy(col(DISTANCE_COL).asc)
      .limit(k)

    result.queryExecution.analyzed
  }

  /**
   * Builds the brute force KNN plan for batch-query mode.
   *
   * @param spark the SparkSession
   * @param corpusDf the resolved corpus DataFrame
   * @param corpusEmbeddingCol the embedding column name in the corpus
   * @param queryDf the resolved query DataFrame
   * @param queryEmbeddingCol the embedding column name in the query table
   * @param k number of nearest neighbors per query
   * @param metric the distance metric to use
   * @return the analyzed LogicalPlan
   */
  def buildBatchQueryPlan(
      spark: SparkSession,
      corpusDf: DataFrame,
      corpusEmbeddingCol: String,
      queryDf: DataFrame,
      queryEmbeddingCol: String,
      k: Int,
      metric: DistanceMetric.Value): LogicalPlan = {
    validateEmbeddingColumn(corpusDf, corpusEmbeddingCol)
    validateEmbeddingColumn(queryDf, queryEmbeddingCol)

    val distanceUdf = createDistanceUdf(metric)
    val castedCorpus = castEmbeddingToDouble(corpusDf, corpusEmbeddingCol)
      .filter(col(corpusEmbeddingCol).isNotNull)

    // Prefix every query column with "_query_" to avoid cross-join column ambiguity:
    //   1. when corpusEmbeddingCol == queryEmbeddingCol (both named "embedding")
    //   2. when corpus and query share other non-embedding columns (e.g. both have "id")
    val corpusCols = castedCorpus.columns.toSet
    val queryWithId = castEmbeddingToDouble(queryDf, queryEmbeddingCol)
      .filter(col(queryEmbeddingCol).isNotNull)
      .withColumnRenamed(queryEmbeddingCol, QUERY_EMB_ALIAS)
      .withColumn(QUERY_ID_COL, monotonically_increasing_id())

    // Rename any query column that clashes with a corpus column (except internal columns)
    val renamedQuery = queryWithId.columns.foldLeft(queryWithId) { (df, qCol) =>
      if (qCol != QUERY_ID_COL && qCol != QUERY_EMB_ALIAS && corpusCols.contains(qCol))
        df.withColumnRenamed(qCol, s"$QUERY_COL_PREFIX$qCol")
      else
        df
    }

    // Cross join corpus with broadcast queries, compute distance, then rank
    val scored = castedCorpus.crossJoin(broadcast(renamedQuery))
      .withColumn(DISTANCE_COL,
        distanceUdf(col(corpusEmbeddingCol), col(QUERY_EMB_ALIAS)))
      .drop(corpusEmbeddingCol)
      .drop(QUERY_EMB_ALIAS)

    val window = Window.partitionBy(QUERY_ID_COL).orderBy(col(DISTANCE_COL).asc)
    val result = scored
      .withColumn(RANK_COL, row_number().over(window))
      .filter(col(RANK_COL) <= k)
      .drop(RANK_COL)
      .orderBy(col(QUERY_ID_COL), col(DISTANCE_COL))

    result.queryExecution.analyzed
  }

  private def createDistanceUdf(metric: DistanceMetric.Value): UserDefinedFunction = {
    metric match {
      case DistanceMetric.COSINE => udf((a: Seq[Double], b: Seq[Double]) => {
        var dot = 0.0
        var normA = 0.0
        var normB = 0.0
        var i = 0
        while (i < a.length) {
          dot += a(i) * b(i)
          normA += a(i) * a(i)
          normB += b(i) * b(i)
          i += 1
        }
        val denom = math.sqrt(normA) * math.sqrt(normB)
        if (denom == 0.0) 1.0 else 1.0 - (dot / denom)
      })

      case DistanceMetric.L2 => udf((a: Seq[Double], b: Seq[Double]) => {
        var sum = 0.0
        var i = 0
        while (i < a.length) {
          val diff = a(i) - b(i)
          sum += diff * diff
          i += 1
        }
        math.sqrt(sum)
      })

      case DistanceMetric.DOT_PRODUCT => udf((a: Seq[Double], b: Seq[Double]) => {
        var dot = 0.0
        var i = 0
        while (i < a.length) {
          dot += a(i) * b(i)
          i += 1
        }
        -dot // negate so lower = more similar
      })
    }
  }

  private[analysis] def validateEmbeddingColumn(df: DataFrame, colName: String): Unit = {
    val fieldOpt = df.schema.fields.find(_.name == colName)
    val field = fieldOpt.getOrElse(
      throw new HoodieAnalysisException(
        s"Embedding column '$colName' not found in table schema. " +
          s"Available columns: ${df.schema.fieldNames.mkString(", ")}"))
    field.dataType match {
      case ArrayType(FloatType, _) | ArrayType(DoubleType, _) | ArrayType(ByteType, _) => // valid
      case other =>
        throw new HoodieAnalysisException(
          s"Embedding column '$colName' has type $other, " +
            "expected array<float>, array<double>, or array<byte>")
    }
  }

  /**
   * Validates that the query vector dimension matches the corpus embedding dimension
   * when the corpus column has VECTOR(dim) metadata. This provides a clear error at
   * analysis time rather than a cryptic ArrayIndexOutOfBoundsException at runtime.
   */
  private def validateQueryVectorDimension(
      df: DataFrame, embeddingCol: String, queryDim: Int): Unit = {
    val field = df.schema.fields.find(_.name == embeddingCol).get
    val typeMetadata = field.metadata
    if (typeMetadata.contains(HoodieSchema.TYPE_METADATA_FIELD)) {
      val typeDescriptor = typeMetadata.getString(HoodieSchema.TYPE_METADATA_FIELD)
      val dimPattern = """VECTOR\((\d+)""".r
      dimPattern.findFirstMatchIn(typeDescriptor).foreach { m =>
        val corpusDim = m.group(1).toInt
        if (corpusDim != queryDim) {
          throw new HoodieAnalysisException(
            s"Query vector dimension ($queryDim) does not match " +
              s"corpus embedding dimension ($corpusDim) for column '$embeddingCol'")
        }
      }
    }
  }

  private def castEmbeddingToDouble(df: DataFrame, colName: String): DataFrame = {
    val field = df.schema(colName)
    field.dataType match {
      case ArrayType(DoubleType, _) => df
      case ArrayType(FloatType, _) | ArrayType(ByteType, _) =>
        df.withColumn(colName, col(colName).cast(ArrayType(DoubleType, containsNull = false)))
      case other =>
        throw new HoodieAnalysisException(
          s"Cannot cast embedding column '$colName' of type $other to array<double>")
    }
  }
}
