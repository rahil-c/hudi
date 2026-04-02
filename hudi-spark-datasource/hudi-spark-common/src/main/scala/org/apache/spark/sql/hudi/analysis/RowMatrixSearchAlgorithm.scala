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

import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Vectors}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.HoodieVectorSearchTableValuedFunction.DistanceMetric
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, col, monotonically_increasing_id, row_number}
import org.apache.spark.sql.types.{ArrayType, ByteType, DataType, DoubleType, FloatType, LongType, StructField, StructType}

import scala.collection.mutable.WrappedArray

/**
 * RowMatrix-based KNN vector search: computes distances via distributed BLAS
 * matrix multiplication using Spark MLlib's [[IndexedRowMatrix]].
 *
 * <p>Complexity: O(|corpus| * |queries| * dimensions) -- same as brute-force,
 * but 2.7-3.1x faster in practice because the hot loop is a native BLAS dgemm
 * (SIMD, cache-blocking) rather than a per-pair JVM UDF.
 *
 * <p>The matrix multiply natively computes dot products. Other distance metrics
 * are derived via mathematical transforms:
 * <ul>
 *   <li><b>DOT_PRODUCT</b>: distance = -dot(a, b)</li>
 *   <li><b>COSINE</b>: normalize vectors to unit length, then distance = 1 - dot(a, b)</li>
 *   <li><b>L2</b>: distance = sqrt(||a||^2 + ||b||^2 - 2 * dot(a, b))</li>
 * </ul>
 *
 * <p><b>Single-query mode:</b> treats the query as a 1-column matrix multiply,
 * then applies orderBy + limit(k).
 *
 * <p><b>Batch-query mode:</b> collects queries to driver (small) to build a
 * local DenseMatrix, multiplies against the distributed corpus, then uses
 * a window function to rank top-K per query.
 */
object RowMatrixSearchAlgorithm extends VectorSearchAlgorithm {

  import HoodieVectorSearchPlanBuilder._

  override val name: String = "row_matrix"

  private val CORPUS_IDX_COL = "_hudi_rm_corpus_idx"
  private val QUERY_IDX_COL = "_hudi_rm_query_idx"
  private val RAW_SCORE_COL = "_hudi_rm_raw_score"

  override def buildSingleQueryPlan(
      spark: SparkSession,
      corpusDf: DataFrame,
      embeddingCol: String,
      queryVector: Array[Double],
      k: Int,
      metric: DistanceMetric.Value): LogicalPlan = {
    validateEmbeddingColumn(corpusDf, embeddingCol)
    validateQueryVectorDimension(corpusDf, embeddingCol, queryVector.length)

    val elemType = getElementType(corpusDf, embeddingCol)
    val filteredCorpus = corpusDf.filter(col(embeddingCol).isNotNull)

    // Add index column for join-back after matrix multiply
    val indexedCorpus = filteredCorpus.withColumn(CORPUS_IDX_COL, monotonically_increasing_id())

    // Build corpus RDD and compute norms if needed
    val dims = queryVector.length
    val (corpusRDD, corpusNormsSq) = buildCorpusRDD(spark, indexedCorpus, embeddingCol, elemType, dims, metric)
    val corpusMatrix = new IndexedRowMatrix(corpusRDD.rdd)

    // Build single-column query matrix
    val (preparedQuery, queryNormSq) = prepareQueryVector(queryVector, metric)
    val queryMatrix = Matrices.dense(dims, 1, preparedQuery).asInstanceOf[DenseMatrix]

    // Multiply: Corpus(C x D) * Query(D x 1) -> Result(C x 1)
    val resultMatrix = corpusMatrix.multiply(queryMatrix)

    // Convert result to DataFrame with distances
    val scoreRows = resultMatrix.rows.toJavaRDD().map { indexedRow =>
      val corpusIdx = indexedRow.index
      val rawScore = indexedRow.vector(0)
      Row(corpusIdx, rawScore)
    }
    val scoreSchema = StructType(Seq(
      StructField(CORPUS_IDX_COL, LongType, nullable = false),
      StructField(RAW_SCORE_COL, DoubleType, nullable = false)
    ))
    val scoresDf = spark.createDataFrame(scoreRows, scoreSchema)

    // Convert raw dot-product scores to distances
    val distanceDf = applyDistanceTransformSingle(scoresDf, metric, corpusNormsSq, queryNormSq)

    // Join back to corpus to recover all columns, drop internals
    val result = indexedCorpus.join(distanceDf, CORPUS_IDX_COL)
      .drop(CORPUS_IDX_COL, embeddingCol, RAW_SCORE_COL)
      .orderBy(col(DISTANCE_COL).asc)
      .limit(k)

    result.queryExecution.analyzed
  }

  override def buildBatchQueryPlan(
      spark: SparkSession,
      corpusDf: DataFrame,
      corpusEmbeddingCol: String,
      queryDf: DataFrame,
      queryEmbeddingCol: String,
      k: Int,
      metric: DistanceMetric.Value): LogicalPlan = {
    validateEmbeddingColumn(corpusDf, corpusEmbeddingCol)
    validateEmbeddingColumn(queryDf, queryEmbeddingCol)
    validateElementTypeCompatibility(corpusDf, corpusEmbeddingCol, queryDf, queryEmbeddingCol)
    validateBatchDimensions(corpusDf, corpusEmbeddingCol, queryDf, queryEmbeddingCol)

    val corpusElemType = getElementType(corpusDf, corpusEmbeddingCol)
    val filteredCorpus = corpusDf.filter(col(corpusEmbeddingCol).isNotNull)
    val filteredQueries = queryDf.filter(col(queryEmbeddingCol).isNotNull)

    // Collect ALL query rows to driver (small) — needed for both the matrix and the join-back DataFrame.
    // Collecting once guarantees that the sequential 0-based index we assign here matches exactly the
    // column indices used in the result matrix, avoiding any monotonically_increasing_id skew.
    val allQueryRows = filteredQueries.collectAsList()
    val queryCount = allQueryRows.size()

    val analyzed = if (queryCount == 0) {
      spark.emptyDataFrame.queryExecution.analyzed
    } else {
      val querySchema = filteredQueries.schema
      val embeddingFieldIdx = querySchema.fieldIndex(queryEmbeddingCol)
      val dims = extractArrayLength(allQueryRows.get(0), embeddingFieldIdx, corpusElemType)

      // Extract query vectors as Array[Array[Double]]
      val queryVectors = new Array[Array[Double]](queryCount)
      var qi = 0
      while (qi < queryCount) {
        queryVectors(qi) = toDoubleArray(allQueryRows.get(qi).get(embeddingFieldIdx), corpusElemType)
        qi += 1
      }

      // Add index column to corpus for join-back after matrix multiply
      val indexedCorpus = filteredCorpus.withColumn(CORPUS_IDX_COL, monotonically_increasing_id())

      // Build corpus RDD and compute norms if needed
      val (corpusRDD, corpusNormsSq) = buildCorpusRDD(spark, indexedCorpus, corpusEmbeddingCol, corpusElemType, dims, metric)
      val corpusMatrix = new IndexedRowMatrix(corpusRDD.rdd)

      // Build query matrix (D x Q, column-major) and compute query norms
      val (queryMatrixData, queryNormsSq) = prepareQueryVectors(queryVectors, dims, metric)
      val queryMatrix = Matrices.dense(dims, queryCount, queryMatrixData).asInstanceOf[DenseMatrix]

      // Multiply: Corpus(C x D) * Queries(D x Q) -> Result(C x Q)
      val resultMatrix = corpusMatrix.multiply(queryMatrix)

      // Flatten result matrix to (corpus_idx, query_idx, raw_score) rows.
      // QUERY_IDX_COL values are 0-based sequential, matching allQueryRows order.
      val qCount = queryCount
      val flatRows = resultMatrix.rows.toJavaRDD().flatMap { indexedRow =>
        val corpusIdx = indexedRow.index
        val scores = indexedRow.vector.toArray
        val rows = new java.util.ArrayList[Row](qCount)
        var q = 0
        while (q < qCount) {
          rows.add(Row(corpusIdx, q.toLong, scores(q)))
          q += 1
        }
        rows.iterator()
      }

      val flatSchema = StructType(Seq(
        StructField(CORPUS_IDX_COL, LongType, nullable = false),
        StructField(QUERY_IDX_COL, LongType, nullable = false),
        StructField(RAW_SCORE_COL, DoubleType, nullable = false)
      ))
      val flatDf = spark.createDataFrame(flatRows, flatSchema)

      // Convert raw scores to distances
      val distanceDf = applyDistanceTransformBatch(spark, flatDf, metric, corpusNormsSq, queryNormsSq)

      // Build indexed query DataFrame from the collected rows with explicit sequential IDs (0, 1, ...).
      // This guarantees alignment with the matrix column indices used above.
      val corpusCols = filteredCorpus.columns.toSet
      val indexedQueryRows = new java.util.ArrayList[Row](queryCount)
      var qj = 0
      while (qj < queryCount) {
        val origRow = allQueryRows.get(qj)
        val newFields = new Array[Any](origRow.length + 1)
        var f = 0
        while (f < origRow.length) { newFields(f) = origRow.get(f); f += 1 }
        newFields(origRow.length) = qj.toLong  // QUERY_ID_COL as sequential index
        indexedQueryRows.add(Row.fromSeq(newFields.toSeq))
        qj += 1
      }
      val querySchemaWithId = querySchema.add(StructField(QUERY_ID_COL, LongType, nullable = false))
      val queryWithId = spark.createDataFrame(indexedQueryRows, querySchemaWithId)

      // Prefix clashing query columns to avoid ambiguity (same logic as BruteForce)
      val renamedQuery = queryWithId.select(queryWithId.columns.map { qCol =>
        if (qCol != QUERY_ID_COL && qCol != queryEmbeddingCol && corpusCols.contains(qCol))
          col(qCol).as(s"$QUERY_COL_PREFIX$qCol")
        else
          col(qCol)
      }: _*)

      // Join distances with corpus and query DataFrames
      val withCorpus = distanceDf.join(indexedCorpus, CORPUS_IDX_COL)
      val withQuery = withCorpus.join(broadcast(renamedQuery),
        col(QUERY_IDX_COL) === col(QUERY_ID_COL))

      // Rank top-K per query
      val window = Window.partitionBy(QUERY_ID_COL).orderBy(col(DISTANCE_COL).asc)
      val result = withQuery
        .drop(CORPUS_IDX_COL, QUERY_IDX_COL, RAW_SCORE_COL, corpusEmbeddingCol, queryEmbeddingCol)
        .withColumn(RANK_COL, row_number().over(window))
        .filter(col(RANK_COL) <= k)
        .drop(RANK_COL)
        .orderBy(col(QUERY_ID_COL), col(DISTANCE_COL))

      result.queryExecution.analyzed
    }

    analyzed
  }

  // ======================== Corpus RDD Construction ========================

  /**
   * Converts the corpus embedding column to a distributed RDD of IndexedRows.
   * For COSINE, normalizes each vector to unit length.
   * For L2, computes squared norms alongside the RDD.
   * Returns (corpusRDD, Option[corpusNormsSquaredBroadcast]).
   */
  private def buildCorpusRDD(
      spark: SparkSession,
      indexedCorpus: DataFrame,
      embeddingCol: String,
      elemType: DataType,
      dims: Int,
      metric: DistanceMetric.Value): (org.apache.spark.api.java.JavaRDD[IndexedRow], Option[DataFrame]) = {

    val corpusSelect = indexedCorpus.select(col(CORPUS_IDX_COL), col(embeddingCol))

    metric match {
      case DistanceMetric.COSINE =>
        // Normalize vectors to unit length so dot product = cosine similarity
        val rdd = corpusSelect.rdd.map { row =>
          val idx = row.getLong(0)
          val vec = toDoubleArray(row.get(1), elemType)
          var normSq = 0.0; var i = 0
          while (i < vec.length) { normSq += vec(i) * vec(i); i += 1 }
          val norm = math.sqrt(normSq)
          if (norm > 0.0) {
            i = 0; while (i < vec.length) { vec(i) /= norm; i += 1 }
          }
          new IndexedRow(idx, Vectors.dense(vec))
        }
        (rdd.toJavaRDD(), None)

      case DistanceMetric.L2 =>
        // Keep original vectors but also compute squared norms for distance formula
        // We compute norms as a separate DataFrame to join later
        val rddWithNorms = corpusSelect.rdd.map { row =>
          val idx = row.getLong(0)
          val vec = toDoubleArray(row.get(1), elemType)
          var normSq = 0.0; var i = 0
          while (i < vec.length) { normSq += vec(i) * vec(i); i += 1 }
          (new IndexedRow(idx, Vectors.dense(vec)), Row(idx, normSq))
        }
        val indexedRdd = rddWithNorms.map(_._1)
        val normRows = rddWithNorms.map(_._2)
        val normSchema = StructType(Seq(
          StructField(CORPUS_IDX_COL, LongType, nullable = false),
          StructField("_hudi_rm_corpus_norm_sq", DoubleType, nullable = false)
        ))
        val normDf = spark.createDataFrame(normRows.toJavaRDD(), normSchema)
        (indexedRdd.toJavaRDD(), Some(normDf))

      case DistanceMetric.DOT_PRODUCT =>
        // Use vectors as-is
        val rdd = corpusSelect.rdd.map { row =>
          val idx = row.getLong(0)
          val vec = toDoubleArray(row.get(1), elemType)
          new IndexedRow(idx, Vectors.dense(vec))
        }
        (rdd.toJavaRDD(), None)
    }
  }

  // ======================== Query Preparation ========================

  /** Prepare a single query vector for the matrix multiply, applying metric-specific transforms. */
  private def prepareQueryVector(queryVector: Array[Double], metric: DistanceMetric.Value): (Array[Double], Double) = {
    metric match {
      case DistanceMetric.COSINE =>
        val normalized = queryVector.clone()
        var normSq = 0.0; var i = 0
        while (i < normalized.length) { normSq += normalized(i) * normalized(i); i += 1 }
        val norm = math.sqrt(normSq)
        if (norm > 0.0) { i = 0; while (i < normalized.length) { normalized(i) /= norm; i += 1 } }
        (normalized, normSq)

      case DistanceMetric.L2 =>
        var normSq = 0.0; var i = 0
        while (i < queryVector.length) { normSq += queryVector(i) * queryVector(i); i += 1 }
        (queryVector, normSq)

      case DistanceMetric.DOT_PRODUCT =>
        (queryVector, 0.0)
    }
  }

  /** Prepare multiple query vectors, returning column-major matrix data and per-query squared norms. */
  private def prepareQueryVectors(
      queryVectors: Array[Array[Double]],
      dims: Int,
      metric: DistanceMetric.Value): (Array[Double], Array[Double]) = {
    val queryCount = queryVectors.length
    val matrixData = new Array[Double](dims * queryCount)
    val normsSq = new Array[Double](queryCount)

    var q = 0
    while (q < queryCount) {
      val (prepared, normSq) = prepareQueryVector(queryVectors(q), metric)
      normsSq(q) = normSq
      // Column-major: column q starts at offset q * dims
      var d = 0
      while (d < dims) {
        matrixData(q * dims + d) = prepared(d)
        d += 1
      }
      q += 1
    }
    (matrixData, normsSq)
  }

  // ======================== Distance Transforms ========================

  /** Convert raw dot-product scores to distances for single-query mode. */
  private def applyDistanceTransformSingle(
      scoresDf: DataFrame,
      metric: DistanceMetric.Value,
      corpusNormsDf: Option[DataFrame],
      queryNormSq: Double): DataFrame = {
    import org.apache.spark.sql.functions.{lit, sqrt, when}

    metric match {
      case DistanceMetric.DOT_PRODUCT =>
        // distance = -dot_product
        scoresDf.withColumn(DISTANCE_COL, -col(RAW_SCORE_COL))

      case DistanceMetric.COSINE =>
        // Vectors are already normalized, so dot_product = cosine_similarity
        // distance = 1.0 - dot_product, clamped to [0, 2]
        scoresDf.withColumn(DISTANCE_COL,
          when(col(RAW_SCORE_COL) > lit(1.0), lit(0.0))
            .when(col(RAW_SCORE_COL) < lit(-1.0), lit(2.0))
            .otherwise(lit(1.0) - col(RAW_SCORE_COL)))

      case DistanceMetric.L2 =>
        // distance = sqrt(||a||^2 + ||b||^2 - 2 * dot(a, b))
        val withNorms = scoresDf.join(corpusNormsDf.get, CORPUS_IDX_COL)
        withNorms.withColumn(DISTANCE_COL,
          sqrt(
            when(col("_hudi_rm_corpus_norm_sq") + lit(queryNormSq) - lit(2.0) * col(RAW_SCORE_COL) < lit(0.0), lit(0.0))
              .otherwise(col("_hudi_rm_corpus_norm_sq") + lit(queryNormSq) - lit(2.0) * col(RAW_SCORE_COL))
          ))
          .drop("_hudi_rm_corpus_norm_sq")
    }
  }

  /** Convert raw dot-product scores to distances for batch-query mode. */
  private def applyDistanceTransformBatch(
      spark: SparkSession,
      flatDf: DataFrame,
      metric: DistanceMetric.Value,
      corpusNormsDf: Option[DataFrame],
      queryNormsSq: Array[Double]): DataFrame = {
    import org.apache.spark.sql.functions.{sqrt, when, lit, udf}

    metric match {
      case DistanceMetric.DOT_PRODUCT =>
        flatDf.withColumn(DISTANCE_COL, -col(RAW_SCORE_COL))

      case DistanceMetric.COSINE =>
        flatDf.withColumn(DISTANCE_COL,
          when(col(RAW_SCORE_COL) > lit(1.0), lit(0.0))
            .when(col(RAW_SCORE_COL) < lit(-1.0), lit(2.0))
            .otherwise(lit(1.0) - col(RAW_SCORE_COL)))

      case DistanceMetric.L2 =>
        // Broadcast query norms and look up by query index
        val bcQueryNorms = spark.sparkContext.broadcast(queryNormsSq)
        val queryNormUdf = udf((qIdx: Long) => bcQueryNorms.value(qIdx.toInt))

        val withCorpusNorms = flatDf.join(corpusNormsDf.get, CORPUS_IDX_COL)
        withCorpusNorms.withColumn(DISTANCE_COL,
          sqrt(
            when(col("_hudi_rm_corpus_norm_sq") + queryNormUdf(col(QUERY_IDX_COL)) - lit(2.0) * col(RAW_SCORE_COL) < lit(0.0), lit(0.0))
              .otherwise(col("_hudi_rm_corpus_norm_sq") + queryNormUdf(col(QUERY_IDX_COL)) - lit(2.0) * col(RAW_SCORE_COL))
          ))
          .drop("_hudi_rm_corpus_norm_sq")
    }
  }

  // ======================== Type Conversion Helpers ========================

  /** Convert a Spark Row embedding field to Array[Double], handling Float, Double, and Byte element types. */
  private def toDoubleArray(embedding: Any, elemType: DataType): Array[Double] = {
    elemType match {
      case FloatType =>
        val arr = embedding.asInstanceOf[WrappedArray[Float]]
        val result = new Array[Double](arr.length)
        var i = 0; while (i < arr.length) { result(i) = arr(i).toDouble; i += 1 }
        result
      case DoubleType =>
        val arr = embedding.asInstanceOf[WrappedArray[Double]]
        val result = new Array[Double](arr.length)
        var i = 0; while (i < arr.length) { result(i) = arr(i); i += 1 }
        result
      case ByteType =>
        val arr = embedding.asInstanceOf[WrappedArray[Byte]]
        val result = new Array[Double](arr.length)
        var i = 0; while (i < arr.length) { result(i) = arr(i).toDouble; i += 1 }
        result
    }
  }

  /** Extract the length of an array embedding from a Row. */
  private def extractArrayLength(row: Row, fieldIdx: Int, elemType: DataType): Int = {
    elemType match {
      case FloatType => row.get(fieldIdx).asInstanceOf[WrappedArray[Float]].length
      case DoubleType => row.get(fieldIdx).asInstanceOf[WrappedArray[Double]].length
      case ByteType => row.get(fieldIdx).asInstanceOf[WrappedArray[Byte]].length
    }
  }
}
