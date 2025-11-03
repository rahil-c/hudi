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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, ScalarSubquery}
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException

/**
 * Logical plan node for hudi_vector_search table-valued function.
 *
 * Performs k-NN vector similarity search on a Hudi table using the specified
 * distance metric.
 *
 * @param args Function arguments: (table_name_or_path, embedding_column, query_vector, top_k, [distance_metric])
 */
case class HoodieVectorSearch(args: Seq[Expression]) extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved: Boolean = false

}

object HoodieVectorSearch {

  val FUNC_NAME = "hudi_vector_search"

  /**
   * Parameters for vector search operation
   *
   * @param tableNameOrPath Name or path of the Hudi table to search
   * @param embeddingColumn Name of the column containing vector embeddings
   * @param queryVector Query vector as array of floats
   * @param topK Number of nearest neighbors to return
   * @param distanceMetric Distance metric to use: "l2", "cosine", or "dot_product"
   */
  case class VectorSearchParams(
    tableNameOrPath: String,
    embeddingColumn: String,
    queryVector: Array[Float],
    topK: Int,
    distanceMetric: String
  )

  /**
   * Parse function arguments into VectorSearchParams
   *
   * @param exprs Function argument expressions
   * @param spark SparkSession for executing subqueries if needed
   * @return Parsed parameters
   */
  def parseOptions(exprs: Seq[Expression], spark: SparkSession): VectorSearchParams = {
    if (exprs.length < 4 || exprs.length > 5) {
      throw new HoodieAnalysisException(
        s"Expect arguments (table_name_or_path, embedding_column, query_vector, top_k, [distance_metric]) for function `$FUNC_NAME`. Got ${exprs.length} arguments."
      )
    }

    // Extract table name or path
    val tableNameOrPath = exprs(0).eval().toString

    // Extract embedding column name
    val embeddingColumn = exprs(1).eval().toString

    // Extract and validate query vector
    val queryVector = extractVector(exprs(2), spark)
    if (queryVector.isEmpty) {
      throw new HoodieAnalysisException("Query vector cannot be empty")
    }

    // Extract and validate top_k
    val topK = exprs(3).eval() match {
      case i: Int => i
      case l: Long => l.toInt
      case other => throw new HoodieAnalysisException(
        s"top_k must be an integer, got ${other.getClass.getSimpleName}"
      )
    }

    if (topK <= 0) {
      throw new HoodieAnalysisException(s"top_k must be positive, got $topK")
    }

    // Extract and validate distance metric (optional, defaults to "l2")
    val distanceMetric = if (exprs.length > 4) {
      val metric = exprs(4).eval().toString.toLowerCase

      val validMetrics = Set("l2", "cosine", "dot_product")
      if (!validMetrics.contains(metric)) {
        throw new HoodieAnalysisException(
          s"Invalid distance metric: '$metric'. Must be one of: ${validMetrics.mkString(", ")}"
        )
      }
      metric
    } else {
      "l2"
    }

    VectorSearchParams(tableNameOrPath, embeddingColumn, queryVector, topK, distanceMetric)
  }

  /**
   * Extract vector from expression (handles ARRAY literals and subqueries)
   *
   * @param expr Expression containing vector data
   * @param spark SparkSession for executing subqueries if needed
   * @return Array of floats representing the vector
   */
  private def extractVector(expr: Expression, spark: SparkSession): Array[Float] = {
    // Handle different expression types to get the actual vector value
    //
    // WHY WE NEED ScalarSubquery HANDLING:
    // Production use cases work with large vectors (768, 1024, 4096 dimensions). Writing these as
    // ARRAY literals in SQL is impractical: ARRAY(0.123, 0.456, ..., 0.789) with hundreds of values.
    // Instead, users store vectors in tables and pass them via subqueries:
    //   (SELECT embedding FROM vectors WHERE id = 'user_123')
    //
    // The problem: ScalarSubquery expressions are marked as "Unevaluable" in Spark - calling
    // expr.eval() throws an exception because they need full query execution context. We must
    // detect subqueries and execute them explicitly using the SparkSession.
    val value = expr match {
      case sq: ScalarSubquery =>
        // Execute the subquery plan to get the result
        // (Cannot use expr.eval() - it throws "Cannot evaluate expression: scalar-subquery")
        val df = Dataset.ofRows(spark, sq.plan)
        val result = df.head()

        // Validate the subquery returns a single column containing the vector array
        if (result.size != 1) {
          throw new HoodieAnalysisException(
            s"Query vector subquery must return exactly one column, got ${result.size} columns"
          )
        }

        result.get(0)

      case _ =>
        // For non-subquery expressions (literals, etc.), evaluate directly
        expr.eval()
    }

    // Now convert the value to Array[Float], regardless of whether it came from a subquery or literal
    value match {
      case gad: org.apache.spark.sql.catalyst.util.GenericArrayData =>
        // Handle Spark's internal array type (returned by ARRAY() in SQL)
        try {
          val arrayData = gad.array
          arrayData.map {
            case d: Double => d.toFloat
            case f: Float => f
            case dec: org.apache.spark.sql.types.Decimal => dec.toFloat
            case bd: java.math.BigDecimal => bd.floatValue()
            case other => throw new HoodieAnalysisException(
              s"Query vector must contain numeric values (Float, Double, or Decimal), got ${other.getClass.getSimpleName}"
            )
          }
        } catch {
          case e: HoodieAnalysisException => throw e
          case e: Exception => throw new HoodieAnalysisException(
            s"Failed to parse query vector: ${e.getMessage}"
          )
        }

      case seq: Seq[_] =>
        try {
          seq.map {
            case d: Double => d.toFloat
            case f: Float => f
            case dec: org.apache.spark.sql.types.Decimal => dec.toFloat
            case bd: java.math.BigDecimal => bd.floatValue()
            case other => throw new HoodieAnalysisException(
              s"Query vector must contain numeric values (Float, Double, or Decimal), got ${other.getClass.getSimpleName}"
            )
          }.toArray
        } catch {
          case e: HoodieAnalysisException => throw e
          case e: Exception => throw new HoodieAnalysisException(
            s"Failed to parse query vector: ${e.getMessage}"
          )
        }

      case arr: Array[_] =>
        try {
          arr.map {
            case d: Double => d.toFloat
            case f: Float => f
            case dec: org.apache.spark.sql.types.Decimal => dec.toFloat
            case bd: java.math.BigDecimal => bd.floatValue()
            case other => throw new HoodieAnalysisException(
              s"Query vector must contain numeric values (Float, Double, or Decimal), got ${other.getClass.getSimpleName}"
            )
          }
        } catch {
          case e: HoodieAnalysisException => throw e
          case e: Exception => throw new HoodieAnalysisException(
            s"Failed to parse query vector: ${e.getMessage}"
          )
        }

      case other => throw new HoodieAnalysisException(
        s"Query vector must be an array, got ${other.getClass.getSimpleName}. " +
        s"Use ARRAY(0.1, 0.2, 0.3) syntax in SQL."
      )
    }
  }
}
