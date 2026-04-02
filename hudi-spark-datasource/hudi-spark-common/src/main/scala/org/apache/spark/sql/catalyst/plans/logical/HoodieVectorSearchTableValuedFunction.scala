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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal}
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException
import org.apache.spark.sql.types.StringType

object HoodieVectorSearchTableValuedFunction {

  val FUNC_NAME = "hudi_vector_search"

  object DistanceMetric extends Enumeration {
    val COSINE, L2, DOT_PRODUCT = Value

    def fromString(s: String): Value = Option(s).map(_.toLowerCase).getOrElse("") match {
      case "cosine" => COSINE
      case "l2" | "euclidean" => L2
      case "dot_product" | "inner_product" => DOT_PRODUCT
      case other => throw new HoodieAnalysisException(
        s"Unsupported distance metric: '$other'. Supported: cosine, l2, dot_product")
    }
  }

  object SearchAlgorithm extends Enumeration {
    val BRUTE_FORCE, ROW_MATRIX = Value

    def fromString(s: String): Value = Option(s).map(_.toLowerCase).getOrElse("") match {
      case "brute_force" => BRUTE_FORCE
      case "row_matrix" => ROW_MATRIX
      case other => throw new HoodieAnalysisException(
        s"Unsupported search algorithm: '$other'. Supported: brute_force, row_matrix")
    }
  }

  sealed trait VectorSearchArgs
  case class SingleQueryArgs(
    tableName: String,
    embeddingCol: String,
    queryVectorExpr: Expression,
    k: Int,
    metric: DistanceMetric.Value,
    algorithm: SearchAlgorithm.Value
  ) extends VectorSearchArgs

  case class BatchQueryArgs(
    corpusTable: String,
    corpusEmbeddingCol: String,
    queryTable: String,
    queryEmbeddingCol: String,
    k: Int,
    metric: DistanceMetric.Value,
    algorithm: SearchAlgorithm.Value
  ) extends VectorSearchArgs

  /**
   * Parse arguments for the hudi_vector_search TVF.
   *
   * Single query mode (4-6 args):
   *   hudi_vector_search('table', 'embedding_col', ARRAY(1.0, 2.0, ...), k [, 'metric'] [, 'algorithm'])
   *   metric defaults to 'cosine'; algorithm defaults to 'brute_force'.
   *
   * Batch query mode (5-7 args):
   *   hudi_vector_search('corpus_table', 'corpus_col', 'query_table', 'query_col', k [, 'metric'] [, 'algorithm'])
   *   metric defaults to 'cosine'; algorithm defaults to 'brute_force'.
   *
   * Mode is distinguished by checking whether both arg[2] and arg[3] are string literals:
   * in batch mode they are the query table name and query column name; in single mode arg[2]
   * is the ARRAY expression and arg[3] is the integer k.
   */
  def parseArgs(exprs: Seq[Expression]): VectorSearchArgs = {
    if (exprs.size < 4 || exprs.size > 7) {
      throw new HoodieAnalysisException(
        s"Function '$FUNC_NAME' expects 4-7 arguments. " +
          "Single query: (table, embedding_col, query_vector, k [, metric] [, algorithm]). " +
          "Batch query: (corpus_table, corpus_col, query_table, query_col, k [, metric] [, algorithm]).")
    }

    def requireStringLiteral(expr: Expression, argName: String): String = expr match {
      case Literal(v, StringType) if v != null => v.toString
      case _ => throw new HoodieAnalysisException(
        s"Function '$FUNC_NAME': argument '$argName' must be a string literal, got: ${expr.sql}")
    }
    val tableName = requireStringLiteral(exprs.head, "table")
    val embeddingCol = requireStringLiteral(exprs(1), "embedding_col")

    // Distinguish single vs batch mode: batch mode has string literals for both arg[2] (query_table)
    // and arg[3] (query_col); single mode has an ARRAY expression at arg[2] and integer k at arg[3].
    val isBatchMode = exprs.size >= 5 &&
      exprs(2).isInstanceOf[Literal] && exprs(2).asInstanceOf[Literal].dataType == StringType &&
      exprs(3).isInstanceOf[Literal] && exprs(3).asInstanceOf[Literal].dataType == StringType

    if (!isBatchMode && exprs.size > 6) {
      throw new HoodieAnalysisException(
        s"Function '$FUNC_NAME' expects 4-7 arguments. " +
          "Single query: (table, embedding_col, query_vector, k [, metric] [, algorithm]). " +
          "Batch query: (corpus_table, corpus_col, query_table, query_col, k [, metric] [, algorithm]).")
    }

    if (isBatchMode) {
      // Batch mode: (corpus_table, corpus_col, query_table, query_col, k [, metric] [, algorithm])
      val queryTable = exprs(2).eval().toString
      val queryCol = exprs(3).eval().toString
      val k = parseK(exprs(4))
      val metric = if (exprs.size >= 6) DistanceMetric.fromString(exprs(5).eval().toString)
      else DistanceMetric.COSINE
      val algorithm = if (exprs.size >= 7) SearchAlgorithm.fromString(exprs(6).eval().toString)
      else SearchAlgorithm.BRUTE_FORCE
      BatchQueryArgs(tableName, embeddingCol, queryTable, queryCol, k, metric, algorithm)
    } else {
      // Single query mode: (table, embedding_col, ARRAY(...), k [, metric] [, algorithm])
      if (exprs.size > 6) {
        throw new HoodieAnalysisException(
          s"Function '$FUNC_NAME' expects 4-6 arguments in single-query mode. " +
            "Single query: (table, embedding_col, query_vector, k [, metric] [, algorithm]).")
      }
      val queryVectorExpr = exprs(2)
      val k = parseK(exprs(3))
      val metric = if (exprs.size >= 5) DistanceMetric.fromString(exprs(4).eval().toString)
      else DistanceMetric.COSINE
      val algorithm = if (exprs.size >= 6) SearchAlgorithm.fromString(exprs(5).eval().toString)
      else SearchAlgorithm.BRUTE_FORCE
      SingleQueryArgs(tableName, embeddingCol, queryVectorExpr, k, metric, algorithm)
    }
  }

  private def parseK(expr: Expression): Int = {
    val rawValue = expr.eval()
    val kValue = try {
      rawValue.toString.toInt
    } catch {
      case _: NumberFormatException =>
        throw new HoodieAnalysisException(
          s"Function '$FUNC_NAME': k must be a positive integer, got '$rawValue'")
    }
    if (kValue <= 0) {
      throw new HoodieAnalysisException(
        s"Function '$FUNC_NAME': k must be a positive integer, got $kValue")
    }
    kValue
  }
}

case class HoodieVectorSearchTableValuedFunction(args: Seq[Expression]) extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved: Boolean = false
}
