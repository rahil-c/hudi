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

    def fromString(s: String): Value = s.toLowerCase match {
      case "cosine" => COSINE
      case "l2" | "euclidean" => L2
      case "dot_product" | "dot" | "inner_product" => DOT_PRODUCT
      case other => throw new HoodieAnalysisException(
        s"Unsupported distance metric: '$other'. Supported: cosine, l2, dot_product")
    }
  }

  sealed trait VectorSearchArgs
  case class SingleQueryArgs(
    tableName: String,
    embeddingCol: String,
    queryVectorExpr: Expression,
    k: Int,
    metric: DistanceMetric.Value
  ) extends VectorSearchArgs

  case class BatchQueryArgs(
    corpusTable: String,
    corpusEmbeddingCol: String,
    queryTable: String,
    queryEmbeddingCol: String,
    k: Int,
    metric: DistanceMetric.Value
  ) extends VectorSearchArgs

  /**
   * Parse arguments for the hudi_vector_search TVF.
   *
   * Single query mode (4-5 args):
   *   hudi_vector_search('table', 'embedding_col', ARRAY(1.0, 2.0, ...), k [, 'metric'])
   *
   * Batch query mode (5-6 args):
   *   hudi_vector_search('corpus_table', 'corpus_col', 'query_table', 'query_col', k [, 'metric'])
   */
  def parseArgs(exprs: Seq[Expression]): VectorSearchArgs = {
    if (exprs.size < 4 || exprs.size > 6) {
      throw new HoodieAnalysisException(
        s"Function '$FUNC_NAME' expects 4-6 arguments. " +
          "Single query: (table, embedding_col, query_vector, k [, metric]). " +
          "Batch query: (corpus_table, corpus_col, query_table, query_col, k [, metric]).")
    }

    val tableName = exprs.head.eval().toString
    val embeddingCol = exprs(1).eval().toString

    // Distinguish single vs batch mode by checking if arg[2] is a string literal
    exprs(2) match {
      case lit: Literal if lit.dataType == StringType =>
        // Batch mode: (corpus_table, corpus_col, query_table, query_col, k [, metric])
        if (exprs.size < 5) {
          throw new HoodieAnalysisException(
            s"Function '$FUNC_NAME' batch mode expects 5-6 arguments: " +
              "(corpus_table, corpus_col, query_table, query_col, k [, metric]).")
        }
        val queryTable = lit.eval().toString
        val queryCol = exprs(3).eval().toString
        val k = parseK(exprs(4))
        val metric = if (exprs.size >= 6) DistanceMetric.fromString(exprs(5).eval().toString)
        else DistanceMetric.COSINE
        BatchQueryArgs(tableName, embeddingCol, queryTable, queryCol, k, metric)

      case queryVectorExpr =>
        // Single query mode: (table, embedding_col, ARRAY(...), k [, metric])
        val k = parseK(exprs(3))
        val metric = if (exprs.size >= 5) DistanceMetric.fromString(exprs(4).eval().toString)
        else DistanceMetric.COSINE
        SingleQueryArgs(tableName, embeddingCol, queryVectorExpr, k, metric)
    }
  }

  private def parseK(expr: Expression): Int = {
    val kValue = try {
      expr.eval().toString.toInt
    } catch {
      case _: NumberFormatException =>
        throw new HoodieAnalysisException(
          s"Function '$FUNC_NAME': k must be a positive integer, got '${expr.eval()}'")
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
