/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.avro

import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType}

import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

import java.util.Collections

import scala.collection.JavaConverters._

/**
 * Post-processes an Arrow schema produced from a Spark schema so that every
 * Hudi BLOB field has its nested `data` child rebuilt as
 * [[ArrowType.LargeBinary]] with the `lance-encoding:blob=true` metadata.
 *
 * This is required because lance-spark's `LanceArrowUtils.toArrowField` only
 * propagates Spark field metadata for top-level struct fields; the raw bytes
 * of a Hudi BLOB live on a nested child and would otherwise remain plain
 * [[ArrowType.Binary]] without the blob-encoding hint — meaning Lance would
 * store INLINE payloads through its default column path rather than the
 * dedicated blob writer.
 *
 * The transformation is a structural no-op for schemas that contain no BLOB
 * fields.
 *
 * See lance-format/lance PR #5193 for the reader-side companion and
 * `org.apache.spark.sql.util.LanceArrowUtils` in lance-spark for the
 * writer-side handling of the `lance-encoding:blob` metadata key.
 */
object BlobLanceSchemaSupport {

  /** Lance column-level metadata key that activates blob-encoded writes. */
  val LANCE_BLOB_ENCODING_KEY: String = "lance-encoding:blob"
  private val LANCE_BLOB_ENCODING_VALUE: String = "true"

  /** Returns true iff the Spark field carries the Hudi BLOB logical-type marker. */
  def isBlobField(field: StructField): Boolean = {
    val md = field.metadata
    md != null &&
      md.contains(HoodieSchema.TYPE_METADATA_FIELD) &&
      HoodieSchema.parseTypeDescriptor(md.getString(HoodieSchema.TYPE_METADATA_FIELD))
        .getType == HoodieSchemaType.BLOB
  }

  /**
   * Walk `sparkSchema` and `arrowSchema` in parallel; wherever a BLOB field is
   * present in the Spark schema, rebuild the corresponding Arrow subtree so
   * that the nested `data` field carries the Lance blob-encoding metadata.
   */
  def annotateBlobFieldsForLance(sparkSchema: StructType, arrowSchema: Schema): Schema = {
    val arrowFields = arrowSchema.getFields.asScala
    val rebuilt = sparkSchema.fields.zip(arrowFields).map {
      case (sparkField, arrowField) => rewriteField(sparkField, arrowField)
    }
    new Schema(rebuilt.toSeq.asJava)
  }

  private def rewriteField(sparkField: StructField, arrowField: Field): Field = {
    sparkField.dataType match {
      case st: StructType if isBlobField(sparkField) =>
        rewriteBlobStruct(arrowField)
      case st: StructType =>
        rewriteChildren(arrowField, st)
      case ArrayType(elementType: StructType, _) =>
        val arrowChildren = arrowField.getChildren
        if (arrowChildren.isEmpty) {
          arrowField
        } else {
          // Arrow list / fixed-size-list has a single child field ("element").
          val elementArrow = arrowChildren.get(0)
          val newElement = rewriteField(
            StructField(elementArrow.getName, elementType, elementArrow.isNullable),
            elementArrow)
          if (newElement eq elementArrow) {
            arrowField
          } else {
            new Field(arrowField.getName, arrowField.getFieldType,
              Collections.singletonList[Field](newElement))
          }
        }
      case _ => arrowField
    }
  }

  private def rewriteChildren(arrowField: Field, sparkStruct: StructType): Field = {
    val arrowChildren = arrowField.getChildren.asScala
    if (arrowChildren.isEmpty) {
      return arrowField
    }
    val newChildren = sparkStruct.fields.zip(arrowChildren).map {
      case (sf, af) => rewriteField(sf, af)
    }
    if (newChildren.zip(arrowChildren).forall { case (a, b) => a eq b }) {
      arrowField
    } else {
      new Field(arrowField.getName, arrowField.getFieldType, newChildren.toSeq.asJava)
    }
  }

  /**
   * Rewrite the Arrow `data` child of a BLOB struct as
   * `LargeBinary` + `lance-encoding:blob=true`. Other children are copied.
   */
  private def rewriteBlobStruct(arrowField: Field): Field = {
    val newChildren = arrowField.getChildren.asScala.toSeq.map { child =>
      if (child.getName == HoodieSchema.Blob.INLINE_DATA_FIELD) {
        val meta = new java.util.HashMap[String, String]()
        meta.put(LANCE_BLOB_ENCODING_KEY, LANCE_BLOB_ENCODING_VALUE)
        val newType = new FieldType(true, ArrowType.LargeBinary.INSTANCE, null, meta)
        new Field(child.getName, newType, Collections.emptyList[Field]())
      } else {
        forceNullableRecursively(child)
      }
    }
    new Field(arrowField.getName, arrowField.getFieldType, newChildren.toSeq.asJava)
  }

  /**
   * Recursively rebuild an Arrow field tree so every field is marked nullable.
   * Lance validates child non-nullability even when the parent struct value is
   * null; for BLOB structs, INLINE rows have a null `reference` and OUT_OF_LINE
   * rows have a null `data`, so all BLOB descendants must tolerate nulls.
   */
  private def forceNullableRecursively(arrowField: Field): Field = {
    val oldType = arrowField.getFieldType
    val newType = new FieldType(true, oldType.getType, oldType.getDictionary, oldType.getMetadata)
    val children = arrowField.getChildren.asScala.toSeq.map(forceNullableRecursively)
    new Field(arrowField.getName, newType, children.toSeq.asJava)
  }
}
