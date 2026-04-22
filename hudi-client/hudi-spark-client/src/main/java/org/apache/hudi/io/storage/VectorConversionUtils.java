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

package org.apache.hudi.io.storage;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.BinaryType$;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Shared utility methods for vector column handling during Parquet read/write.
 *
 * Vectors are stored as Parquet FIXED_LEN_BYTE_ARRAY columns. On read, Spark maps these
 * to BinaryType. This class provides the canonical conversion between the binary
 * representation and Spark's typed ArrayData (float[], double[], byte[]).
 *
 */
public final class VectorConversionUtils {

  private VectorConversionUtils() {
  }

  /**
   * Detects VECTOR columns in a HoodieSchema record and returns a map of field ordinal
   * to the corresponding {@link HoodieSchema.Vector} schema.
   *
   * @param schema a HoodieSchema of type RECORD (or null)
   * @return map from field index to Vector schema; empty map if schema is null or has no vectors
   */
  public static Map<Integer, HoodieSchema.Vector> detectVectorColumns(HoodieSchema schema) {
    Map<Integer, HoodieSchema.Vector> vectorColumnInfo = new HashMap<>();
    if (schema == null) {
      return vectorColumnInfo;
    }
    List<HoodieSchemaField> fields = schema.getFields();
    for (int i = 0; i < fields.size(); i++) {
      HoodieSchema fieldSchema = fields.get(i).schema().getNonNullType();
      if (fieldSchema.getType() == HoodieSchemaType.VECTOR) {
        vectorColumnInfo.put(i, (HoodieSchema.Vector) fieldSchema);
      }
    }
    return vectorColumnInfo;
  }

  /**
   * Builds the {@link HoodieSchema#VECTOR_COLUMNS_METADATA_KEY} footer value
   * from a Spark {@link StructType} by detecting VECTOR metadata annotations and
   * delegating to {@link HoodieSchema#serializeVectorColumnsMetadata}.
   *
   * @param schema Spark StructType (may be null)
   * @return comma-separated descriptor list, or empty string if no VECTOR columns
   * @see HoodieSchema#serializeVectorColumnsMetadata(java.util.Map)
   */
  public static String buildVectorColumnsFooterValue(StructType schema) {
    if (schema == null) {
      return "";
    }
    StructField[] fields = schema.fields();
    Map<Integer, HoodieSchema.Vector> detected = detectVectorColumnsFromMetadata(schema);
    // Walk fields in ordinal order (not detected.entrySet()) so the footer value
    // is emitted in stable, schema-ordered form. detectVectorColumnsFromMetadata
    // returns a plain HashMap, whose iteration order is unspecified.
    LinkedHashMap<String, HoodieSchema.Vector> named = new LinkedHashMap<>();
    for (int i = 0; i < fields.length; i++) {
      HoodieSchema.Vector vec = detected.get(i);
      if (vec != null) {
        named.put(fields[i].name(), vec);
      }
    }
    return HoodieSchema.serializeVectorColumnsMetadata(named);
  }

  /**
   * Detects VECTOR columns from Spark StructType metadata annotations.
   * Fields with metadata key {@link HoodieSchema#TYPE_METADATA_FIELD} starting with "VECTOR"
   * are parsed and included.
   *
   * @param schema Spark StructType
   * @return map from field index to Vector schema; empty map if no vectors found
   */
  public static Map<Integer, HoodieSchema.Vector> detectVectorColumnsFromMetadata(StructType schema) {
    Map<Integer, HoodieSchema.Vector> vectorColumnInfo = new HashMap<>();
    if (schema == null) {
      return vectorColumnInfo;
    }
    StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      StructField field = fields[i];
      if (field.metadata().contains(HoodieSchema.TYPE_METADATA_FIELD)) {
        String typeStr = field.metadata().getString(HoodieSchema.TYPE_METADATA_FIELD);
        if (typeStr.startsWith("VECTOR")) {
          HoodieSchema parsed = HoodieSchema.parseTypeDescriptor(typeStr);
          if (parsed.getType() == HoodieSchemaType.VECTOR) {
            vectorColumnInfo.put(i, (HoodieSchema.Vector) parsed);
          }
        }
      }
    }
    return vectorColumnInfo;
  }

  /**
   * Replaces ArrayType with BinaryType for VECTOR columns so the Parquet reader
   * can read FIXED_LEN_BYTE_ARRAY data without type mismatch.
   *
   * @param structType     the original Spark schema
   * @param vectorColumns  map of ordinal to vector info (only the key set is used)
   * @return a new StructType with vector columns replaced by BinaryType
   */
  public static StructType replaceVectorColumnsWithBinary(StructType structType, Map<Integer, ?> vectorColumns) {
    StructField[] fields = structType.fields();
    StructField[] newFields = new StructField[fields.length];
    for (int i = 0; i < fields.length; i++) {
      if (vectorColumns.containsKey(i)) {
        // Preserve the original field metadata (including hudi_type) so that downstream code
        // calling detectVectorColumnsFromMetadata on the modified schema still finds vectors.
        newFields[i] = new StructField(fields[i].name(), BinaryType$.MODULE$, fields[i].nullable(), fields[i].metadata());
      } else {
        newFields[i] = fields[i];
      }
    }
    return new StructType(newFields);
  }

  /**
   * Converts binary bytes from a FIXED_LEN_BYTE_ARRAY Parquet column back to a typed array
   * based on the vector's element type and dimension.
   *
   * @param bytes        raw bytes read from Parquet
   * @param vectorSchema the vector schema describing dimension and element type
   * @return an ArrayData containing the decoded float[], double[], or byte[] array
   * @throws IllegalArgumentException if byte array length doesn't match expected size
   */
  public static ArrayData convertBinaryToVectorArray(byte[] bytes, HoodieSchema.Vector vectorSchema) {
    return convertBinaryToVectorArray(bytes, vectorSchema.getDimension(), vectorSchema.getVectorElementType());
  }

  /**
   * Converts binary bytes from a FIXED_LEN_BYTE_ARRAY Parquet column back to a typed array.
   *
   * @param bytes    raw bytes read from Parquet
   * @param dim      vector dimension (number of elements)
   * @param elemType element type (FLOAT, DOUBLE, or INT8)
   * @return an ArrayData containing the decoded float[], double[], or byte[] array
   * @throws IllegalArgumentException if byte array length doesn't match expected size
   */
  public static ArrayData convertBinaryToVectorArray(byte[] bytes, int dim,
                                                     HoodieSchema.Vector.VectorElementType elemType) {
    int expectedSize = dim * elemType.getElementSize();
    checkArgument(bytes.length == expectedSize,
        "Vector byte array length mismatch: expected " + expectedSize + " but got " + bytes.length);
    ByteBuffer buffer = ByteBuffer.wrap(bytes).order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    switch (elemType) {
      case FLOAT:
        float[] floats = new float[dim];
        for (int j = 0; j < dim; j++) {
          floats[j] = buffer.getFloat();
        }
        return new GenericArrayData(floats);
      case DOUBLE:
        double[] doubles = new double[dim];
        for (int j = 0; j < dim; j++) {
          doubles[j] = buffer.getDouble();
        }
        return new GenericArrayData(doubles);
      case INT8:
        byte[] int8s = new byte[dim];
        buffer.get(int8s);
        // Use UnsafeArrayData to avoid boxing each byte into a Byte object.
        // GenericArrayData(byte[]) would box every element into Object[].
        return UnsafeArrayData.fromPrimitiveArray(int8s);
      default:
        throw new UnsupportedOperationException(
            "Unsupported vector element type: " + elemType);
    }
  }

  /**
   * Returns a {@link Function} that converts a single {@link InternalRow} by converting binary
   * vector columns back to typed arrays and then applying the given projection callback.
   *
   * <p>Ordinals in {@code vectorColumns} must be relative to {@code readSchema} — the schema
   * that has {@code BinaryType} for vector columns (as produced by
   * {@link #replaceVectorColumnsWithBinary}).
   *
   * <p><b>Thread safety:</b> The returned function is NOT thread-safe; it reuses a single
   * {@link GenericInternalRow} buffer across calls. Each call to this factory creates its own
   * buffer, so separate functions returned by separate calls are independent.
   *
   * @param readSchema         the Spark schema of incoming rows (BinaryType for vector columns)
   * @param vectorColumns      map of ordinal → Vector schema for vector columns, keyed by
   *                           ordinals relative to {@code readSchema}
   * @param projectionCallback called with the converted {@link GenericInternalRow}; must copy
   *                           any data it needs to retain (e.g. {@code UnsafeProjection::apply})
   * @return a function that converts one row and returns the projected result
   */
  public static Function<InternalRow, InternalRow> buildRowMapper(
      StructType readSchema,
      Map<Integer, HoodieSchema.Vector> vectorColumns,
      Function<InternalRow, InternalRow> projectionCallback) {
    GenericInternalRow converted = new GenericInternalRow(readSchema.fields().length);
    return row -> {
      convertRowVectorColumns(row, converted, readSchema, vectorColumns);
      return projectionCallback.apply(converted);
    };
  }

  /**
   * Converts vector columns in a row from binary (BinaryType) back to typed arrays,
   * copying non-vector columns as-is. The caller must supply a pre-allocated
   * {@link GenericInternalRow} for reuse across iterations to reduce GC pressure.
   *
   * @param row           the source row (with BinaryType for vector columns)
   * @param result        a pre-allocated GenericInternalRow to write into (reused across calls)
   * @param readSchema    the Spark schema of the source row (BinaryType for vector columns)
   * @param vectorColumns map of ordinal to Vector schema for vector columns
   */
  public static void convertRowVectorColumns(InternalRow row, GenericInternalRow result,
                                             StructType readSchema,
                                             Map<Integer, HoodieSchema.Vector> vectorColumns) {
    int numFields = readSchema.fields().length;
    for (int i = 0; i < numFields; i++) {
      if (row.isNullAt(i)) {
        result.setNullAt(i);
      } else if (vectorColumns.containsKey(i)) {
        result.update(i, convertBinaryToVectorArray(row.getBinary(i), vectorColumns.get(i)));
      } else {
        // Non-vector column: copy value as-is using the read schema's data type
        result.update(i, row.get(i, readSchema.apply(i).dataType()));
      }
    }
  }

  /**
   * Re-attaches {@link HoodieSchema#TYPE_METADATA_FIELD} to Spark fields that were
   * Arrow {@code FixedSizeList<Float32|Float64, dim>} in the Lance file.
   * {@code LanceArrowUtils.fromArrowSchema} drops all field metadata, so without this
   * step VECTOR columns written by Hudi are indistinguishable from plain arrays.
   *
   * <p>Only fields whose names appear in the {@link HoodieSchema#VECTOR_COLUMNS_METADATA_KEY}
   * footer value are considered. This avoids false-positive annotation of Arrow
   * {@code FixedSizeList<Float/Double>} columns in externally-written Lance files that
   * weren't intended as Hudi VECTORs. Only top-level fields are inspected; nested
   * structs are not recursed.
   *
   * @param arrowSchema     original Arrow schema from the Lance file
   * @param convertedSpark  Spark schema from {@code LanceArrowUtils.fromArrowSchema}
   *                        (same field count and order as {@code arrowSchema})
   * @return StructType with VECTOR metadata restored on eligible fields
   */
  public static StructType restoreVectorMetadataFromArrowSchema(
          Schema arrowSchema, StructType convertedSpark) {
    if (arrowSchema == null || convertedSpark == null) {
      return convertedSpark;
    }
    java.util.Set<String> vectorFieldNames = HoodieSchema.parseVectorColumnNames(
        arrowSchema.getCustomMetadata().get(HoodieSchema.VECTOR_COLUMNS_METADATA_KEY));
    if (vectorFieldNames.isEmpty()) {
      return convertedSpark;
    }
    List<Field> arrowFields = arrowSchema.getFields();
    StructField[] sparkFields = convertedSpark.fields();
    StructField[] newFields = new StructField[sparkFields.length];
    for (int i = 0; i < sparkFields.length; i++) {
      StructField sf = sparkFields[i];
      if (!vectorFieldNames.contains(sf.name())) {
        newFields[i] = sf;
        continue;
      }
      String descriptor = deriveVectorDescriptor(arrowFields.get(i));
      if (descriptor == null) {
        newFields[i] = sf;
      } else {
        newFields[i] = new StructField(
            sf.name(),
            sf.dataType(),
            sf.nullable(),
            new MetadataBuilder()
                .withMetadata(sf.metadata())
                .putString(HoodieSchema.TYPE_METADATA_FIELD, descriptor)
                .build());
      }
    }
    return new StructType(newFields);
  }

  /**
   * Derives Hudi's VECTOR type descriptor for an Arrow field if it represents a
   * fixed-size list of single- or double-precision floats, otherwise returns null.
   */
  private static String deriveVectorDescriptor(Field arrowField) {
    ArrowType type = arrowField.getType();
    if (!(type instanceof ArrowType.FixedSizeList)) {
      return null;
    }
    int dim = ((ArrowType.FixedSizeList) type).getListSize();
    List<Field> children = arrowField.getChildren();
    if (children.size() != 1) {
      return null;
    }
    ArrowType childType = children.get(0).getType();
    if (!(childType instanceof ArrowType.FloatingPoint)) {
      return null;
    }
    FloatingPointPrecision precision = ((ArrowType.FloatingPoint) childType).getPrecision();
    HoodieSchema.Vector.VectorElementType elementType;
    if (precision == FloatingPointPrecision.SINGLE) {
      elementType = HoodieSchema.Vector.VectorElementType.FLOAT;
    } else if (precision == FloatingPointPrecision.DOUBLE) {
      elementType = HoodieSchema.Vector.VectorElementType.DOUBLE;
    } else {
      return null;
    }
    return HoodieSchema.createVector(dim, elementType).toTypeDescriptor();
  }
}
