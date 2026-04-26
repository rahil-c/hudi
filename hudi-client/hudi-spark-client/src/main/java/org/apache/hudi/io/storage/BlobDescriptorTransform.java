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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.exception.HoodieException;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.lance.spark.vectorized.BlobStructAccessor;
import org.lance.spark.vectorized.LanceArrowColumnVector;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Per-row transform that rewrites BLOB columns from Lance's DESCRIPTOR shape into the Hudi BLOB
 * shape. Composed into {@link LanceRecordIterator} for DESCRIPTOR-mode reads.
 *
 * <p>In DESCRIPTOR mode Lance surfaces the blob-stream coordinates of INLINE payloads as a
 * {@code Struct<position, size>} in the BLOB {@code data} child instead of materializing the
 * bytes. For each row this transform:
 * <ul>
 *   <li>If {@code type == OUT_OF_LINE}, copies the existing {@code reference} through.</li>
 *   <li>If {@code type == INLINE}, reads {@code {position, size}} via {@link BlobStructAccessor}
 *       and synthesizes a reference whose {@code external_path} points at the current
 *       {@code .lance} file. The {@code type} is preserved as {@code INLINE} so downstream
 *       consumers see the original storage mode; {@code read_blob()} resolves both cases.</li>
 * </ul>
 */
final class BlobDescriptorTransform {

  private static final UTF8String OUT_OF_LINE_UTF8 =
      UTF8String.fromString(HoodieSchema.Blob.OUT_OF_LINE);
  private static final UTF8String INLINE_UTF8 =
      UTF8String.fromString(HoodieSchema.Blob.INLINE);

  /** Cached child vectors and accessor for a single BLOB column. */
  private static final class BlobColInfo {
    final ColumnVector typeVec;
    final ColumnVector dataVec;
    final ColumnVector refVec;
    final BlobStructAccessor blobStructAccessor;

    BlobColInfo(ColumnVector parent) {
      // HoodieSchema.Blob child ordering: {type, data, reference}.
      this.typeVec = parent.getChild(0);
      this.dataVec = parent.getChild(1);
      this.refVec = parent.getChild(2);
      this.blobStructAccessor = ((LanceArrowColumnVector) this.dataVec).getBlobStructAccessor();
    }
  }

  private final Set<String> blobFieldNames;
  private final UTF8String lanceFilePathUtf8;
  private final String lanceFilePath;

  private StructField[] outputFields;
  /** Blob column metadata keyed by column index; only blob columns have entries. */
  private Map<Integer, BlobColInfo> blobColInfoMap;

  BlobDescriptorTransform(Set<String> blobFieldNames, String lanceFilePath) {
    this.blobFieldNames = blobFieldNames;
    this.lanceFilePathUtf8 = UTF8String.fromString(lanceFilePath);
    this.lanceFilePath = lanceFilePath;
  }

  /**
   * Called once on the first batch to capture per-column blob metadata from the column vectors.
   */
  void init(ColumnVector[] columnVectors, StructType sparkSchema) {
    this.outputFields = sparkSchema.fields();
    this.blobColInfoMap = new HashMap<>();
    for (int i = 0; i < outputFields.length; i++) {
      if (blobFieldNames.contains(outputFields[i].name())) {
        blobColInfoMap.put(i, new BlobColInfo(columnVectors[i]));
      }
    }
  }

  /**
   * Transform a single row, rewriting BLOB columns from Lance DESCRIPTOR shape into Hudi shape.
   */
  UnsafeRow transformRow(ColumnarBatch batch, int rowId,
                         ColumnVector[] columnVectors, UnsafeProjection projection) {
    InternalRow batchRow = batch.getRow(rowId);
    Object[] rowBuffer = new Object[outputFields.length];
    for (int i = 0; i < outputFields.length; i++) {
      BlobColInfo col = blobColInfoMap.get(i);
      if (col != null) {
        rowBuffer[i] = batchRow.isNullAt(i) ? null : buildBlobOutputRow(col, rowId);
      } else {
        rowBuffer[i] = batchRow.isNullAt(i) ? null : batchRow.get(i, outputFields[i].dataType());
      }
    }
    return projection.apply(new GenericInternalRow(rowBuffer)).copy();
  }

  /**
   * Build one BLOB row. OUT_OF_LINE source rows pass the existing {@code reference} through;
   * INLINE source rows get a synthesized reference pointing at the current {@code .lance} file
   * with {@code {position, size}} from the descriptor. The {@code type} field is preserved.
   */
  private InternalRow buildBlobOutputRow(BlobColInfo col, int rowId) {
    // Caller already checked parent struct is non-null via batchRow.isNullAt(i).
    if (col.typeVec.isNullAt(rowId)) {
      throw new HoodieException("Malformed Lance BLOB row at rowId=" + rowId
          + " (file: " + lanceFilePath + "): payload struct is non-null but type is null");
    }
    UTF8String type = col.typeVec.getUTF8String(rowId);

    if (type.equals(OUT_OF_LINE_UTF8)) {
      InternalRow refRow = col.refVec.isNullAt(rowId) ? null : new ColumnarRow(col.refVec, rowId);
      return new GenericInternalRow(new Object[] { OUT_OF_LINE_UTF8, null, refRow });
    }

    if (!type.equals(INLINE_UTF8)) {
      throw new HoodieException("Unexpected BLOB type '" + type + "' at rowId=" + rowId
          + " (file: " + lanceFilePath + "); expected INLINE or OUT_OF_LINE");
    }

    // INLINE source row — rewrite into a reference pointing at the .lance blob stream.
    // Defensively handle null data (e.g. a row with type=INLINE but null payload).
    if (col.dataVec.isNullAt(rowId)) {
      return new GenericInternalRow(new Object[] { INLINE_UTF8, null, null });
    }

    long position = col.blobStructAccessor.getPosition(rowId);
    long size = col.blobStructAccessor.getSize(rowId);
    InternalRow refRow = new GenericInternalRow(
        new Object[] { lanceFilePathUtf8, position, size, Boolean.TRUE });
    return new GenericInternalRow(new Object[] { INLINE_UTF8, null, refRow });
  }
}
