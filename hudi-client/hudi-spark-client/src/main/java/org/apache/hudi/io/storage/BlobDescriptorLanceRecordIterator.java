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
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
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
import org.lance.file.LanceFileReader;
import org.lance.spark.vectorized.BlobStructAccessor;
import org.lance.spark.vectorized.LanceArrowColumnVector;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Iterator for Lance files opened in {@link org.lance.file.BlobReadMode#DESCRIPTOR} that rewrites
 * every BLOB column into Hudi's OUT_OF_LINE shape before surfacing rows to Spark.
 *
 * <p>In DESCRIPTOR mode Lance surfaces the blob-stream coordinates of INLINE payloads as a
 * {@code Struct<position, size>} in the BLOB {@code data} child instead of materializing the
 * bytes. For each row we:
 * <ul>
 *   <li>Read the BLOB row's {@code type}.</li>
 *   <li>If {@code type == OUT_OF_LINE}, copy the existing {@code reference} struct through.</li>
 *   <li>Otherwise (INLINE), read {@code {position, size}} via {@link BlobStructAccessor} and
 *       synthesize an OUT_OF_LINE reference whose {@code external_path} points at the current
 *       {@code .lance} file. {@code read_blob()} then resolves the bytes via a regular pread
 *       on that file.</li>
 * </ul>
 *
 * <p>{@code outputSchema} is the Hudi BLOB shape ({@code data: Binary},
 * {@code reference: Struct<...>}), not Lance's descriptor shape.
 */
public class BlobDescriptorLanceRecordIterator implements ClosableIterator<UnsafeRow> {

  /** Cached child vectors of a BLOB column; only populated for blob indices. */
  private static final class BlobColInfo {
    final ColumnVector typeVec;
    final ColumnVector dataVec;
    final ColumnVector refVec;

    BlobColInfo(ColumnVector parent) {
      // HoodieSchema.Blob child ordering: {type, data, reference}.
      this.typeVec = parent.getChild(0);
      this.dataVec = parent.getChild(1);
      this.refVec = parent.getChild(2);
    }
  }

  private final BufferAllocator allocator;
  private final LanceFileReader lanceReader;
  private final ArrowReader arrowReader;
  private final StructType outputSchema;
  private final UnsafeProjection projection;
  private final String lanceFilePath;
  private final Set<String> blobFieldNames;
  private final UTF8String outOfLineUtf8;
  private final UTF8String lanceFilePathUtf8;

  private ColumnarBatch currentBatch;
  private ColumnVector[] columnVectors;
  /** {@code blobCols[i]} is non-null iff output field {@code i} is a BLOB column. */
  private BlobColInfo[] blobCols;
  private int rowIdInBatch;
  private int batchRowCount;
  private boolean closed = false;

  /**
   * @param allocator       Arrow buffer allocator for memory management
   * @param lanceReader     Lance file reader
   * @param arrowReader     Arrow reader already opened in DESCRIPTOR mode
   * @param outputSchema    Hudi-shape Spark schema for the emitted rows
   * @param lanceFilePath   Lance file path; used for error messages and as the
   *                        {@code external_path} of synthesized OUT_OF_LINE references
   * @param blobFieldNames  top-level BLOB column names whose per-row values should be rewritten
   */
  public BlobDescriptorLanceRecordIterator(BufferAllocator allocator,
                                            LanceFileReader lanceReader,
                                            ArrowReader arrowReader,
                                            StructType outputSchema,
                                            String lanceFilePath,
                                            Set<String> blobFieldNames) {
    this.allocator = allocator;
    this.lanceReader = lanceReader;
    this.arrowReader = arrowReader;
    this.outputSchema = outputSchema;
    this.projection = UnsafeProjection.create(outputSchema);
    this.lanceFilePath = lanceFilePath;
    this.blobFieldNames = blobFieldNames;
    this.outOfLineUtf8 = UTF8String.fromString(HoodieSchema.Blob.OUT_OF_LINE);
    this.lanceFilePathUtf8 = UTF8String.fromString(lanceFilePath);
  }

  @Override
  public boolean hasNext() {
    if (currentBatch != null && rowIdInBatch < batchRowCount) {
      return true;
    }

    if (currentBatch != null) {
      currentBatch.close();
      currentBatch = null;
    }

    try {
      // Skip empty batches: loadNextBatch() can legitimately return zero rows (e.g. after
      // filter pushdown); terminating on the first empty batch would silently drop subsequent
      // non-empty ones.
      while (arrowReader.loadNextBatch()) {
        VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
        if (columnVectors == null) {
          buildColumnVectors(root);
        }
        currentBatch = new ColumnarBatch(columnVectors, root.getRowCount());
        batchRowCount = root.getRowCount();
        rowIdInBatch = 0;
        if (batchRowCount > 0) {
          return true;
        }
        currentBatch.close();
        currentBatch = null;
      }
    } catch (IOException e) {
      throw new HoodieException("Failed to read next batch from Lance file: " + lanceFilePath, e);
    }

    return false;
  }

  @Override
  public UnsafeRow next() {
    if (!hasNext()) {
      throw new IllegalStateException("No more records available");
    }
    int rowId = rowIdInBatch++;
    InternalRow batchRow = currentBatch.getRow(rowId);
    StructField[] fields = outputSchema.fields();
    Object[] rowBuffer = new Object[fields.length];
    for (int i = 0; i < fields.length; i++) {
      if (blobCols[i] != null) {
        rowBuffer[i] = buildBlobOutputRow(i, rowId);
      } else {
        rowBuffer[i] = batchRow.isNullAt(i) ? null : batchRow.get(i, fields[i].dataType());
      }
    }
    return projection.apply(new GenericInternalRow(rowBuffer)).copy();
  }

  /**
   * Build one BLOB row in Hudi OUT_OF_LINE shape. OUT_OF_LINE source rows pass the existing
   * {@code reference} through; INLINE source rows get a synthesized reference pointing at the
   * current {@code .lance} file with {@code {position, size}} from the descriptor.
   */
  private InternalRow buildBlobOutputRow(int columnIdx, int rowId) {
    if (columnVectors[columnIdx].isNullAt(rowId)) {
      return null;
    }

    BlobColInfo col = blobCols[columnIdx];
    if (col.typeVec.isNullAt(rowId)) {
      throw new HoodieException("Malformed Lance BLOB row at rowId=" + rowId
          + " (file: " + lanceFilePath + "): payload struct is non-null but type is null");
    }
    UTF8String type = col.typeVec.getUTF8String(rowId);

    if (type.equals(outOfLineUtf8)) {
      InternalRow refRow = col.refVec.isNullAt(rowId) ? null : new ColumnarRow(col.refVec, rowId);
      return new GenericInternalRow(new Object[] { outOfLineUtf8, null, refRow });
    }

    // INLINE source row — rewrite Lance's descriptor into a Hudi OUT_OF_LINE reference.
    BlobStructAccessor bsa = ((LanceArrowColumnVector) col.dataVec).getBlobStructAccessor();
    long position = bsa.getPosition(rowId);
    long size = bsa.getSize(rowId);
    InternalRow refRow = new GenericInternalRow(
        new Object[] { lanceFilePathUtf8, position, size, Boolean.TRUE });
    return new GenericInternalRow(new Object[] { outOfLineUtf8, null, refRow });
  }

  private void buildColumnVectors(VectorSchemaRoot root) {
    List<FieldVector> fieldVectors = root.getFieldVectors();
    Map<String, FieldVector> byName = new HashMap<>(fieldVectors.size() * 2);
    for (FieldVector fv : fieldVectors) {
      byName.put(fv.getName(), fv);
    }
    StructField[] sparkFields = outputSchema.fields();
    if (sparkFields.length != fieldVectors.size()) {
      throw new HoodieException("Lance batch column count " + fieldVectors.size()
          + " does not match expected Spark schema size " + sparkFields.length
          + " for file: " + lanceFilePath);
    }
    columnVectors = new ColumnVector[sparkFields.length];
    blobCols = new BlobColInfo[sparkFields.length];
    for (int i = 0; i < sparkFields.length; i++) {
      String name = sparkFields[i].name();
      FieldVector fv = byName.get(name);
      if (fv == null) {
        throw new HoodieException("Lance batch missing expected column '" + name
            + "' for file: " + lanceFilePath + "; available columns: " + byName.keySet());
      }
      columnVectors[i] = new LanceArrowColumnVector(fv);
      if (blobFieldNames.contains(name)) {
        blobCols[i] = new BlobColInfo(columnVectors[i]);
      }
    }
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    ColumnarBatch batch = currentBatch;
    currentBatch = null;
    LanceCloseables.closeAll(batch, arrowReader, lanceReader, allocator);
  }
}
