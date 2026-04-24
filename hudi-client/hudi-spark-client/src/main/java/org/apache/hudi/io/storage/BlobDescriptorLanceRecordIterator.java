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
import org.apache.hudi.exception.HoodieIOException;

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
 *   <li>If {@code type == OUT_OF_LINE} (source row was written with an external reference),
 *       copy the existing {@code reference} struct through unchanged.</li>
 *   <li>Otherwise (source row was INLINE), read {@code {position, size}} via
 *       {@link BlobStructAccessor} and synthesize an OUT_OF_LINE reference whose
 *       {@code external_path} points at the current {@code .lance} file. {@code read_blob()}
 *       then resolves the bytes via a regular pread on that file, same as any other
 *       OUT_OF_LINE BLOB.</li>
 * </ul>
 *
 * <p>The {@code outputSchema} passed in is the Hudi BLOB shape ({@code data: Binary},
 * {@code reference: Struct<...>}), not the Lance descriptor shape — Lance's per-row
 * {@code Struct<position, size>} is contained entirely inside this iterator.
 */
public class BlobDescriptorLanceRecordIterator implements ClosableIterator<UnsafeRow> {

  // HoodieSchema.Blob struct children ordering: {type, data, reference}.
  private static final int BLOB_TYPE_IDX = 0;
  private static final int BLOB_DATA_IDX = 1;
  private static final int BLOB_REF_IDX = 2;
  // EXTERNAL_REFERENCE has 4 children: external_path, offset, length, managed.
  private static final int REF_CHILD_COUNT = 4;

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
  // Per-batch cached child vectors for blob columns so per-row access is an array lookup
  // instead of three getChild() calls.
  private ColumnVector[] blobTypeVecs;
  private ColumnVector[] blobDataVecs;
  private ColumnVector[] blobRefVecs;
  // Reusable row buffer plus one reusable reference buffer per blob column. Safe to reuse
  // because projection.apply(...).copy() detaches the UnsafeRow before next() is called again.
  private Object[] rowBuffer;
  private Object[][] refBuffers;
  private int rowIdInBatch;
  private int batchRowCount;
  private boolean closed = false;

  /**
   * @param allocator       Arrow buffer allocator for memory management
   * @param lanceReader     Lance file reader
   * @param arrowReader     Arrow reader already opened in DESCRIPTOR mode
   * @param outputSchema    Hudi-shape Spark schema for the emitted rows (BLOB {@code data} is
   *                        {@code Binary}, not the Lance descriptor struct)
   * @param lanceFilePath   Lance file path; used for error messages and as the
   *                        {@code external_path} of OUT_OF_LINE references synthesized from
   *                        INLINE source rows
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
      // Skip empty batches: loadNextBatch() can legitimately return a zero-row batch (e.g.
      // after filter pushdown); terminating on the first empty batch would silently drop
      // subsequent non-empty batches.
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
    for (int i = 0; i < fields.length; i++) {
      if (blobFieldNames.contains(fields[i].name())) {
        rowBuffer[i] = buildBlobOutputRow(i, rowId);
      } else {
        rowBuffer[i] = batchRow.isNullAt(i) ? null : batchRow.get(i, fields[i].dataType());
      }
    }
    return projection.apply(new GenericInternalRow(rowBuffer)).copy();
  }

  /**
   * Build one BLOB row in Hudi OUT_OF_LINE shape.
   * <ul>
   *   <li>Source row was OUT_OF_LINE: copy the existing {@code reference} through unchanged.</li>
   *   <li>Source row was INLINE: read {@code {position, size}} from the descriptor struct and
   *       synthesize a reference pointing at the current {@code .lance} file.</li>
   * </ul>
   */
  private InternalRow buildBlobOutputRow(int columnIdx, int rowId) {
    if (columnVectors[columnIdx].isNullAt(rowId)) {
      return null;
    }

    ColumnVector typeVec = blobTypeVecs[columnIdx];
    if (typeVec.isNullAt(rowId)) {
      throw new HoodieException("Malformed Lance BLOB row at rowId=" + rowId
          + " (file: " + lanceFilePath + "): payload struct is non-null but type is null");
    }
    UTF8String type = typeVec.getUTF8String(rowId);

    if (type.equals(outOfLineUtf8)) {
      // Source row was OUT_OF_LINE — reference was populated on write, pass it through.
      ColumnVector refVec = blobRefVecs[columnIdx];
      InternalRow refRow = refVec.isNullAt(rowId) ? null : new ColumnarRow(refVec, rowId);
      return new GenericInternalRow(new Object[] { outOfLineUtf8, null, refRow });
    }

    // Source row was INLINE — Lance returned the {position, size} descriptor. Synthesize an
    // OUT_OF_LINE reference whose external_path is the Lance file itself, so read_blob()
    // can pread the bytes out of the blob stream.
    long position = 0L;
    long size = 0L;
    ColumnVector dataVec = blobDataVecs[columnIdx];
    if (dataVec instanceof LanceArrowColumnVector) {
      BlobStructAccessor bsa = ((LanceArrowColumnVector) dataVec).getBlobStructAccessor();
      if (bsa != null && !bsa.isNullAt(rowId)) {
        Long p = bsa.getPosition(rowId);
        Long s = bsa.getSize(rowId);
        if (p != null) {
          position = p;
        }
        if (s != null) {
          size = s;
        }
      }
    }
    Object[] refBuf = refBuffers[columnIdx];
    refBuf[0] = lanceFilePathUtf8;
    refBuf[1] = position;
    refBuf[2] = size;
    refBuf[3] = Boolean.TRUE;
    return new GenericInternalRow(new Object[] { outOfLineUtf8, null, new GenericInternalRow(refBuf) });
  }

  /**
   * Build a {@link ColumnVector} for each top-level column in {@code outputSchema}, looking up
   * Arrow vectors by name. Also cache the per-column blob child vectors so per-row access is an
   * array lookup instead of a {@code getChild} walk.
   */
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
    blobTypeVecs = new ColumnVector[sparkFields.length];
    blobDataVecs = new ColumnVector[sparkFields.length];
    blobRefVecs = new ColumnVector[sparkFields.length];
    refBuffers = new Object[sparkFields.length][];
    rowBuffer = new Object[sparkFields.length];
    for (int i = 0; i < sparkFields.length; i++) {
      String name = sparkFields[i].name();
      FieldVector fv = byName.get(name);
      if (fv == null) {
        throw new HoodieException("Lance batch missing expected column '" + name
            + "' for file: " + lanceFilePath + "; available columns: " + byName.keySet());
      }
      columnVectors[i] = new LanceArrowColumnVector(fv);
      if (blobFieldNames.contains(name)) {
        blobTypeVecs[i] = columnVectors[i].getChild(BLOB_TYPE_IDX);
        blobDataVecs[i] = columnVectors[i].getChild(BLOB_DATA_IDX);
        blobRefVecs[i] = columnVectors[i].getChild(BLOB_REF_IDX);
        refBuffers[i] = new Object[REF_CHILD_COUNT];
      }
    }
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;

    IOException arrowException = null;
    Exception lanceException = null;

    if (currentBatch != null) {
      currentBatch.close();
      currentBatch = null;
    }

    if (arrowReader != null) {
      try {
        arrowReader.close();
      } catch (IOException e) {
        arrowException = e;
      }
    }

    if (lanceReader != null) {
      try {
        lanceReader.close();
      } catch (Exception e) {
        lanceException = e;
      }
    }

    if (allocator != null) {
      allocator.close();
    }

    if (arrowException != null) {
      throw new HoodieIOException("Failed to close Arrow reader", arrowException);
    }
    if (lanceException != null) {
      throw new HoodieException("Failed to close Lance reader", lanceException);
    }
  }
}
