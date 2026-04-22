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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared iterator implementation for reading Lance files and converting Arrow batches to Spark rows.
 * This iterator is used by both Hudi's internal Lance reader and Spark datasource integration.
 *
 * <p>The iterator manages the lifecycle of:
 * <ul>
 *   <li>BufferAllocator - Arrow memory management</li>
 *   <li>LanceFileReader - Lance file handle</li>
 *   <li>ArrowReader - Arrow batch reader</li>
 *   <li>ColumnarBatch - Current batch being iterated</li>
 * </ul>
 *
 * <p>Records are converted to {@link UnsafeRow} using {@link UnsafeProjection} for efficient
 * serialization and memory management.
 *
 * <p><b>BLOB handling.</b> When {@code blobFieldNames} is non-empty, the iterator assumes Lance
 * was opened in {@code BlobReadMode.DESCRIPTOR} and performs the descriptor → Hudi OUT_OF_LINE
 * transformation in-line, so the {@link UnsafeProjection} only ever sees the final Hudi shape
 * (i.e. {@code data: Binary = null} + {@code reference: {external_path, offset, length, managed}}).
 * Reading the Lance {@code position}/{@code size} descriptor goes through lance-spark's public
 * {@link BlobStructAccessor#getPosition}/{@link BlobStructAccessor#getSize} API — no reflection
 * on Arrow internals is required.
 *
 * <p>{@code outputSchema} must be in the Hudi output shape, not the Lance descriptor shape.
 */
public class LanceRecordIterator implements ClosableIterator<UnsafeRow> {

  private final BufferAllocator allocator;
  private final LanceFileReader lanceReader;
  private final ArrowReader arrowReader;
  private final StructType outputSchema;
  private final UnsafeProjection projection;
  private final String path;
  private final Set<String> blobFieldNames;
  private final UTF8String outOfLineTypeUtf8;
  private final UTF8String lancePathUtf8;

  private ColumnarBatch currentBatch;
  private ColumnVector[] columnVectors;
  private int rowIdInBatch;
  private int batchRowCount;
  private boolean closed = false;

  /**
   * Creates a new Lance record iterator for non-blob reads.
   *
   * @param allocator Arrow buffer allocator for memory management
   * @param lanceReader Lance file reader
   * @param arrowReader Arrow reader for batch reading
   * @param schema Spark output schema for the records
   * @param path Lance file path (used for error messages and, when blobs are present, as the
   *             {@code external_path} of OUT_OF_LINE references synthesized from Lance-INLINE rows)
   */
  public LanceRecordIterator(BufferAllocator allocator,
                             LanceFileReader lanceReader,
                             ArrowReader arrowReader,
                             StructType schema,
                             String path) {
    this(allocator, lanceReader, arrowReader, schema, path, Collections.emptySet());
  }

  /**
   * Creates a new Lance record iterator with blob descriptor awareness.
   *
   * @param allocator Arrow buffer allocator for memory management
   * @param lanceReader Lance file reader
   * @param arrowReader Arrow reader for batch reading
   * @param outputSchema Hudi-shape Spark schema for the output rows (e.g. blob {@code data} is
   *                     {@code Binary}, not {@code Struct<position,size>})
   * @param path Lance file path (used for error messages and as the {@code external_path} of the
   *             OUT_OF_LINE reference when remapping Lance-INLINE descriptor rows)
   * @param blobFieldNames names of top-level {@link HoodieSchema.Blob} columns. The iterator reads
   *                       these columns' underlying descriptor vectors directly and synthesizes
   *                       Hudi OUT_OF_LINE reference rows so the projection never navigates into a
   *                       blob descriptor struct.
   */
  public LanceRecordIterator(BufferAllocator allocator,
                             LanceFileReader lanceReader,
                             ArrowReader arrowReader,
                             StructType outputSchema,
                             String path,
                             Set<String> blobFieldNames) {
    this.allocator = allocator;
    this.lanceReader = lanceReader;
    this.arrowReader = arrowReader;
    this.outputSchema = outputSchema;
    this.projection = UnsafeProjection.create(outputSchema);
    this.path = path;
    this.blobFieldNames = blobFieldNames;
    this.outOfLineTypeUtf8 = UTF8String.fromString(HoodieSchema.Blob.OUT_OF_LINE);
    this.lancePathUtf8 = UTF8String.fromString(path);
  }

  @Override
  public boolean hasNext() {
    if (currentBatch != null && rowIdInBatch < batchRowCount) {
      return true;
    }

    // Close previous batch before loading next
    if (currentBatch != null) {
      currentBatch.close();
      currentBatch = null;
    }

    try {
      if (arrowReader.loadNextBatch()) {
        VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
        if (columnVectors == null) {
          buildColumnVectors(root);
        }
        currentBatch = new ColumnarBatch(columnVectors, root.getRowCount());
        batchRowCount = root.getRowCount();
        rowIdInBatch = 0;
        return batchRowCount > 0;
      }
    } catch (IOException e) {
      throw new HoodieException("Failed to read next batch from Lance file: " + path, e);
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
    InternalRow outputRow = blobFieldNames.isEmpty()
        ? batchRow
        : buildOutputRow(batchRow, rowId);
    return projection.apply(outputRow).copy();
  }

  /**
   * Build a {@link ColumnVector} for each top-level column in {@code outputSchema}, looking up
   * Arrow vectors by name. Lance-spark 0.4.0's {@link VectorSchemaRoot} may return fields in the
   * file's on-disk order, which would misalign the {@link UnsafeProjection}; doing a name-keyed
   * lookup protects against that.
   *
   * <p>Cached on the first batch and reused thereafter (Arrow reuses buffers across batches).
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
          + " for file: " + path);
    }
    columnVectors = new ColumnVector[sparkFields.length];
    for (int i = 0; i < sparkFields.length; i++) {
      String name = sparkFields[i].name();
      FieldVector fv = byName.get(name);
      if (fv == null) {
        throw new HoodieException("Lance batch missing expected column '" + name
            + "' for file: " + path + "; available columns: " + byName.keySet());
      }
      columnVectors[i] = new LanceArrowColumnVector(fv);
    }
  }

  /**
   * For rows with blob columns: for each output column, either copy the value from the batch row
   * (non-blob) or synthesize a Hudi OUT_OF_LINE row (blob). Returned row matches {@code outputSchema}.
   */
  private InternalRow buildOutputRow(InternalRow batchRow, int rowId) {
    StructField[] fields = outputSchema.fields();
    Object[] out = new Object[fields.length];
    for (int i = 0; i < fields.length; i++) {
      if (blobFieldNames.contains(fields[i].name())) {
        out[i] = buildBlobOutputRow(columnVectors[i], rowId);
      } else {
        out[i] = batchRow.isNullAt(i) ? null : batchRow.get(i, fields[i].dataType());
      }
    }
    return new GenericInternalRow(out);
  }

  /**
   * Read the Lance-returned payload struct for {@code rowId} and synthesize a Hudi BLOB row.
   * <ul>
   *   <li>If the source row has {@code type = OUT_OF_LINE}, copy the existing {@code reference}
   *       sub-struct (path/offset/length/managed) and emit {@code data = null}.</li>
   *   <li>Otherwise (Lance stored the payload INLINE in its blob stream), read
   *       {@code position}/{@code size} via {@link LanceArrowColumnVector#getBlobStructAccessor()}
   *       and synthesize an OUT_OF_LINE reference pointing at the current {@code .lance} file
   *       so {@code BatchedBlobReader} can pread the bytes later.</li>
   * </ul>
   */
  private InternalRow buildBlobOutputRow(ColumnVector payloadCol, int rowId) {
    if (payloadCol.isNullAt(rowId)) {
      return null;
    }

    // HoodieSchema.Blob defines the struct as {type, data, reference} in that order;
    // both the Lance descriptor shape and the Hudi output shape share these child names
    // and ordering, so index-by-name lookup against the *output* schema would just return
    // 0/1/2 here. Use the constants directly for clarity.
    final int typeIdx = 0;
    final int dataIdx = 1;
    final int refIdx = 2;

    ColumnVector typeVec = payloadCol.getChild(typeIdx);
    UTF8String type = typeVec.isNullAt(rowId) ? null : typeVec.getUTF8String(rowId);

    if (type != null && type.equals(outOfLineTypeUtf8)) {
      // OUT_OF_LINE source row: reference is already populated by Lance; copy it through.
      ColumnVector refVec = payloadCol.getChild(refIdx);
      InternalRow refRow = refVec.isNullAt(rowId) ? null : new ColumnarRow(refVec, rowId);
      return new GenericInternalRow(new Object[] {type, null, refRow});
    }

    // INLINE source row: Lance stored the bytes in its blob stream and returns a
    // {position, size} descriptor. Remap to a Hudi OUT_OF_LINE reference that points
    // at the Lance file itself.
    long position = 0L;
    long size = 0L;
    ColumnVector dataVec = payloadCol.getChild(dataIdx);
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
    InternalRow refRow = new GenericInternalRow(new Object[] {
        lancePathUtf8,
        position,
        size,
        Boolean.TRUE
    });
    return new GenericInternalRow(new Object[] {outOfLineTypeUtf8, null, refRow});
  }

  @Override
  public void close() {
    // Make close() idempotent - safe to call multiple times
    if (closed) {
      return;
    }
    closed = true;

    IOException arrowException = null;
    Exception lanceException = null;

    // Close current batch if exists
    if (currentBatch != null) {
      currentBatch.close();
      currentBatch = null;
    }

    // Close Arrow reader
    if (arrowReader != null) {
      try {
        arrowReader.close();
      } catch (IOException e) {
        arrowException = e;
      }
    }

    // Close Lance reader
    if (lanceReader != null) {
      try {
        lanceReader.close();
      } catch (Exception e) {
        lanceException = e;
      }
    }

    // Always close allocator
    if (allocator != null) {
      allocator.close();
    }

    // Throw any exceptions that occurred
    if (arrowException != null) {
      throw new HoodieIOException("Failed to close Arrow reader", arrowException);
    }
    if (lanceException != null) {
      throw new HoodieException("Failed to close Lance reader", lanceException);
    }
  }
}
