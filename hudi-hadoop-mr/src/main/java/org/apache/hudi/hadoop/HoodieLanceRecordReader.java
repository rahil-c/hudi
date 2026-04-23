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

package org.apache.hudi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * Record reader for Lance base files in Hive.
 * <p>
 * TODO: Lance reading in Hive is not yet supported. The Spark datasource path
 * reads Lance files through {@code HoodieFileGroupReader} via
 * {@code SparkFileFormatInternalRowReaderContext}, which handles both COW and MOR.
 * To support Lance in Hive, a non-Spark Lance file reader needs to be implemented
 * (e.g., in {@code HoodieAvroFileReaderFactory}) and wired into
 * {@code HiveHoodieReaderContext.getFileRecordIterator()}.
 * <p>
 * This class is retained because {@code HoodieLanceInputFormat} is required for
 * catalog/metastore registration during CREATE TABLE operations.
 */
public class HoodieLanceRecordReader implements RecordReader<NullWritable, ArrayWritable> {

  private static final String UNSUPPORTED_MSG =
      "Lance reading through Hive InputFormat is not yet supported. "
          + "Use the Spark datasource path (spark.read.format(\"hudi\")) to read Lance tables.";

  public HoodieLanceRecordReader(Configuration conf, InputSplit split, JobConf job) {
    // no-op: constructor kept for HoodieLanceInputFormat instantiation during catalog registration
  }

  @Override
  public boolean next(NullWritable key, ArrayWritable value) throws IOException {
    throw new UnsupportedOperationException(UNSUPPORTED_MSG);
  }

  @Override
  public NullWritable createKey() {
    return null;
  }

  @Override
  public ArrayWritable createValue() {
    return new ArrayWritable(Writable.class, new Writable[0]);
  }

  @Override
  public long getPos() throws IOException {
    return 0;
  }

  @Override
  public void close() throws IOException {
    // no-op
  }

  @Override
  public float getProgress() throws IOException {
    return 0;
  }
}
