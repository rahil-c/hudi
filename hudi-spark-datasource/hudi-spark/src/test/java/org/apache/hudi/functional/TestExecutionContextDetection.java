/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.functional;

import org.apache.hudi.common.util.ExecutionContext;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test to verify ExecutionContext detection logic works correctly
 * in different Spark execution modes.
 */
public class TestExecutionContextDetection {

  private SparkSession spark;
  private JavaSparkContext jsc;

  @BeforeEach
  public void setUp() {
    SparkConf conf = new SparkConf()
        .setAppName("ExecutionContextDetectionTest")
        .setMaster("local[4]") // Use local mode with 4 threads
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    
    spark = SparkSession.builder().config(conf).getOrCreate();
    jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testExecutionContextDetectionOnDriver() {
    // Test 1: On driver thread, should detect as driver
    System.out.println("=== Driver Context Test ===");
    System.out.println("TaskContext.get(): " + TaskContext.get());
    System.out.println("ExecutionContext.isOnSparkDriver(): " + ExecutionContext.isOnSparkDriver());
    System.out.println("ExecutionContext.isOnSparkExecutor(): " + ExecutionContext.isOnSparkExecutor());
    System.out.println("Thread: " + Thread.currentThread().getName());
    
    // On driver, TaskContext.get() should be null
    assertFalse(ExecutionContext.isOnSparkExecutor(), "Should not detect executor on driver");
    assertTrue(ExecutionContext.isOnSparkDriver(), "Should detect driver");
  }

  @Test
  public void testExecutionContextDetectionInTasks() {
    // Test 2: Inside Spark tasks, should detect as executor context
    System.out.println("=== Task Context Test ===");
    
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
    JavaRDD<Integer> rdd = jsc.parallelize(data, 4); // 4 partitions
    
    // Collect results from each task
    List<String> results = rdd.map(x -> {
      // This code runs inside Spark tasks
      TaskContext taskContext = TaskContext.get();
      boolean isExecutor = ExecutionContext.isOnSparkExecutor();
      boolean isDriver = ExecutionContext.isOnSparkDriver();
      String threadName = Thread.currentThread().getName();
      
      String result = String.format("Value=%d, TaskContext=%s, isExecutor=%b, isDriver=%b, Thread=%s, TaskId=%s", 
                                   x, 
                                   taskContext != null ? "non-null" : "null",
                                   isExecutor,
                                   isDriver,
                                   threadName,
                                   taskContext != null ? taskContext.taskAttemptId() : "none");
      
      System.out.println("TASK: " + result);
      return result;
    }).collect();
    
    // Analyze results
    System.out.println("\n=== Analysis ===");
    for (String result : results) {
      System.out.println(result);
      
      // In local mode with multiple threads, TaskContext should be non-null inside tasks
      if (result.contains("TaskContext=non-null")) {
        assertTrue(result.contains("isExecutor=true"), 
                  "Task with non-null TaskContext should be detected as executor");
        assertFalse(result.contains("isDriver=true"), 
                   "Task with non-null TaskContext should not be detected as driver");
      }
    }
    
    // At least some tasks should have been detected as executor context
    boolean anyExecutorDetected = results.stream()
        .anyMatch(r -> r.contains("isExecutor=true"));
    
    assertTrue(anyExecutorDetected, 
              "At least some tasks should be detected as executor context. Results: " + results);
  }

  @Test 
  public void testTaskContextBehaviorDirectly() {
    // Test 3: Direct TaskContext behavior test
    System.out.println("=== Direct TaskContext Test ===");
    
    List<Integer> data = Arrays.asList(1, 2, 3, 4);
    JavaRDD<Integer> rdd = jsc.parallelize(data, 2);
    
    List<Boolean> taskContextResults = rdd.map(x -> {
      TaskContext tc = TaskContext.get();
      boolean hasTaskContext = tc != null;
      
      if (hasTaskContext) {
        System.out.println(String.format("Task %d: TaskContext.taskAttemptId()=%d, partitionId()=%d, stageId()=%d", 
                                        x, tc.taskAttemptId(), tc.partitionId(), tc.stageId()));
      } else {
        System.out.println(String.format("Task %d: TaskContext is null", x));
      }
      
      return hasTaskContext;
    }).collect();
    
    System.out.println("TaskContext results: " + taskContextResults);
    
    // In any Spark execution mode, TaskContext.get() should be non-null inside tasks
    assertTrue(taskContextResults.stream().anyMatch(b -> b), 
              "At least some tasks should have non-null TaskContext");
  }
}