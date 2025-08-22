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

package org.apache.hudi.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to detect execution context (driver vs executor) in distributed environments.
 * This helps identify performance issues where expensive operations like MetaClient creation
 * happen on executor nodes instead of driver nodes.
 */
public class ExecutionContext {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionContext.class);
  
  private static final String SPARK_EXECUTOR_ID_PROPERTY = "spark.executor.id";

  // System property to enable/disable validation (for testing)
  private static final String VALIDATION_ENABLED_PROPERTY = "hoodie.metaclient.validation.enabled";
  private static final boolean DEFAULT_VALIDATION_ENABLED = true;
  
  /**
   * Detects if current thread is running on Spark driver.
   * Uses TaskContext.get() via reflection to determine execution context.
   * 
   * @return true if running on Spark driver, false otherwise
   */
  public static boolean isOnSparkDriver() {
    return !isOnSparkExecutor();
  }
  
  /**
   * Detects if current thread is running on Spark executor.
   * Uses TaskContext.get() via reflection to determine execution context.
   * 
   * @return true if running on Spark executor, false otherwise
   */
  public static boolean isOnSparkExecutor() {
    try {
      // Use reflection to check for TaskContext.get() without compile-time dependency
      Class<?> taskContextClass = Class.forName("org.apache.spark.TaskContext");
      java.lang.reflect.Method getMethod = taskContextClass.getMethod("get");
      Object taskContext = getMethod.invoke(null);
      
      // If TaskContext.get() returns non-null, we're on an executor
      boolean isExecutor = taskContext != null;
      LOG.debug("Execution context check - TaskContext.get(): {}, isExecutor: {}", 
                taskContext != null ? "non-null" : "null", isExecutor);
      return isExecutor;
    } catch (ClassNotFoundException e) {
      // Spark not available, assume driver/non-Spark environment
      LOG.debug("Spark TaskContext not found, assuming driver/non-Spark environment");
      return false;
    } catch (Exception e) {
      LOG.debug("Failed to determine execution context using TaskContext, assuming driver", e);
      return false;
    }
  }
  
  /**
   * Gets the current Spark executor ID.
   * 
   * @return executor ID or "unknown" if not available
   */
  public static String getExecutorId() {
    String executorId = System.getProperty(SPARK_EXECUTOR_ID_PROPERTY);
    return executorId != null ? executorId : "unknown";
  }
  
  /**
   * Checks if MetaClient validation is enabled.
   * Can be disabled for testing or specific scenarios.
   * 
   * @return true if validation should be performed
   */
  public static boolean isValidationEnabled() {
    String enabled = System.getProperty(VALIDATION_ENABLED_PROPERTY);
    if (enabled != null) {
      return Boolean.parseBoolean(enabled);
    }
    return DEFAULT_VALIDATION_ENABLED;
  }
  
  /**
   * Gets current execution context as a string for logging.
   * 
   * @return human-readable execution context
   */
  public static String getContextInfo() {
    if (isOnSparkDriver()) {
      return "Spark Driver";
    } else if (isOnSparkExecutor()) {
      return "Spark Executor (ID: " + getExecutorId() + ")";
    } else {
      return "Non-Spark Environment";
    }
  }
}