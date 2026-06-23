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
package org.apache.iceberg.connect.data;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectorMetrics implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectorMetrics.class);

  public static final ConnectorMetrics NOOP = new ConnectorMetrics(false);

  private final AtomicLong recordsReceivedTotal = new AtomicLong();
  private final AtomicLong recordsWrittenTotal = new AtomicLong();
  private final AtomicLong recordsDroppedTotal = new AtomicLong();
  private final AtomicLong recordConversionErrorsTotal = new AtomicLong();
  private final AtomicLong commitTotal = new AtomicLong();
  private final AtomicLong commitSuccessTotal = new AtomicLong();
  private final AtomicLong commitFailureTotal = new AtomicLong();
  private final AtomicLong dataFilesWrittenTotal = new AtomicLong();
  private final AtomicLong deleteFilesWrittenTotal = new AtomicLong();
  private final AtomicLong schemaEvolutionsTotal = new AtomicLong();
  private final AtomicLong tablesAutoCreatedTotal = new AtomicLong();
  private final AtomicLong commitTimeoutTotal = new AtomicLong();
  private volatile Supplier<Integer> activeWritersSupplier;
  private final boolean enabled;

  private ConnectorMetrics(boolean enabled) {
    this.enabled = enabled;
    if (enabled) {
      LOG.info("Connector metrics enabled");
    }
  }

  /**
   * Creates connector metrics. Kafka 3.x does not expose PluginMetrics to sink tasks, so this keeps
   * counter-only metrics until the runtime can wire counters to the Kafka Connect metrics backend.
   */
  public static ConnectorMetrics create(Object sinkTaskContext) {
    return new ConnectorMetrics(true);
  }

  public void recordsReceived(int count) {
    if (enabled) {
      recordsReceivedTotal.addAndGet(count);
    }
  }

  public void recordWritten(String tableName) {
    if (enabled) {
      recordsWrittenTotal.incrementAndGet();
    }
  }

  public void recordDropped() {
    if (enabled) {
      recordsDroppedTotal.incrementAndGet();
    }
  }

  public void recordConversionError(String tableName) {
    if (enabled) {
      recordConversionErrorsTotal.incrementAndGet();
    }
  }

  public void commitStarted() {
    if (enabled) {
      commitTotal.incrementAndGet();
    }
  }

  public void commitSucceeded(long durationMs) {
    if (enabled) {
      commitSuccessTotal.incrementAndGet();
    }
  }

  public void commitFailed(long durationMs) {
    if (enabled) {
      commitFailureTotal.incrementAndGet();
    }
  }

  public void filesWritten(String tableName, int dataFiles, int deleteFiles) {
    if (enabled) {
      dataFilesWrittenTotal.addAndGet(dataFiles);
      deleteFilesWrittenTotal.addAndGet(deleteFiles);
    }
  }

  public void flushCompleted(String tableName, long durationMs) {
    // Duration tracking can be wired to runtime metrics when Kafka Connect exposes PluginMetrics.
  }

  public void registerActiveWriters(Supplier<Integer> supplier) {
    if (enabled) {
      this.activeWritersSupplier = supplier;
    }
  }

  public void schemaEvolved(String tableName) {
    if (enabled) {
      schemaEvolutionsTotal.incrementAndGet();
    }
  }

  public void tableAutoCreated(String tableName) {
    if (enabled) {
      tablesAutoCreatedTotal.incrementAndGet();
    }
  }

  public void commitTimedOut() {
    if (enabled) {
      commitTimeoutTotal.incrementAndGet();
    }
  }

  public long recordsReceivedTotal() {
    return recordsReceivedTotal.get();
  }

  public long recordsWrittenTotal() {
    return recordsWrittenTotal.get();
  }

  public long recordsDroppedTotal() {
    return recordsDroppedTotal.get();
  }

  public long recordConversionErrorsTotal() {
    return recordConversionErrorsTotal.get();
  }

  public long commitTotal() {
    return commitTotal.get();
  }

  public long commitSuccessTotal() {
    return commitSuccessTotal.get();
  }

  public long commitFailureTotal() {
    return commitFailureTotal.get();
  }

  public long dataFilesWrittenTotal() {
    return dataFilesWrittenTotal.get();
  }

  public long deleteFilesWrittenTotal() {
    return deleteFilesWrittenTotal.get();
  }

  public int activeWriters() {
    Supplier<Integer> supplier = activeWritersSupplier;
    return supplier != null ? supplier.get() : 0;
  }

  public long schemaEvolutionsTotal() {
    return schemaEvolutionsTotal.get();
  }

  public long tablesAutoCreatedTotal() {
    return tablesAutoCreatedTotal.get();
  }

  public long commitTimeoutTotal() {
    return commitTimeoutTotal.get();
  }

  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void close() {
    // Counter-only metrics do not allocate external resources.
  }
}
