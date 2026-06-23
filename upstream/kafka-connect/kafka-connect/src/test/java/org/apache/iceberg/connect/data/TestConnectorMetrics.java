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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class TestConnectorMetrics {

  @Test
  public void testNoopInstance() {
    ConnectorMetrics metrics = ConnectorMetrics.NOOP;
    assertThat(metrics.isEnabled()).isFalse();

    metrics.recordsReceived(10);
    metrics.recordWritten("db.table");
    metrics.recordDropped();
    metrics.recordConversionError("db.table");
    metrics.commitStarted();
    metrics.commitSucceeded(100L);
    metrics.commitFailed(50L);
    metrics.filesWritten("db.table", 3, 1);
    metrics.flushCompleted("db.table", 200L);
    metrics.schemaEvolved("db.table");
    metrics.tableAutoCreated("db.table");
    metrics.commitTimedOut();
    metrics.close();

    assertThat(metrics.recordsReceivedTotal()).isEqualTo(0);
    assertThat(metrics.recordsWrittenTotal()).isEqualTo(0);
    assertThat(metrics.commitTotal()).isEqualTo(0);
  }

  @Test
  public void testCreateReturnsEnabledInstance() {
    ConnectorMetrics metrics = ConnectorMetrics.create(null);
    assertThat(metrics.isEnabled()).isTrue();
  }

  @Test
  public void testOperationalCounters() {
    ConnectorMetrics metrics = ConnectorMetrics.create(null);

    metrics.recordsReceived(5);
    metrics.recordsReceived(3);
    metrics.recordWritten("db.orders");
    metrics.recordWritten("db.users");
    metrics.recordDropped();
    metrics.recordConversionError("db.orders");

    assertThat(metrics.recordsReceivedTotal()).isEqualTo(8);
    assertThat(metrics.recordsWrittenTotal()).isEqualTo(2);
    assertThat(metrics.recordsDroppedTotal()).isEqualTo(1);
    assertThat(metrics.recordConversionErrorsTotal()).isEqualTo(1);
  }

  @Test
  public void testCommitCounters() {
    ConnectorMetrics metrics = ConnectorMetrics.create(null);

    metrics.commitStarted();
    metrics.commitStarted();
    metrics.commitSucceeded(100L);
    metrics.commitFailed(50L);
    metrics.commitTimedOut();

    assertThat(metrics.commitTotal()).isEqualTo(2);
    assertThat(metrics.commitSuccessTotal()).isEqualTo(1);
    assertThat(metrics.commitFailureTotal()).isEqualTo(1);
    assertThat(metrics.commitTimeoutTotal()).isEqualTo(1);
  }

  @Test
  public void testWritePathCountersAndGauge() {
    ConnectorMetrics metrics = ConnectorMetrics.create(null);

    assertThat(metrics.activeWriters()).isEqualTo(0);
    metrics.registerActiveWriters(() -> 42);
    metrics.filesWritten("db.orders", 3, 1);
    metrics.filesWritten("db.users", 2, 0);

    assertThat(metrics.activeWriters()).isEqualTo(42);
    assertThat(metrics.dataFilesWrittenTotal()).isEqualTo(5);
    assertThat(metrics.deleteFilesWrittenTotal()).isEqualTo(1);
  }

  @Test
  public void testSchemaAndTableCounters() {
    ConnectorMetrics metrics = ConnectorMetrics.create(null);

    metrics.schemaEvolved("db.orders");
    metrics.schemaEvolved("db.orders");
    metrics.tableAutoCreated("db.new_table");

    assertThat(metrics.schemaEvolutionsTotal()).isEqualTo(2);
    assertThat(metrics.tablesAutoCreatedTotal()).isEqualTo(1);
  }
}
