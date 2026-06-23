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

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkWriter {

  private static final Logger LOG = LoggerFactory.getLogger(SinkWriter.class);

  private final IcebergSinkConfig config;
  private final Map<TopicPartition, Offset> sourceOffsets;
  private final RecordRouter router;
  private ErrantRecordReporter reporter;

  public SinkWriter(Catalog catalog, IcebergSinkConfig config) {
    this.config = config;
    this.sourceOffsets = Maps.newHashMap();
    if (config.dynamicTablesEnabled()) {
      router = new RecordRouter.DynamicRecordRouter(catalog, config);
    } else if (config.tablesRouteWith() == null && config.tablesRouteField() != null) {
      router = new RecordRouter.StaticRecordRouter(catalog, config);
    } else if (config.tablesRouteWith() != null) {
      try {
        router =
            config
                .tablesRouteWith()
                .getDeclaredConstructor(Catalog.class, IcebergSinkConfig.class)
                .newInstance(catalog, config);
      } catch (NoSuchMethodException
          | InstantiationException
          | IllegalAccessException
          | InvocationTargetException e) {
        throw new IllegalArgumentException(
            "Cannot create router from iceberg.tables.route-with", e);
      }
    } else {
      router = new RecordRouter.AllTablesRecordRouter(catalog, config);
    }
  }

  public void setReporter(ErrantRecordReporter reporter) {
    this.reporter = reporter;
  }

  public void close() {
    router.close();
    sourceOffsets.clear();
  }

  public SinkWriterResult completeWrite() {
    List<IcebergWriterResult> writerResults = router.completeWrite();
    Map<TopicPartition, Offset> offsets = Maps.newHashMap(sourceOffsets);

    router.clearWriters();
    sourceOffsets.clear();

    return new SinkWriterResult(writerResults, offsets);
  }

  public void save(Collection<SinkRecord> sinkRecords) {
    for (SinkRecord record : sinkRecords) {
      try {
        this.save(record);
      } catch (DataException ex) {
        if (this.reporter != null) {
          this.reporter.report(record, ex);
        }
        if (this.config.errorTolerance().equalsIgnoreCase(ErrorTolerance.ALL.toString())) {
          LOG.error(
              "Data exception encountered while saving record but tolerated due to error tolerance settings. "
                  + "To change this behavior, set 'errors.tolerance' to 'none':",
              ex);
        } else {
          throw ex;
        }
      }
    }
  }

  private void save(SinkRecord record) {
    // the consumer stores the offsets that corresponds to the next record to consume,
    // so increment the record offset by one
    OffsetDateTime timestamp =
        record.timestamp() == null
            ? null
            : OffsetDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), ZoneOffset.UTC);
    // use the original topic and partition to track offsets, as SMTs may have changed
    // record.topic() and record.kafkaPartition() (e.g. RegexRouter). The framework's
    // context.assignment() and consumer offset management use the original values.
    sourceOffsets.put(
        new TopicPartition(record.originalTopic(), record.originalKafkaPartition()),
        new Offset(record.originalKafkaOffset() + 1, timestamp));

    router.routeRecord(record);
  }
}
