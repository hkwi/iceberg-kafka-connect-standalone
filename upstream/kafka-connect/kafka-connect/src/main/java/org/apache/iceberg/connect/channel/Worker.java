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
package org.apache.iceberg.connect.channel;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.Offset;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.connect.data.SinkWriterResult;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Worker extends Channel {
  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

  private final IcebergSinkConfig config;
  private final SinkTaskContext context;
  private final SinkWriter sinkWriter;
  private final ConcurrentLinkedQueue<Envelope> controlEventQueue;
  private final ExecutorService pollingExecutor;
  private final AtomicBoolean running;
  private final AtomicReference<Exception> asyncError;
  private final Duration pollInterval;
  private final String taskId;

  Worker(
      IcebergSinkConfig config,
      KafkaClientFactory clientFactory,
      SinkWriter sinkWriter,
      SinkTaskContext context) {
    // pass transient consumer group ID to which we never commit offsets
    super(
        "worker",
        config.controlGroupIdPrefix() + UUID.randomUUID(),
        config,
        clientFactory,
        context);

    this.config = config;
    this.context = context;
    this.sinkWriter = sinkWriter;
    this.taskId = config.connectorName() + "-" + config.taskId();
    this.controlEventQueue = new ConcurrentLinkedQueue<>();
    this.pollingExecutor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread thread = new Thread(r, "iceberg-worker-control-poller-" + taskId);
              thread.setDaemon(true);
              return thread;
            });
    this.running = new AtomicBoolean(false);
    this.asyncError = new AtomicReference<>();
    this.pollInterval = Duration.ofMillis(config.controlPollIntervalMs());
  }

  @Override
  void start() {
    running.set(true);
    try {
      pollingExecutor.execute(this::backgroundPoll);
    } catch (RuntimeException e) {
      running.set(false);
      throw new ConnectException(String.format("Worker %s failed to start", taskId), e);
    }

    LOG.info(
        "Worker {} started async control topic polling with interval {}ms",
        taskId,
        pollInterval.toMillis());
  }

  private void backgroundPoll() {
    LOG.info("Worker {} control topic polling thread started", taskId);
    try {
      initializeConsumer();
      while (running.get() && !Thread.currentThread().isInterrupted()) {
        try {
          consumeAvailable(pollInterval);
        } catch (WakeupException e) {
          LOG.debug("Worker {} control consumer woke up", taskId, e);
          break;
        }
      }
    } catch (Exception e) {
      if (running.compareAndSet(true, false)) {
        asyncError.compareAndSet(null, e);
        LOG.error("Worker {} failed while polling control events", taskId, e);
      }
    } finally {
      LOG.info("Worker {} control topic polling thread stopped", taskId);
    }
  }

  void process() {
    Exception error = asyncError.getAndSet(null);
    if (error != null) {
      throw new ConnectException(
          String.format("Worker %s failed while polling control events", taskId), error);
    }

    Envelope envelope;
    while ((envelope = controlEventQueue.poll()) != null) {
      handleStartCommit(((StartCommit) envelope.event().payload()).commitId());
    }
  }

  @Override
  protected boolean receive(Envelope envelope) {
    Event event = envelope.event();
    if (event.payload().type() != PayloadType.START_COMMIT) {
      return false;
    }

    controlEventQueue.offer(envelope);
    return true;
  }

  private void handleStartCommit(UUID commitId) {
    SinkWriterResult results = sinkWriter.completeWrite();

    // include all assigned topic partitions even if no messages were read
    // from a partition, as the coordinator will use that to determine
    // when all data for a commit has been received
    List<TopicPartitionOffset> assignments =
        context.assignment().stream()
            .map(
                tp -> {
                  Offset offset = results.sourceOffsets().get(tp);
                  if (offset == null) {
                    offset = Offset.NULL_OFFSET;
                  }
                  return new TopicPartitionOffset(
                      tp.topic(), tp.partition(), offset.offset(), offset.timestamp());
                })
            .collect(Collectors.toList());

    List<Event> events =
        results.writerResults().stream()
            .map(
                writeResult ->
                    new Event(
                        config.connectGroupId(),
                        new DataWritten(
                            writeResult.partitionStruct(),
                            commitId,
                            writeResult.tableReference(),
                            writeResult.dataFiles(),
                            writeResult.deleteFiles())))
            .collect(Collectors.toList());

    Event readyEvent = new Event(config.connectGroupId(), new DataComplete(commitId, assignments));
    events.add(readyEvent);

    send(events, results.sourceOffsets());
  }

  @Override
  void stop() {
    RuntimeException failure = null;

    try {
      stopBackgroundPolling();
    } catch (RuntimeException e) {
      failure = Channel.appendFailure(failure, e);
    }

    try {
      super.stop();
    } catch (RuntimeException e) {
      failure = Channel.appendFailure(failure, e);
    }

    try {
      sinkWriter.close();
    } catch (RuntimeException e) {
      failure = Channel.appendFailure(failure, e);
    }

    if (failure != null) {
      throw failure;
    }
  }

  private void stopBackgroundPolling() {
    running.set(false);
    wakeupConsumer();
    pollingExecutor.shutdownNow();
    try {
      if (!pollingExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
        throw new ConnectException(
            String.format("Worker %s control polling thread did not stop", taskId));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException(
          String.format("Interrupted while stopping worker %s control polling thread", taskId), e);
    } finally {
      controlEventQueue.clear();
      asyncError.set(null);
    }
  }

  void save(Collection<SinkRecord> sinkRecords) {
    sinkWriter.save(sinkRecords);
  }

  int pendingEventCount() {
    return controlEventQueue.size();
  }
}
