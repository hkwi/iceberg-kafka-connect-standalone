# Local Overlay Patches

This repository tracks `apache/iceberg` as recorded in `UPSTREAM.md`. Keep any
not-yet-upstream changes as separate commits on top of the upstream sync commit
so they can be refreshed or dropped cleanly.

## apache/iceberg#14618: Error handling with DLQ support

- PR: https://github.com/apache/iceberg/pull/14618
- Captured PR head commit: 358a2a42af2b1c09585aa3a45a6b1f28da12febb
- Local Apache checkout commit: 6a055166c Kafka Connect: Apply DLQ error handling from PR #14618
- Standalone handling: one overlay commit on top of the upstream sync commit

Refresh procedure if the PR receives more commits before merge:

1. Update `/home/ubuntu/iceberg/apache-iceberg` from `apache/iceberg` main.
2. Rebuild the local `pr-14618-dlq-support` commit from the latest PR diff.
3. Copy the affected `kafka-connect/` files into `upstream/kafka-connect/` here.
4. Amend or replace the standalone overlay commit.

When #14618 is merged into upstream main, run `scripts/sync-upstream.sh` from
this repository, verify the overlay diff is now empty or intentionally changed,
and drop this overlay commit/entry.

## apache/iceberg#11623: Topic-based record routing

- PR: https://github.com/apache/iceberg/pull/11623
- Captured PR head commit: 5bc81bf57273cd1e2949a81026bcc07afd1d3850
- Local Apache checkout commit: 08932ab75 Kafka Connect: Apply topic routing from PR #11623
- Standalone handling: one overlay commit on top of the #14618 overlay commit

This overlay was resolved against #14618 so that `SinkWriter` keeps the DLQ
error handling from #14618 while delegating record routing and writer ownership
to the #11623 `RecordRouter` implementation.

Refresh procedure if the PR receives more commits before merge:

1. Update `/home/ubuntu/iceberg/apache-iceberg` from `apache/iceberg` main.
2. Rebuild the local `pr-11623-topic-routing` commit from the latest PR diff.
3. Re-apply that commit on top of `pr-14618-dlq-support` and resolve conflicts,
   preserving both DLQ handling and topic routing.
4. Copy the affected `kafka-connect/` files into `upstream/kafka-connect/` here.
5. Amend or replace the standalone #11623 overlay commit.

When #11623 is merged into upstream main, run `scripts/sync-upstream.sh` from
this repository, verify the overlay diff is now empty or intentionally changed,
and drop this overlay commit/entry. If #14618 and #11623 merge in a different
order, refresh or drop the overlay commits from oldest upstream inclusion to
newest so the remaining local diff stays reviewable.

## apache/iceberg#15027: ZonedDateTime conversion support

- PR: https://github.com/apache/iceberg/pull/15027
- Captured PR head commit: 2201bfa0d59f82b070cfadddd98479d2b327ebfe
- Local Apache checkout commit: 6261ebfd7 Kafka Connect: Apply ZonedDateTime conversion from PR #15027
- Standalone handling: one overlay commit on top of the #14618 and #11623 overlay commits

This overlay adds `ZonedDateTime` support to Kafka Connect record conversion and
schema inference for Iceberg timestamp values.

Refresh procedure if the PR receives more commits before merge:

1. Update `/home/ubuntu/iceberg/apache-iceberg` from `apache/iceberg` main.
2. Rebuild the local `pr-15027-zoneddatetime` commit from the latest PR diff.
3. Copy the affected `kafka-connect/` files into `upstream/kafka-connect/` here.
4. Amend or replace the standalone #15027 overlay commit.

When #15027 is merged into upstream main, run `scripts/sync-upstream.sh` from
this repository, verify the overlay diff is now empty or intentionally changed,
and drop this overlay commit/entry.

## apache/iceberg#16434: Bounded retry for transient commit failures

- PR: https://github.com/apache/iceberg/pull/16434
- Captured PR head commit: e9c4265b1eacd8da6da24384be3d9aaa82888ad5
- Local Apache checkout commit: 989c9d861 Kafka Connect: Apply bounded commit retry from PR #16434
- Standalone handling: one overlay commit on top of the #14618, #11623, and #15027 overlay commits

This overlay adds `iceberg.control.commit.max-consecutive-failures`, preserving
the default one-failure behavior while allowing operators to tolerate a bounded
number of consecutive transient `CommitFailedException` failures.

Refresh procedure if the PR receives more commits before merge:

1. Update `/home/ubuntu/iceberg/apache-iceberg` from `apache/iceberg` main.
2. Rebuild the local `pr-16434-bounded-commit-retry` commit from the latest PR diff.
3. Re-apply the config change on top of the existing standalone overlays so
   `IcebergSinkConfig` keeps the DLQ and routing settings from earlier overlays.
4. Copy the affected `kafka-connect/` files into `upstream/kafka-connect/` here.
5. Amend or replace the standalone #16434 overlay commit.

When #16434 is merged into upstream main, run `scripts/sync-upstream.sh` from
this repository, verify the overlay diff is now empty or intentionally changed,
and drop this overlay commit/entry.

## apache/iceberg#16156: Synchronous coordinator shutdown

- PR: https://github.com/apache/iceberg/pull/16156
- Captured PR head commit: 60a460783cc847cc02b81d86ffed4684263ad3c2
- Local Apache checkout commit: 5b2c1c0d8 Kafka Connect: Apply synchronous coordinator stop from PR #16156
- Standalone handling: one overlay commit on top of the #14618, #11623, #15027, and #16434 overlay commits

This overlay joins the coordinator thread during shutdown so a revoked leader task
finishes coordinator cleanup before another coordinator can be elected. The PR's
unrelated removal of `taskId` from the defensive close warning is intentionally
not included so logs retain the committer identity.

Refresh procedure if the PR receives more commits before merge:

1. Update `/home/ubuntu/iceberg/apache-iceberg` from `apache/iceberg` main.
2. Rebuild the local `pr-16156-zombie-coordinator` commit from the latest PR diff.
3. Re-apply the shutdown change on top of the existing standalone overlays so
   `CommitterImpl` keeps the #14618 errant-record reporter setup.
4. Copy or re-apply the affected `kafka-connect/` files into `upstream/kafka-connect/` here.
5. Amend or replace the standalone #16156 overlay commit.

When #16156 is merged into upstream main, run `scripts/sync-upstream.sh` from
this repository, verify whether the remaining diff is only the retained warning
message context, and then drop or refresh this overlay commit/entry accordingly.

## apache/iceberg#16366: Retriable transactional commit failures during rebalance

- PR: https://github.com/apache/iceberg/pull/16366
- Captured PR head commit: f7e62c8e87d26f236c46df1eaac89f5bade8e3be
- Local Apache checkout commit: 1eb597e23 Kafka Connect: Apply retriable rebalance commit handling from PR #16366
- Standalone handling: one overlay commit on top of the #14618, #11623, #15027, #16434, and #16156 overlay commits

This overlay translates configured transactional producer commit failures to
`RetriableException` so Kafka Connect can re-deliver the batch after a rebalance
settles. The default configured classes match PR #16366: `CommitFailedException`,
`InvalidProducerEpochException`, and `ProducerFencedException`.

The standalone integration preserves prior overlays while applying this PR:

1. `CommitterImpl` keeps #14618 errant-record reporter setup and #16156
   synchronous coordinator shutdown.
2. `SinkWriter.close()` follows the #11623 `RecordRouter` shape and only adds
   `sourceOffsets.clear()`; there is no local `writers` map to clear.
3. `IcebergSinkConfig` keeps the #14618 error tolerance, #11623 routing, and
   #16434 bounded commit retry settings alongside the new retriable exception
   list.

Refresh procedure if the PR receives more commits before merge:

1. Update `/home/ubuntu/iceberg/apache-iceberg` from `apache/iceberg` main.
2. Rebuild the local `pr-16366-retriable-rebalance-commit` commit from the latest
   PR diff.
3. Re-apply the overlapping files on top of the existing standalone overlays,
   preserving the three integration points listed above.
4. Copy or re-apply the affected `kafka-connect/` files into `upstream/kafka-connect/` here.
5. Amend or replace the standalone #16366 overlay commit.

When #16366 is merged into upstream main, run `scripts/sync-upstream.sh` from
this repository, verify the remaining diff against the local overlays, and drop
or refresh this overlay commit/entry accordingly.

## apache/iceberg#16360: Recover after control topic offset reset

- PR: https://github.com/apache/iceberg/pull/16360
- Captured PR head commit: e4f0164d5baa26dfebf8c169aad3a197f61df7aa
- Local Apache checkout commit: 55e3f8340 Kafka Connect: Apply control topic reset recovery from PR #16360
- Standalone handling: one overlay commit on top of the #14618, #11623, #15027, #16434, #16156, and #16366 overlay commits

This overlay handles Kafka cluster recreation or control-topic reset scenarios
where new control-topic offsets are lower than offsets stored in the latest
Iceberg snapshot. For reset partitions, the stale snapshot offset is removed
from the deduplication baseline so new `DataWritten` events can be committed and
the stored offset baseline is reset to the new control topic offsets.

Refresh procedure if the PR receives more commits before merge:

1. Update `/home/ubuntu/iceberg/apache-iceberg` from `apache/iceberg` main.
2. Rebuild the local `pr-16360-control-topic-reset` commit from the latest PR diff.
3. Re-apply the `Coordinator` changes on top of the existing standalone overlays,
   preserving #16434 bounded commit retry and #16366 retriable transactional commit
   handling in the same class.
4. Copy or re-apply the affected `kafka-connect/` files into `upstream/kafka-connect/` here.
5. Amend or replace the standalone #16360 overlay commit.

When #16360 is merged into upstream main, run `scripts/sync-upstream.sh` from
this repository, verify the remaining diff against the local overlays, and drop
or refresh this overlay commit/entry accordingly.

## apache/iceberg#16915: Case-insensitive name mapping lookup

- PR: https://github.com/apache/iceberg/pull/16915
- Captured PR head commit: 40910565ae7865813b68d2a4c26156715bcba846
- Local Apache checkout commit: dbb9083fa Kafka Connect: Apply case-insensitive name mapping from PR #16915
- Standalone handling: one overlay commit on top of the existing local overlay stack

This overlay fixes `RecordConverter` lookup when a table name mapping is present
and `iceberg.tables.schema-case-insensitive=true`. Mapping aliases and fallback
field names are normalized with `Locale.ROOT` for case-insensitive lookup so
schema evolution does not create duplicate fields for case-only name differences.

Refresh procedure if the PR receives more commits before merge:

1. Update `/home/ubuntu/iceberg/apache-iceberg` from `apache/iceberg` main.
2. Rebuild the local `pr-16915-case-insensitive-name-mapping` commit from the latest PR diff.
3. Copy the affected `kafka-connect/` files into `upstream/kafka-connect/` here, preserving the
   existing #15027 ZonedDateTime conversion overlay in `RecordConverter` if it is still local.
4. Amend or replace the standalone #16915 overlay commit.

When #16915 is merged into upstream main, run `scripts/sync-upstream.sh` from
this repository, verify the remaining diff against the local overlays, and drop
or refresh this overlay commit/entry accordingly.

## apache/iceberg#16606: Decimal inference for BigDecimal values

- PR: https://github.com/apache/iceberg/pull/16606
- Captured PR head commit: f61f96a196e7d3bf5ca46d58ffe244ddf3d08ebb
- Local Apache checkout commit: c06287a2a Kafka Connect: Apply decimal inference fix from PR #16606
- Standalone handling: one overlay commit on top of the existing local overlay stack

This overlay normalizes inferred `BigDecimal` types so values with scale larger
than precision, such as `0.001`, become valid Iceberg decimals, and values with
negative scale, such as `1E+2`, are represented as scale 0 decimals. This avoids
schema evolution creating invalid decimal types that later fail writes.

The standalone integration applies only the targeted decimal inference and tests
so existing local overlays are preserved, including #15027 `ZonedDateTime` type
inference in `SchemaUtils` and the #14618/#11623 `TestSinkWriter` coverage.

Refresh procedure if the PR receives more commits before merge:

1. Update `/home/ubuntu/iceberg/apache-iceberg` from `apache/iceberg` main.
2. Rebuild the local `pr-16606-decimal-inference` commit from the latest PR diff.
3. Re-apply the affected `SchemaUtils` and test changes on top of the existing
   standalone overlays, preserving `ZonedDateTime` inference and prior SinkWriter tests.
4. Amend or replace the standalone #16606 overlay commit.

When #16606 is merged into upstream main, run `scripts/sync-upstream.sh` from
this repository, verify the remaining diff against the local overlays, and drop
or refresh this overlay commit/entry accordingly.

## apache/iceberg#16828: UUID logical type schema conversion

- PR: https://github.com/apache/iceberg/pull/16828
- Captured PR head commit: 371bc7db8448948b8af4479c3801aaa314730aec
- Local Apache checkout commit: b2b384c78 Kafka Connect: Apply UUID schema conversion from PR #16828
- Standalone handling: one overlay commit on top of the existing local overlay stack

This overlay maps Kafka Connect schemas named `uuid` on STRING and BYTES values
to Iceberg `UUIDType` instead of falling back to string or binary. The Apache PR
also adds a core Avro schema conversion test; the standalone overlay carries only
the Kafka Connect code and tests because this repository builds the connector
modules.

The standalone integration preserves the existing local `SchemaUtils` overlays,
including #15027 `ZonedDateTime` inference and #16606 decimal inference.

Refresh procedure if the PR receives more commits before merge:

1. Update `/home/ubuntu/iceberg/apache-iceberg` from `apache/iceberg` main.
2. Rebuild the local `pr-16828-avro-uuid-schema` commit from the latest PR diff.
3. Re-apply the Kafka Connect `SchemaUtils` and `TestSchemaUtils` changes on top
   of the existing standalone overlays.
4. Amend or replace the standalone #16828 overlay commit.

When #16828 is merged into upstream main, run `scripts/sync-upstream.sh` from
this repository, verify the remaining diff against the local overlays, and drop
or refresh this overlay commit/entry accordingly.

## apache/iceberg#16602: Per-instance KafkaMetadataTransform configuration

- PR: https://github.com/apache/iceberg/pull/16602
- Captured PR head commit: 4592ea64daac62370bf77e8bd53dc3e8b9f3c8a3
- Local Apache checkout commit: a4a3765ad Kafka Connect: Apply metadata transform config fix from PR #16602
- Standalone handling: one overlay commit on top of the existing local overlay stack

This overlay changes `KafkaMetadataTransform.recordAppender` from a static field
to an instance field so one transform instance's `configure()` call cannot leak
its metadata field configuration into another instance.

Refresh procedure if the PR receives more commits before merge:

1. Update `/home/ubuntu/iceberg/apache-iceberg` from `apache/iceberg` main.
2. Rebuild the local `pr-16602-transform-instance-config` commit from the latest PR diff.
3. Copy or re-apply the affected transform source and test files into
   `upstream/kafka-connect/` here.
4. Amend or replace the standalone #16602 overlay commit.

When #16602 is merged into upstream main, run `scripts/sync-upstream.sh` from
this repository, verify the remaining diff against the local overlays, and drop
or refresh this overlay commit/entry accordingly.

## apache/iceberg#16604: Mongo array timestamp and date conversion

- PR: https://github.com/apache/iceberg/pull/16604
- Captured PR head commit: b9aad0fc8fb90201b3d77af4e2c838ed78fc9aff
- Local Apache checkout commit: 41a56c613 Kafka Connect: Apply Mongo array temporal fix from PR #16604
- Standalone handling: one overlay commit on top of the existing local overlay stack

This overlay fixes `MongoDataConverter` array conversion for BSON DATE_TIME and
TIMESTAMP values. Array elements now use `asDateTime().getValue()` for epoch
millis and `asTimestamp().getTime()` for timestamp seconds, matching the scalar
conversion paths instead of reading the values through integer accessors.

Refresh procedure if the PR receives more commits before merge:

1. Update `/home/ubuntu/iceberg/apache-iceberg` from `apache/iceberg` main.
2. Rebuild the local `pr-16604-mongo-array-temporals` commit from the latest PR diff.
3. Copy or re-apply the affected Mongo transform source and test files into
   `upstream/kafka-connect/` here.
4. Amend or replace the standalone #16604 overlay commit.

When #16604 is merged into upstream main, run `scripts/sync-upstream.sh` from
this repository, verify the remaining diff against the local overlays, and drop
or refresh this overlay commit/entry accordingly.

## apache/iceberg#16072: Auto-create namespace AccessDenied handling

- PR: https://github.com/apache/iceberg/pull/16072
- Captured PR head commit: 37d7de0f1edc20212f58bcc70b412c59bb331687
- Local Apache checkout commit: 54f5f8d3f Kafka Connect: Apply auto-create AccessDenied fix from PR #16072
- Standalone handling: Kafka Connect overlay commit plus an Iceberg dependency requirement

This overlay changes Kafka Connect auto-create namespace handling to check each
namespace level with `namespaceExists()` before calling `createNamespace()`. That
avoids calling Glue `CreateDatabase` for namespaces that already exist, which is
important when the connector principal can read the Glue database but lacks
`glue:CreateDatabase` permission.

The Apache PR also changes `aws/GlueCatalog` so AWS SDK `AccessDeniedException`
from `createNamespace()` is wrapped as Iceberg `ForbiddenException`. That source
is not part of this standalone repository; it lives in the `iceberg-aws` artifact
used by the runtime build. For full #16072 behavior, build the runtime with an
Iceberg dependency set that includes the local Apache checkout commit listed
above, or with an upstream release after #16072 is merged.

Refresh procedure if the PR receives more commits before merge:

1. Update `/home/ubuntu/iceberg/apache-iceberg` from `apache/iceberg` main.
2. Rebuild the local `pr-16072-auto-create-access-denied` commit from the latest PR diff.
3. Re-apply or copy the Kafka Connect `IcebergWriterFactory` source and test files into
   `upstream/kafka-connect/` here.
4. Re-publish the Iceberg dependency artifacts from the refreshed Apache checkout if the
   `aws/GlueCatalog` part is needed in local runtime builds.
5. Amend or replace the standalone #16072 overlay commit.

When #16072 is merged into upstream main, run `scripts/sync-upstream.sh` from
this repository, verify the remaining diff against the local overlays, ensure the
runtime uses an Iceberg artifact containing the GlueCatalog fix, and drop or
refresh this overlay commit/entry accordingly.

## apache/iceberg#16843: Robust sink cleanup on shutdown failures

- PR: https://github.com/apache/iceberg/pull/16843
- Captured PR head commit: c852c178abdb7d12e28d27ce1ef841ca15333d05
- Local Apache checkout commit: 7020ff8e5 Kafka Connect: Apply robust cleanup from PR #16843
- Standalone handling: one overlay commit integrated with the existing local cleanup and retry overlays

This overlay makes shutdown cleanup continue after failures in committer, worker,
coordinator, producer, or consumer close paths. The original failure is still
propagated to Kafka Connect, but later cleanup steps are no longer skipped. It
also removes the unused channel-level `AdminClient` and waits for the coordinator
thread to finish from `CoordinatorThread.terminate()`.

The standalone integration preserves existing local overlays while applying this PR:

1. `CommitterImpl.startWorker()` keeps the #14618 errant-record reporter setup.
2. `CommitterImpl.processControlEvents()` keeps the #16366 retriable rebalance
   handling that stops the worker and rethrows `RetriableException`.
3. `Channel.send()` keeps the #16366 transactional commit retry translation.
4. The #16156 coordinator join behavior moves from `CommitterImpl.stopCoordinator()`
   to the #16843 `CoordinatorThread.terminate()` implementation.

Refresh procedure if the PR receives more commits before merge:

1. Update `/home/ubuntu/iceberg/apache-iceberg` from `apache/iceberg` main.
2. Rebuild the local `pr-16843-robust-sink-cleanup` commit from the latest PR diff.
3. Re-apply the affected Kafka Connect files on top of the existing standalone overlays,
   preserving the four integration points listed above.
4. Run the #16843 channel tests plus `TestChannelRetry` to verify #16366 remains intact.
5. Amend or replace the standalone #16843 overlay commit.

When #16843 is merged into upstream main, run `scripts/sync-upstream.sh` from
this repository, verify the remaining diff against the local overlays, and drop
or refresh this overlay commit/entry accordingly.

## apache/iceberg#16772: Debezium source timestamp CDC metadata

- PR: https://github.com/apache/iceberg/pull/16772
- Captured PR head commit: d43c27fdaaf924d781bfaa396de603a49c7b2b4e
- Local Apache checkout commit: 1ca3df8cb Kafka Connect: Apply Debezium source timestamp from PR #16772
- Standalone handling: one Kafka Connect transforms overlay commit on top of the existing local overlay stack

This overlay keeps the Debezium source database timestamp in CDC metadata. The
transform already exposes the connector event timestamp as `_cdc.ts`; this adds
`_cdc.source_ts` from Debezium `source.ts_ms` for both schema and schemaless
records so consumers can distinguish connector processing time from source
database change time.

The Apache PR also updates `docs/docs/kafka-connect.md`. This standalone
repository does not carry the Apache docs tree, so the local overlay contains
only the transform source and tests.

Refresh procedure if the PR receives more commits before merge:

1. Update `/home/ubuntu/iceberg/apache-iceberg` from `apache/iceberg` main.
2. Rebuild the local `pr-16772-debezium-source-timestamp` commit from the latest PR diff.
3. Copy or re-apply the affected Kafka Connect transforms source and test files into
   `upstream/kafka-connect/` here.
4. Amend or replace the standalone #16772 overlay commit.

When #16772 is merged into upstream main, run `scripts/sync-upstream.sh` from
this repository, verify the remaining diff against the local overlays, and drop
or refresh this overlay commit/entry accordingly.

## apache/iceberg#16681: Avro temporal logical type support

- PR: https://github.com/apache/iceberg/pull/16681
- Captured PR head commit: 5f86dd88c835e269ab6d1d89c667517025bb0fe6
- Local Apache checkout commit: 4d56e0e2f Kafka Connect: Apply Avro temporal logical types from PR #16681
- Standalone handling: Kafka Connect overlay commit plus an Iceberg core dependency requirement

This overlay teaches the Kafka Connect sink to preserve temporal semantics for
AvroConverter schemas that arrive as named numeric values, including
`timestamp-micros`, `timestamp-nanos`, `local-timestamp-millis`,
`local-timestamp-micros`, `local-timestamp-nanos`, and `time-micros`. The
Connect schema is threaded into nested struct, list, and map value conversion so
numeric temporals are scaled as millis, micros, or nanos instead of always being
interpreted as milliseconds.

The Apache PR also changes the Iceberg core Avro read path for `time-millis` and
`local-timestamp-*` logical types. Those core files are not part of this
standalone repository. For full #16681 behavior, build the runtime with Iceberg
core/data artifacts from the local Apache checkout commit listed above, or with
an upstream release after #16681 is merged.

The standalone integration preserves existing local overlays while applying this PR:

1. `SchemaUtils` keeps #15027 `ZonedDateTime` inference.
2. `SchemaUtils` keeps #16606 decimal precision/scale normalization.
3. `SchemaUtils` keeps #16828 UUID schema-name mapping for STRING and BYTES.
4. `RecordConverter` keeps #16915 case-insensitive name mapping lookup.

Refresh procedure if the PR receives more commits before merge:

1. Update `/home/ubuntu/iceberg/apache-iceberg` from `apache/iceberg` main.
2. Rebuild the local `pr-16681-avro-temporal-logical-types` commit from the latest PR diff.
3. Re-apply the Kafka Connect `RecordConverter`, `SchemaUtils`, and tests on top
   of the existing standalone overlays, preserving the four integration points listed above.
4. Re-publish the Iceberg dependency artifacts from the refreshed Apache checkout if the
   core Avro read-path part is needed in local runtime builds.
5. Amend or replace the standalone #16681 overlay commit.

When #16681 is merged into upstream main, run `scripts/sync-upstream.sh` from
this repository, verify the remaining diff against the local overlays, ensure the
runtime uses Iceberg artifacts containing the core Avro changes, and drop or
refresh this overlay commit/entry accordingly.

## apache/iceberg#16355: Connector names in coordinator and committer threads

- PR: https://github.com/apache/iceberg/pull/16355
- Captured PR head commit: 5745df1d5556a292ccbaabdef267e91978aa2591
- Local Apache checkout commit: 66a1a3dbb Kafka Connect: Apply connector thread names from PR #16355
- Standalone handling: one channel overlay commit on top of the existing local overlay stack

This overlay includes the connector name in background thread names so logs from
workers running multiple connectors can identify the responsible connector. The
coordinator thread is named `iceberg-coord-<connectorName>`, and the coordinator
committer pool uses `iceberg-committer-<connectorName>-%d`.

The standalone integration preserves existing local channel overlays while applying this PR:

1. `CoordinatorThread.terminate()` keeps #16843 synchronous shutdown and failure aggregation.
2. `CommitterImpl.startWorker()` keeps #14618 errant-record reporter setup.
3. `CommitterImpl.processControlEvents()` keeps #16366 retriable rebalance handling.
4. `Coordinator` keeps #16360 control-topic reset recovery and #16434 bounded commit retry logic.

Refresh procedure if the PR receives more commits before merge:

1. Update `/home/ubuntu/iceberg/apache-iceberg` from `apache/iceberg` main.
2. Rebuild the local `pr-16355-thread-names` commit from the latest PR diff.
3. Re-apply the channel source and test changes on top of the existing standalone overlays,
   preserving the four integration points listed above.
4. Run the channel tests plus `TestChannelRetry` to verify the retry overlay remains intact.
5. Amend or replace the standalone #16355 overlay commit.

When #16355 is merged into upstream main, run `scripts/sync-upstream.sh` from
this repository, verify the remaining diff against the local overlays, and drop
or refresh this overlay commit/entry accordingly.

## apache/iceberg#16282: Skip replayed committed control events during recovery

- Issue: https://github.com/apache/iceberg/issues/16282
- Standalone handling: issue-driven local overlay commit on top of the existing local overlay stack

This overlay prevents a coordinator recovery path from registering the same files twice when an
Iceberg table commit succeeds but the coordinator crashes before committing the control-topic
consumer offsets. In that window, the next coordinator may re-consume the same `DataWritten` event.
When control-topic offsets appear lower than the offsets stored in the latest table snapshot, the
coordinator now treats `DataWritten` events whose `commitId` is already present in the table snapshot
ancestry as replayed and skips them before building append or row-delta operations.

The fix intentionally uses the Kafka Connect commit ID recorded in Iceberg snapshot properties,
rather than file-path scanning, because the Kafka transaction cannot atomically include the external
Iceberg commit. The snapshot commit ID is the connector's idempotence marker for the external side
effect, while still allowing a real control-topic reset with new commit IDs to write new files.

The standalone integration preserves existing local channel overlays:

1. #16360 control-topic reset recovery still allows new commit IDs after a reset.
2. #16434 bounded commit retry remains unchanged.
3. #16366 retriable transactional commit handling remains unchanged.
4. #16843 robust coordinator cleanup remains unchanged.

When upstream adds a fix for #16282, run `scripts/sync-upstream.sh`, compare the upstream approach
against this commit-id replay filter, and drop or refresh this local overlay accordingly.

## apache/iceberg#16084: Decouple worker control-topic polling from record processing

- PR: https://github.com/apache/iceberg/pull/16084
- Related issue: https://github.com/apache/iceberg/issues/14818
- Captured PR head commit: 6cb20637fef5bef5cff348784c322be00ee27678
- Standalone handling: adapted local overlay commit on top of the existing local overlay stack

This overlay moves the worker's control-topic consumer polling out of the main record-processing
path. The worker now owns a background control-topic polling thread, buffers `START_COMMIT` events
in a thread-safe queue, and drains that queue from `Worker.process()`. This avoids the previous
`consumeAvailable(Duration.ZERO)` hot-path poll, which could starve control consumer group join,
heartbeat, or rebalance progress and produce symptoms like #14818's intermittent
`UNKNOWN_MEMBER_ID`/missed heartbeat behavior.

The standalone integration intentionally differs from the raw PR in a few places to preserve local
overlays and current channel behavior:

1. `Channel.stop()` keeps #16843 failure aggregation while exposing `initializeConsumer()` and
   `wakeupConsumer()` for the worker poller.
2. `CommitterImpl.processControlEvents()` preserves #16366 retriable handling and additionally stops
   the worker when async control polling fails.
3. `Worker.stop()` still aggregates shutdown failures from background polling, channel close, and
   `SinkWriter.close()`.
4. Tests avoid adding Awaitility to the standalone build and use a small local wait helper instead.

Refresh procedure if #16084 receives more commits before merge:

1. Update `/home/ubuntu/iceberg/apache-iceberg` from `apache/iceberg` main.
2. Re-read PR #16084 and compare its `Channel`, `Worker`, `CommitterImpl`, and config changes
   against this adapted overlay.
3. Re-apply only the async control-topic polling semantics needed by standalone, preserving #14618,
   #16366, #16843, and #16282 behavior.
4. Run `./gradlew -q :iceberg-kafka-connect:test` from this repository.
5. Amend or replace the standalone #16084 overlay commit.

When #16084 is merged into upstream main, run `scripts/sync-upstream.sh`, compare upstream's worker
polling implementation against this adapted overlay, and drop or refresh this local commit/entry.

## apache/iceberg#15344: Convert Avro timestamp-millis Date values for long fields

- Issue: https://github.com/apache/iceberg/issues/15344
- Standalone handling: issue-driven local overlay commit on top of the existing local overlay stack

This overlay allows `RecordConverter.convertLong` to accept `java.util.Date` values by returning
`Date.getTime()`. This covers existing Iceberg tables whose field type is `LONG` while an Avro
`timestamp-millis` value arrives from Kafka Connect's Avro converter as a `Date`, preserving the
logical type's physical epoch-millis value instead of failing with `Cannot convert to long`.

The schema inference path is intentionally unchanged: `Date` values still infer as Iceberg
timestamps, so auto-created tables keep timestamp semantics rather than silently creating `LONG`
columns.

When upstream adds a fix for #15344, run `scripts/sync-upstream.sh`, compare whether upstream keeps
this existing-LONG behavior and the timestamp inference behavior, and drop or refresh this local
overlay accordingly.

## apache/iceberg#16598: Identifier field validation and auto-create schema alignment

- PR: https://github.com/apache/iceberg/pull/16598
- Captured PR head commit: 00c7e156abcd08e329ebd3d0600bbb9515c036e0
- Standalone handling: adapted local overlay commit on top of the existing local overlay stack

This overlay makes auto-created tables honor configured `id-columns` by passing the matching field
IDs into the Iceberg schema's identifier field set. It also rejects invalid identifier-field
configurations earlier and consistently as Kafka Connect `DataException`s: dotted id-column paths,
missing id columns, optional or floating-point identifier fields, and per-table `id-columns` used
with `iceberg.tables.schema-force-optional=true`.

The startup config validation also rejects the global combination of
`iceberg.tables.schema-force-optional=true` with `iceberg.tables.default-id-columns`, because forced
optional fields cannot satisfy Iceberg's required identifier field contract.

The standalone integration keeps existing local overlays unchanged and applies #16598 only around
`IcebergSinkConfig`, `IcebergWriterFactory`, and `RecordUtils` error handling.

Refresh procedure if #16598 receives more commits before merge:

1. Update `/home/ubuntu/iceberg/apache-iceberg` from `apache/iceberg` main.
2. Re-read PR #16598 and compare its config validation, auto-create schema construction, and
   `RecordUtils.createTableWriter` error contract against this overlay.
3. Re-apply the identifier-field behavior on top of the standalone overlay stack without changing
   routing, DLQ, or coordinator recovery behavior.
4. Run the focused config/writer/record utility tests plus `./gradlew -q :iceberg-kafka-connect:test`.
5. Amend or replace the standalone #16598 overlay commit.

When #16598 is merged into upstream main, run `scripts/sync-upstream.sh`, compare upstream's
identifier-field validation against this overlay, and drop or refresh this local commit/entry.
