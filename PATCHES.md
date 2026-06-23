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
