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
