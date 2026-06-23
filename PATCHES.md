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
