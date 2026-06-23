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
