<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Iceberg Kafka Connect Standalone

This repository is a standalone build and tracking branch for Apache Iceberg's
Kafka Connect sink connector. It exists to make the connector easier to build,
test, and patch independently while still following `apache/iceberg` as the
source of truth.

The copied upstream source lives under `upstream/kafka-connect/`. Iceberg modules
outside Kafka Connect are consumed as Maven artifacts, so this repository can
build the connector without carrying the full Apache Iceberg tree.

## What This Repository Does

- Tracks the Kafka Connect source from `apache/iceberg`.
- Builds the connector, transforms, events module, and runtime distribution as a
  standalone Gradle project.
- Keeps local not-yet-upstream fixes as separate overlay commits.
- Pulls selected Apache Iceberg PRs and issue fixes into this standalone branch
  when they are small enough to validate locally and useful for connector users.
- Records how each overlay differs from the original Apache PR so it can be
  refreshed, replaced, or dropped when upstream catches up.

This is not intended to fork the connector permanently. The normal lifecycle for
an overlay is: evaluate an Apache PR or issue, adapt it if needed, test it here,
keep it as a separate commit, and remove it once the equivalent change is merged
into Apache Iceberg main.

## Upstream Tracking

`UPSTREAM.md` records the Apache Iceberg snapshot currently copied into this
repository:

- repository: `apache/iceberg`
- source path: `kafka-connect/`
- copied destination: `upstream/kafka-connect/`

To refresh from Apache Iceberg main:

```bash
scripts/sync-upstream.sh --iceberg-ref main
```

GitHub Actions runs the same sync on a schedule and opens a pull request when
Apache Iceberg changes the copied Kafka Connect source or related build inputs.

## Local Overlay Patches

`PATCHES.md` is the index of local overlays. Each entry records:

- the original `apache/iceberg#NUM` PR or issue,
- the captured upstream head commit when applicable,
- what the change is meant to fix or improve,
- how it was adapted for this standalone repository,
- what needs attention when the upstream PR changes or merges.

The Git history follows the same model. Overlay commits should include a commit
body with `Source: apache/iceberg#NUM`, plus a short summary of purpose,
standalone integration, and any adjustment notes. This keeps the branch
reviewable even when multiple upstream PRs touch the same connector files.

## Build

Build the runtime distribution:

```bash
./gradlew :iceberg-kafka-connect-runtime:distZip
```

By default, the build uses `1.12.0-SNAPSHOT` Iceberg artifacts from `mavenLocal()`
or configured Maven repositories. Use another Iceberg version with:

```bash
./gradlew -PicebergVersion=1.11.0 :iceberg-kafka-connect-runtime:distZip
```

Some runtime integration tasks require the matching Iceberg runtime artifacts,
such as `iceberg-aws`, `iceberg-gcp`, `iceberg-bigquery`, and `iceberg-azure`, to
exist in `mavenLocal()` when using the default snapshot version.

## GitHub Actions Artifacts

The `Build Connector Distribution` workflow builds the standalone connector zip
files and uploads them as workflow artifacts. The uploaded artifact contains both
the standard runtime distribution and the Hive runtime distribution from:

```text
upstream/kafka-connect/kafka-connect-runtime/build/distributions/*.zip
```

The workflow uses Iceberg `1.11.0` artifacts by default so it can run from public
Maven repositories. Manual runs can choose another Iceberg artifact version with
the `iceberg-version` workflow input. Snapshot versions such as
`1.12.0-SNAPSHOT` require the corresponding Iceberg artifacts to be available to
the workflow.

## Test

Run the connector unit tests:

```bash
./gradlew :iceberg-kafka-connect:test
```

Run transform tests:

```bash
./gradlew :iceberg-kafka-connect-transforms:test
```

Compile runtime integration tests with a released Iceberg dependency set:

```bash
./gradlew -PicebergVersion=1.11.0 :iceberg-kafka-connect-runtime:compileIntegrationJava
```

Full runtime integration tests use the Docker Compose setup copied from Apache
Iceberg and may require local Docker/Testcontainers support plus the matching
Iceberg artifacts for the selected version.

## Working With Upstream PRs

When evaluating an Apache Iceberg PR for this standalone branch:

1. Read the PR description, diff, and review comments.
2. Decide whether the change is safe and useful to carry locally.
3. Prefer a direct adaptation over a raw cherry-pick when existing overlays have
   already changed the affected code structure.
4. Keep behavior-changing overlays separate from build/test/documentation
   overlays.
5. Add or update focused tests whenever the change affects runtime behavior.
6. Update `PATCHES.md` and write a commit body that references the original
   `apache/iceberg#NUM`.

If a PR is broad, design-sensitive, or duplicates an existing overlay with a
different policy, leave it out and document the decision in discussion rather
than forcing it into the stack.

## Dropping Overlays

When Apache Iceberg merges a PR already carried here:

1. Run `scripts/sync-upstream.sh --iceberg-ref main`.
2. Compare the local overlay diff against the new upstream source.
3. Drop the local overlay commit if upstream now contains the same behavior.
4. Refresh the overlay if standalone still needs an adaptation.
5. Remove or update the corresponding `PATCHES.md` entry.

Keeping each overlay as its own commit is what makes this process manageable.
