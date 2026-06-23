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

This repository tracks Apache Iceberg's Kafka Connect sink connector as a
standalone build. The upstream Iceberg sources are copied into
`upstream/kafka-connect/`; all other Iceberg modules are consumed as Maven
artifacts.

## Build

```bash
./gradlew :iceberg-kafka-connect-runtime:distZip
```

By default, the build uses `1.12.0-SNAPSHOT` Iceberg artifacts from
`mavenLocal()` or Maven repositories. Use another version with:

```bash
./gradlew -PicebergVersion=1.11.0 :iceberg-kafka-connect-runtime:distZip
```

## Sync Upstream

```bash
scripts/sync-upstream.sh --iceberg-ref main
```

GitHub Actions runs the same sync on a schedule and opens a pull request when
`apache/iceberg` changes the copied Kafka Connect source or build inputs.
