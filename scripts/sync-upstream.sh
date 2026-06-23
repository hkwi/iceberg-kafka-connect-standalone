#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: sync-upstream.sh [--iceberg-repo URL] [--iceberg-ref REF] [--local-iceberg DIR]

Copies kafka-connect/ from apache/iceberg into upstream/kafka-connect and records
the upstream commit in UPSTREAM.md.

Options:
  --iceberg-repo URL  Upstream repository. Defaults to
                      https://github.com/apache/iceberg.git.
  --iceberg-ref REF   Branch, tag, or commit to sync. Defaults to main.
  --local-iceberg DIR Use an existing local Iceberg checkout instead of cloning.
  -h, --help          Show this help.
USAGE
}

repo_url="https://github.com/apache/iceberg.git"
iceberg_ref="main"
local_iceberg=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --iceberg-repo)
      repo_url="$2"
      shift 2
      ;;
    --iceberg-ref)
      iceberg_ref="$2"
      shift 2
      ;;
    --local-iceberg)
      local_iceberg="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -n "${local_iceberg}" ]]; then
  iceberg_dir="$(cd "${local_iceberg}" && pwd)"
  git -C "${iceberg_dir}" rev-parse --verify "${iceberg_ref}^{commit}" >/dev/null
else
  iceberg_dir="${repo_root}/.upstream-cache/iceberg"
  if [[ ! -d "${iceberg_dir}/.git" ]]; then
    mkdir -p "$(dirname "${iceberg_dir}")"
    git clone --no-tags "${repo_url}" "${iceberg_dir}"
  else
    git -C "${iceberg_dir}" remote set-url origin "${repo_url}"
    git -C "${iceberg_dir}" fetch --no-tags origin
  fi
  git -C "${iceberg_dir}" fetch --no-tags origin "${iceberg_ref}"
fi

upstream_commit="$(git -C "${iceberg_dir}" rev-parse "${iceberg_ref}^{commit}")"

if [[ ! -d "${iceberg_dir}/kafka-connect" ]]; then
  echo "No kafka-connect directory found in ${iceberg_dir}" >&2
  exit 1
fi

mkdir -p "${repo_root}/upstream"
mkdir -p "${repo_root}/gradle"
rsync -a --delete --exclude build/ --exclude bin/ --exclude .gradle/ "${iceberg_dir}/kafka-connect/" "${repo_root}/upstream/kafka-connect/"
rsync -a --delete "${iceberg_dir}/gradle/wrapper/" "${repo_root}/gradle/wrapper/"
cp "${iceberg_dir}/gradlew" "${repo_root}/gradlew"
chmod +x "${repo_root}/gradlew"

cat > "${repo_root}/UPSTREAM.md" <<EOF
# Upstream Apache Iceberg Snapshot

- Repository: apache/iceberg
- Ref: ${iceberg_ref}
- Commit: ${upstream_commit}
- Source path: kafka-connect/
EOF

echo "Synced apache/iceberg ${upstream_commit}"
