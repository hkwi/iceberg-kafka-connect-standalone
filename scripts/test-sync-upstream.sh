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

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
test_root="$(mktemp -d)"
trap 'rm -rf "${test_root}"' EXIT

upstream_repo="${test_root}/iceberg"
standalone_repo="${test_root}/standalone"

git init -q "${upstream_repo}"
git -C "${upstream_repo}" config user.name "Sync Test"
git -C "${upstream_repo}" config user.email "sync-test@example.com"
mkdir -p "${upstream_repo}/kafka-connect"
cat > "${upstream_repo}/kafka-connect/example.txt" <<'EOF'
one
two
three
four
five
six
seven
EOF
git -C "${upstream_repo}" add kafka-connect/example.txt
git -C "${upstream_repo}" commit -q -m "Add base"
base_commit="$(git -C "${upstream_repo}" rev-parse HEAD)"
base_blob="$(git -C "${upstream_repo}" rev-parse HEAD:kafka-connect/example.txt)"

sed -i 's/^five$/upstream-five/' "${upstream_repo}/kafka-connect/example.txt"
git -C "${upstream_repo}" add kafka-connect/example.txt
git -C "${upstream_repo}" commit -q -m "Change upstream line"
upstream_commit="$(git -C "${upstream_repo}" rev-parse HEAD)"

git init -q "${standalone_repo}"
git -C "${standalone_repo}" config user.name "Sync Test"
git -C "${standalone_repo}" config user.email "sync-test@example.com"
mkdir -p "${standalone_repo}/scripts" "${standalone_repo}/upstream/kafka-connect"
cp "${script_dir}/sync-upstream.sh" "${standalone_repo}/scripts/sync-upstream.sh"
cat > "${standalone_repo}/upstream/kafka-connect/example.txt" <<'EOF'
one
standalone-two
three
four
five
six
seven
EOF
cat > "${standalone_repo}/UPSTREAM.md" <<EOF
# Upstream Apache Iceberg Snapshot

- Repository: apache/iceberg
- Ref: main
- Commit: ${base_commit}
- Source path: kafka-connect/
EOF
git -C "${standalone_repo}" add .
git -C "${standalone_repo}" commit -q -m "Add standalone snapshot"

if git -C "${standalone_repo}" cat-file -e "${base_blob}^{blob}" 2>/dev/null; then
  echo "base blob unexpectedly exists in standalone repository" >&2
  exit 1
fi

"${standalone_repo}/scripts/sync-upstream.sh" \
  --local-iceberg "${upstream_repo}" \
  --iceberg-ref "${upstream_commit}"

result_file="${standalone_repo}/upstream/kafka-connect/example.txt"
grep -qx 'standalone-two' "${result_file}"
grep -qx 'upstream-five' "${result_file}"
grep -qx -- "- Commit: ${upstream_commit}" "${standalone_repo}/UPSTREAM.md"

echo "sync-upstream regression test passed"
