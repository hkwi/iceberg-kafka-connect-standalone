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

# ---------------------------------------------------------------------------
# Second scenario: upstream and the standalone overlay edit the same line, so
# the three-way merge must leave a resolvable conflict. Before this fix, the
# patch's "new" blob only existed upstream, so git apply --3way produced a
# dangling stage-3 entry, the conflict rendered the upstream side as empty, and
# the script exited non-zero. The blob-import step must make every referenced
# blob self-contained, the conflict must survive with both sides populated, the
# script must exit 0, and UPSTREAM.md must still be updated.
# ---------------------------------------------------------------------------
sed -i 's/^seven$/standalone-seven/' "${standalone_repo}/upstream/kafka-connect/example.txt"
git -C "${standalone_repo}" add .
git -C "${standalone_repo}" commit -q -m "Overlay edits the same line upstream will edit"

sed -i 's/^seven$/upstream-seven/' "${upstream_repo}/kafka-connect/example.txt"
git -C "${upstream_repo}" add kafka-connect/example.txt
git -C "${standalone_repo}" add . 2>/dev/null || true
git -C "${upstream_repo}" commit -q -m "Upstream edits the same line"
conflict_upstream_commit="$(git -C "${upstream_repo}" rev-parse HEAD)"
conflict_new_blob="$(git -C "${upstream_repo}" rev-parse HEAD:kafka-connect/example.txt)"

if git -C "${standalone_repo}" cat-file -e "${conflict_new_blob}^{blob}" 2>/dev/null; then
  echo "conflict new blob unexpectedly exists in standalone repository" >&2
  exit 1
fi

"${standalone_repo}/scripts/sync-upstream.sh"   --local-iceberg "${upstream_repo}"   --iceberg-ref "${conflict_upstream_commit}"

# The conflict must be present with both the overlay and the upstream line.
grep -qx '<<<<<<< ours' "${result_file}"

grep -q 'standalone-seven' "${result_file}"
grep -q 'upstream-seven' "${result_file}"
# The referenced blob must now be self-contained in the standalone repository.
git -C "${standalone_repo}" cat-file -e "${conflict_new_blob}^{blob}"
# UPSTREAM.md must have advanced to the new commit despite the conflict.
grep -qx -- "- Commit: ${conflict_upstream_commit}" "${standalone_repo}/UPSTREAM.md"

echo "sync-upstream regression test passed"
