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

Applies kafka-connect changes from apache/iceberg to upstream/kafka-connect and
records the upstream commit in UPSTREAM.md. Existing standalone overlay changes
are preserved by applying the upstream old-to-new diff on top of the current tree.

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
current_upstream_commit=""
if [[ -f "${repo_root}/UPSTREAM.md" ]]; then
  current_upstream_commit="$(sed -n 's/^- Commit: //p' "${repo_root}/UPSTREAM.md")"
fi

if [[ ! -d "${iceberg_dir}/kafka-connect" ]]; then
  echo "No kafka-connect directory found in ${iceberg_dir}" >&2
  exit 1
fi

if [[ "${current_upstream_commit}" == "${upstream_commit}" ]]; then
  echo "apache/iceberg is already synced at ${upstream_commit}"
  exit 0
fi

mkdir -p "${repo_root}/upstream"
mkdir -p "${repo_root}/gradle"

if [[ -n "${current_upstream_commit}" ]] && git -C "${iceberg_dir}" cat-file -e "${current_upstream_commit}^{commit}" 2>/dev/null; then
  patch_file="$(mktemp)"
  mapped_patch_file="$(mktemp)"
  cleanup() {
    rm -f "${patch_file}" "${mapped_patch_file}"
  }
  trap cleanup EXIT

  git -C "${iceberg_dir}" diff --binary "${current_upstream_commit}" "${upstream_commit}" -- \
    kafka-connect gradle/wrapper gradlew > "${patch_file}"

  if [[ ! -s "${patch_file}" ]]; then
    echo "No kafka-connect, Gradle wrapper, or gradlew changes between ${current_upstream_commit} and ${upstream_commit}"
    exit 0
  fi

  sed \
    -e 's# a/kafka-connect/# a/upstream/kafka-connect/#g' \
    -e 's# b/kafka-connect/# b/upstream/kafka-connect/#g' \
    -e 's#^diff --git a/kafka-connect/#diff --git a/upstream/kafka-connect/#g' \
    -e 's# b/kafka-connect/# b/upstream/kafka-connect/#g' \
    -- "${patch_file}" > "${mapped_patch_file}"

  # The patch's base and new blobs may exist only in the upstream repository. Expose them so
  # --3way can merge upstream changes around standalone overlays instead of falling back to
  # context matching.
  iceberg_object_dir="$(git -C "${iceberg_dir}" rev-parse --path-format=absolute --git-path objects)"
  alternate_object_dirs="${iceberg_object_dir}"
  if [[ -n "${GIT_ALTERNATE_OBJECT_DIRECTORIES:-}" ]]; then
    alternate_object_dirs="${alternate_object_dirs}:${GIT_ALTERNATE_OBJECT_DIRECTORIES}"
  fi

  # Import the blobs the patch references (old and new) into this repository. git apply --3way
  # does not copy objects it reads through GIT_ALTERNATE_OBJECT_DIRECTORIES, so on a three-way
  # conflict the index would keep a stage-3 entry pointing at a blob that only exists upstream.
  # Once the alternate reference is gone the blob is dangling and the conflict cannot be
  # resolved. We copy every referenced blob (old and new) into this repository so all three
  # merge sides are self-contained. The id list is parsed with a single-quoted Python -c
  # program because POSIX awk/grep treat the double-dot in the index field inconsistently
  # across implementations, and a -c argument does not compete with the shell for stdin.
  patch_blob_list="$(python3 -c '
import sys
path = sys.argv[1]
with open(path, "r", errors="replace") as fh:
    for line in fh:
        parts = line.split()
        if len(parts) < 2 or parts[0] != "index":
            continue
        # "old..new" splits into [old, "", new] because the two dots are adjacent.
        ids = [i for i in parts[1].split(".") if i]
        if len(ids) != 2:
            continue
        for i in ids:
            # Skip the all-zero id (000000000 or 0) used for absent files.
            if set(i) != {"0"}:
                print(i)
' "${mapped_patch_file}")"
  if [[ -n "${patch_blob_list}" ]]; then
    while read -r blob_sha; do
      [[ -n "${blob_sha}" ]] || continue
      if ! git -C "${repo_root}" cat-file -e "${blob_sha}^{blob}" 2>/dev/null; then
        GIT_ALTERNATE_OBJECT_DIRECTORIES="${alternate_object_dirs}" \
          git -C "${repo_root}" cat-file blob "${blob_sha}" \
          | git -C "${repo_root}" hash-object -w --stdin >/dev/null
      fi
    done <<< "${patch_blob_list}"
  fi

  if ! GIT_ALTERNATE_OBJECT_DIRECTORIES="${alternate_object_dirs}" \
      git -C "${repo_root}" apply --3way --binary "${mapped_patch_file}"; then
    # A genuine three-way conflict: the standalone overlay and upstream changed the same
    # lines. Keep the unmerged files (with conflict markers) in the working tree so the sync
    # pull request surfaces them for manual resolution; the workflow skips the connector
    # build while conflicts remain.
    echo "Upstream sync left unresolved three-way conflicts:"
    git -C "${repo_root}" diff --name-only --diff-filter=U
  fi
else
  rsync -a --delete --exclude build/ --exclude bin/ --exclude .gradle/ "${iceberg_dir}/kafka-connect/" "${repo_root}/upstream/kafka-connect/"
  rsync -a --delete "${iceberg_dir}/gradle/wrapper/" "${repo_root}/gradle/wrapper/"
  cp "${iceberg_dir}/gradlew" "${repo_root}/gradlew"
  chmod +x "${repo_root}/gradlew"
fi

cat > "${repo_root}/UPSTREAM.md" <<EOF
# Upstream Apache Iceberg Snapshot

- Repository: apache/iceberg
- Ref: ${iceberg_ref}
- Commit: ${upstream_commit}
- Source path: kafka-connect/
EOF

echo "Synced apache/iceberg ${upstream_commit}"
