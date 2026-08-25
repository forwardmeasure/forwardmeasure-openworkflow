#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Rendered via `helmfile build`, not a raw multi-file yq merge of the
# environment files directly - several are .gotmpl files with unrendered
# Go-template expressions in scalar values (e.g. every imageVersions.*.tag
# in gcp-openworkflow-prod.yaml.gotmpl is `{{ requiredEnv
# "OPENWORKFLOW_VERSION" | quote }}`), and yq parsing that raw text
# misreads `{{ }}` as YAML flow-mapping syntax, silently corrupting the
# value instead of erroring. Confirmed the hard way: a raw `yq ea ireduce`
# merge of gcp-openworkflow-prod's files turned every one of those tags
# into literal garbage (`tag: {}:` instead of `tag: "1.0.0"`) with no
# error at all. Mirrors forwardmeasure-platform's own
# scripts/environment-value.sh, including its select(di==0): `helmfile
# build` emits one document per top-level helmfiles: entry (11 of them
# here), each with its own renderedvalues: block - without narrowing to
# one, every value comes back as N identical copies joined by `---`
# separators (confirmed md5-identical across all of them - safe to take
# just the first).
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ENVIRONMENT="${1:?environment is required}"
KEY="${2:?value key is required}"

helmfile --file "${SCRIPT_DIR}/helmfile.yaml.gotmpl" --environment "${ENVIRONMENT}" build 2>/dev/null \
  | yq 'select(di==0) | .renderedvalues' \
  | yq -r ".${KEY} // \"\""
