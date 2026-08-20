#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

map=docs/migration/standalone-migration-map.md
retirement=docs/migration/retirement-checklist.md
import_schema=config/migration/definition-import-manifest-v1.schema.json
attestation_schema=config/migration/retirement-attestation-v1.schema.json

jq empty "$import_schema" "$attestation_schema"
jq -e '
  .properties.source.properties.product.enum == ["OKS", "OAE"] and
  (.properties.definitions.items.properties.sourceDigest.pattern | contains("{64}")) and
  (.properties.definitions.items.properties.lifecycleEvidence.items.properties.state.enum | index("PUBLISHED"))
' "$import_schema" >/dev/null
jq -e '
  (.properties.executionDrain.enum | index("engine-retained")) and
  (.properties.shadowComparison.enum | index("not-applicable-no-source-deployment")) and
  .properties.consumerMigration.enum == ["verified"] and
  .properties.restoreTest.enum == ["verified"]
' "$attestation_schema" >/dev/null

for required in 'Coordinates and APIs' 'Configuration and deployment' 'Data objects and topics' 'Pekko' 'helmfile' 'Studio' 'Cutover sequence'; do
  rg -q "$required" "$map"
done
rg -q 'remain pinned to their original engine' "$map"
rg -q 'never moves an already-started execution between engines' "$map"
rg -q 'never reported as a successful comparison' "$retirement"
rg -Uq 'only after[^\n]*\n[^\n]*rollback window' "$retirement"

test "$(git -C /home/pn/Documents/code/forwardmeasure/openworkflow-kafka-streams rev-parse HEAD)" = 97db2233dec401b4df0413b00f346e53df60b9d7
test "$(git -C /home/pn/Documents/code/forwardmeasure/openworkflow-actor-engine rev-parse HEAD)" = 77e8784c32508e81c3d00d802f549550380a8df9
test -f openworkflow-api-specifications/openworkflow-services-api-spec/definition-management.openapi.yaml
test -f openworkflow-api-specifications/openworkflow-services-api-spec/execution-management.openapi.yaml

if rg -n '<artifactId>(openworkflow-kafka-streams-parent|openworkflow-actor-engine-parent)</artifactId>' --glob 'pom.xml' .; then
  echo 'Standalone parent dependency remains in the unified reactor' >&2
  exit 1
fi

echo 'WP11 verified: replacement maps are source-pinned, import and retirement evidence are machine-constrained, live engine state translation is forbidden, and retirement remains gated by drain, restore, consumer and rollback evidence.'
