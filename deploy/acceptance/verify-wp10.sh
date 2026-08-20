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

namespace=${OPENWORKFLOW_NAMESPACE:-forwardmeasure-openworkflow}
identity_namespace=${OPENWORKFLOW_IDENTITY_NAMESPACE:-keycloak}
identity_service=${OPENWORKFLOW_IDENTITY_SERVICE:-keycloak}
local_port=${OPENWORKFLOW_KEYCLOAK_PORT:-18081}
base="http://127.0.0.1:${local_port}"
policy=${OPENWORKFLOW_ENTITY_POLICY:-config/keycloak/entity-intelligence-capability-pack-v1.json}

kubectl -n "$identity_namespace" port-forward svc/"$identity_service" "${local_port}:8080" >/tmp/openworkflow-wp10-port-forward.log 2>&1 &
forward_pid=$!
trap 'kill "$forward_pid" 2>/dev/null || true' EXIT
for _ in $(seq 1 30); do
  curl -fsS "$base/realms/master" >/dev/null 2>&1 && break
  sleep 1
done

token=$(curl -fsS -X POST "$base/realms/master/protocol/openid-connect/token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode client_id=admin-cli \
  --data-urlencode grant_type=password \
  --data-urlencode "username=${KEYCLOAK_ADMIN_USERNAME:-admin}" \
  --data-urlencode "password=${KEYCLOAK_ADMIN_PASSWORD:-openworkflow-k1-admin}" | jq -r .access_token)
auth=(-H "Authorization: Bearer $token")
admin="$base/admin/realms/openworkflow"
client=00000000-0000-0000-0000-000000000001
orgs=$(curl -fsS "${auth[@]}" "$admin/organizations?briefRepresentation=false")
roles=$(curl -fsS "${auth[@]}" "$admin/clients/$client/roles")

[ "$(printf '%s' "$orgs" | jq '[.[] | select(.attributes["forwardmeasure.tenant-did"][0] == ("did:forwardmeasure:tenant:" + .attributes["forwardmeasure.tenant-id"][0])) | select(.attributes["forwardmeasure.capability-pack.entity-intelligence"][0] == "1")] | length')" -eq 2 ]
[ "$(printf '%s' "$roles" | jq '[.[].name | select(startswith("entity-intelligence-"))] | length')" -eq 11 ]
[ "$(printf '%s' "$roles" | jq '[.[].name | select(. == "workflow-internal")] | length')" -eq 0 ]

for organization_id in $(printf '%s' "$orgs" | jq -r '.[].id'); do
  groups=$(curl -fsS "${auth[@]}" "$admin/organizations/$organization_id/groups")
  [ "$(printf '%s' "$groups" | jq '[.[].name | select(startswith("entity-intelligence-") and . != "entity-intelligence-workflow-invoker")] | length')" -eq 10 ]
  [ "$(printf '%s' "$groups" | jq '[.[].name | select(. == "entity-intelligence-workflow-invoker")] | length')" -eq 0 ]
  for group_id in $(printf '%s' "$groups" | jq -r '.[] | select(.name | startswith("entity-intelligence-")) | .id'); do
    members=$(curl -fsS "${auth[@]}" "$admin/organizations/$organization_id/groups/$group_id/members")
    [ "$(printf '%s' "$members" | jq 'length')" -eq 0 ]
  done
done

jq -e '
  .constraints.referencePopulationMakerChecker == true and
  .constraints.memberRoleAssignmentOnPackInstall == false and
  .constraints.databaseSchemaSelectorInTokensForbidden == true and
  (.roleGrants["entity-intelligence-reference-population-submitter"] | index("reference-population:approve") | not) and
  (.roleGrants["entity-intelligence-reference-population-approver"] | index("reference-population:submit") | not) and
  .roleGrants["entity-intelligence-workflow-invoker"] == ["execution:start"]
' "$policy" >/dev/null

echo "WP10 verified: Entity Intelligence roles and empty Organization groups reconcile for both tenants, trusted DIDs remain server-side, maker-checker grants are separated, and the workload invoker is narrow and non-human."
