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

set -euo pipefail

namespace=forwardmeasure-openworkflow
identity_namespace=${OPENWORKFLOW_IDENTITY_NAMESPACE:-keycloak}
identity_service=${OPENWORKFLOW_IDENTITY_SERVICE:-keycloak}
local_port=${OPENWORKFLOW_K1_KEYCLOAK_PORT:-18080}
keycloak_url="http://127.0.0.1:${local_port}"
tenant_a=11111111-1111-1111-1111-111111111111
tenant_b=22222222-2222-2222-2222-222222222222

kubectl -n "$namespace" rollout status deployment/postgresql --timeout=120s
kubectl -n "$identity_namespace" rollout status statefulset/"$identity_service" --timeout=180s
kubectl -n "$namespace" wait --for=condition=complete job/openworkflow-tenant-reconciliation --timeout=120s
kubectl -n "$namespace" wait --for=condition=complete job/openworkflow-migrations --timeout=120s

port_forward_log=$(mktemp)
kubectl -n "$identity_namespace" port-forward service/"$identity_service" "${local_port}:8080" >"$port_forward_log" 2>&1 &
port_forward_pid=$!
trap 'kill "$port_forward_pid" 2>/dev/null || true; rm -f "$port_forward_log"' EXIT

for _ in $(seq 1 30); do
  curl -fsS "${keycloak_url}/realms/openworkflow/.well-known/authzen-configuration" >/dev/null && break
  sleep 1
done

secret_value() {
  kubectl -n "$namespace" get secret openworkflow-foundation \
    -o "jsonpath={.data.$1}" | base64 --decode
}

admin_username=$(secret_value KC_BOOTSTRAP_ADMIN_USERNAME)
admin_password=$(secret_value KC_BOOTSTRAP_ADMIN_PASSWORD)
client_secret=$(secret_value OPENWORKFLOW_CLIENT_SECRET)
admin_token=$(curl -fsS -X POST "${keycloak_url}/realms/master/protocol/openid-connect/token" \
  -d client_id=admin-cli -d "username=${admin_username}" -d "password=${admin_password}" \
  -d grant_type=password | jq -er .access_token)

printf 'Verifying Keycloak Organizations...\n'
organizations=$(curl -fsS -H "Authorization: Bearer ${admin_token}" \
  "${keycloak_url}/admin/realms/openworkflow/organizations")
for tenant_id in "$tenant_a" "$tenant_b"; do
  organization_id=$(jq -er --arg tenant_id "$tenant_id" \
    '.[] | select(.alias == (if $tenant_id | startswith("1111") then "tenant-a" else "tenant-b" end)) | .id' \
    <<<"$organizations")
  curl -fsS -H "Authorization: Bearer ${admin_token}" \
    "${keycloak_url}/admin/realms/openworkflow/organizations/${organization_id}" \
    | jq -e --arg tenant_id "$tenant_id" \
      '.enabled == true and .attributes["forwardmeasure.tenant-id"] == [$tenant_id] and .attributes["forwardmeasure.capability-pack.openworkflow"] == ["1"]' \
      >/dev/null
done

printf 'Verifying tenant schemas...\n'
expected_schemas=$(printf 't_%s\nt_%s\n' "${tenant_a//-/}" "${tenant_b//-/}")
actual_schemas=$(kubectl -n "$namespace" exec deployment/postgresql -- \
  psql -U openworkflow -d openworkflow -Atc \
  "select schema_name from information_schema.schemata where schema_name like 't_%' order by schema_name")
test "$actual_schemas" = "$expected_schemas"

printf 'Verifying AuthZEN cross-tenant denial...\n'
client_token=$(curl -fsS -X POST "${keycloak_url}/realms/openworkflow/protocol/openid-connect/token" \
  -d client_id=openworkflow -d "client_secret=${client_secret}" \
  -d grant_type=client_credentials | jq -er .access_token)
organization_a_id=$(jq -er '.[] | select(.alias == "tenant-a") | .id' <<<"$organizations")
correlation_id=k1-cross-tenant-denial
headers=$(mktemp)
trap 'kill "$port_forward_pid" 2>/dev/null || true; rm -f "$port_forward_log" "$headers"' EXIT
decision=$(curl -fsS -D "$headers" -X POST \
  "${keycloak_url}/realms/openworkflow/authzen/access/v1/evaluation" \
  -H "Authorization: Bearer ${client_token}" -H 'Content-Type: application/json' \
  -H "X-Request-ID: ${correlation_id}" \
  --data "{\"subject\":{\"type\":\"user\",\"id\":\"id:k1-actor\",\"properties\":{\"active_organization_id\":\"${organization_a_id}\",\"organization_roles\":[\"workflow-viewer\"]}},\"resource\":{\"type\":\"openworkflow-definition\",\"id\":\"tenant-b-definition\",\"properties\":{\"tenant_id\":\"${tenant_b}\"}},\"action\":{\"name\":\"definition:read\"},\"context\":{\"active_organization_id\":\"${organization_a_id}\",\"tenant_id\":\"${tenant_b}\",\"policy_version\":\"1\"}}" \
  | jq -r .decision)
test "$decision" = false
tr -d '\r' <"$headers" | grep -Fiqx "X-Request-ID: ${correlation_id}"

printf 'K1 verified: two Organizations, two tenant schemas, and AuthZEN cross-tenant denial.\n'
