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

framework=${1:?usage: verify-k2.sh quarkus|spring|micronaut}
case "$framework" in
  quarkus) service_port=18101 ;;
  spring) service_port=18102 ;;
  micronaut) service_port=18103 ;;
  *) printf 'Unsupported framework: %s\n' "$framework" >&2; exit 2 ;;
esac

namespace=forwardmeasure-openworkflow
keycloak_port=18100
service="openworkflow-definition-${framework}"
definition="k2-${framework}-$(date +%s)"
work=$(mktemp -d)
cleanup() {
  kill "${keycloak_pid:-}" "${service_pid:-}" 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

kubectl -n "$namespace" rollout status "deployment/${service}" --timeout=180s
kubectl -n "$namespace" port-forward service/keycloak "${keycloak_port}:8080" >"$work/keycloak.log" 2>&1 &
keycloak_pid=$!
kubectl -n "$namespace" port-forward "service/${service}" "${service_port}:8080" >"$work/service.log" 2>&1 &
service_pid=$!

for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:${keycloak_port}/realms/openworkflow" >/dev/null; then
    break
  fi
  sleep 1
done
sleep 1

token() {
  local username=$1
  curl -fsS -H 'Host: keycloak:8080' -X POST \
    "http://127.0.0.1:${keycloak_port}/realms/openworkflow/protocol/openid-connect/token" \
    -d client_id=forwardmeasure-openworkflow \
    -d client_secret=openworkflow-k1-client \
    -d "username=${username}" \
    -d password=openworkflow-k2 \
    -d grant_type=password \
    --data-urlencode 'scope=openid organization:tenant-a' | jq -er .access_token
}

author=$(token author)
approver=$(token approver)
publisher=$(token publisher)
api="http://127.0.0.1:${service_port}/v1/workflow-definitions"

request() {
  local method=$1 url=$2 bearer=$3 correlation=$4 body=${5:-}
  local args=(-sS -o "$work/body" -w '%{http_code}' -X "$method" "$url"
    -H "Authorization: Bearer ${bearer}" -H "X-Correlation-ID: ${correlation}")
  if [[ -n "$body" ]]; then
    args+=(-H 'Content-Type: application/json' --data "$body")
  fi
  curl "${args[@]}"
}

invalid=$(jq -n --arg key "${definition}-invalid" \
  '{definitionKey:$key,displayName:"Invalid K2",sourceDocument:"not: [valid"}')
status=$(request POST "$api" "$author" "${framework}-invalid" "$invalid")
if [[ "$status" =~ ^2 ]]; then
  printf '%s admitted an invalid definition\n' "$framework" >&2
  exit 1
fi

source_one=$(jq -Rs . < openworkflow-definition/src/test/resources/compiler-golden/basic.workflow.yaml)
create=$(jq -n --arg key "$definition" --argjson source "$source_one" \
  '{definitionKey:$key,displayName:"K2 acceptance",sourceDocument:$source}')
test "$(request POST "$api" "$author" "${framework}-create" "$create")" = 201
test "$(jq -r .revisionNumber "$work/body")" = 1

source_two=$(sed 's/ready: true/ready: false/' \
  openworkflow-definition/src/test/resources/compiler-golden/basic.workflow.yaml | jq -Rs .)
revise=$(jq -n --argjson source "$source_two" \
  '{displayName:"K2 acceptance revision two",sourceDocument:$source}')
test "$(request POST "$api/$definition/revisions" "$author" "${framework}-revise" "$revise")" = 201
test "$(jq -r .revisionNumber "$work/body")" = 2

test "$(request POST "$api/$definition/revisions/2/submit" "$author" "${framework}-submit")" = 200
test "$(request POST "$api/$definition/revisions/2/approve" "$approver" "${framework}-approve")" = 200
test "$(request POST "$api/$definition/revisions/2/publish" "$publisher" "${framework}-publish")" = 200
published_digest=$(jq -er .resolvedDigest "$work/body")

kubectl -n "$namespace" rollout restart "deployment/${service}" >/dev/null
kubectl -n "$namespace" rollout status "deployment/${service}" --timeout=180s >/dev/null
kill "$service_pid" 2>/dev/null || true
kubectl -n "$namespace" port-forward "service/${service}" "${service_port}:8080" >"$work/service-after-restart.log" 2>&1 &
service_pid=$!
sleep 2
test "$(request GET "$api/$definition/revisions/2" "$publisher" "${framework}-retrieve")" = 200
test "$(jq -er .resolvedDigest "$work/body")" = "$published_digest"

printf 'K2 %s verified: invalid rejected, revision two published, restart preserved digest %s.\n' \
  "$framework" "$published_digest"
