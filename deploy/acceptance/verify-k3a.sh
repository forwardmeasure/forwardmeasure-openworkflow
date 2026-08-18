#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license
# agreements. See the NOTICE file distributed with this work for additional information regarding
# copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
#

set -euo pipefail

namespace=forwardmeasure-openworkflow
runtime=${OPENWORKFLOW_K3A_RUNTIME:-openworkflow-pekko-postgresql}
service=${OPENWORKFLOW_ACCEPTANCE_SERVICE:-$runtime}
disruption_runtime=${OPENWORKFLOW_ACCEPTANCE_DISRUPTION_RUNTIME:-$runtime}
runtime_selector=${OPENWORKFLOW_ACCEPTANCE_RUNTIME_SELECTOR:-app=$runtime}
disruption_selector=${OPENWORKFLOW_ACCEPTANCE_DISRUPTION_SELECTOR:-app=$disruption_runtime}
persistence=${OPENWORKFLOW_K3A_PERSISTENCE:-PostgreSQL}
expected_engine=${OPENWORKFLOW_ACCEPTANCE_ENGINE:-pekko}
checkpoint=${OPENWORKFLOW_ACCEPTANCE_CHECKPOINT:-K3A}
keycloak_port=18100
service_port=18104
work=$(mktemp -d)
cleanup() {
  kill "${keycloak_pid:-}" "${service_pid:-}" 2>/dev/null || true
}
trap cleanup EXIT

kubectl -n "$namespace" rollout status deployment/"$runtime" --timeout=180s
gateway=$(kubectl -n "$namespace" get pods -l "$runtime_selector" \
  -o jsonpath='{.items[0].metadata.name}')
kubectl -n "$namespace" port-forward service/keycloak "${keycloak_port}:8080" >"$work/keycloak.log" 2>&1 &
keycloak_pid=$!
service_target="service/$service"
if [[ "$service" == "$runtime" ]]; then
  # Pin the gateway transport to the pod deliberately excluded from disruption. A service-level
  # port-forward may select the owner pod and disappear exactly when the recovery assertion runs.
  service_target="pod/$gateway"
fi
kubectl -n "$namespace" port-forward "$service_target" "${service_port}:8080" >"$work/service.log" 2>&1 &
service_pid=$!

for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:${keycloak_port}/realms/openworkflow" >/dev/null \
      && curl -sS "http://127.0.0.1:${service_port}/" >/dev/null; then
    break
  fi
  sleep 1
done

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

request() {
  local method=$1 url=$2 bearer=$3 correlation=$4 body=${5:-}
  local args=(-sS -o "$work/body" -w '%{http_code}' -X "$method" "$url"
    -H "Authorization: Bearer ${bearer}" -H "X-Correlation-ID: ${correlation}"
    -H "Idempotency-Key: ${correlation}")
  if [[ -n "$body" ]]; then
    args+=(-H 'Content-Type: application/json' --data "$body")
  fi
  curl "${args[@]}"
}

expect_status() {
  local expected=$1 stage=$2
  shift 2
  local actual
  actual=$(request "$@")
  if [[ "$actual" != "$expected" ]]; then
    printf '%s failed: expected HTTP %s, received %s\n' "$stage" "$expected" "$actual" >&2
    cat "$work/body" >&2
    exit 1
  fi
}

author=$(token author)
approver=$(token approver)
publisher=$(token publisher)
controller=$(token controller)
definition="k3a-$(date +%s)"
correlation_prefix="$definition"
definition_api="http://127.0.0.1:${service_port}/v1/workflow-definitions"
execution_api="http://127.0.0.1:${service_port}/api/v1/executions"

source_document=$(sed 's/wait: PT1S/wait: PT15S/' \
  openworkflow-definition/src/test/resources/compiler-golden/basic.workflow.yaml | jq -Rs .)
create=$(jq -n --arg key "$definition" --argjson source "$source_document" \
  '{definitionKey:$key,displayName:"K3A acceptance",sourceDocument:$source}')
expect_status 201 definition-create POST "$definition_api" "$author" "${correlation_prefix}-create" "$create"
expect_status 200 definition-submit POST "$definition_api/$definition/revisions/1/submit" "$author" "${correlation_prefix}-submit"
expect_status 200 definition-approve POST "$definition_api/$definition/revisions/1/approve" "$approver" "${correlation_prefix}-approve"
expect_status 200 definition-publish POST "$definition_api/$definition/revisions/1/publish" "$publisher" "${correlation_prefix}-publish"
revision_id=$(jq -er .revisionId "$work/body")

start=$(jq -n --arg revision "$revision_id" '{revisionId:$revision,input:{customer:"Ada"}}')
owner=
for attempt in $(seq 1 10); do
  expect_status 202 execution-start POST "$execution_api" "$controller" \
    "${correlation_prefix}-start-${attempt}" "$start"
  execution_id=$(jq -er .id "$work/body")
  test "$(jq -er .engineId "$work/body")" = "$expected_engine"
  if [[ "$expected_engine" = kafka-streams ]]; then
    for pod in $(kubectl -n "$namespace" get pods -l "$disruption_selector" \
      -o jsonpath='{.items[*].metadata.name}'); do
      if [[ "$disruption_runtime" != "$runtime" || "$pod" != "$gateway" ]]; then
        owner=$pod
        break
      fi
    done
    [[ -n "$owner" ]] && break
  fi
  for _ in $(seq 1 50); do
    for pod in $(kubectl -n "$namespace" get pods -l app="$runtime" \
      -o jsonpath='{.items[*].metadata.name}'); do
      if kubectl -n "$namespace" logs "$pod" --since=2m 2>/dev/null \
          | grep -Fq "Activating workflow execution 11111111-1111-1111-1111-111111111111:${execution_id}"; then
        owner=$pod
        break 2
      fi
    done
    sleep 0.1
  done
  [[ -n "$owner" ]] || { printf 'Unable to locate owner for %s\n' "$execution_id" >&2; exit 1; }
  [[ "$owner" != "$gateway" ]] && break
  owner=
done
[[ -n "$owner" ]] || { printf 'Unable to select an execution owned away from gateway %s\n' "$gateway" >&2; exit 1; }

expect_status 200 execution-running GET "$execution_api/$execution_id" "$controller" \
  "${correlation_prefix}-running"
test "$(jq -er .state "$work/body")" = RUNNING

kubectl -n "$namespace" delete pod "$owner" --wait=false >/dev/null
kubectl -n "$namespace" rollout status deployment/"$disruption_runtime" --timeout=180s >/dev/null

for _ in $(seq 1 90); do
  expect_status 200 execution-query GET "$execution_api/$execution_id" "$controller" "${correlation_prefix}-query"
  state=$(jq -er .state "$work/body")
  [[ "$state" = COMPLETED ]] && break
  sleep 1
done
test "$state" = COMPLETED
jq -e '.output.ready == true' "$work/body" >/dev/null

expect_status 200 execution-after-disruption GET "$execution_api/$execution_id" "$controller" "${correlation_prefix}-after-disruption"
test "$(jq -er .state "$work/body")" = COMPLETED
jq -e '.output.ready == true' "$work/body" >/dev/null

printf '%s %s verified: three-node %s runtime recovered %s after terminating pod %s mid-execution.\n' \
  "$checkpoint" "$persistence" "$expected_engine" "$execution_id" "$owner"
