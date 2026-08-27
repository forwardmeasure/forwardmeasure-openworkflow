#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
set -euo pipefail

namespace=${OPENWORKFLOW_ACCEPTANCE_NAMESPACE:-forwardmeasure-openworkflow}
identity_namespace=${OPENWORKFLOW_IDENTITY_NAMESPACE:-keycloak}
identity_service=${OPENWORKFLOW_IDENTITY_SERVICE:-keycloak}
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
work=$(mktemp -d)

cleanup() {
  kill "${keycloak_pid:-}" "${service_pid:-}" 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

if [[ ${OPENWORKFLOW_K5_CHILD:-false} != true ]]; then
  for profile in postgresql cassandra; do
    OPENWORKFLOW_K5_CHILD=true \
    OPENWORKFLOW_K5_RUNTIME="openworkflow-pekko-$profile" \
    OPENWORKFLOW_K5_SERVICE="openworkflow-pekko-$profile" \
    OPENWORKFLOW_K5_SELECTOR="app=openworkflow-pekko-$profile" \
    OPENWORKFLOW_K5_ENGINE=pekko \
    OPENWORKFLOW_K5_PROFILE="$profile" \
      "$script_dir/verify-k5.sh"
  done
  for framework in quarkus spring micronaut; do
    OPENWORKFLOW_K5_CHILD=true \
    OPENWORKFLOW_K5_RUNTIME=openworkflow-kafka \
    OPENWORKFLOW_K5_SERVICE="openworkflow-definition-$framework" \
    OPENWORKFLOW_K5_SELECTOR=app=openworkflow-kafka \
    OPENWORKFLOW_K5_ENGINE=kafka-streams \
    OPENWORKFLOW_K5_PROFILE="$framework" \
      "$script_dir/verify-k5.sh"
  done
  printf 'K5 verified: durable pause/relocation/resume and in-flight cancellation across both engines, both Pekko stores, and all framework gateways.\n'
  exit 0
fi

runtime=${OPENWORKFLOW_K5_RUNTIME:?runtime is required}
service=${OPENWORKFLOW_K5_SERVICE:?service is required}
selector=${OPENWORKFLOW_K5_SELECTOR:?selector is required}
engine=${OPENWORKFLOW_K5_ENGINE:?engine is required}
profile=${OPENWORKFLOW_K5_PROFILE:?profile is required}
keycloak_port=$((18100 + RANDOM % 300))
service_port=$((18400 + RANDOM % 300))

kubectl -n "$namespace" rollout status "deployment/$runtime" --timeout=240s
gateway=$(kubectl -n "$namespace" get pods -l "$selector" -o jsonpath='{.items[0].metadata.name}')
kubectl -n "$identity_namespace" port-forward service/"$identity_service" "$keycloak_port:8080" >"$work/keycloak.log" 2>&1 &
keycloak_pid=$!
start_service_forward() {
  local target="service/$service"
  [[ "$service" != "$runtime" ]] || target="pod/$gateway"
  kubectl -n "$namespace" port-forward "$target" "$service_port:8080" >"$work/service.log" 2>&1 &
  service_pid=$!
}
start_service_forward

for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:$keycloak_port/realms/openworkflow" >/dev/null \
      && curl -sS "http://127.0.0.1:$service_port/" >/dev/null; then
    break
  fi
  sleep 1
done

token() {
  local username=$1
  curl -fsS -H 'Host: keycloak:8080' -X POST \
    "http://127.0.0.1:$keycloak_port/realms/openworkflow/protocol/openid-connect/token" \
    -d client_id=openworkflow -d client_secret=openworkflow-k1-client \
    -d "username=$username" -d password=openworkflow-k2 -d grant_type=password \
    --data-urlencode 'scope=openid organization:tenant-a' | jq -er .access_token
}
author=$(token author)
approver=$(token approver)
publisher=$(token publisher)
controller=$(token controller)

api="http://127.0.0.1:$service_port/api/v1/executions"
request() {
  local method=$1 url=$2 bearer=$3 correlation=$4 body=${5:-} version=${6:-}
  local args=(-sS -o "$work/body" -w '%{http_code}' -X "$method" "$url"
    -H "Authorization: Bearer $bearer" -H "X-Correlation-ID: $correlation"
    -H "Idempotency-Key: $correlation")
  [[ -z "$version" ]] || args+=(-H "If-Match: $version")
  [[ -z "$body" ]] || args+=(-H 'Content-Type: application/json' --data "$body")
  curl "${args[@]}"
}
expect() {
  local expected=$1 stage=$2; shift 2
  local actual
  actual=$(request "$@")
  [[ "$actual" == "$expected" ]] || {
    printf '%s: expected HTTP %s, got %s\n' "$stage" "$expected" "$actual" >&2
    cat "$work/body" >&2
    exit 1
  }
}
await_state() {
  local execution=$1 expected=$2 prefix=$3 state=
  for attempt in $(seq 1 120); do
    expect 200 "$prefix-query" GET "$api/$execution" "$controller" "$prefix-query-$attempt"
    state=$(jq -er .state "$work/body")
    [[ "$state" == "$expected" ]] && return 0
    [[ "$state" =~ ^(COMPLETED|FAILED|CANCELLED)$ ]] && break
    sleep 1
  done
  printf '%s did not reach %s (last state %s)\n' "$execution" "$expected" "$state" >&2
  return 1
}
await_active_boundary() {
  local execution=$1 prefix=$2 state=
  for attempt in $(seq 1 120); do
    expect 200 "$prefix-query" GET "$api/$execution" "$controller" "$prefix-query-$attempt"
    state=$(jq -er .state "$work/body")
    [[ "$state" == RUNNING || "$state" == WAITING ]] && return 0
    [[ "$state" =~ ^(COMPLETED|FAILED|CANCELLED)$ ]] && break
    sleep 1
  done
  printf '%s did not reach an active safe boundary (last state %s)\n' "$execution" "$state" >&2
  return 1
}
start_waiting_execution() {
  local prefix=$1 revision=${OPENWORKFLOW_K5_REVISION_ID:?published waiting revision is required}
  expect 202 "$prefix-start" POST "$api" "$controller" "$prefix-start" \
    "$(jq -cn --arg revision "$revision" '{revisionId:$revision,input:{checkpoint:"K5"}}')"
  jq -er .id "$work/body"
}
control() {
  local execution=$1 operation=$2 prefix=$3
  expect 200 "$prefix-read" GET "$api/$execution" "$controller" "$prefix-read"
  local version
  version=$(jq -er .version "$work/body")
  expect 202 "$prefix-$operation" POST "$api/$execution/$operation" "$controller" "$prefix-$operation" \
    "$(jq -cn --arg reason "K5 $operation disruption proof" '{reason:$reason}')" "$version"
}
relocate_owner() {
  local execution=$1 owner= survivor=
  if [[ "$engine" == kafka-streams ]]; then
    kubectl -n "$namespace" rollout restart "deployment/$runtime" >/dev/null
    kubectl -n "$namespace" rollout status "deployment/$runtime" --timeout=240s >/dev/null
    return
  fi
  for _ in $(seq 1 60); do
    for pod in $(kubectl -n "$namespace" get pods -l "$selector" -o jsonpath='{.items[*].metadata.name}'); do
      if kubectl -n "$namespace" logs "$pod" --since=5m 2>/dev/null \
          | grep -Fq "Activating workflow execution 11111111-1111-1111-1111-111111111111:$execution"; then
        owner=$pod
        break 2
      fi
    done
    sleep 0.2
  done
  [[ -n "$owner" ]] || { printf 'Unable to locate owner for %s\n' "$execution" >&2; exit 1; }
  for pod in $(kubectl -n "$namespace" get pods -l "$selector" -o jsonpath='{.items[*].metadata.name}'); do
    [[ "$pod" == "$owner" ]] || { survivor=$pod; break; }
  done
  [[ -n "$survivor" ]] || { printf 'Unable to select survivor for %s\n' "$execution" >&2; exit 1; }
  if [[ "$gateway" == "$owner" ]]; then
    kill "$service_pid" 2>/dev/null || true
    wait "$service_pid" 2>/dev/null || true
    gateway=$survivor
    start_service_forward
    for _ in $(seq 1 30); do
      curl -sS "http://127.0.0.1:$service_port/" >/dev/null 2>&1 && break
      sleep 1
    done
  fi
  kubectl -n "$namespace" delete pod "$owner" --wait=false >/dev/null
  kubectl -n "$namespace" rollout status "deployment/$runtime" --timeout=240s >/dev/null
  kill "$service_pid" 2>/dev/null || true
  wait "$service_pid" 2>/dev/null || true
  gateway=$(kubectl -n "$namespace" get pods -l "$selector" -o json \
    | jq -er '.items[] | select(.metadata.deletionTimestamp == null) | select(any(.status.conditions[]?; .type == "Ready" and .status == "True")) | .metadata.name' \
    | head -1)
  start_service_forward
  for _ in $(seq 1 30); do
    curl -sS "http://127.0.0.1:$service_port/" >/dev/null 2>&1 && break
    sleep 1
  done
}

prefix="k5-$engine-$profile-$(date +%s)-$RANDOM"
definition_api="http://127.0.0.1:$service_port/v1/workflow-definitions"
definition_key=${prefix//[^a-zA-Z0-9_-]/-}
source_document=$(sed 's/wait: PT1S/wait: PT30S/' \
  openworkflow-definition/src/test/resources/compiler-golden/basic.workflow.yaml | jq -Rs .)
create=$(jq -n --arg key "$definition_key" --argjson source "$source_document" \
  '{definitionKey:$key,displayName:"K5 durable control",sourceDocument:$source}')
expect 201 "$prefix-create" POST "$definition_api" "$author" "$prefix-create" "$create"
expect 200 "$prefix-submit" POST "$definition_api/$definition_key/revisions/1/submit" "$author" "$prefix-submit"
expect 200 "$prefix-approve" POST "$definition_api/$definition_key/revisions/1/approve" "$approver" "$prefix-approve"
expect 200 "$prefix-publish" POST "$definition_api/$definition_key/revisions/1/publish" "$publisher" "$prefix-publish"
export OPENWORKFLOW_K5_REVISION_ID
OPENWORKFLOW_K5_REVISION_ID=$(jq -er .revisionId "$work/body")

paused=$(start_waiting_execution "$prefix-pause")
await_active_boundary "$paused" "$prefix-before-pause"
control "$paused" pause "$prefix"
await_state "$paused" PAUSED "$prefix-paused"

relocate_owner "$paused"
control "$paused" resume "$prefix"
await_state "$paused" COMPLETED "$prefix-resumed"

cancelled=$(start_waiting_execution "$prefix-cancel")
await_active_boundary "$cancelled" "$prefix-before-cancel"
control "$cancelled" cancel "$prefix-cancel"
await_state "$cancelled" CANCELLED "$prefix-cancelled"
relocate_owner "$cancelled"
sleep 5
expect 200 "$prefix-terminal-read" GET "$api/$cancelled" "$controller" "$prefix-terminal-read"
test "$(jq -er .state "$work/body")" = CANCELLED

printf 'K5 %s/%s verified: %s resumed after owner relocation; %s remained irreversibly cancelled.\n' \
  "$engine" "$profile" "$paused" "$cancelled"
