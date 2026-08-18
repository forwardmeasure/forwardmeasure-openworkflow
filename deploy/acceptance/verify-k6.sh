#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

if [[ ${OPENWORKFLOW_K6_CHILD:-false} != true ]]; then
  for profile in postgresql cassandra; do
    OPENWORKFLOW_K6_CHILD=true \
    OPENWORKFLOW_K6_SERVICE="openworkflow-pekko-$profile" \
    OPENWORKFLOW_K6_ENGINE=pekko \
      "$script_dir/verify-k6.sh"
  done
  for framework in quarkus spring micronaut; do
    OPENWORKFLOW_K6_CHILD=true \
    OPENWORKFLOW_K6_SERVICE="openworkflow-definition-$framework" \
    OPENWORKFLOW_K6_ENGINE=kafka-streams \
      "$script_dir/verify-k6.sh"
  done
  printf 'K6 verified: immutable resource-bundle execution and NATS delivery across both engines, both Pekko stores, and all framework gateways.\n'
  exit 0
fi

context=${OPENWORKFLOW_ACCEPTANCE_CONTEXT:-kind-openworkflow-acceptance}
namespace=${OPENWORKFLOW_ACCEPTANCE_NAMESPACE:-forwardmeasure-openworkflow}
service=${OPENWORKFLOW_K6_SERVICE:-openworkflow-definition-quarkus}
expected_engine=${OPENWORKFLOW_K6_ENGINE:-kafka-streams}
keycloak_port=$((18100 + RANDOM % 300))
service_port=$((18400 + RANDOM % 300))
nats_port=$((18700 + RANDOM % 300))
work=$(mktemp -d)

cleanup() {
  kill "${keycloak_pid:-}" "${service_pid:-}" "${nats_pid:-}" 2>/dev/null || true
}
trap cleanup EXIT

for fixture in k7-echo k7-event-sink k7-grpcbin k7-nats k7-ctk-fixture; do
  kubectl --context "$context" -n "$namespace" rollout status "deployment/$fixture" --timeout=180s >/dev/null
done
kubectl --context "$context" -n "$namespace" rollout status "deployment/$service" --timeout=240s >/dev/null
kubectl --context "$context" -n "$namespace" port-forward service/keycloak "$keycloak_port:8080" >"$work/keycloak.log" 2>&1 &
keycloak_pid=$!
kubectl --context "$context" -n "$namespace" port-forward "service/$service" "$service_port:8080" >"$work/service.log" 2>&1 &
service_pid=$!
kubectl --context "$context" -n "$namespace" port-forward service/k7-nats "$nats_port:8222" >"$work/nats.log" 2>&1 &
nats_pid=$!

ready=false
for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:$keycloak_port/realms/openworkflow" >/dev/null 2>&1 \
      && curl -sS "http://127.0.0.1:$service_port/" >/dev/null 2>&1 \
      && curl -fsS "http://127.0.0.1:$nats_port/varz" >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
test "$ready" = true

token() {
  curl -fsS -H 'Host: keycloak:8080' -X POST \
    "http://127.0.0.1:$keycloak_port/realms/openworkflow/protocol/openid-connect/token" \
    -d client_id=forwardmeasure-openworkflow -d client_secret=openworkflow-k1-client \
    -d "username=$1" -d password=openworkflow-k2 -d grant_type=password \
    --data-urlencode 'scope=openid organization:tenant-a' | jq -er .access_token
}
author=$(token author)
approver=$(token approver)
publisher=$(token publisher)
controller=$(token controller)

request() {
  local method=$1 url=$2 bearer=$3 correlation=$4 body=${5:-}
  local args=(-sS -o "$work/body" -w '%{http_code}' -X "$method" "$url"
    -H "Authorization: Bearer $bearer" -H "X-Correlation-ID: $correlation"
    -H "Idempotency-Key: $correlation")
  [[ -z "$body" ]] || args+=(-H 'Content-Type: application/json' --data "$body")
  curl "${args[@]}"
}
expect() {
  local expected=$1 stage=$2
  shift 2
  local actual
  actual=$(request "$@")
  [[ "$actual" = "$expected" ]] || {
    printf '%s: expected HTTP %s, received %s\n' "$stage" "$expected" "$actual" >&2
    cat "$work/body" >&2
    exit 1
  }
}

prefix="k6-nats-$(date +%s)-$RANDOM"
definition_api="http://127.0.0.1:$service_port/v1/workflow-definitions"
execution_api="http://127.0.0.1:$service_port/api/v1/executions"
source_document=$(cat <<'YAML'
document:
  dsl: '1.0.3'
  namespace: acceptance
  name: nats-resource-bundle
  version: '1.0.0'
do:
  - publish:
      call: asyncapi
      with:
        document:
          endpoint: http://k7-event-sink/contracts/nats-asyncapi.yaml
        operation: publishEvidence
        server:
          name: acceptance
        message:
          payload:
            checkpoint: K6
            proof: immutable-resource-bundle
YAML
)
create=$(jq -cn --arg key "$prefix" --arg source "$source_document" \
  '{definitionKey:$key,displayName:"K6 immutable NATS acceptance",sourceDocument:$source}')
expect 201 definition-create POST "$definition_api" "$author" "$prefix-create" "$create"
expect 200 definition-submit POST "$definition_api/$prefix/revisions/1/submit" "$author" "$prefix-submit"
expect 200 definition-approve POST "$definition_api/$prefix/revisions/1/approve" "$approver" "$prefix-approve"
expect 200 definition-publish POST "$definition_api/$prefix/revisions/1/publish" "$publisher" "$prefix-publish"
revision_id=$(jq -er .revisionId "$work/body")

before=$(curl -fsS "http://127.0.0.1:$nats_port/varz" | jq -er .in_msgs)
start=$(jq -cn --arg revision "$revision_id" '{revisionId:$revision,input:{checkpoint:"K6"}}')
expect 202 execution-start POST "$execution_api" "$controller" "$prefix-start" "$start"
execution_id=$(jq -er .id "$work/body")
test "$(jq -er .engineId "$work/body")" = "$expected_engine"

state=
for attempt in $(seq 1 120); do
  expect 200 execution-query GET "$execution_api/$execution_id" "$controller" "$prefix-query-$attempt"
  state=$(jq -er .state "$work/body")
  [[ "$state" = COMPLETED ]] && break
  [[ "$state" =~ ^(FAILED|CANCELLED)$ ]] && {
    printf 'Execution %s terminated as %s\n' "$execution_id" "$state" >&2
    cat "$work/body" >&2
    exit 1
  }
  sleep 1
done
test "$state" = COMPLETED
after=$(curl -fsS "http://127.0.0.1:$nats_port/varz" | jq -er .in_msgs)
test "$after" -gt "$before"

printf 'K6 verified: %s completed through %s and delivered %s NATS message(s) from its immutable resource bundle.\n' \
  "$execution_id" "$expected_engine" "$((after - before))"
