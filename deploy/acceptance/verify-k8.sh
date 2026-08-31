#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license
# agreements. See the NOTICE file distributed with this work for additional information regarding
# copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
#
# Repeatable pause/cancel lifecycle load: drives run-lifecycle-load.mjs against a real deployment,
# obtaining the four actor tokens (author/approver/publisher/controller) the real
# execution-management API requires (see run-lifecycle-load.mjs's own header comment) the same way
# verify-k3a.sh/verify-k5.sh already do, keeping the Node script itself free of cluster/Keycloak
# wiring so it stays purely about driving load and measuring latency percentiles.

set -euo pipefail

namespace=${OPENWORKFLOW_ACCEPTANCE_NAMESPACE:-forwardmeasure-openworkflow}
identity_namespace=${OPENWORKFLOW_IDENTITY_NAMESPACE:-keycloak}
identity_service=${OPENWORKFLOW_IDENTITY_SERVICE:-keycloak}
runtime=${OPENWORKFLOW_K8_RUNTIME:-openworkflow-pekko-postgresql}
service=${OPENWORKFLOW_K8_SERVICE:-$runtime}
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
keycloak_port=18100
service_port=18117
work=$(mktemp -d)
cleanup() {
  kill "${keycloak_pid:-}" "${service_pid:-}" 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

kubectl -n "$namespace" rollout status "deployment/$runtime" --timeout=180s
kubectl -n "$identity_namespace" port-forward service/"$identity_service" "${keycloak_port}:8080" >"$work/keycloak.log" 2>&1 &
keycloak_pid=$!
kubectl -n "$namespace" port-forward "service/$service" "${service_port}:8080" >"$work/service.log" 2>&1 &
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
    -d client_id=openworkflow \
    -d client_secret=openworkflow-k1-client \
    -d "username=${username}" \
    -d password=openworkflow-k2 \
    -d grant_type=password \
    --data-urlencode 'scope=openid organization:tenant-a' | jq -er .access_token
}

export OPENWORKFLOW_BASE_URL="http://127.0.0.1:${service_port}"
export OPENWORKFLOW_AUTHOR_TOKEN=$(token author)
export OPENWORKFLOW_APPROVER_TOKEN=$(token approver)
export OPENWORKFLOW_PUBLISHER_TOKEN=$(token publisher)
export OPENWORKFLOW_CONTROLLER_TOKEN=$(token controller)

node "$script_dir/run-lifecycle-load.mjs"
