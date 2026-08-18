#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
set -euo pipefail

namespace=forwardmeasure-openworkflow
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

for framework in quarkus spring micronaut; do
  deployment="openworkflow-studio-$framework"
  kubectl -n "$namespace" rollout status "deployment/$deployment" --timeout=180s
  local_port=$((18200 + RANDOM % 300))
  kubectl -n "$namespace" port-forward "service/$deployment" "$local_port:8080" >/tmp/${deployment}-port-forward.log 2>&1 &
  forward_pid=$!
  trap 'kill ${forward_pid:-} 2>/dev/null || true' EXIT
  for _ in $(seq 1 30); do
    curl -fsS "http://127.0.0.1:$local_port/studio/config.js" | grep -Fq 'apiBasePath' && break
    sleep 1
  done
  curl -fsS "http://127.0.0.1:$local_port/studio/" | grep -Fq 'OpenWorkflow Studio'
  kill "$forward_pid" 2>/dev/null || true
  wait "$forward_pid" 2>/dev/null || true

  OPENWORKFLOW_K3A_RUNTIME=openworkflow-definition-$framework \
  OPENWORKFLOW_ACCEPTANCE_SERVICE=openworkflow-definition-$framework \
  OPENWORKFLOW_ACCEPTANCE_DISRUPTION_RUNTIME=openworkflow-kafka \
  OPENWORKFLOW_ACCEPTANCE_RUNTIME_SELECTOR="app=openworkflow-definition,framework=$framework" \
  OPENWORKFLOW_ACCEPTANCE_DISRUPTION_SELECTOR=app=openworkflow-kafka \
  OPENWORKFLOW_K3A_PERSISTENCE="Studio-$framework-Kafka" \
  OPENWORKFLOW_ACCEPTANCE_ENGINE=kafka-streams \
  OPENWORKFLOW_ACCEPTANCE_CHECKPOINT=K4 \
    "$script_dir/verify-k3a.sh"
done

for profile in postgresql cassandra; do
  OPENWORKFLOW_K3A_RUNTIME=openworkflow-pekko-$profile \
  OPENWORKFLOW_K3A_PERSISTENCE="Studio-Pekko-$profile" \
  OPENWORKFLOW_ACCEPTANCE_ENGINE=pekko \
  OPENWORKFLOW_ACCEPTANCE_CHECKPOINT=K4 \
    "$script_dir/verify-k3a.sh"
done

printf 'K4 verified: all Studio hosts plus independent author/approver/publisher/start/observe journeys across both engines.\n'
