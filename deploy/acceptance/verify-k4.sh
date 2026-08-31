#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
set -euo pipefail

# A platform deploys exactly the framework selected by workflowPlatformFramework, not all
# three simultaneously (deploy/helmfile/helmfiles/studios.yaml.gotmpl) - so this checks the one
# framework actually live, parameterized the same way verify-k2.sh already is, instead of
# hardcoding a loop over all three that would fail outright on the two not deployed.
framework=${1:?usage: verify-k4.sh quarkus|spring|micronaut}

namespace=forwardmeasure-openworkflow
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

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

OPENWORKFLOW_K3A_RUNTIME=openworkflow-definition-management-$framework \
OPENWORKFLOW_ACCEPTANCE_SERVICE=openworkflow-definition-management-$framework \
OPENWORKFLOW_ACCEPTANCE_DISRUPTION_RUNTIME=openworkflow-engine-kafka-streams-$framework \
OPENWORKFLOW_ACCEPTANCE_RUNTIME_SELECTOR="app.kubernetes.io/name=openworkflow-definition-management-$framework" \
OPENWORKFLOW_ACCEPTANCE_DISRUPTION_SELECTOR="app.kubernetes.io/name=openworkflow-engine-kafka-streams-$framework" \
OPENWORKFLOW_K3A_PERSISTENCE="Studio-$framework-Kafka" \
OPENWORKFLOW_ACCEPTANCE_ENGINE=kafka-streams \
OPENWORKFLOW_ACCEPTANCE_CHECKPOINT=K4 \
  "$script_dir/verify-k3a.sh"

# Independent engine flavors deployed simultaneously (execution-engines.yaml.gotmpl) - unlike
# Studio/definition-management, this loop was never framework-collapsed, but the release names
# are still framework-suffixed (openworkflow-engine-pekko-<profile>-<framework>), which the
# original version of this loop never accounted for either.
for profile in postgresql cassandra; do
  OPENWORKFLOW_K3A_RUNTIME=openworkflow-engine-pekko-$profile-$framework \
  OPENWORKFLOW_K3A_PERSISTENCE="Studio-Pekko-$profile" \
  OPENWORKFLOW_ACCEPTANCE_ENGINE=pekko \
  OPENWORKFLOW_ACCEPTANCE_CHECKPOINT=K4 \
    "$script_dir/verify-k3a.sh"
done

printf 'K4 %s verified: Studio host plus independent author/approver/publisher/start/observe journeys across both engines.\n' \
  "$framework"
