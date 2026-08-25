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
# This repo never installs istio/cert-manager/external-secrets/keycloak
# itself (see helmfiles/core-infrastructure.yaml.gotmpl) - on a real cloud
# cluster ($cloud set), forwardmeasure-platform's own install.sh is assumed
# to have already installed them. Nothing enforced that assumption: running
# this repo's install.sh alone against an empty real-cloud cluster used to
# hang ~10 minutes at the migrations stage (Job stuck on a Secret that only
# forwardmeasure-platform's platform-secrets release ever creates), time
# out, roll back, and abort - with no indication of the real cause. This
# fails in seconds instead, with a message that names the actual problem.
#
# Not a re-check of forwardmeasure-platform's own preflight.sh (that
# validates values/Terraform-provisioned prerequisites - Secret Manager
# entries, storage buckets - a different, earlier checkpoint). This checks
# whether forwardmeasure-platform's K8s-level install actually happened.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ENVIRONMENT="${1:?Usage: $0 <configured-environment> [stage]}"
REQUESTED_STAGE="${2:-}"

CLOUD="$("${SCRIPT_DIR}/environment-value.sh" "${ENVIRONMENT}" cloudProvider)"
if [[ -z "${CLOUD}" ]]; then
  echo "Standalone environment (${ENVIRONMENT}, no cloudProvider) - doesn't depend on a real ClusterSecretStore/Gateway/keycloak, nothing to check."
  exit 0
fi

fail() {
  echo "Preflight failed for ${ENVIRONMENT}: $1" >&2
  echo "forwardmeasure-platform's own install.sh (or install-platform.sh, which runs both in order) must be applied to this cluster first." >&2
  exit 1
}

kubectl --request-timeout=10s cluster-info >/dev/null 2>&1 || fail "cannot reach the cluster (check kubeconfig/context)"

kubectl --request-timeout=10s get clustersecretstore -o json 2>/dev/null \
  | jq -e '.items | length > 0 and any(.[]; .status.conditions[]? | select(.type == "Ready" and .status == "True"))' \
  >/dev/null \
  || fail "no Ready ClusterSecretStore found (forwardmeasure-platform's platform-secrets release provides this)"

kubectl --request-timeout=10s get gateway platform-gateway -n istio-system -o json 2>/dev/null \
  | jq -e '.status.conditions[]? | select(.type == "Programmed" and .status == "True")' \
  >/dev/null \
  || fail "Gateway platform-gateway not found or not Programmed in istio-system (forwardmeasure-platform's platform-routing release provides this)"

# Skipped only for stage=migrations: forwardmeasure-platform's own install.sh
# deliberately runs that one stage before Keycloak exists - its Postgres
# role, which migrations creates, is what Keycloak needs to start. Every
# other stage, including a standalone full run (REQUESTED_STAGE unset),
# keeps this check - it's what turns a ~10 minute hang-then-rollback with no
# indication of the real cause (see this file's header) into a same-second,
# actionable failure.
if [[ "${REQUESTED_STAGE}" != "migrations" ]]; then
  kubectl --request-timeout=10s get statefulset -n keycloak -l app.kubernetes.io/instance=keycloak -o json 2>/dev/null \
    | jq -e '.items | length > 0 and any(.[]; (.status.readyReplicas // 0) >= 1)' \
    >/dev/null \
    || fail "no Ready keycloak StatefulSet found in namespace keycloak (forwardmeasure-platform's keycloak release provides this)"
fi

echo "Platform prerequisites validated for ${ENVIRONMENT}."
