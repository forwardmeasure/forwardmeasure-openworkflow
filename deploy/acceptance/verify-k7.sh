#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
helmfile_path="$root/deploy/helmfile/helmfile.yaml.gotmpl"
context=${OPENWORKFLOW_ACCEPTANCE_CONTEXT:-kind-openworkflow-acceptance}
namespace=${OPENWORKFLOW_ACCEPTANCE_NAMESPACE:-forwardmeasure-openworkflow}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

for chart in openworkflow-foundation openworkflow-k2-security openworkflow-definition-service \
  openworkflow-pekko-runtime openworkflow-kafka-runtime openworkflow-studio; do
  helm lint "$root/deploy/helmfile/releases/$chart" >/dev/null
done

for profile in local development ci kind; do
  helmfile -f "$helmfile_path" -e "$profile" template >"$work/$profile.yaml"
  test "$(yq eval-all '[select((.kind == "Deployment" or .kind == "StatefulSet") and .metadata.name == "keycloak")] | length' "$work/$profile.yaml")" -eq 0
done

production_env=(
  OPENWORKFLOW_VERSION=1.0.0
  OPENWORKFLOW_OTLP_ENDPOINT=http://otel-collector:4317
  OPENWORKFLOW_DATABASE_URL=jdbc:postgresql://database.example/openworkflow
  OPENWORKFLOW_DATABASE_USERNAME=openworkflow
  OPENWORKFLOW_KEYCLOAK_URL=https://identity.example
  OPENWORKFLOW_KEYCLOAK_ISSUER=https://identity.example/realms/openworkflow
  OPENWORKFLOW_KAFKA_BOOTSTRAP_SERVERS=kafka.example:9092
  OPENWORKFLOW_CREDENTIALS_SECRET=openworkflow-production-credentials
  OPENWORKFLOW_HTTP_EGRESS_ALLOWLIST=tenant=operations.example
  OPENWORKFLOW_DEFINITION_RESOURCE_ALLOWED_HOSTS=contracts.example
  OPENWORKFLOW_EXTERNAL_EGRESS_CIDR=10.0.0.0/8
)
env "${production_env[@]}" \
  OPENWORKFLOW_PEKKO_POSTGRESQL_ENDPOINT=jdbc:postgresql://journal.example/openworkflow \
  helmfile -f "$helmfile_path" -e production-postgresql template >"$work/production-postgresql.yaml"
env "${production_env[@]}" \
  OPENWORKFLOW_PEKKO_CASSANDRA_ENDPOINT=cassandra.example:9042 \
  OPENWORKFLOW_CASSANDRA_LOCAL_DATACENTER=dc1 \
  helmfile -f "$helmfile_path" -e production-cassandra template >"$work/production-cassandra.yaml"

for profile in production-postgresql production-cassandra; do
  manifest="$work/$profile.yaml"
  test "$(yq eval-all '[select(.kind == "NetworkPolicy")] | length' "$manifest")" -ge 8
  test "$(yq eval-all '[select(.kind == "Deployment" and .metadata.name == "postgresql")] | length' "$manifest")" -eq 0
  test "$(yq eval-all '[select(.kind == "Deployment" and .metadata.name == "keycloak")] | length' "$manifest")" -eq 0
  test "$(yq eval-all '[select(.metadata.name == "k7-echo")] | length' "$manifest")" -eq 0
  test "$(yq eval-all '[select(.kind == "Deployment" and .metadata.name == "openworkflow-definition-quarkus" and .spec.replicas == 2)] | length' "$manifest")" -eq 1
  test "$(yq eval-all '[select(.kind == "Deployment" and .metadata.name == "openworkflow-definition-quarkus" and .spec.template.metadata.annotations."instrumentation.opentelemetry.io/inject-java" == "true")] | length' "$manifest")" -eq 1
  test "$(yq eval-all '[select(.kind == "NetworkPolicy") | .spec.egress[]? | select(.to[]?.ipBlock.cidr == "10.0.0.0/8")] | length' "$manifest")" -ge 8
done
test "$(yq eval-all '[select(.kind == "Deployment" and .metadata.name == "openworkflow-pekko-postgresql")] | length' "$work/production-postgresql.yaml")" -eq 1
test "$(yq eval-all '[select(.kind == "Deployment" and .metadata.name == "openworkflow-pekko-cassandra")] | length' "$work/production-postgresql.yaml")" -eq 0
test "$(yq eval-all '[select(.kind == "Deployment" and .metadata.name == "openworkflow-pekko-cassandra")] | length' "$work/production-cassandra.yaml")" -eq 1
test "$(yq eval-all '[select(.kind == "Deployment" and .metadata.name == "openworkflow-pekko-postgresql")] | length' "$work/production-cassandra.yaml")" -eq 0

kubectl --context "$context" apply --dry-run=server -f "$work/kind.yaml" >/dev/null
helmfile -f "$helmfile_path" -e kind sync --sync-args=--force-conflicts >/dev/null

deployments=(
  openworkflow-definition-quarkus openworkflow-definition-spring openworkflow-definition-micronaut
  openworkflow-kafka openworkflow-pekko-postgresql openworkflow-pekko-cassandra
  openworkflow-studio-quarkus openworkflow-studio-spring openworkflow-studio-micronaut
)
for deployment in "${deployments[@]}"; do
  kubectl --context "$context" -n "$namespace" rollout status "deployment/$deployment" --timeout=300s >/dev/null
  kubectl --context "$context" -n "$namespace" get deployment "$deployment" -o json | jq -e '
    .spec.template.spec.automountServiceAccountToken == false and
    .spec.template.spec.securityContext.runAsNonRoot == true and
    .spec.template.spec.containers[0].securityContext.allowPrivilegeEscalation == false and
    .spec.template.spec.containers[0].securityContext.readOnlyRootFilesystem == true and
    (.spec.template.spec.containers[0].startupProbe != null) and
    (.spec.template.spec.containers[0].readinessProbe != null) and
    (.spec.template.spec.containers[0].livenessProbe != null) and
    (.spec.template.spec.containers[0].resources.requests.memory != null) and
    (.spec.template.spec.containers[0].lifecycle.preStop != null)' >/dev/null
done

test "$(kubectl --context "$context" -n "$namespace" get pdb -o json | jq '[.items[] | select(.metadata.name | startswith("openworkflow-"))] | length')" -ge 9
kubectl --context "$context" -n "$namespace" rollout restart deployment/openworkflow-kafka >/dev/null
kubectl --context "$context" -n "$namespace" rollout status deployment/openworkflow-kafka --timeout=300s >/dev/null
"$root/deploy/acceptance/verify-k6.sh"

printf 'K7 verified: six profiles render, production is externalized and isolated, hardened workloads roll safely, and the complete K6 journey remains green.\n'
