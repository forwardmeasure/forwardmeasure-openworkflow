#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ENVIRONMENT="${1:?Usage: $0 <environment>}"
NAMESPACE="$(helmfile --file "${SCRIPT_DIR}/helmfile.yaml.gotmpl" \
  --environment "${ENVIRONMENT}" build \
  | yq -r '.renderedvalues.openworkflowNamespace' \
  | head -1)"

[[ -n "${NAMESPACE}" && "${NAMESPACE}" != "null" ]] || {
  echo "Unable to resolve openworkflowNamespace for ${ENVIRONMENT}." >&2
  exit 1
}

echo "Waiting for OpenWorkflow workloads in ${NAMESPACE}."
kubectl --namespace "${NAMESPACE}" wait --for=condition=Available \
  deployment --all --timeout=15m

if kubectl --namespace "${NAMESPACE}" get statefulset --no-headers 2>/dev/null | grep -q .; then
  kubectl --namespace "${NAMESPACE}" rollout status statefulset --all --timeout=20m
fi

if kubectl --namespace "${NAMESPACE}" get job --no-headers 2>/dev/null | grep -q .; then
  kubectl --namespace "${NAMESPACE}" wait --for=condition=Complete job --all --timeout=15m
fi

echo "OpenWorkflow workloads are ready for ${ENVIRONMENT}."
