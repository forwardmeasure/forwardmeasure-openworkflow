#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ENVIRONMENT="${1:?Usage: $0 <environment> [stage]}"
REQUESTED_STAGE="${2:-}"

for command in kubectl helm helmfile yq jq; do
  command -v "${command}" >/dev/null || {
    echo "Required command is unavailable: ${command}" >&2
    exit 1
  }
done

"${SCRIPT_DIR}/scripts/resolve-image-digests.sh" "${ENVIRONMENT}"
"${SCRIPT_DIR}/validate.sh" "${ENVIRONMENT}"
"${SCRIPT_DIR}/scripts/preflight.sh" "${ENVIRONMENT}" "${REQUESTED_STAGE}"
kubectl apply -k "${SCRIPT_DIR}/manifests/base"

# Gateway API CRDs (Gateway/HTTPRoute/GatewayClass) - charts/openworkflow-gateway's
# resources need these to be schema-valid on the cluster. Idempotent re-apply
# on a real cloud cluster where forwardmeasure-platform's own install.sh
# already applied the same CRDs first - harmless, not worth gating on $cloud.
GATEWAY_API_VERSION="$(${SCRIPT_DIR}/scripts/environment-value.sh "${ENVIRONMENT}" gatewayApi.version)"
kubectl apply -f "https://github.com/kubernetes-sigs/gateway-api/releases/download/${GATEWAY_API_VERSION}/standard-install.yaml"

apply_stage() {
  local stage="$1"
  echo "Applying OpenWorkflow stage: ${stage}"
  # --allow-no-matching-release: some stages are conditionally empty
  # depending on environment (core-infrastructure is permanently empty;
  # cassandra only has releases when engines.pekkoCassandra is true - see
  # helmfiles/cassandra.yaml.gotmpl) - without this flag helmfile exits
  # nonzero on a selector matching zero releases (confirmed directly),
  # which would abort this loop under set -e before the next stage ran.
  helmfile --file "${SCRIPT_DIR}/helmfile.yaml.gotmpl" \
    --environment "${ENVIRONMENT}" --selector "stage=${stage}" \
    apply --allow-no-matching-release
}

if [[ -n "${REQUESTED_STAGE}" ]]; then
  apply_stage "${REQUESTED_STAGE}"
  exit 0
fi

# Mirrors helmfile.yaml.gotmpl's helmfiles: list order exactly (each
# release's own stage label, in file-load order).
for stage in core-infrastructure cassandra foundation migrations security definitions execution studio acceptance routing; do
  apply_stage "${stage}"
done

"${SCRIPT_DIR}/scripts/readiness.sh" "${ENVIRONMENT}"
