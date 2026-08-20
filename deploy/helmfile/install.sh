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

"${SCRIPT_DIR}/validate.sh" "${ENVIRONMENT}"
kubectl apply -k "${SCRIPT_DIR}/manifests/base"

apply_stage() {
  local stage="$1"
  echo "Applying OpenWorkflow stage: ${stage}"
  helmfile --file "${SCRIPT_DIR}/helmfile.yaml.gotmpl" \
    --environment "${ENVIRONMENT}" --selector "stage=${stage}" apply
}

if [[ -n "${REQUESTED_STAGE}" ]]; then
  apply_stage "${REQUESTED_STAGE}"
  exit 0
fi

for stage in foundation definitions execution studio acceptance; do
  apply_stage "${stage}"
done

"${SCRIPT_DIR}/scripts/readiness.sh" "${ENVIRONMENT}"
