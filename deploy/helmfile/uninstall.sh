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

destroy_stage() {
  local stage="$1"
  echo "Destroying OpenWorkflow stage: ${stage}"
  helmfile --file "${SCRIPT_DIR}/helmfile.yaml.gotmpl" \
    --environment "${ENVIRONMENT}" --selector "stage=${stage}" destroy
}

if [[ -n "${REQUESTED_STAGE}" ]]; then
  destroy_stage "${REQUESTED_STAGE}"
  exit 0
fi

# Exact reverse of install.sh's stage order (routing was applied last, so it
# comes down first) - same "core-infrastructure" exclusion as install.sh:
# that stage permanently has zero releases (see
# helmfiles/core-infrastructure.yaml.gotmpl) and helmfile exits nonzero on a
# selector matching zero releases, which would abort this loop under set -e.
for stage in routing acceptance studio execution definitions security migrations foundation; do
  destroy_stage "${stage}"
done

# Deliberately not removed by this script:
# - The forwardmeasure-openworkflow namespace itself (manifests/base) -
#   install.sh creates it as a prerequisite, not a release; leaving it
#   avoids second-guessing whether something else in it (e.g. Secrets
#   synced by forwardmeasure-platform's own platform-secrets release, which
#   this repo doesn't own) should also go.
# - Gateway API CRDs - cluster-wide and shared with forwardmeasure-platform's
#   own install; removing them here could break an unrelated release.
echo "OpenWorkflow releases destroyed for ${ENVIRONMENT}."
