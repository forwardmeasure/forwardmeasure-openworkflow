#!/usr/bin/env bash
set -euo pipefail

HELMFILE_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ENVIRONMENT="${1:?environment is required}"

case "${ENVIRONMENT}" in
  production-*) OVERLAY="environments/${ENVIRONMENT}.yaml.gotmpl" ;;
  *) OVERLAY="environments/${ENVIRONMENT}.yaml" ;;
esac

for relative_path in \
  environments/chart-versions.yaml \
  environments/image-versions.yaml \
  environments/base.yaml \
  "${OVERLAY}"; do
  absolute_path="${HELMFILE_DIR}/${relative_path}"
  [[ -f "${absolute_path}" ]] || {
    echo "OpenWorkflow environment file is missing: ${absolute_path}" >&2
    exit 1
  }
  printf '%s\n' "${absolute_path}"
done
