#!/usr/bin/env bash
set -euo pipefail

HELMFILE_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ENVIRONMENT="${1:?environment is required}"

# Mirrors helmfile.yaml.gotmpl's $envLayers dict exactly - keep both in sync
# by hand whenever an environment is added, renamed, or removed. The
# previous hasPrefix "production-"/eq "development" inference broke
# silently under any environment rename that changed its name's prefix
# (confirmed: this is exactly what would have happened renaming
# production-postgresql -> gcp-openworkflow-prod).
case "${ENVIRONMENT}" in
  local) FILES=(environments/base.yaml environments/local.yaml) ;;
  ci) FILES=(environments/base.yaml environments/ci.yaml) ;;
  kind) FILES=(environments/base.yaml environments/kind.yaml) ;;
  development) FILES=(environments/base.yaml environments/gcp.yaml environments/development.yaml.gotmpl) ;;
  gcp-openworkflow-prod) FILES=(environments/base.yaml environments/gcp.yaml environments/gcp-openworkflow-prod.yaml.gotmpl) ;;
  production-multi-engine) FILES=(environments/base.yaml environments/gcp.yaml environments/production-multi-engine.yaml.gotmpl) ;;
  *)
    echo "Unknown environment: ${ENVIRONMENT}" >&2
    exit 1
    ;;
esac

for relative_path in \
  environments/chart-versions.yaml \
  environments/image-versions.yaml \
  "${FILES[@]}"; do
  absolute_path="${HELMFILE_DIR}/${relative_path}"
  [[ -f "${absolute_path}" ]] || {
    echo "OpenWorkflow environment file is missing: ${absolute_path}" >&2
    exit 1
  }
  printf '%s\n' "${absolute_path}"
done
