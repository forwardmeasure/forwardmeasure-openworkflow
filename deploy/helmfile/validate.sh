#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ENVIRONMENT="${1:-local}"
BUILD_OUTPUT="$(mktemp)"
TEMPLATE_OUTPUT="$(mktemp)"
trap 'rm -f "${BUILD_OUTPUT}" "${TEMPLATE_OUTPUT}"' EXIT

for command in helm helmfile yq jq; do
  command -v "${command}" >/dev/null || {
    echo "Required command is unavailable: ${command}" >&2
    exit 1
  }
done

"${SCRIPT_DIR}/scripts/environment-files.sh" "${ENVIRONMENT}" >/dev/null

yq -e '.chartVersions | type == "!!map"' \
  "${SCRIPT_DIR}/environments/chart-versions.yaml" >/dev/null
yq -e 'has("versions") | not' "${SCRIPT_DIR}/environments/base.yaml" >/dev/null
yq -e 'has("chartVersions") | not' "${SCRIPT_DIR}/environments/base.yaml" >/dev/null
yq -e 'has("imageVersions") | not' "${SCRIPT_DIR}/environments/base.yaml" >/dev/null

yq -o=json '.imageVersions' "${SCRIPT_DIR}/environments/image-versions.yaml" \
  | jq -e '
      [.. | objects | select(has("repository"))]
      | length > 0
        and all(
          (.repository | type == "string" and length > 0)
          and (((.tag // "") | length > 0) or ((.digest // "") | test("^sha256:[0-9a-f]{64}$")))
          and (((.digest // "") == "") or ((.digest // "") | test("^sha256:[0-9a-f]{64}$")))
        )' >/dev/null

if yq -o=json '.imageVersions' "${SCRIPT_DIR}/environments/image-versions.yaml" \
    | jq -e '[.. | objects | .tag? // empty] | any(. == "latest")' >/dev/null; then
  echo "OpenWorkflow image inventory contains an unpinned latest tag." >&2
  exit 1
fi

# Not every releases/ subdirectory is a local chart - cert-manager,
# external-secrets, istio-base, istio-istiod, and keycloak are values-only
# directories referencing upstream/repository charts (jetstack/cert-manager
# and similar), with no Chart.yaml of their own to lint.
while IFS= read -r chart; do
  [[ -f "${chart}/Chart.yaml" ]] || continue
  helm lint "${chart}"
done < <(find "${SCRIPT_DIR}/releases" -mindepth 1 -maxdepth 1 -type d -print | sort)

helmfile --file "${SCRIPT_DIR}/helmfile.yaml.gotmpl" \
  --environment "${ENVIRONMENT}" build >"${BUILD_OUTPUT}"
helmfile --file "${SCRIPT_DIR}/helmfile.yaml.gotmpl" \
  --environment "${ENVIRONMENT}" template >"${TEMPLATE_OUTPUT}"

for stage in foundation definitions execution studio acceptance; do
  grep -q "stage: ${stage}" "${BUILD_OUTPUT}" || {
    echo "OpenWorkflow Helmfile stage is absent: ${stage}" >&2
    exit 1
  }
done

# Explicit list, not a name-prefix match - a prefix match silently stops
# firing under any rename that changes the matched prefix (confirmed: this
# is exactly what would have happened to production-* here when
# production-postgresql was renamed to gcp-openworkflow-prod, missing this
# safety check entirely under the new name).
production_environments=(gcp-openworkflow-prod production-multi-engine)
if printf '%s\n' "${production_environments[@]}" | grep -qx "${ENVIRONMENT}"; then
  for forbidden_name in openworkflow-acceptance-fixtures openworkflow-k2-security redpanda postgresql cassandra; do
    if grep -Eq "^[[:space:]]*name:[[:space:]]+${forbidden_name}$" "${TEMPLATE_OUTPUT}"; then
      echo "Production render contains a development fixture: ${forbidden_name}" >&2
      exit 1
    fi
  done
fi

echo "OpenWorkflow Helmfile manifests validated for ${ENVIRONMENT}."
