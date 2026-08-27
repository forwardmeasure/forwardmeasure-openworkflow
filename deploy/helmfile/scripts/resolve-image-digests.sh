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
# Refreshes environments/image-versions.yaml's digests to whatever each
# repository's configured tag currently resolves to on the registry, for
# cloud environments only. Every cloud-flavored environment file (see
# gcp-openworkflow-prod.yaml.gotmpl, production-multi-engine.yaml.gotmpl)
# sets every imageVersions.*.tag to the same $OPENWORKFLOW_VERSION -
# resolving that tag here, once per repository, covers all of them.
#
# Why this exists: releases/openworkflow-service/templates/workload.yaml's
# image reference is `repository@digest` whenever digest is non-empty,
# ignoring tag entirely - so a stale committed digest silently overrides
# whatever OPENWORKFLOW_VERSION is set to, and the only way to actually
# deploy a new build has been to bypass helmfile with `kubectl set image`
# and then remember to hand-update this file afterward (exactly what
# happened, and was later found to have drifted, in the 2026-08-25/08-27
# Studio debugging session). This makes that reconciliation automatic
# instead of a manually-remembered follow-up step.
#
# Only refreshes an entry that ALREADY has a non-empty digest pinned. An
# empty digest (every spring/micronaut framework variant today) means that
# framework is deliberately tag-only/not deployed yet - resolving one for it
# would silently switch its deploy behavior from "whatever :tag currently
# is" to "pinned," which is a decision for whoever actually starts deploying
# that framework, not this script.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ENVIRONMENT="${1:?Usage: $0 <environment>}"
IMAGE_VERSIONS_FILE="${SCRIPT_DIR}/environments/image-versions.yaml"

CLOUD="$("${SCRIPT_DIR}/scripts/environment-value.sh" "${ENVIRONMENT}" cloudProvider)"
if [[ -z "${CLOUD}" ]]; then
  echo "Standalone environment (${ENVIRONMENT}, no cloudProvider) - image-versions.yaml's committed digests are used as-is, nothing to resolve against a real registry."
  exit 0
fi

for command in docker jq; do
  command -v "${command}" >/dev/null || {
    echo "Required command is unavailable: ${command} (needed to resolve image digests for cloud environment ${ENVIRONMENT})" >&2
    exit 1
  }
done
: "${OPENWORKFLOW_VERSION:?OPENWORKFLOW_VERSION must be set for cloud environment ${ENVIRONMENT} - every imageVersions.*.tag override in its own environment file resolves from it}"

# repo_path/digest_path are yq expressions into IMAGE_VERSIONS_FILE.
#
# `docker manifest inspect --verbose`, not `skopeo inspect`, despite skopeo
# being the more obviously-purpose-built tool here - confirmed directly that
# skopeo's per-invocation fresh auth-token fetch flakes against Docker Hub's
# token endpoint under the back-to-back calls this loop makes (repeated
# "unauthorized: incorrect username or password" against a real, correctly
# configured credential, each one clearing on its very next retry), while
# `docker` (session/token reuse across invocations) did not fail once across
# the same sequence of repositories.
resolve() {
  local repo_path="$1" digest_path="$2"
  local repository current resolved
  repository="$(yq -r "${repo_path}" "${IMAGE_VERSIONS_FILE}")"
  current="$(yq -r "${digest_path}" "${IMAGE_VERSIONS_FILE}")"
  if [[ -z "${current}" ]]; then
    return 0
  fi
  local attempt output
  for attempt in 1 2 3; do
    if output="$(docker manifest inspect --verbose "${repository}:${OPENWORKFLOW_VERSION}" 2>&1)" \
        && resolved="$(jq -r '.Descriptor.digest // empty' <<<"${output}")" \
        && [[ -n "${resolved}" ]]; then
      break
    fi
    if [[ "${attempt}" == 3 ]]; then
      echo "Failed to resolve ${repository}:${OPENWORKFLOW_VERSION} after ${attempt} attempts: ${output}" >&2
      exit 1
    fi
    sleep 2
  done
  if [[ "${resolved}" != "${current}" ]]; then
    echo "${digest_path}: ${current} -> ${resolved}"
    yq -i "${digest_path} = \"${resolved}\"" "${IMAGE_VERSIONS_FILE}"
  fi
}

for entry in definitionManagement executionManagement studio operationAdapter; do
  for framework in quarkus spring micronaut; do
    resolve ".imageVersions.${entry}.repositories.${framework}" ".imageVersions.${entry}.digests.${framework}"
  done
done
for engine in kafka-streams pekko; do
  for framework in quarkus spring micronaut; do
    resolve ".imageVersions.engines.${engine}.repositories.${framework}" ".imageVersions.engines.${engine}.digests.${framework}"
  done
done
resolve ".imageVersions.tenantProvisioning.repository" ".imageVersions.tenantProvisioning.digest"
resolve ".imageVersions.migrations.repository" ".imageVersions.migrations.digest"

echo "image-versions.yaml digests are up to date with ${OPENWORKFLOW_VERSION}."
