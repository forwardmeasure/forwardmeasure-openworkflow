#!/usr/bin/env bash
set -euo pipefail

scanner_image="${TRIVY_IMAGE:-aquasec/trivy@sha256:62b1e65e8869bc4b4c6aa4fa2b21595256c7c2f6018a9d9ad61caf87187c1969}"
scanner_cache="${TRIVY_CACHE_DIR:-/tmp/openworkflow-trivy-cache}"
evidence_dir="${IMAGE_SCAN_EVIDENCE_DIR:-target/supply-chain}"

if (( $# == 0 )); then
    printf 'usage: %s host=image [host=image ...]\n' "$0" >&2
    exit 64
fi
if [[ ! -S /var/run/docker.sock ]]; then
    printf 'Docker socket is unavailable: /var/run/docker.sock\n' >&2
    exit 69
fi
for required_command in docker jq realpath sha256sum; do
    if ! command -v "$required_command" >/dev/null 2>&1; then
        printf '%s is required\n' "$required_command" >&2
        exit 69
    fi
done

mkdir -p "$scanner_cache" "$evidence_dir"
evidence_dir="$(realpath "$evidence_dir")"
status=0

for coordinate in "$@"; do
    host="${coordinate%%=*}"
    image="${coordinate#*=}"
    if [[ "$coordinate" != *=* || ! "$host" =~ ^[a-z0-9-]+$ || -z "$image" ]]; then
        printf 'invalid image coordinate %q; expected host=image\n' "$coordinate" >&2
        exit 64
    fi

    image_id="$(docker image inspect "$image" --format '{{.Id}}')"
    report="$evidence_dir/$host-image-trivy.json"
    sbom="$evidence_dir/$host-image.cdx.json"

    if ! docker run --rm \
            -v /var/run/docker.sock:/var/run/docker.sock \
            -v "$scanner_cache:/root/.cache/" \
            -v "$evidence_dir:/evidence" \
            "$scanner_image" image \
            --skip-version-check \
            --scanners vuln \
            --severity HIGH,CRITICAL \
            --format json \
            --output "/evidence/$host-image-trivy.json" \
            --exit-code 1 \
            "$image"; then
        status=1
    fi

    docker run --rm \
        -v /var/run/docker.sock:/var/run/docker.sock \
        -v "$scanner_cache:/root/.cache/" \
        -v "$evidence_dir:/evidence" \
        "$scanner_image" image \
        --skip-version-check \
        --format cyclonedx \
        --output "/evidence/$host-image.cdx.json" \
        "$image"

    scanned_id="$(jq -r '.Metadata.ImageID' "$report")"
    if [[ "$scanned_id" != "$image_id" ]]; then
        printf '%s scan/image mismatch: expected %s, report contains %s\n' \
            "$host" "$image_id" "$scanned_id" >&2
        status=1
    fi
    high="$(jq '[.Results[].Vulnerabilities[]? | select(.Severity == "HIGH")] | length' "$report")"
    critical="$(jq '[.Results[].Vulnerabilities[]? | select(.Severity == "CRITICAL")] | length' "$report")"
    components="$(jq '.components | length' "$sbom")"
    printf '%s image=%s high=%s critical=%s components=%s\n' \
        "$host" "$image_id" "$high" "$critical" "$components"
    sha256sum "$report" "$sbom"
done

exit "$status"
