# ForwardMeasure OpenWorkflow deployment

This Helmfile installs the OpenWorkflow application tier. It does not install
the shared platform or cloud infrastructure.

Production deployments require the services supplied by the other tiers:

- `openworkflow-k8s-setup`: managed Kubernetes, PostgreSQL, object storage,
  network, and IAM infrastructure;
- `forwardmeasure-platform`: identity, Kafka, schema registry, secrets,
  routing, observability endpoints, and other shared cluster services.

The `local`, `development`, `ci`, and `kind` environments may install the
repository-local foundation fixtures. Production environments disable those
fixtures and connect to externally managed services.

## Release stages

The installation order is:

1. `foundation`: local database, broker, tenant reconciliation, and migrations
   when the selected environment enables them;
2. `definitions`: identity-policy bootstrap and the Quarkus, Spring, and
   Micronaut definition services;
3. `execution`: Pekko PostgreSQL, Pekko Cassandra, and Kafka Streams engines
   selected by the environment;
4. `studio`: the three framework-specific Studio deployments;
5. `acceptance`: non-production protocol fixtures when explicitly enabled.

Checkpoint labels remain available for the conformance scripts. Operational
commands use the stable `stage` label.

## Configuration ownership

Values are merged in this order:

1. `environments/chart-versions.yaml` — versions of repository charts only;
2. `environments/image-versions.yaml` — all application, migration,
   provisioning, foundation, security-bootstrap, and acceptance-fixture image
   coordinates;
3. `environments/base.yaml` — cloud-neutral OpenWorkflow configuration;
4. `environments/<environment>.yaml[.gotmpl]` — deployment-specific endpoints,
   sizing, hardening, enabled engines, and image overrides.

Every current OpenWorkflow chart is repository-local, so its `Chart.yaml` is
the version authority and `chart-versions.yaml` is intentionally empty. Add an
entry there only when a release begins consuming a repository or OCI chart.

Image tags are independent of chart versions. Production overlays set all
ForwardMeasure application image tags from `OPENWORKFLOW_VERSION`; immutable
digests can be supplied under the corresponding `imageVersions` entry.

## Validate and render

Validation checks the value-layer contract, image coordinates, chart lint,
stage labels, the complete Helmfile render, and exclusion of local fixtures
from production output.

```bash
cd deploy/helmfile
./validate.sh local
./validate.sh kind

helmfile --file helmfile.yaml.gotmpl \
  --environment local template > /tmp/openworkflow-local.yaml
```

The production overlays use `requiredEnv`; Helmfile reports every missing
deployment value rather than substituting an unsafe default.

## Install and operate

Install the complete selected environment:

```bash
cd deploy/helmfile
./install.sh local
```

Apply one stage when performing a controlled update:

```bash
./install.sh local execution
```

Check an existing deployment without changing it:

```bash
./scripts/readiness.sh local
```

Remove OpenWorkflow releases in reverse dependency order:

```bash
./uninstall.sh local
```

The namespace is retained deliberately. Cloud infrastructure and shared
platform services are never removed by this repository.
