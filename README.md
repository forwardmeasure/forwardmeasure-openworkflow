# ForwardMeasure OpenWorkflow

ForwardMeasure OpenWorkflow is a multi-engine implementation of the Open
Workflow Specification. It provides a common definition, governance, security,
API, query, Studio, and deployment plane with selectable Apache Kafka Streams
and Apache Pekko execution engines.

Keycloak is shared platform infrastructure. This repository consumes its URL,
issuer, realm, client, and projected credentials, but does not deploy or own the
Keycloak workload.

The [project manifesto](PROJECT_MANIFESTO.md) defines the governing product and
architecture decisions. The [unification and enhancement
plan](IMPLEMENTATION_PLAN.md) defines their delivery order and acceptance
gates.

Release operation is documented in [operations](docs/operations.md), with the supported runtime
matrix in [compatibility](docs/compatibility-matrix.md). Standalone OKS/OAE consumers use the
[migration map](docs/migration/standalone-migration-map.md) and the evidence-gated
[retirement checklist](docs/migration/retirement-checklist.md).
