# OKS/OAE standalone migration map

This is the authoritative WP11 replacement map. A migration is forward-only. Existing executions
remain pinned to their original engine until they reach a terminal state; neither Kafka Streams
state nor Pekko persistence is translated.

## Coordinates and APIs

| Standalone | Unified replacement |
|---|---|
| `com.forwardmeasure.openworkflow:oks-openworkflow-model` | `com.forwardmeasure.openworkflow:openworkflow-model` |
| `com.forwardmeasure.openworkflow:oks-definition` | `com.forwardmeasure.openworkflow:openworkflow-definition` |
| `com.forwardmeasure.openworkflow:oks-expression-jq` | `com.forwardmeasure.openworkflow:openworkflow-expression-jq` |
| `com.forwardmeasure.openworkflow:oks-workflow-runtime-*` | `com.forwardmeasure.openworkflow:openworkflow-engine-kafka-streams` |
| `com.forwardmeasure.openworkflow:openworkflow-actor-runtime` | `com.forwardmeasure.openworkflow:openworkflow-engine-pekko-core` |
| standalone definition-management clients | generated clients from `openworkflow-api-specifications/openworkflow-services-api-spec/definition-management.openapi.yaml` |
| standalone execution-management clients | generated clients from `openworkflow-api-specifications/openworkflow-services-api-spec/execution-management.openapi.yaml` |
| standalone query clients | `openworkflow-execution-query-api` query contracts |
| standalone Studio | unified `/studio/` application and `/api/openworkflow/*` capability APIs |

Consumers import `com.forwardmeasure.openworkflow:openworkflow-bom` and remove direct versions.
The common engine API replaces either standalone runtime API; engine implementation classes are
not a compatibility surface.

## Configuration and deployment

| Standalone concern | Unified value |
|---|---|
| OKS bootstrap servers/application id | `kafka.bootstrapServers`, `kafka.applicationId` |
| OAE persistence profile | `pekko.persistenceProfile=postgresql|cassandra` |
| service tenant/schema headers | trusted Keycloak active Organization; no schema header or token claim |
| standalone security/RBAC | Keycloak Organizations plus AuthZEN, fail closed |
| separate service charts | `deploy/helmfile`, selected environment profile |
| separate Studio URLs | one Studio host per framework, same API contract |

Production uses an existing credentials Secret and external PostgreSQL, Kafka, Keycloak, OTLP and,
for the Cassandra profile, Cassandra endpoints. Embedded foundation services and acceptance
fixtures are forbidden in production renders.

## Data objects and topics

| Source | Treatment |
|---|---|
| OKS workflow definitions | Export source bytes, resolved resource bytes, SHA-256 digests and lifecycle evidence into the versioned import manifest. Import as new immutable unified revisions only after digest verification. |
| OAE workflow definitions | Same governed import; never copy rows into unified tables by hand. |
| OKS execution topics/state stores | Drain in OKS. Do not rename, replay or translate into the unified Kafka application. |
| OAE Pekko journal/snapshots | Drain in OAE with its original serializer set. Do not copy into unified persistence. |
| common product PostgreSQL | Provision tenant schemas with `openworkflow-migrations`; use API/import tooling for domain data. |
| unified Kafka | New application id and topics; exact names are deployment-owned and must not collide with OKS. |
| unified Pekko | New persistence IDs and projection offsets; PostgreSQL and Cassandra are alternative stores, not migration formats. |

## Definition import contract

Each source revision is represented by `config/migration/definition-import-manifest-v1.schema.json`.
The importer must reject a source/resolved digest mismatch, missing tenant UUID, duplicate revision
number, non-contiguous lifecycle history, or publication without author/reviewer/publisher evidence.
When history evidence is absent, import the verified revision as `DRAFT`; never invent approval or
publication actors or timestamps. Preserve the source repository, commit, source identity and
export timestamp as provenance.

## Cutover sequence

1. Inventory consumers and capture immutable source backups and digests.
2. Deploy unified services without routing production traffic; run schema migrations.
3. Import verified definitions and compare reads/digests against the source.
4. Route only new executions to the unified engine. Drain every pre-cutover execution in place.
5. Shadow definition/query reads where a source deployment exists; compare normalized documents,
   lifecycle, digests and terminal execution summaries. Do not shadow side effects.
6. Freeze standalone writes, observe the rollback window, then stop artifact publication only when
   every retirement gate is attested.

Rollback reverses routing for new work, never moves an already-started execution between engines,
and restores the pre-cutover standalone writer only while its retained data remains compatible.
