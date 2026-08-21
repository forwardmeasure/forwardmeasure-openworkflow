# Source provenance ledger

This ledger is the WP0 inventory for code and tests considered for consolidation into
`forwardmeasure-openworkflow`. It records review disposition before any source is copied. A row
ending in `/**` covers every descendant Maven module unless a more-specific row overrides it.
Aggregator POMs are build history only and are discarded; this product owns its parent, BOM,
version policy, and module graph.

Inventory snapshots:

- OKS: `openworkflow-kafka-streams` at
  `97db2233dec401b4df0413b00f346e53df60b9d7` (181 reactor POMs).
- OAE: `openworkflow-actor-engine` at
  `77e8784c32508e81c3d00d802f549550380a8df9` (75 reactor POMs).
- Both source worktrees were clean when inventoried on 2026-08-17.
- `openworkflow-kafka-streams/docs/implementation-failure-report.md` is absent. No conclusions
  are attributed to that missing report.

Disposition means:

- **Adopt** — preserve reviewed behavior or fixtures with provenance; adapt names and build wiring.
- **Refactor** — retain useful behavior or tests only after moving them behind the locked common
  product boundaries.
- **Discard** — do not copy; the target architecture supplies the capability or prohibits it.

No row authorizes bulk copying. Every later adoption must add its exact source path, destination,
substantive changes, and passing focused test to the adoption log at the end of this document.

## OKS inventory

| Source module/path | Disposition | Target and rationale | Existing evidence to review |
|---|---|---|---|
| `/pom.xml` | Discard | New parent/BOM owns all versions and plugins. | Reactor build only. |
| `oks-openworkflow-model/**` | Discard | Use official SDK reader/types; do not retain generated competing models. | Model generation/build checks inform schema-boundary review. |
| `oks-expression-jq/**` | Adopt | `openworkflow-expression-jq`; portable deterministic jq behavior. | Existing jq unit tests. |
| `oks-definition/**` | Refactor | `openworkflow-definition`; retain validation/compiler/resource-resolution behavior after removing OKS coupling. | Compiler, schema compatibility, resolution, and digest tests. |
| `oks-durable-processing/**` | Refactor | Kafka engine internals only; never a common engine/FSM. | Core and Kafka Streams tests. |
| `oks-workflow-runtime/**` | Refactor | `openworkflow-engine-kafka-streams`; place runtime behind common engine SPI. | API/core/topology behavior tests. |
| `oks-component-lifecycle/**` | Discard | Framework bindings own lifecycle integration; no duplicate portable lifecycle framework. | Contract/unit tests may inform host acceptance cases. |
| `oks-architecture-tests/**` | Refactor | `openworkflow-architecture-tests`; rewrite against manifesto boundaries. | Existing ArchUnit rules. |
| `oks-persistence-jpa/**` | Discard | `forwardmeasure-jpa` is the mandatory persistence foundation. | Persistence behavior is reviewed only for domain requirements. |
| `oks-database-migration/**` | Refactor | `openworkflow-migrations`; retain application changelog intent, discard duplicate migration framework/API/three service hosts. | CLI/core/Liquibase tests and generated contract checks. |
| `oks-workflow-data/**` | Refactor | Common application/JPA boundary where specification behavior requires it. | API and JPA tests. |
| `oks-operation-adapter/**` | Refactor | `openworkflow-operation-adapter`; retain specification-backed adapters after removing Kafka/product duplication. | Kafka dispatcher tests and adapter contracts. |
| `oks-call/**` | Refactor | Later WP8 operation adapters; consolidate with OAE protocols. | External-call adapter tests. |
| `oks-run/**` | Refactor | Later WP8 run semantics behind common plan/engine contracts. | Run adapter tests. |
| `oks-identity-spi/**` | Discard | Trusted Keycloak Organization tenant/actor context replaces the duplicate identity model. | Tests inform negative-context cases only. |
| `oks-security-spi/**` | Discard | Mandatory Keycloak AuthZEN adapter; no embedded RBAC or fallback. | Action vocabulary may inform WP2 policy tests. |
| `oks-human-task/oks-human-task-api/**` | Refactor | Later normative/adopted human-task common contract. | API fixtures. |
| `oks-human-task/oks-human-task-core/**` | Refactor | Later portable human-task application behavior. | Core unit tests. |
| `oks-human-task/oks-human-task-kafka-streams/**` | Refactor | Kafka-specific human-task adapter only. | Kafka behavior tests. |
| `oks-human-task/oks-human-task-management/**` | Refactor | One later contract-first API and portable application/JAX-RS adapter. | Application, JAX-RS, Kafka, and generated-client tests. |
| `oks-human-task/oks-human-task-presentation/**` | Refactor | One shared Studio webapp. | Presentation tests. |
| `oks-human-task/oks-a2ui/**` | Refactor | Optional presentation overlay only if explicitly adopted with human-task scope. | Overlay tests. |
| `oks-human-task/pom.xml` | Discard | Target reactor owns aggregation. | Aggregation only. |
| `oks-studio/oks-studio-core/**` | Refactor | One `openworkflow-studio` webapp; no separate product plane. | Studio core tests. |
| `oks-studio/oks-studio-web/**` | Refactor | Reuse useful UI assets/components only after losslessness and API-boundary review. | Web build and component test. |
| `oks-studio/pom.xml` | Discard | Target reactor owns aggregation. | Aggregation only. |
| `oks-query/**` | Refactor | `openworkflow-execution-query`; canonical API/projection behavior only, never engine storage. | Auth, HTTP, and query tests. |
| `oks-controller-core/**` | Refactor | Split valid application orchestration into capability services; remove product-controller duplication. | No direct tests; require new focused coverage. |
| `oks-workflow-computation-core/**` | Refactor | Kafka engine adapter/application edge only. | No direct tests; require engine contract fixtures. |
| `oks-workflow-definition-management/**` | Refactor | One WP3 contract/application/JAX-RS/JPA family; regenerate all bindings and replace Kafka/JPA boundaries. | Application, JPA, and Kafka tests plus OpenAPI sources. |
| `oks-workflow-execution-management/**` | Refactor | One WP4/WP7 contract/application/JAX-RS family; engine commands go through SPI. | Application and JAX-RS tests plus OpenAPI sources. |
| `oks-workflow-data-management/**` | Refactor | Later capability only where required by the specification/common data boundary. | Application tests and OpenAPI sources. |
| `oks-workflow-event-ingress/**` | Refactor | `openworkflow-eventing`; one contract-first ingress API with engine-specific publishers. | Application/Kafka tests and OpenAPI sources. |
| `oks-frameworks/quarkus/**` | Refactor | Thin Quarkus binding/service/Studio hosts; replace persistence/security/business implementations. | Quarkus tests and service configuration. |
| `oks-frameworks/spring/**` | Refactor | Thin Spring binding/service/Studio hosts; remove Spring persistence/business implementations. | Spring tests and service configuration. |
| `oks-frameworks/micronaut/**` | Refactor | Thin Micronaut binding/service/Studio hosts; remove Micronaut persistence/business implementations. | Micronaut tests and service configuration. |
| `oks-frameworks/oks-framework-service-acceptance-tests/**` | Adopt | Common black-box three-host acceptance suite. | Existing five acceptance fixtures. |
| `oks-frameworks/pom.xml` | Discard | Target reactor owns aggregation. | Aggregation only. |
| `oks-conformance-tests/**` | Adopt | `openworkflow-conformance-tests`; retain deterministic 1.0.3 fixtures after common-engine parameterization. | Existing conformance corpus. |
| `oks-coverage/**` | Discard | New parent owns aggregate coverage policy. | Report aggregation only. |

## OAE inventory

| Source module/path | Disposition | Target and rationale | Existing evidence to review |
|---|---|---|---|
| `/pom.xml` | Discard | New parent/BOM owns all versions and plugins. | Reactor build only. |
| `openworkflow-model/**` | Adopt | `openworkflow-model`; official SDK boundary and pinned-reader test. | `OfficialSdkContractTest` (strengthened in WP0 with exact digest). |
| `openworkflow-expression-jq/**` | Adopt | `openworkflow-expression-jq`; compare with OKS and retain one implementation. | Existing jq unit tests. |
| `openworkflow-definition/**` | Refactor | `openworkflow-definition`; reconcile with stronger OKS behavior into one compiler. | Compiler/schema/resource tests. |
| `openworkflow-engine-api/**` | Refactor | Common engine SPI; broaden from actor-specific assumptions to both providers without sharing internals. | API unit test. |
| `openworkflow-actor-runtime/**` | Refactor | `openworkflow-engine-pekko-core`; retain typed event-sourced FSM and remove provisional duplicate paths. | Actor behavior, recovery, serialization, and timer tests. |
| `openworkflow-persistence/openworkflow-persistence-core/**` | Refactor | Pekko persistence-profile selection inside Pekko engine. | Profile selection test. |
| `openworkflow-persistence/openworkflow-persistence-postgresql/**` | Refactor | Pekko PostgreSQL journal/snapshot tenant-scoped adapter; never JPA business entities. | PostgreSQL profile tests. |
| `openworkflow-persistence/openworkflow-persistence-cassandra/**` | Refactor | Pekko Cassandra tenant-qualified persistence adapter. | Cassandra profile test. |
| `openworkflow-persistence/openworkflow-persistence-contract-tests/**` | Adopt | Shared Pekko recovery parity fixtures for both backends. | Five contract fixtures. |
| `openworkflow-persistence/pom.xml` | Discard | Persistence profiles live beneath the Pekko engine. | Aggregation only. |
| `openworkflow-migrations/**` | Refactor | Sole `openworkflow-migrations` executable; compose common/JPA and conditional relational engine migrations. | Migration unit tests. |
| `openworkflow-definition-management/openworkflow-definition-management-application/**` | Refactor | One WP3 application service with governance/maker-checker added. | Application tests. |
| `openworkflow-definition-management/openworkflow-definition-management-postgresql/**` | Refactor | Standard JPA repository/service on `forwardmeasure-jpa`; preserve relational behavior, not implementation. | PostgreSQL repository test. |
| `openworkflow-definition-management/openworkflow-definition-management-cassandra/**` | Discard | Common product definition data is tenant-schema PostgreSQL, not Cassandra. | Cassandra test may inform contract semantics only. |
| `openworkflow-definition-management/openworkflow-definition-management-contract-tests/**` | Adopt | WP3 common repository/application contract fixtures. | Existing contract fixture. |
| `openworkflow-definition-management/openworkflow-definition-management-jaxrs/**` | Refactor | Portable implementation of newly generated JAX-RS interfaces. | JAX-RS test. |
| `openworkflow-definition-management/api-language-bindings/**` | Refactor | Keep reviewed OpenAPI intent; regenerate Java models/client/server, TypeScript, and Python from one contract. | Generator/drift outputs. |
| `openworkflow-definition-management/pom.xml` | Discard | Target capability reactor owns aggregation. | Aggregation only. |
| `openworkflow-execution-management/openworkflow-execution-management-application/**` | Refactor | Common WP4/WP7 application service. | Application test. |
| `openworkflow-execution-management/openworkflow-execution-management-pekko/**` | Refactor | Pekko engine control adapter behind common SPI. | Pekko adapter tests. |
| `openworkflow-execution-management/openworkflow-execution-management-jaxrs/**` | Refactor | Portable generated-interface implementation. | JAX-RS test. |
| `openworkflow-execution-management/api-language-bindings/**` | Refactor | Regenerate all bindings from one reviewed common contract. | Generator/drift outputs. |
| `openworkflow-execution-management/pom.xml` | Discard | Target capability reactor owns aggregation. | Aggregation only. |
| `openworkflow-execution-query/openworkflow-execution-query-api/**` | Refactor | Canonical query contracts only. | API compile evidence. |
| `openworkflow-execution-query/openworkflow-execution-query-core/**` | Refactor | Canonical projection/query core, independent of Pekko persistence. | Core tests. |
| `openworkflow-execution-query/openworkflow-execution-query-postgresql/**` | Refactor | Common query JPA adapter on `forwardmeasure-jpa`. | PostgreSQL test. |
| `openworkflow-execution-query/openworkflow-execution-query-cassandra/**` | Discard | Canonical product query plane is tenant PostgreSQL; Cassandra remains Pekko-native persistence. | Test may inform query contract only. |
| `openworkflow-execution-query/openworkflow-execution-query-contract-tests/**` | Adopt | Common canonical query contract fixtures. | Existing contract fixture. |
| `openworkflow-execution-query/openworkflow-execution-query-jaxrs/**` | Refactor | Portable generated-interface implementation. | JAX-RS test. |
| `openworkflow-execution-query/api-language-bindings/**` | Refactor | Regenerate clients/server from common query OpenAPI. | Generator/drift outputs. |
| `openworkflow-execution-query/pom.xml` | Discard | Target capability reactor owns aggregation. | Aggregation only. |
| `openworkflow-eventing/openworkflow-eventing-core/**` | Refactor | Later common event semantics and ports. | Six core tests. |
| `openworkflow-eventing/openworkflow-eventing-jaxrs/**` | Refactor | Portable event ingress adapter from generated contract. | JAX-RS test. |
| `openworkflow-eventing/openworkflow-eventing-postgresql/**` | Refactor | Application-owned event data only through `forwardmeasure-jpa`. | No direct tests; require focused contract coverage. |
| `openworkflow-eventing/openworkflow-eventing-cassandra/**` | Discard | No Cassandra common product/event repository. | Behavior reviewed only for engine adapter needs. |
| `openworkflow-eventing/pom.xml` | Discard | Target capability reactor owns aggregation. | Aggregation only. |
| `openworkflow-operation-adapter/openworkflow-operation-adapter-core/**` | Refactor | Portable effect ports and adapter orchestration. | Eight core tests. |
| `openworkflow-operation-adapter/openworkflow-operation-adapter-runner/**` | Refactor | Secured runner behind durable effect intent. | Runner tests. |
| `openworkflow-operation-adapter/openworkflow-operation-adapter-agent-protocols/**` | Refactor | Later WP8 specification-defined agent protocols. | A2A/MCP tests. |
| `openworkflow-operation-adapter/openworkflow-operation-adapter-grpc/**` | Refactor | Later WP8 gRPC call adapter. | gRPC test. |
| `openworkflow-operation-adapter/openworkflow-operation-adapter-asyncapi-*/**` | Refactor | Later WP8 protocol adapters; retain only specification-backed behavior. | Per-protocol tests. |
| `openworkflow-operation-adapter/openworkflow-operation-adapter-postgresql/**` | Refactor | Application-owned adapter state through `forwardmeasure-jpa` if still required. | No direct tests; require focused coverage. |
| `openworkflow-operation-adapter/openworkflow-operation-adapter-cassandra/**` | Discard | No Cassandra common operation-adapter repository. | Behavior reviewed only for engine-native needs. |
| `openworkflow-operation-adapter/pom.xml` | Discard | Target capability reactor owns aggregation. | Aggregation only. |
| `framework-bindings/quarkus/**` | Refactor | Thin Quarkus service and Studio hosts, using official JPA adapter and common YAML. | Service/Studio tests. |
| `framework-bindings/spring/**` | Refactor | Thin Spring Boot service and Studio hosts, using official JPA adapter and common YAML. | Service/Studio tests. |
| `framework-bindings/micronaut/**` | Refactor | Thin Micronaut service and Studio hosts, using official JPA adapter and common YAML. | Service/Studio tests. |
| `framework-bindings/pom.xml` | Discard | Target reactor owns aggregation. | Aggregation only. |
| `openworkflow-studio/webapp/**` | Refactor | One Maven-built Studio webapp; reconcile useful OKS UI behavior. | Webapp test/build. |
| `openworkflow-studio/pom.xml` | Discard | Target reactor owns aggregation. | Aggregation only. |
| `openworkflow-architecture-tests/**` | Refactor | Rewrite and expand against all locked architecture laws. | Existing ArchUnit test. |

## Adoption log

| Adopted source | Destination | Substantive changes | Focused evidence |
|---|---|---|---|
| OAE `openworkflow-model/src/test/.../OfficialSdkContractTest.java` | `openworkflow-model/src/test/.../OfficialSdkContractTest.java` | Retained official reader/invalid-document checks; added exact resource ID and SHA-256 pin; property values come from the parent. | `./mvnw -Pfocused -pl openworkflow-model -am test` |
| Serverless Workflow specification `v1.0.3` (`9b5b1da29e9d4fff2358580241e11aab22704a16`) `examples/*.yaml` and `.ci/validation/test/fixtures/invalid/*.yaml` | `openworkflow-model/src/test/resources/official-v1.0.3/**` | Vendored the complete 66-example and 3-invalid official corpus, normalized only with a trailing blank line, recorded upstream identity, and pinned every vendored byte in `SHA256SUMS`. The official `run-script-with-stdin-and-arguments.yaml` uses a pre-1.0 shape rejected by the tag's schema and pinned SDK; that discrepancy is asserted explicitly. | `./mvnw -Pfocused -pl openworkflow-model test` |
| OAE `openworkflow-expression-jq/src/main/**` and `src/test/**` (behaviorally identical to reviewed OKS `oks-expression-jq`) | `openworkflow-expression-jq/src/main/**` and `src/test/**` | Retained deterministic jq evaluation and error behavior; migrated deprecated Jackson iteration calls and added serialization IDs required by the Java 25 warning gate. | `./mvnw -Pfocused -pl openworkflow-expression-jq -am test` (8 tests) |
| OAE `openworkflow-definition/src/main/**` and `src/test/**`, reconciled with OKS `oks-definition` event-data-schema behavior | `openworkflow-definition/src/main/**` and `src/test/**` | Retained the stronger immutable compiler/profile, pinned schema, validation, resource graph, catalogue, schema-analysis, and digest behavior; preserved OKS literal event `dataschema` discovery/resolution/validation instead of accepting OAE's regression; renamed compiler/digest profiles for the unified product, closed mutable JSON accessor leaks, removed engine-specific wording, and migrated deprecated Jackson iteration calls. | `./mvnw -Pfocused -pl openworkflow-definition -am test` (76 definition tests) |
| OAE `openworkflow-engine-api/**` and OKS execution/runtime contracts (design inputs only) | `openworkflow-engine/openworkflow-engine-api/**` | Replaced actor- and Kafka-specific shapes with one newly authored portable SPI: trusted tenant/actor context, immutable revision/execution identity, engine-pinned command envelopes, durable acknowledgements, lifecycle/errors/effects/timers, canonical event/query/projection records, selection, and liveness/readiness. OAE `ResourceAuthorization` was deliberately discarded because WP2 owns AuthZEN authorization. | `./mvnw -Pfocused -pl openworkflow-engine/openworkflow-engine-contract-tests -am test` |
| OAE/OKS engine behavior tests (requirements input) | `openworkflow-engine/openworkflow-engine-contract-tests/**` | Authored one reusable JUnit provider contract for both engines plus a test-only fake proving start durability, command idempotency, pinning, pause/resume/cancel guarantees, projection observation, readiness, trusted selection, JSON immutability, and portable serialization. No fake ships in the main engine API. | `./mvnw -Pfocused -pl openworkflow-engine/openworkflow-engine-contract-tests -am test` (11 tests) |
| Keycloak 26.7 AuthZEN and Organizations official API documentation; OKS security vocabulary as requirements input only | `openworkflow-authorization/**`, `openworkflow-tenant-provisioning/**`, `openworkflow-tenant-provisioning/src/main/resources/openworkflow-capability-pack-v1.json` | Newly authored active-Organization-only contracts, mandatory fail-closed AuthZEN Evaluation/Evaluations HTTP adapter, bounded full-context cache, audit correlation, shared-role/Organization reconciliation, and versioned resource/action policy. No source security engine or merged token-role authorization was copied. | `./mvnw -Pfocused -pl openworkflow-authorization/openworkflow-authorization-authzen,openworkflow-tenant-provisioning -am test` |
| OAE/OKS migration behavior as requirements input; published `forwardmeasure-jpa-liquibase:1.0.0` | `openworkflow-migrations/**` | Newly authored deployment-owned tenant schema runner. Schema identifiers come only from `forwardmeasure-jpa` `TenantSchema`; the composed Liquibase changelog includes the published JPA foundation and the OpenWorkflow common changelog. | `./mvnw -Pfocused -pl openworkflow-migrations -am test` |
| OAE/OKS definition-management persistence behavior as requirements input; published `forwardmeasure-jpa-core:1.0.0` and `forwardmeasure-testcontainers` BOM | `openworkflow-definition-management/openworkflow-definition-management-jpa/**`, `openworkflow-migrations/src/main/resources/db/changelog/openworkflow-common.xml` | Newly authored tenant-schema JPA definitions/revisions and repositories plus validation, review, publication, lifecycle-history tables. Revision content is protected by an immutable SHA-256 identity and a PostgreSQL update-rejection trigger; actor references use the common per-tenant actor table. | `./mvnw -Pfocused clean verify` (120 tests, including real PostgreSQL migration idempotency, tenant isolation, lifecycle-state update, and immutable-content rejection) |
| OKS `oks-operation-adapter/oks-operation-adapter-api/src/**` and `oks-operation-adapter/oks-operation-adapter-http/src/**` | `openworkflow-operation-adapter/openworkflow-operation-adapter-api/src/**` and `openworkflow-operation-adapter/openworkflow-operation-adapter-http/src/**` | Retained reviewed HTTP/OpenAPI request construction, parameter serialization, digest authentication, endpoint policy, and response/error behavior as a temporary Kafka compatibility edge; migrated Jackson 2.22 APIs and Java 25 serialization warnings. The API's old OKS runtime coupling is not treated as the common product boundary. | `./mvnw -s .mvn/central-settings.xml -pl openworkflow-operation-adapter/openworkflow-operation-adapter-http -am test` (19 adapter tests) |
| OAE `openworkflow-operation-adapter/openworkflow-operation-adapter-core/src/**` | `openworkflow-operation-adapter/openworkflow-operation-adapter-core/src/**` | Refactored portable HTTP, authentication, egress, secret, timeout, AsyncAPI HTTP/WebSocket/SSE, protocol-routing, and durable outbox behavior onto the common engine identities/descriptors; adapted legacy DID fixtures to canonical UUID tenant identity and migrated Jackson 2.22 iteration APIs. Pekko projection version ownership moved to `forwardmeasure-platform`. | `./mvnw -s .mvn/central-settings.xml -pl openworkflow-operation-adapter/openworkflow-operation-adapter-core -am test` (19 adapter tests; 208 upstream tests) |
| OAE `openworkflow-operation-adapter/openworkflow-operation-adapter-grpc/src/**` | `openworkflow-operation-adapter/openworkflow-operation-adapter-grpc/src/**` | Retained dynamic transport from the persisted pinned proto, all four gRPC interaction modes, durable observation/backpressure, owned cancellation, egress enforcement, and expression/secret-backed authentication. Centralized the coherent gRPC/protobuf/Guava/Gson/Error Prone stack in `forwardmeasure-platform`; no protocol version is owned by the product module. | `./mvnw -s .mvn/central-settings.xml -pl openworkflow-operation-adapter/openworkflow-operation-adapter-grpc -am test` |
| OAE `openworkflow-operation-adapter/openworkflow-operation-adapter-asyncapi-{amqp,cloud,jms,kafka,mqtt,nats,pulsar,redis,stomp}/src/**` | Matching `openworkflow-operation-adapter/openworkflow-operation-adapter-asyncapi-*/src/**` modules | Retained specification-backed publish/subscribe transports, durable observation acknowledgements, owned cancellation, protocol authentication, and egress policy. Migrated Jackson, Kafka 4.3, NATS, and Lettuce APIs under the Java 25 warning gate; centralized every client-stack version and convergence override in `forwardmeasure-platform`. | Focused `-pl` reactor tests for all nine AsyncAPI transport modules with `-am test` |
| OAE `openworkflow-operation-adapter/openworkflow-operation-adapter-{postgresql,cassandra}/src/**` | Matching unified persistence-specific operation-outbox modules | Retained the durable-offset Pekko HTTP and protocol projections, combined duplicate launch code, added mandatory AuthZEN decorators, and composed the complete protocol routing table in the Quarkus Pekko host. Projection artifact versions are centralized in `forwardmeasure-platform`. | Persistence outbox compile gate, operation authorization tests, Pekko wire golden, Quarkus affected-reactor and architecture tests |
| OAE `openworkflow-operation-adapter/openworkflow-operation-adapter-agent-protocols/src/**` | `openworkflow-operation-adapter/openworkflow-operation-adapter-agent-protocols/src/**` | Retained A2A agent-card security negotiation, JSON-RPC HTTP, MCP HTTP session initialization, and policy-allowlisted MCP stdio; adapted canonical UUID tenant headers and current Jackson/resource-consumption APIs. | `./mvnw -s .mvn/central-settings.xml -pl openworkflow-operation-adapter/openworkflow-operation-adapter-agent-protocols -am test` |
| OAE `openworkflow-operation-adapter/openworkflow-operation-adapter-runner/src/**` | `openworkflow-operation-adapter/openworkflow-operation-adapter-runner/src/**` | Retained policy-constrained local-process and digest-pinned OCI run semantics, detached/await modes, bounded output, environment/volume/port controls, cancellation, and durable observations; adapted canonical UUID tenant propagation and current Jackson iteration APIs. | `./mvnw -s .mvn/central-settings.xml -pl openworkflow-operation-adapter/openworkflow-operation-adapter-runner -am test` |
| OKS `oks-operation-adapter/oks-operation-adapter-kafka/src/main/**` and the committed-effect HTTP integration scenario | `openworkflow-operation-adapter/openworkflow-operation-adapter-kafka/**` | Retained the external, asynchronous Kafka dispatcher with committed-effect consumption, definition-resource cache, checkpoints, bounded in-flight work, correlated durable observation commands, stable idempotency, dead letters, and offset-after-observation semantics. Replaced the discarded permissive security SPI with newly authored mandatory AuthZEN operation authorization, trusted Organization/role propagation, mounted tenant-secret resolution, fail-closed HTTP egress, ephemeral credential destruction, and a host-neutral lifecycle wired into all three Kafka hosts without importing Pekko. | `./mvnw -s .mvn/central-settings.xml -pl openworkflow-operation-adapter/openworkflow-operation-adapter-kafka -am test` (focused security/wire tests plus real Redpanda and real HTTP target) |

## Correction decisions — definition/execution/query rework (2026-08-18)

The row above ("OAE/OKS definition-management persistence behavior... Newly authored
tenant-schema JPA definitions/revisions and repositories plus validation, review,
publication, lifecycle-history tables") overstates what was actually delivered: the
`workflow_validation`/`workflow_review`/`workflow_publication`/`workflow_lifecycle_history`
tables it names have no `@Entity` classes at all — `JpaDefinitionRepository` writes
them through hand-built `EntityManager.createNativeQuery(...)` string SQL (8 call
sites), including a raw `actor` table upsert that duplicates published
`forwardmeasure-jpa-identity` `ActorRepository`/`ActorService` behavior instead of
using it. The same defect was independently confirmed in
`openworkflow-execution-management-jpa` (`JpaExecutionRepository`) and
`openworkflow-execution-query-jpa` (`JpaExecutionQueryRepository`/
`JpaExecutionProjectionStore`/`JpaTenantRoutingExecutionStore`) — a systemic pattern
across all three capability modules with real code, not an isolated regression. A
correction plan for all three (`/home/pn/.claude/plans/sorted-wondering-hoare.md`)
was reviewed and approved by the product owner on 2026-08-18. Decisions recorded here
so a later session does not re-litigate them:

- **Do not port OKS's JPA entity classes for definition-management.** OKS's own
  `WorkflowEntity` maps to table `workflow`; its `WorkflowDefinitionEntity` maps to
  table `workflow_definition` meaning "one row per version." The unified repo's own
  already-migrated `workflow_definition` table means "stable container" (with
  `workflow_revision` as the version table) — same table name, incompatible shape.
  Porting OKS's classes verbatim would corrupt the already-correct, already-applied
  schema. OKS contributes HTTP addressing (`workflowId`/`definitionId`) and the
  lifecycle-operation contract shape only.
- **`ManagedWorkflowRevision` (definition-management's hand-written domain record) is
  removed**, not preserved. New rule (also proposed as a `PROJECT_MANIFESTO.md` §3.6
  amendment): a canonical/domain type distinct from both the JPA entity and the
  generated API DTO is introduced only when the same object must cross a boundary a
  direct entity-to-DTO MapStruct mapping cannot serve (used by code that must not
  depend on JPA at all, such as an `ExecutionEngineProvider` implementation;
  constructed before a durable row exists; or serialized onto a non-JPA transport).
  `ManagedWorkflowRevision` fails this test (grepped: consumed only inside
  `openworkflow-definition-management`'s own jpa/application/jaxrs code) and is
  replaced by a MapStruct mapper going directly from `WorkflowRevisionEntity`/
  `WorkflowDefinitionEntity` to the generated DTOs, matching the established pattern
  already in production at `/home/pn/Documents/code/forwardmeasure/entity-intelligence`
  (`EvidenceApiMapper`). `CanonicalExecution` (execution-management) passes the test
  — it is dispatched to `ExecutionEngineProvider.submit(...)`, whose Pekko/Kafka
  Streams implementations have no JPA dependency — and is kept.
- **`prevent_workflow_revision_content_update()` (the Postgres immutability trigger)
  is relaxed, not left as-is.** It previously blocked content changes at every
  lifecycle state, including DRAFT. OKS's `updateWorkflowDefinition` operation
  (`UpdateWorkflowDefinitionRequest{source}`, "Update a draft workflow definition")
  is kept and requires DRAFT-state content mutation to be legal; the trigger now
  allows content changes only while `OLD.lifecycle_state = 'DRAFT'`, raising once a
  revision has left DRAFT. Identity (`definition_id`, `revision_number`) and
  authorship (`author_actor_id`) remain unconditionally immutable at every lifecycle
  state, including DRAFT — only the content columns gained the DRAFT exception.
  Manifesto §3.5's "editing or rejection produces a new revision" governs post-review
  integrity, not an unsubmitted draft. Since no environment runs this schema yet, the
  fix is folded directly into the existing changesets (`openworkflow-130` through
  `-160` gained their `version` column inline in the original `createTable`;
  `openworkflow-270`'s existing trigger redefinition carries the final DRAFT-aware
  logic directly) rather than appended as new additive/ALTER-style changesets — there
  is no deployed state to preserve compatibility with. Verified with a new
  `WorkflowRevisionDraftMutabilityTest` (real PostgreSQL via Testcontainers) proving
  DRAFT content edits succeed, non-DRAFT content edits raise, and identity/authorship
  edits raise regardless of state; `DefinitionPlaneMigrationTest`'s changeset-count
  assertion stays at 25 (unchanged) since nothing was appended.
- **No execution-management/execution-query contract split.** Considered (following
  OAE's precedent of separate control/query contracts) and rejected: manifesto §3.13
  assigns query *persistence/projection code* ownership to
  `openworkflow-execution-query`, not a separate public contract file, and OAE is a
  weak precedent given the rest of its API design was independently rejected this
  session for being under-scoped. `openworkflow-execution-management/openapi/
  execution.openapi.yaml` stays one file covering both control and query operations.
- **Consumer grep re-run 2026-08-18**, confirming no new coupling since WP0
  inventory: outside `openworkflow-definition-management/`, only the three
  framework-binding composition roots reference its types —
  `openworkflow-framework-bindings/quarkus/openworkflow-quarkus-binding/.../
  {OpenWorkflowQuarkusBinding,QuarkusDefinitionManagementResource,
  QuarkusDefinitionManagementOperations}.java`;
  `openworkflow-framework-bindings/spring/openworkflow-spring-binding/.../
  OpenWorkflowSpringBinding.java`; `openworkflow-framework-bindings/micronaut/
  openworkflow-micronaut-binding/.../{OpenWorkflowMicronautBinding,
  OpenWorkflowMicronautSerde,MicronautDefinitionManagementController,
  MicronautDefinitionManagementOperations}.java`. Also found and not previously
  recorded: `openworkflow-framework-bindings/micronaut/openworkflow-micronaut-service/
  src/main/resources/META-INF/openworkflow-orm.xml` explicitly enumerates
  `WorkflowDefinitionEntity`/`WorkflowRevisionEntity` by fully-qualified class name
  for Micronaut's Hibernate ORM scanning — the four new entities
  (`WorkflowValidationEntity`, `WorkflowReviewEntity`, `WorkflowPublicationEntity`,
  `WorkflowLifecycleHistoryEntity`) must be added to this file or Micronaut will not
  see them, with no compile-time signal if forgotten.

## OpenAPI 3.1.x resolved (2026-08-18): generator-version bump plus a template patch

Manifesto §3.8 requires OpenAPI 3.1.x. The pinned `openapi-generator-maven-plugin`
(7.21.0, in `forwardmeasure-platform`) generates broken Java for any schema with
`additionalProperties: false` under a `3.1.x`-declared document: the `jaxrs-spec`
generator's `additional_properties.mustache` partial unconditionally emits
`putAdditionalProperty`/`getAdditionalProperties`/`getAdditionalProperty` methods
that call `this.put()`/`this.get()`/`return this;` as if the class implemented `Map`,
gated only on the generator's internal `additionalProperties` truthiness flag — with
no check that the class declaration was also given `extends HashMap<...>` (a separate
computation, gated on `parent`). Under OpenAPI 3.1's JSON-Schema-aligned semantics,
the `additionalProperties` flag is truthy even when the schema says `false`, while
`parent` correctly stays unset — the two computations disagree, and the result
doesn't compile. This is a bug in openapi-generator's Java-side `CodegenModel`
construction, not something a template conditional alone can fix, and it reproduces
identically on 7.24.0 (the newest version cached locally) — confirmed by an actual
compile, not just template inspection.

Two changes, both verified end to end with real `mvn clean compile` runs (not
standalone plugin invocations, which gave false-positive results earlier in this
investigation because CLI `-D` overrides for the plugin's `configOptions` map
parameter silently don't apply — trust only real reactor builds for this generator):

1. **`openapi-generator.version` bumped 7.21.0 → 7.24.0** in `forwardmeasure-platform`
   (root aggregator pom, artifactId `forwardmeasure-platform`). Confirmed
   safe for every other consumer that actually invokes the plugin without a local
   override: `forwardmeasure-agents`, `forwardmeasure-nlp`, `forwardmeasure-platform`
   all rebuild clean (real `mvn clean compile` per repo, not just spec validation).
   `entity-intelligence` pins its own copy of the property independently and is
   unaffected either way. This is a shared cross-repo change with real blast radius —
   product-owner approved before applying it, and each affected sibling repo was
   individually rebuilt to confirm, not assumed safe.
2. **A targeted template override**, mirroring the precedent already established in
   `data-fabric-api-specifications/oas-generator-templates` (that project's own
   custom `pojo.mustache` also omits `additionalProperties` handling entirely, for
   the same reason: every schema there is `additionalProperties: false` too, so the
   accessor methods are never wanted). New file:
   `openworkflow-api-specifications/oas-generator-templates/additional_properties.mustache`,
   intentionally emitting nothing, overriding the stock `JavaJaxRS/spec/additional_properties.mustache`
   partial. Wired into `openworkflow-definition-management-models`'s
   `openapi-generator-maven-plugin` execution via `<templateDirectory>`. Only the
   `-models` module needs it — `-server-jaxrs` doesn't generate models,
   `-client-apachehttp` reuses `-models`'s output, and the TypeScript/Python
   generators are unrelated toolchains never exposed to this bug. Apply the same
   `<templateDirectory>` entry to every future capability's `-models` module (or
   equivalent) rather than rediscovering this.

Both `definition-management.openapi.yaml` and (from this point forward)
`execution-management.openapi.yaml` are `openapi: 3.1.0`, matching manifesto §3.8
without any contract-shape compromise.

## execution-management contract centralized and corrected (2026-08-18)

Same treatment as definition-management, contract-and-codegen work only (persistence
layer is Phase 3, sequenced after definition-management's per the plan). Moved
`openworkflow-execution-management/openapi/execution.openapi.yaml` to
`openworkflow-api-specifications/openworkflow-services-api-spec/execution-management.openapi.yaml`
(21 KB → full contract, `openapi: 3.1.0` from the start, using the already-fixed
`<templateDirectory>` override) and relocated its five generator modules
(`openworkflow-execution-{models,server-jaxrs,client-apachehttp,client-typescript,client-python}`)
under `openworkflow-api-language-bindings/{java,typescript,python}/
workflow-execution-management-service/...`, mirroring definition-management's move.
Two real fixes applied, matching the plan and verified by a real `mvn clean compile`
of all five modules:

- Split the single generic `POST /api/v1/executions/{executionId}/{operation}`
  (`operation: enum[pause,resume,cancel]`) into three distinct operationIds —
  `pauseExecution`, `resumeExecution`, `cancelExecution` — each its own path
  (`/pause`, `/resume`, `/cancel`), matching OKS's precedent of distinct operations
  rather than a generic action route.
- Added `Execution.workflowId` (readOnly UUID) alongside the existing `revisionId` so
  Studio/list views don't need a second lookup to show which workflow an execution
  belongs to. `revisionId` itself was deliberately *not* renamed to `definitionId`
  (which would have matched definition-management's new public terminology more
  closely) — that's real Java/persistence churn belonging to Phase 3's rewrite, not
  this contract-only pass.

Also brought up to the same completeness bar as definition-management, none of which
existed in the original: a top-level `security: [BearerAuth]` declaration (the
original contract had no security scheme at all), consistent `application/problem+json`
error responses (400/401/403/404/409/412/500) via `common-definitions.yaml` on every
operation (the original had only bare `404`/`409` with no schema), the shared
`If-Match` parameter (was previously a bare `integer`, not a quoted-string ETag —
inconsistent with HTTP's ETag semantics and with definition-management's own
convention), and a proper `ExecutionHistoryPage{items, next_after_sequence}` wrapper
replacing a bare unbounded array that had cursor-style query parameters
(`afterSequence`/`limit`) but no way for a caller to know if more pages existed.

`deploy/acceptance/verify-wp11.sh` and `docs/migration/standalone-migration-map.md`
both had hardcoded references to the old `openworkflow-{definition,execution}-management/
openapi/*.yaml` paths (the acceptance script's checks would have started failing
silently-until-run) — both updated to the new centralized paths; `verify-wp11.sh`
re-run and confirmed passing.

## Definition-management persistence rebuilt (2026-08-18): steps 5+6 landed together

Plan steps 5 ("new entities and repositories") and 6 (`JpaDefinitionRepository`
rebuild) landed as one change — Maven compiles `openworkflow-definition-management-jpa`
as a single unit, so there was no useful intermediate state to commit between them.
`ManagedWorkflowRevision` removal (the second half of step 6) is **not** included
here; it is still deferred to its own later pass along with the addressing change
(step 8), per the plan. `JpaDefinitionRepository`'s external shape (still keyed by
`(definitionKey, revisionNumber)`, still producing/consuming `ManagedWorkflowRevision`)
is deliberately unchanged in this pass — only its internals moved off raw SQL.

**Four new entities**, all `extends AbstractBaseEntity<Long>` (version-only optimistic
lock, no audit/uuid — matching the plan's table), each with its own
`@SequenceGenerator(allocationSize = 50)` matching the already-migrated pooled
sequences:

- `WorkflowValidationEntity` (`workflow_validation`) — `@ManyToOne WorkflowRevisionEntity
  revision`, `valid`, `findings` (`@JdbcTypeCode(SqlTypes.JSON)` over
  `List<String>`/`jsonb`, matching `data-fabric`'s `Investigation.java` convention),
  `validatorProfile`, `validatedAt`.
- `WorkflowReviewEntity` (`workflow_review`) — `revision`, `reviewAction`, `actor`
  (`@ManyToOne Actor`), `revisionDigest`, nullable `reason`, `createdAt`. Populates the
  `reason` column that was previously left NULL by the raw-SQL path.
- `WorkflowPublicationEntity` (`workflow_publication`) — `@OneToOne revision` (unique,
  matching the existing constraint), `actor`, `revisionDigest`, `publishedAt`, nullable
  `deprecatedAt`; a `deprecate()` method throws `IllegalStateException` if already
  deprecated.
- `WorkflowLifecycleHistoryEntity` (`workflow_lifecycle_history`) — `revision`,
  nullable `fromState`/`toState` (`WorkflowLifecycleState` enums), `actor`,
  `correlationId`, `createdAt`.

Matching minimal repositories (`WorkflowValidationRepository`, `WorkflowReviewRepository`,
`WorkflowLifecycleHistoryRepository` — just `extends AbstractBaseRepository<T,Long>`;
`WorkflowPublicationRepository` adds `findByRevision(WorkflowRevisionEntity)` used by
the deprecate path). `WorkflowDefinitionRepository`/`WorkflowRevisionRepository` were
upgraded from `AbstractBaseRepository` to `AbstractAuditedEntityRepository` per the
plan (adds `findByUuid`, not yet used outside tests but required before step 8's
addressing change can land).

**`version` columns**: per the user's explicit "treat all changesets as brand new, no
ALTER TABLE, no one is using the system" direction, these were folded directly into
the original `createTable` blocks of changesets `openworkflow-130` through `-160`
rather than added as new additive changesets — see the migration-consolidation note
above/below for the full rationale. Total changeset count is unchanged at 20 in
`openworkflow-common.xml` (25 across the full tenant migration), confirming true
consolidation, not addition.

**`WorkflowRevisionEntity.authorActorId` (raw `long`) replaced with `Actor author`**
(`@ManyToOne(fetch = LAZY, optional = false) @JoinColumn(name = "author_actor_id")`) —
column and FK already existed from the original migration; this was a pure Java-side
change. The constructor's last parameter changed from `long authorActorId` to
`Actor author` (null-checked), and `getAuthorActorId()` was replaced with `getAuthor()`.

**`WorkflowActorResolver`** (new) replaces the raw-SQL `on conflict ... do nothing`
actor upsert with `ActorRepository.findByIdentity("keycloak", subjectIdentifier)
.orElseGet(() -> provision(...))`. Deliberately does **not** implement the plan's
originally-sketched "catch-unique-violation-and-reread" fallback for the
first-request race — documented inline in its Javadoc that a JPA flush failure can
leave the persistence context unusable for same-transaction recovery, so a
catch-and-retry there would be unsound, not just unnecessary. This is a considered
simplification, not an oversight; revisit only if the race is observed in practice.
Built in `openworkflow-definition-management-jpa` as planned, so Phase 3
(execution-management) can reuse it via the compile dependency it already needs on
this module's entities.

**`JpaDefinitionRepository` rewrite** — constructor now builds and binds all six
repositories plus a `WorkflowActorResolver`, replacing the old direct `EntityManager`
native-query calls entirely. `save()` resolves the author `Actor` before constructing
`WorkflowRevisionEntity`; `recordValidation()`/`recordTransition()`/`recordHistory()`
persist through the typed repositories; `map()` reads
`entity.getAuthor().getSubjectIdentifier()` to reconstruct the domain
`authorActorId` string for `ManagedWorkflowRevision`.

**Real bugs found and fixed in the pre-existing `WorkflowDefinitionRepositoryTest`**,
surfaced by wiring the new entities into its manually-built Hibernate `SessionFactory`:
its `seedActor()` helper used `identity_type = 'USER'` (not a valid `IdentityType`
enum value — only `HUMAN`/`SERVICE` exist), left `identity_provider` NULL (would never
match `WorkflowActorResolver`'s `"keycloak"` lookup), and hardcoded `id = 1` (risking a
PK collision with the sequence). Removed `seedActor()` and its two call sites entirely,
letting the new auto-provisioning path in `WorkflowActorResolver` be exercised
directly instead. `Actor.class` plus the four new entity classes were added to the
test's manual `addAnnotatedClass(...)` registration chain (a raw
`org.hibernate.cfg.Configuration`, not a managed persistence unit, so registration
doesn't happen automatically).

**Two build fixes surfaced along the way**: `forwardmeasure-jpa-identity` was missing
from `openworkflow-definition-management-jpa/pom.xml` (needed for `Actor`); `hibernate-core`
was test-scoped, which broke compilation of `WorkflowValidationEntity`'s main-source
`@JdbcTypeCode`/`SqlTypes` usage — moved to compile scope. Separately,
`WorkflowValidationEntity.findings` (`List<String>`) triggered a `-Werror` `[serial]`
lint warning; used `@SuppressWarnings("serial")` with an inline comment rather than
`transient`, since `transient` would silently stop the field from persisting under
JPA's field-access strategy.

Verified against real PostgreSQL via Testcontainers:
`WorkflowDefinitionRepositoryTest` (Tests run: 1, Failures: 0, Errors: 0) and a full
`mvn test` of the module (exit 0), plus the standing
`WorkflowRevisionDraftMutabilityTest` from the migration-consolidation step above.
