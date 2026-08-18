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
| Keycloak 26.7 AuthZEN and Organizations official API documentation; OKS security vocabulary as requirements input only | `openworkflow-authorization/**`, `openworkflow-tenant-provisioning/**`, `config/keycloak/openworkflow-capability-pack-v1.json` | Newly authored active-Organization-only contracts, mandatory fail-closed AuthZEN Evaluation/Evaluations HTTP adapter, bounded full-context cache, audit correlation, shared-role/Organization reconciliation, and versioned resource/action policy. No source security engine or merged token-role authorization was copied. | `./mvnw -Pfocused -pl openworkflow-authorization/openworkflow-authorization-authzen,openworkflow-tenant-provisioning -am test` |
| OAE/OKS migration behavior as requirements input; published `forwardmeasure-jpa-liquibase:1.0.0` | `openworkflow-migrations/**` | Newly authored deployment-owned tenant schema runner. Schema identifiers come only from `forwardmeasure-jpa` `TenantSchema`; the composed Liquibase changelog includes the published JPA foundation and the OpenWorkflow common changelog. | `./mvnw -Pfocused -pl openworkflow-migrations -am test` |
| OAE/OKS definition-management persistence behavior as requirements input; published `forwardmeasure-jpa-core:1.0.0` and `forwardmeasure-testcontainers` BOM | `openworkflow-definition-management/openworkflow-definition-management-jpa/**`, `openworkflow-migrations/src/main/resources/db/changelog/openworkflow-common.xml` | Newly authored tenant-schema JPA definitions/revisions and repositories plus validation, review, publication, lifecycle-history tables. Revision content is protected by an immutable SHA-256 identity and a PostgreSQL update-rejection trigger; actor references use the common per-tenant actor table. | `./mvnw -Pfocused clean verify` (120 tests, including real PostgreSQL migration idempotency, tenant isolation, lifecycle-state update, and immutable-content rejection) |
| OKS `oks-operation-adapter/oks-operation-adapter-api/src/**` and `oks-operation-adapter/oks-operation-adapter-http/src/**` | `openworkflow-operation-adapter/openworkflow-operation-adapter-api/src/**` and `openworkflow-operation-adapter/openworkflow-operation-adapter-http/src/**` | Retained reviewed HTTP/OpenAPI request construction, parameter serialization, digest authentication, endpoint policy, and response/error behavior as a temporary Kafka compatibility edge; migrated Jackson 2.22 APIs and Java 25 serialization warnings. The API's old OKS runtime coupling is not treated as the common product boundary. | `./mvnw -s .mvn/central-settings.xml -pl openworkflow-operation-adapter/openworkflow-operation-adapter-http -am test` (19 adapter tests) |
| OAE `openworkflow-operation-adapter/openworkflow-operation-adapter-core/src/**` | `openworkflow-operation-adapter/openworkflow-operation-adapter-core/src/**` | Refactored portable HTTP, authentication, egress, secret, timeout, AsyncAPI HTTP/WebSocket/SSE, protocol-routing, and durable outbox behavior onto the common engine identities/descriptors; adapted legacy DID fixtures to canonical UUID tenant identity and migrated Jackson 2.22 iteration APIs. Pekko projection version ownership moved to `forwardmeasure-java-parent`. | `./mvnw -s .mvn/central-settings.xml -pl openworkflow-operation-adapter/openworkflow-operation-adapter-core -am test` (19 adapter tests; 208 upstream tests) |
| OAE `openworkflow-operation-adapter/openworkflow-operation-adapter-grpc/src/**` | `openworkflow-operation-adapter/openworkflow-operation-adapter-grpc/src/**` | Retained dynamic transport from the persisted pinned proto, all four gRPC interaction modes, durable observation/backpressure, owned cancellation, egress enforcement, and expression/secret-backed authentication. Centralized the coherent gRPC/protobuf/Guava/Gson/Error Prone stack in `forwardmeasure-java-parent`; no protocol version is owned by the product module. | `./mvnw -s .mvn/central-settings.xml -pl openworkflow-operation-adapter/openworkflow-operation-adapter-grpc -am test` |
| OAE `openworkflow-operation-adapter/openworkflow-operation-adapter-asyncapi-{amqp,cloud,jms,kafka,mqtt,nats,pulsar,redis,stomp}/src/**` | Matching `openworkflow-operation-adapter/openworkflow-operation-adapter-asyncapi-*/src/**` modules | Retained specification-backed publish/subscribe transports, durable observation acknowledgements, owned cancellation, protocol authentication, and egress policy. Migrated Jackson, Kafka 4.3, NATS, and Lettuce APIs under the Java 25 warning gate; centralized every client-stack version and convergence override in `forwardmeasure-java-parent`. | Focused `-pl` reactor tests for all nine AsyncAPI transport modules with `-am test` |
| OAE `openworkflow-operation-adapter/openworkflow-operation-adapter-{postgresql,cassandra}/src/**` | Matching unified persistence-specific operation-outbox modules | Retained the durable-offset Pekko HTTP and protocol projections, combined duplicate launch code, added mandatory AuthZEN decorators, and composed the complete protocol routing table in the Quarkus Pekko host. Projection artifact versions are centralized in `forwardmeasure-java-parent`. | Persistence outbox compile gate, operation authorization tests, Pekko wire golden, Quarkus affected-reactor and architecture tests |
| OAE `openworkflow-operation-adapter/openworkflow-operation-adapter-agent-protocols/src/**` | `openworkflow-operation-adapter/openworkflow-operation-adapter-agent-protocols/src/**` | Retained A2A agent-card security negotiation, JSON-RPC HTTP, MCP HTTP session initialization, and policy-allowlisted MCP stdio; adapted canonical UUID tenant headers and current Jackson/resource-consumption APIs. | `./mvnw -s .mvn/central-settings.xml -pl openworkflow-operation-adapter/openworkflow-operation-adapter-agent-protocols -am test` |
| OAE `openworkflow-operation-adapter/openworkflow-operation-adapter-runner/src/**` | `openworkflow-operation-adapter/openworkflow-operation-adapter-runner/src/**` | Retained policy-constrained local-process and digest-pinned OCI run semantics, detached/await modes, bounded output, environment/volume/port controls, cancellation, and durable observations; adapted canonical UUID tenant propagation and current Jackson iteration APIs. | `./mvnw -s .mvn/central-settings.xml -pl openworkflow-operation-adapter/openworkflow-operation-adapter-runner -am test` |
| OKS `oks-operation-adapter/oks-operation-adapter-kafka/src/main/**` and the committed-effect HTTP integration scenario | `openworkflow-operation-adapter/openworkflow-operation-adapter-kafka/**` | Retained the external, asynchronous Kafka dispatcher with committed-effect consumption, definition-resource cache, checkpoints, bounded in-flight work, correlated durable observation commands, stable idempotency, dead letters, and offset-after-observation semantics. Replaced the discarded permissive security SPI with newly authored mandatory AuthZEN operation authorization, trusted Organization/role propagation, mounted tenant-secret resolution, fail-closed HTTP egress, ephemeral credential destruction, and a host-neutral lifecycle wired into all three Kafka hosts without importing Pekko. | `./mvnw -s .mvn/central-settings.xml -pl openworkflow-operation-adapter/openworkflow-operation-adapter-kafka -am test` (focused security/wire tests plus real Redpanda and real HTTP target) |
