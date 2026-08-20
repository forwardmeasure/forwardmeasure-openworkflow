# OpenWorkflow Implementation Correction Handoff

**Purpose:** This document records implementation mistakes introduced during the OKS, OAE, Data Fabric evidence-service, and unified OpenWorkflow work. It is intended to give a replacement implementer enough evidence and direction to repair the code without inheriting the same assumptions.

**Status:** The affected definition-management implementations must be treated as noncompliant until the correction plan and acceptance gates below pass.

## 1. Authorities and precedence

Use these sources in this order:

1. `/home/pn/Documents/code/forwardmeasure/forwardmeasure-openworkflow/PROJECT_MANIFESTO.md`
2. `/home/pn/Documents/code/forwardmeasure/forwardmeasure-openworkflow/IMPLEMENTATION_PLAN.md`
3. The existing OKS workflow-definition-management OpenAPI contract:
   `/home/pn/Documents/code/forwardmeasure/openworkflow-kafka-streams/oks-workflow-definition-management/openapi-specifications/workflow-definition-management.openapi.yaml`
4. The established contract-first implementation pattern demonstrated by:
   `/home/pn/Documents/code/forwardmeasure/data-fabric/data-fabric-components/java/data-fabric-services/entity-intelligence-framework/entity-intelligence-ingestion-service/entity-intelligence-ingestion-api`
5. Published ForwardMeasure JPA APIs and conventions in:
   `/home/pn/Documents/code/forwardmeasure/forwardmeasure-jpa`
6. Published ForwardMeasure database-migration APIs and conventions in:
   `/home/pn/Documents/code/forwardmeasure/forwardmeasure-database-migrations`

Do not reinterpret locked decisions in the manifesto or implementation plan. If an apparent conflict exists, stop and document it rather than inventing another contract or architecture.

## 2. Required API implementation pattern

The required flow is:

```text
OpenAPI source contract
    -> generated API interface
    -> thin API implementation implementing that interface
    -> operation-specific handlers
    -> injected application/domain services
    -> injected persistence-service interfaces
    -> ForwardMeasure JPA service implementations
    -> ForwardMeasure JPA repositories
    -> explicit JPA entities
```

Responsibilities must remain separated:

- The OpenAPI document is the public HTTP authority.
- Generated API interfaces and generated DTOs are never hand-edited.
- The API implementation implements the generated interface and delegates each operation to a handler.
- The API implementation does not contain business rules, persistence calls, actor-resolution workflows, lifecycle logic, or manual routing annotations duplicating the contract.
- Handlers coordinate one API use case and inject services. They do not inject `EntityManager` or repositories.
- Application/domain services enforce business invariants and own Jakarta transaction boundaries where appropriate.
- Persistence services isolate entity/repository access from handlers and transport code.
- Repositories extend the appropriate standard ForwardMeasure JPA repository bases.
- `EntityManager` is confined to ForwardMeasure JPA infrastructure and the smallest framework composition hook required to bind repositories.
- API DTOs, domain types, persistence entities, authorization resources, and engine events are distinct types.
- MapStruct mappers belong at boundaries; do not hand-copy fields unless a semantic transformation genuinely requires it.

The ingestion implementation illustrates the intended top-level shape:

```text
generated IngestionApi
    -> IngestionApiImpl implements IngestionApi
    -> operation handlers
    -> injected services
```

## 3. Primary contract mistake

The existing OKS contract should have been reused as the common definition-management contract for OKS, OAE, and the unified product. Instead, three incompatible contracts were created.

### 3.1 OKS contract

Authoritative existing contract:

`/home/pn/Documents/code/forwardmeasure/openworkflow-kafka-streams/oks-workflow-definition-management/openapi-specifications/workflow-definition-management.openapi.yaml`

SHA-256 observed during the audit:

```text
0131cde455915ab5887857d0c9e78c1865fb5891c813aa9875907f41033a3b87
```

It defines:

- workflow create, list, retrieve, update, and delete;
- workflow-definition create, list, retrieve, update, and delete;
- definition validation;
- definition publication and publication-status retrieval;
- definition deprecation; and
- published-workflow discovery and immutable-version retrieval.

### 3.2 OAE divergence

OAE introduced:

`/home/pn/Documents/code/forwardmeasure/openworkflow-actor-engine/openworkflow-definition-management/openapi-specifications/definition-management.openapi.yaml`

SHA-256 observed during the audit:

```text
317d064f8ca62e5789daf67141c7b3d1181f1e478b11f9e97473889d43c7655a
```

This was an unjustified second API. It exposed only:

- `listWorkflowDefinitions`;
- `admitWorkflowDefinition`; and
- `getWorkflowDefinition`.

It invented `/v1/workflow-definitions/{namespace}/{name}/{version}` and omitted most of the established definition-management capability. Definition management is common product-plane behavior; the Pekko engine did not justify a different public API.

### 3.3 Unified OpenWorkflow divergence

The unified repository then introduced:

`/home/pn/Documents/code/forwardmeasure/forwardmeasure-openworkflow/openworkflow-definition-management/openapi/definition-management.openapi.yaml`

SHA-256 observed during the audit:

```text
4270af5c60539f4753f6c87896e56c582153e83b4afe1c00ff69fb7d35c245eb
```

This was an unjustified third API. It changed the addressing model to `definitionKey` and `revisionNumber`, removed workflow CRUD, removed explicit update/delete and publication-status operations, removed published-workflow discovery, and collapsed lifecycle transitions into a generic `{action}` endpoint.

This broke generated-client and HTTP compatibility with OKS and did not preserve OAE compatibility either.

## 4. OKS implementation mistakes

Affected root:

`/home/pn/Documents/code/forwardmeasure/openworkflow-kafka-streams/oks-workflow-definition-management`

The OKS module at least retained the fuller contract and has JPA entities, repositories, and services. However, the API implementation pattern is wrong.

### 4.1 Generated abstract classes instead of interfaces

The generated JAX-RS module configures:

```xml
<interfaceOnly>false</interfaceOnly>
```

Resources therefore extend generated abstract classes, for example:

- `WorkflowDefinitionsResource extends WorkflowDefinitionsApi`
- `WorkflowsResource extends WorkflowsApi`
- `WorkflowDefinitionPublicationResource extends WorkflowDefinitionPublicationApi`
- `PublishedWorkflowsResource extends PublishedWorkflowsApi`

The required pattern is a generated API interface and a thin implementation that implements it.

### 4.2 Missing handler layer

The resource classes directly:

- resolve the authenticated actor;
- apply pagination defaults;
- parse and emit ETags;
- map API models to commands and results;
- invoke broad application services; and
- construct HTTP responses.

These are operation-handler responsibilities. Introduce explicit handlers for each use case and reduce the API implementation to delegation.

### 4.3 Framework duplication

Framework-specific resources and persistence factories were added, especially in the Micronaut and Spring modules. The intended portable API implementation should be hosted by all frameworks, not reimplemented per framework.

Review all classes under:

- `framework-bindings/quarkus`
- `framework-bindings/spring`
- `framework-bindings/micronaut`

Keep only the minimum framework composition necessary to expose the common implementation and bind ForwardMeasure JPA integration.

### 4.4 Do not discard valid OKS persistence work blindly

OKS contains explicit entities, repositories, and service interfaces/implementations under `oks-workflow-definition-management-jpa`. These must be reviewed against the manifesto and current `forwardmeasure-jpa` API. Reuse valid domain behavior and mappings where compatible; do not copy the incorrect REST/resource architecture.

## 5. OAE implementation mistakes

Affected root:

`/home/pn/Documents/code/forwardmeasure/openworkflow-actor-engine/openworkflow-definition-management`

The principal error was failing to reuse the OKS contract. This caused:

- incompatible paths and operation identifiers;
- a reduced public feature set;
- duplicate generated DTOs, server bindings, and clients;
- framework-host tests tied to the wrong endpoints; and
- avoidable migration and consolidation work.

OAE engine-specific behavior should remain behind the common execution-engine boundary. It must not define a separate definition-management API.

## 6. Unified OpenWorkflow implementation mistakes

Affected root:

`/home/pn/Documents/code/forwardmeasure/forwardmeasure-openworkflow/openworkflow-definition-management`

### 6.1 Third incompatible API

The unified contract is neither the OKS contract nor the OAE contract. Replace it with the approved common contract based on the OKS source. Do not try to preserve the invented endpoints merely because code and tests currently use them; compatibility with an accidental rewrite is not an architectural requirement.

Before replacement, confirm whether any genuinely approved maker-checker additions are absent from OKS. Add such capabilities deliberately to the one contract using explicit operations. Do not use a generic `{action}` endpoint to hide domain transitions.

Expected explicit lifecycle operations include create, validate, revise/update as approved, submit, withdraw, approve, reject, publish, deprecate, retrieve, and list. Reconcile these with the existing OKS paths without silently deleting existing operations.

### 6.2 Wrong transport-to-persistence wiring

The current effective chain is:

```text
generated DefinitionsApi
    -> DefinitionManagementResource
    -> DefinitionManagementOperations
    -> JpaDefinitionManagementOperations
    -> new DefinitionManagementService(...)
    -> new JpaDefinitionRepository(...)
    -> EntityManager
```

Problems include:

- no operation-specific handler layer;
- a broad resource-to-service dependency;
- a persistence adapter that constructs an application service;
- manual construction of repositories per call;
- direct `EntityManager` ownership in application-facing orchestration;
- tenant identity passed into an ad hoc repository adapter; and
- boundaries flowing in the wrong direction.

`JpaDefinitionManagementOperations` and `JpaDefinitionRepository` should not survive in their current form.

### 6.3 Incomplete persistence model

Only `WorkflowDefinitionEntity` and `WorkflowRevisionEntity` currently exist. WP3 and the manifesto require explicit persistence for at least:

- workflow identity;
- immutable workflow revisions;
- reproducible validation results;
- review submissions and withdrawals;
- approvals and rejections with actor, reason, digest, and timestamps;
- publication state and history;
- deprecation state/history; and
- applicable authorization/audit correlation.

The design must enforce immutable revisions, digest-bound approval, and author-not-approver/publisher rules through domain services and appropriate persistence constraints/locking—not through mutable DTO state.

### 6.4 Package and type separation violations

Persistent classes currently live in a generic `definition.persistence` package. The manifesto requires explicit `entity` packages and distinct entity, repository, persistence-service, application-service, handler, mapper, and transport packages.

Generated DTOs must not become entities or domain objects. Entities must not implement API or authorization interfaces.

### 6.5 Testing mistakes

Current tests rely heavily on in-memory repository fakes and manually created `EntityManager` fixtures. Those tests can supplement but cannot replace:

- published `forwardmeasure-jpa-contract-tests` behavior;
- PostgreSQL Testcontainers integration through published ForwardMeasure testcontainer modules;
- schema-per-tenant isolation tests;
- framework-equivalence tests for Quarkus, Spring, and Micronaut;
- OpenAPI generation/drift checks; and
- end-to-end restart and exact published-digest verification.

## 7. Data Fabric evidence-service mistake

Affected root:

`/home/pn/Documents/code/forwardmeasure/data-fabric/data-fabric-components/java/data-fabric-services/entity-intelligence-framework/entity-intelligence-evidence-service`

`InvestigationEvidenceResource` is a handwritten JAX-RS contract. It declares paths, HTTP verbs, parameters, and media types directly rather than implementing a generated API interface. No evidence-specific OpenAPI source contract was found in the module during the audit.

Additional problems:

- handlers return `jakarta.ws.rs.core.Response`, leaking transport concerns;
- handlers carry transaction annotations and combine transport orchestration with application behavior;
- the `entity-intelligence-evidence-common` module contains CDI/JAX-RS-oriented handler code rather than being genuinely portable; and
- generated-client/server contract drift cannot be detected because the resource itself defines the HTTP surface.

Repair it using the ingestion API pattern:

```text
evidence OpenAPI contract
    -> generated EvidenceApi interface and models
    -> EvidenceApiImpl implements EvidenceApi
    -> evidence operation handlers
    -> injected services
```

Preserve approved behavior, routes, and payloads when deriving the OpenAPI contract from the currently deployed evidence API. Do not casually break consumers while correcting the implementation pattern.

## 8. Authorization API and undocumented REST endpoint mistake

Affected modules:

- `/home/pn/Documents/code/forwardmeasure/forwardmeasure-openworkflow/openworkflow-authorization/openworkflow-authorization-api`
- `/home/pn/Documents/code/forwardmeasure/forwardmeasure-openworkflow/openworkflow-definition-management/openworkflow-definition-management-jaxrs`

`openworkflow-authorization-api` is intended to be a portable internal Java SPI. Its types—such as `AuthorizationService`, `AuthorizationRequest`, `AuthorizationDecision`, `AuthorizationResource`, `AuthorizationAction`, and `ActiveOrganization`—form an in-process boundary used by application services. The AuthZEN module implements that SPI and calls Keycloak's authoritative AuthZEN Evaluation/Evaluations HTTP API.

Therefore, the absence of an OpenWorkflow-owned OpenAPI document inside `openworkflow-authorization-api` is not itself the error. Creating a competing OpenAPI definition for Keycloak AuthZEN would also be wrong.

The actual errors are:

- the internal Java SPI and a product-facing authorization HTTP capability were not clearly distinguished;
- `StudioAuthorizationResource` introduced a handwritten `/api/v1/authorizations` JAX-RS endpoint;
- that endpoint is absent from the authoritative generated product API contract;
- its routes, request semantics, response shape, and compatibility therefore have no contract-first authority;
- it injects `AuthorizationService` directly and performs authorization request construction in the transport resource; and
- no generated client/server drift gate covers it.

The correction must first determine whether the Studio authorization endpoint is an approved public product capability. If it is approved, add it explicitly to the appropriate product OpenAPI contract, generate its API interface and models, and implement it through the same thin implementation -> handler -> service pattern. If it is not approved, remove the endpoint and have Studio obtain capability information through an approved API. Do not turn the internal authorization SPI into a REST module merely to legitimize the accidental endpoint.

The required distinction is:

```text
Internal authorization boundary:
application services -> AuthorizationService SPI -> Keycloak AuthZEN adapter

Optional product HTTP boundary, only if approved:
product OpenAPI -> generated interface -> thin implementation -> handler -> authorization service
```

## 9. Migration-authority gap

There is no separate OpenWorkflow migration manifesto. Current authorities are:

- `PROJECT_MANIFESTO.md`;
- `IMPLEMENTATION_PLAN.md`;
- `/home/pn/Documents/code/forwardmeasure/forwardmeasure-jpa/docs/migrations.md`; and
- the published `forwardmeasure-database-migrations` project APIs/conventions.

The unified repository currently has an `openworkflow-migrations` module and Liquibase changelogs. Audit these against the single deployment-owned migration-runner requirement. Application services must not run migrations at startup, and ORM schema creation/update must remain disabled.

If an additional migration authority is needed, obtain explicit product-owner approval before creating it.

## 10. Required correction order

Do not continue to later work packages while these foundations remain invalid.

1. Read the manifesto and implementation plan completely.
2. Preserve unrelated user changes and record the starting Git state of all affected repositories.
3. Diff the OKS, OAE, and unified OpenAPI contracts operation by operation and schema by schema.
4. Declare the OKS contract the starting public authority.
5. Propose only the minimal explicit contract additions required for approved maker-checker governance; do not implement speculative changes.
6. Generate API interfaces, models, and clients from the single contract.
7. Add an automated drift check proving generated artifacts derive from that contract.
8. Implement a thin generated-interface implementation delegating each operation to an explicit handler.
9. Implement application/domain services with lifecycle invariants and Jakarta transaction boundaries.
10. Implement explicit persistence-service interfaces and implementations.
11. Reconcile the OKS JPA model with the manifesto's complete definition, revision, validation, review, publication, and audit model.
12. Use ForwardMeasure JPA entities, repositories, tenancy, identity, metamodel, MapStruct, Liquibase, locking, and contract-test facilities as prescribed; do not recreate them locally.
13. Bind the same portable implementation through Quarkus, Spring, and Micronaut using only minimal framework adapters.
14. Run focused tests after each layer is corrected.
15. Run Kubernetes K2 only after all focused gates pass.
16. Do not claim WP3 complete until restart persistence, exact digest retrieval, maker-checker, tenant isolation, and three-framework parity are proven.

## 11. Acceptance gates

The repair is not complete unless all of the following are demonstrated.

### Contract gates

- One definition-management OpenAPI source is authoritative.
- All required OKS operations remain present unless an explicitly approved compatibility decision removes one.
- Maker-checker additions are explicit operations and schemas, not a generic `{action}` route.
- Generated Java server interfaces, Java models/client, TypeScript client, and Python client are produced from the same source.
- CI fails on contract/generated-artifact drift.
- No handwritten resource duplicates contract annotations.
- No product-facing authorization endpoint exists outside an approved generated OpenAPI contract.
- `openworkflow-authorization-api` remains an internal Java SPI and does not duplicate the Keycloak AuthZEN wire contract.

### Layering gates

- API implementation implements generated interfaces.
- Each API method delegates to an operation handler.
- API implementation contains no persistence or domain lifecycle logic.
- Handlers inject application services, not repositories or `EntityManager`.
- Concrete application services inject domain/persistence-service interfaces and own transactional use cases.
- Repository injection is limited to persistence/application-service implementations permitted by the manifesto.
- `EntityManager` appears only in approved ForwardMeasure JPA/framework binding and integration-test locations.
- Architecture tests enforce these rules.

### Persistence gates

- Explicit entity packages and distinct domain/API/entity types exist.
- Repositories use published ForwardMeasure JPA bases.
- Persistence services use published ForwardMeasure JPA service patterns.
- MapStruct handles ordinary boundary mapping.
- Canonical JPA metamodel is generated and used for fixed attributes.
- PostgreSQL Testcontainers repository contracts pass.
- Tenant operations fail closed without scope and cannot fall back to `public`.
- Cross-tenant reads and writes are denied.
- Immutable revisions and optimistic locking are enforced.
- Validation, review, publication, deprecation, and audit data survive restart.

### Domain gates

- Lifecycle transitions match the approved manifesto.
- Editing or rejection creates a new immutable revision where required.
- Approval is bound to the exact digest.
- An author cannot approve or publish their own revision.
- Administrator status does not bypass maker-checker.
- AuthZEN remains fail closed and is evaluated using the trusted active organization.

### Framework and deployment gates

- Quarkus, Spring, and Micronaut expose the same contract and behavior.
- The same portable API implementation and handlers are used by all three hosts.
- YAML is used for host configuration.
- ORM schema generation is disabled.
- The deployment-owned migration runner applies the composed changelog.
- K2 admits invalid/valid definitions, creates a revision, approves and publishes it, restarts the pod, and retrieves the exact published digest for each framework distribution.

## 12. Explicit warnings to the replacement implementer

- Do not assume passing tests prove architectural compliance; many current tests encode the wrong architecture.
- Do not retain an API merely because generated code already exists for it.
- Do not merge three contracts by inventing a fourth.
- Do not use engine differences to justify product-plane API differences.
- Do not inject repositories or `EntityManager` into REST resources or handlers.
- Do not construct services or repositories inside request handling.
- Do not make generic `{action}` endpoints substitute for explicit governed operations.
- Do not use API DTOs as persistence entities.
- Do not write framework-specific copies of portable resources or business services.
- Do not confuse an internal module named `*-api` with a public HTTP API; determine the boundary from its role and consumers.
- Do not add handwritten Studio or authorization endpoints outside the authoritative product OpenAPI contract.
- Do not declare a work package complete before its prescribed focused tests and Kubernetes checkpoint pass.

## 13. Accountability summary

The implementation failed because the existing OKS contract was not treated as reusable product authority; OAE was given an incompatible reduced API; the unified repository was then given a third incompatible API; and working HTTP behavior was accepted despite violations of the required handler, service, repository, entity, tenancy, migration, and framework boundaries. Similar contract-first discipline was also omitted from the Data Fabric evidence service. The internal authorization SPI was not clearly separated from product HTTP APIs, and an undocumented handwritten Studio authorization endpoint was introduced outside the generated contract.

The replacement work must begin by restoring one contract and one enforced architecture, not by patching individual endpoints in the current design.
