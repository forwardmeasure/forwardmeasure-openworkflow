# ForwardMeasure OpenWorkflow Project Manifesto

**Project:** `forwardmeasure-openworkflow`  
**Status:** Approved architectural authority<br>
**Created:** 2026-08-17T12:53:58-04:00  
**Last amended:** 2026-09-01 — human-task management confirmed as a committed capability<br>
**Specification:** Open Workflow `1.0.3`, pinned  
**Java SDK:** `io.serverlessworkflow` `7.29.0.Final`, pinned  
**Persistence foundation:** `forwardmeasure-jpa` `1.0.0`  
**Identity and authorization:** Keycloak `26.7.1` Organizations and AuthZEN  
**Execution engines:** Apache Kafka Streams and Apache Pekko

The ordered work packages, incremental Kubernetes checkpoints, and testing
cadence are defined in [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md). This
manifesto is the authority for product scope, architectural boundaries, and
completion claims. The implementation plan is the authority for delivery
order and acceptance gates.

## 1. Founding mandate

ForwardMeasure OpenWorkflow is one multi-module Java product implementing the
complete Open Workflow `1.0.3` specification through a common product plane and
two durable, selectable execution engines:

- Apache Kafka Streams; and
- Apache Pekko Typed, Persistence, Cluster Sharding, and Projections.

The useful, proven portions of `openworkflow-kafka-streams` (OKS) and
`openworkflow-actor-engine` (OAE) will be consolidated into this repository.
Their duplicate product models, APIs, security, persistence abstractions,
framework hosts, Studio applications, and deployment structures will not
survive as competing implementations.

The resulting product will provide:

1. definition admission, validation, immutable revisions, review, approval,
   publication, and deprecation;
2. execution start, durable pause, resume, cancellation, recovery, and query;
3. equivalent observable workflow behavior from Kafka Streams and Pekko;
4. schema-per-tenant PostgreSQL product persistence built on
   `forwardmeasure-jpa`;
5. PostgreSQL and Cassandra persistence profiles for Pekko's event-sourced
   runtime;
6. Keycloak Organization tenancy and AuthZEN authorization;
7. contract-first APIs with generated Java, TypeScript, and Python bindings;
8. portable application and transport implementations;
9. equivalent Quarkus, Spring Boot, and Micronaut distributions;
10. one Maven-built Studio application with three thin framework hosts; and
11. governed, durable human-task management integrated with both engines; and
12. Maven-built images and Helm/Helmfile/Kustomize Kubernetes deployment.

This is a consolidation program, not permission to invent a third workflow
engine or a second persistence/security/framework foundation.

## 2. Meaning of 100 percent

One hundred percent means the complete agreed product has executable evidence.
It requires:

- every normative Open Workflow `1.0.3` construct and required variant;
- schema and semantic validation, immutable compilation, execution,
  persistence, recovery, query, API, and Studio behavior for that surface;
- identical common behavioral contracts passing for Kafka Streams and Pekko;
- Pekko recovery equivalence with PostgreSQL and Cassandra;
- Quarkus, Spring Boot, and Micronaut API and operational equivalence;
- tenant isolation and authenticated actor attribution at every boundary;
- fail-closed AuthZEN decisions for every protected operation;
- maker-checker governance for definition approval/publication;
- durable start, pause, resume, and cancel semantics;
- durable human-task creation, assignment, claiming, multi-stage decision,
  rework, expiry, cancellation, recovery, query, and workflow resumption from
  both engines;
- real infrastructure and process-level disruption evidence;
- official CTK/example coverage plus ForwardMeasure conformance fixtures;
- serialization and upgrade compatibility;
- container and Kubernetes product journeys; and
- a final green full-suite run after the focused gates are green.

Parsing without execution, execution without recovery, a single engine, a
single persistence profile, a single host framework, mocked persistence, empty
generated modules, or pods that merely become Ready do not establish complete
implementation.

Milestone 4 is the 100-percent milestone. Earlier milestones are useful
vertical product increments and will not be misrepresented as complete.

## 3. Non-negotiable architecture laws

### 3.1 One product plane, two engine providers

The following capabilities exist exactly once:

- Open Workflow model boundary and compiler;
- definition and immutable revision storage;
- validation and governance lifecycle;
- publication model;
- tenant and actor context;
- authorization;
- public API definitions and language bindings;
- execution admission and engine selection;
- canonical execution/query model;
- audit model;
- human-task model, lifecycle, management API, query model, and presentation;
- Studio; and
- deployment model.

Kafka Streams and Pekko implement the same `ExecutionEngineProvider` contract.
Each engine owns only the state and mechanisms required to execute, recover,
and project its workflows.

An execution is assigned an engine when admitted and remains pinned to it.
Engine choice comes from trusted deployment or tenant policy and is recorded
with the execution. A caller cannot change an existing execution's engine or
smuggle an engine choice through workflow data.

The engines do not share internal persistence formats:

- Kafka owns its topics, tenant-qualified keys, state stores, changelogs,
  offsets, and topology state.
- Pekko owns its journal events, snapshots, persistence identifiers,
  projection offsets, sharding, and recovery protocol.
- Both engines project into the common user-facing execution model.

### 3.2 Open Workflow `1.0.3` is pinned

The product targets Open Workflow `1.0.3`, not "latest" and not a moving
compatibility range. Java SDK `7.29.0.Final` supplies the adopted schema and
official Java model boundary.

The build will record and verify the embedded schema digest. Dependency
automation must not upgrade the SDK or specification version. Moving beyond
`1.0.3` is a future product decision requiring an explicit manifesto amendment
and compatibility plan.

ForwardMeasure owns the project parent POM, release lifecycle, plugin policy,
and Java baseline. The build imports the official Serverless Workflow BOM; it
does not inherit `serverlessworkflow-parent`.

The official Java SDK reader and types are the public syntax model. The
ForwardMeasure compiler adds deterministic resource resolution, semantic
validation, digest pinning, and an immutable internal execution plan. It does
not publish a competing fork of the specification model.

### 3.3 Tenancy is schema-per-tenant, not shared-tenant storage

There is no shared application tenant table and no common tenant data schema.
Every tenant owns a PostgreSQL schema using the `forwardmeasure-jpa` convention:

```text
t_<tenant UUID without hyphens>
```

Only a validated internal tenant identifier can construct the schema name.
Neither an HTTP parameter, workflow document, token-supplied schema string, nor
arbitrary organization attribute is executed as a database identifier.

The active Keycloak Organization establishes trusted tenant context. That
context is propagated through:

- definition and execution application calls;
- JPA tenant scope and transactions;
- commands, events, replies, timers, and effects;
- Kafka record keys/headers and topic policy;
- Pekko entity and persistence identifiers;
- canonical projections and queries;
- AuthZEN evaluations; and
- audit records and telemetry.

Tenant context is mandatory and fail closed. Unscoped persistence cannot fall
back to `public`. An asynchronous boundary opens a new validated tenant scope
on its destination thread; it never propagates an open `EntityManager` or
transaction across threads.

### 3.4 Keycloak/AuthZEN is the authorization system

Keycloak `26.7.1` is pinned. One Keycloak Organization represents one tenant.
Shared OIDC clients and reusable client roles are created once per environment.
Per-tenant realms, clients, and tenant-prefixed roles are not generated.

Keycloak AuthZEN Evaluation/Evaluations APIs are mandatory in every
environment. Authorization fails closed when AuthZEN is unavailable, rejects
the request, or returns an unusable response. There is no custom RBAC engine,
embedded Data Fabric authorization implementation, Cerbos/OpenFGA side path,
or local role-based fallback.

Authorization uses the trusted active Organization and the role set belonging
to that Organization. Merged top-level `realm_access` or `resource_access`
claims cannot authorize a tenant operation because roles from multiple
Organizations could otherwise combine.

The authorization modules remain small:

```text
openworkflow-authorization/
  openworkflow-authorization-api
  openworkflow-authorization-authzen
  openworkflow-authorization-testkit
```

They define resource/action evaluation ports, the AuthZEN client adapter,
trusted context construction, batch evaluation, failure behavior, and test
fixtures. They do not become a policy engine.

Domain services remain responsible for valid lifecycle transitions and
business invariants. AuthZEN determines whether the authenticated actor may
request a valid transition.

### 3.5 Definition governance uses immutable maker-checker control

The definition lifecycle is:

```text
DRAFT -> IN_REVIEW -> APPROVED -> PUBLISHED -> DEPRECATED
               \-> REJECTED -> new immutable revision
```

The initial OpenWorkflow roles are:

- `workflow-author`;
- `workflow-approver`;
- `workflow-publisher`;
- `workflow-execution-controller`;
- `workflow-auditor`; and
- `workflow-administrator`.

AuthZEN actions include definition create/read/list/update/delete, validate,
submit, withdraw, approve, reject, publish, and deprecate; and execution
start/read/list/pause/resume/cancel plus authorized audit access.

Permission requires both an AuthZEN allow decision and satisfaction of domain
invariants. The author of a revision cannot approve or publish that revision.
Approval is bound to its immutable digest. Editing or rejection produces a new
revision. Administrator status does not silently bypass maker-checker.

Approver and publisher may be the same non-author actor unless a future tenant
policy deliberately demands a three-person rule.

### 3.6 `forwardmeasure-jpa` is the relational persistence foundation

The project imports
`com.forwardmeasure.jpa:forwardmeasure-jpa-bom:1.0.0` and uses the published
ForwardMeasure JPA modules. The version is selected once in the root parent. It
does not copy or locally reinvent their base entities, repositories, services,
tenant scope, transaction behavior, schema connection handling, migrations,
locking, framework adapters, or persistence contract tests.

The applicable modules are explicit:

| ForwardMeasure JPA module | OpenWorkflow use |
|---|---|
| `forwardmeasure-jpa-bom` | Central dependency alignment imported by the root BOM/parent. |
| `forwardmeasure-jpa-core` | Provider-neutral entity, repository, paging/specification, and application persistence-service bases. |
| `forwardmeasure-jpa-identity` | Tenant-local actors, audited/owned entities, actor resolution, and ownership services. |
| `forwardmeasure-jpa-tenancy` | Validated tenant identifiers, `TenantSchema`, and fail-closed `TenantScope`. |
| `forwardmeasure-jpa-liquibase` | Foundational changelog fragments and tenant-schema migration integration. |
| `forwardmeasure-jpa-locking` | Transaction-scoped named locks only where a documented business invariant requires serialization. |
| `forwardmeasure-jpa-async-task` | Optional durable application task lifecycle; not a workflow engine or default dependency. |
| `forwardmeasure-jpa-contract-tests` | Shared persistence behavior run against OpenWorkflow domain repositories and hosts. |
| `forwardmeasure-jpa-quarkus` | Quarkus schema-tenancy and Hibernate ORM integration. |
| `forwardmeasure-jpa-spring` | Spring Boot schema-tenancy and Hibernate ORM integration. |
| `forwardmeasure-jpa-micronaut` | Micronaut schema-tenancy and Hibernate ORM integration. |

The following dependency boundary is mandatory:

```text
HTTP/JAX-RS resource, message consumer, workflow processor
                        |
                        v
             application/domain service interface
                        |
                        v
       application/domain service implementation
          (Jakarta Transaction boundary)
                        |
                        v
            standard-JPA domain repository
                        |
                        v
       forwardmeasure-jpa repository + framework adapter
                        |
                        v
               framework-owned EntityManager
```

Resources, generated-server implementations, message consumers, schedulers,
engine entry points, and projections do not inject a repository or
`EntityManager`. They inject application/domain service interfaces.

Concrete application/domain service implementations may inject the domain
repositories needed for their use case and own the Jakarta Transaction
boundary. No other application code injects repositories. Domain repositories
extend the appropriate `forwardmeasure-jpa` standard-JPA base. `EntityManager`
is confined to the ForwardMeasure JPA/repository infrastructure and the
smallest framework composition hook required to bind that repository. It never
appears throughout business code as an ad hoc persistence API.

Architecture tests will reject `EntityManager`, `PersistenceContext`, Spring
Data repositories, Panache entities/repositories, Micronaut Data repositories,
and framework transaction annotations outside approved persistence/framework
adapter packages. They also reject repository injection outside concrete
application/domain service implementations and persistence adapters. Tests may
use an `EntityManager` only in repository/framework integration fixtures whose
purpose requires it.

ForwardMeasure JPA owns transaction and persistence-context conventions:

- a validated tenant scope is opened before the framework-managed transaction;
- an unscoped operation fails instead of using `public`;
- ORM schema generation is disabled;
- repositories are standard JPA and shared across hosts;
- service implementations use Jakarta Transaction semantics;
- `open-in-view` behavior is disabled;
- repository streams never escape their transaction; and
- optimistic locking comes from the shared entity foundation.

Persistent domain classes live in explicit `entity` packages. Public API DTOs,
domain commands/plans, and persistence entities are different types. MapStruct
mappers live at the adapter boundary; entities are not API DTOs and do not
implement authorization interfaces.

Persistent entities follow the foundation's Lombok conventions and generate
the canonical JPA metamodel at compilation. Fixed repository attributes use
that metamodel rather than string property names. MapStruct plus the
Lombok/MapStruct binding is configured centrally in the root build; handwritten
field-by-field DTO/entity conversion is used only when generated mapping would
obscure a material semantic transformation.

Persistence integration tests consume the published
`forwardmeasure-testcontainers` PostgreSQL/JUnit/framework modules. This
repository does not grow another generic container lifecycle library.

Application-owned relational tables use this foundation. It does not wrap or
replace third-party engine persistence mechanisms:

- Pekko PostgreSQL journal/snapshot tables are owned by the selected Pekko
  persistence plugin and its tenant-scoped adapter, not by JPA entities.
- Cassandra journal/snapshot tables are not JPA.
- Kafka topics, stores, and changelogs are not JPA.
- Common product tables and canonical query projections are application-owned
  and therefore use the ForwardMeasure JPA boundary.

### 3.7 Migrations have one deployment owner

`openworkflow-migrations` is the sole migration executable. It composes:

1. the applicable `forwardmeasure-jpa-liquibase` foundational changelogs;
2. common OpenWorkflow product changelogs;
3. conditionally enabled relational engine changelogs; and
4. explicit seed data such as approved named-lock definitions.

The migration executable enumerates/provisions tenant schemas and invokes the
tenant schema migrator before application workloads use a changed schema.
Migrations are a bounded Kubernetes Job and an explicit local command. Runtime
services do not run migrations at startup, and ORM tools do not create/update
schemas.

Liquibase is the relational schema authority. Cassandra uses versioned,
idempotent CQL migrations owned by the relevant Pekko persistence adapter.
Kafka topology/topic provisioning is owned by the Kafka deployment profile,
not a JPA migration.

### 3.8 API definitions are the public contract authority

Each public capability is contract-first. Its reviewed OpenAPI YAML is the
single public HTTP contract and is stored beside the capability:

```text
openworkflow-<capability>/
  openapi-specifications/
    <capability>.openapi.yaml
  api-language-bindings/
    java/
      openworkflow-<capability>-models
      openworkflow-<capability>-client-apachehttp
      openworkflow-<capability>-server-jaxrs
    typescript/
      openworkflow-<capability>-client-typescript
    python/
      openworkflow-<capability>-client-python
  openworkflow-<capability>-application/
  openworkflow-<capability>-jaxrs/
  openworkflow-<capability>-jpa/
  openworkflow-<capability>-contract-tests/
```

The initial public API families are:

- workflow definition management;
- workflow execution management/control;
- workflow execution query/history;
- workflow event ingress where required by the specification;
- human task management; and
- operational health/readiness endpoints that do not leak framework-specific
  models.

OpenAPI generation rules are mandatory:

- API definitions use OpenAPI `3.1.x`, versioned `/v1` paths, bearer security,
  and consistent RFC 9457 problem details.
- Java DTO models are generated once and reused by the client and server
  contracts.
- The Java client uses the selected Apache HTTP client generator unless a
  future explicit decision replaces it consistently.
- The Java server artifact contains generated Jakarta JAX-RS interfaces only.
- TypeScript and Python clients are generated from the same YAML.
- Generation timestamps are disabled for reproducibility.
- Specifications are validated during the build.
- Generated source is never hand-edited or copied into handwritten source
  directories.
- A clean regeneration/drift test must reproduce committed/generated outputs
  according to the repository's chosen generation policy.
- Breaking API changes require an explicit versioning/migration decision.
- Mutation APIs define consistent idempotency, ETag/optimistic concurrency, and
  conflict semantics; collection APIs define deterministic bounded pagination.

API DTOs do not become domain models or JPA entities. The handwritten portable
JAX-RS adapter implements the generated server interface, maps DTOs to domain
commands through explicit mappers, obtains authenticated tenant/actor context
from trusted providers, invokes application services, and maps domain failures
to the contract's problem response.

Clients never supply authoritative tenant IDs, actor IDs, approval identities,
database schema names, engine persistence IDs, or authorization decisions in
request bodies. Idempotency/correlation headers and optimistic version fields
are defined consistently across API families.

### 3.9 Portable application and transport modules remain framework-neutral

The application module owns use-case orchestration, transaction-independent
domain decisions, ports, and stable service interfaces. It may depend on:

- domain/model/compiler modules;
- generated API models only at an explicit mapping edge;
- authorization and persistence service interfaces; and
- engine/query/event ports.

It may not depend on Quarkus, Spring, Micronaut, Hibernate implementation APIs,
Kafka Streams, Pekko, HTTP clients, or Kubernetes libraries.

The portable JAX-RS module owns generated-interface implementations, DTO/domain
mappers, problem mapping, and transport validation. It may depend on Jakarta
JAX-RS/CDI APIs and application services. It may not create repositories,
transactions, engine runtimes, persistence contexts, or framework-specific
security objects.

Dependency direction is enforced:

```text
generated API contracts <- portable JAX-RS adapter -> application/domain ports
                                                     ^
                                                     |
                      JPA, AuthZEN, Kafka, and Pekko adapters

framework executable -> composes every required adapter; owns no domain logic
```

Cycles, service-to-framework dependencies, and engine-to-HTTP dependencies are
build failures.

### 3.10 Framework bindings are equivalent composition roots

Quarkus, Spring Boot, and Micronaut are first-class, simultaneous source/build
targets. A deployment selects one framework distribution; the product does not
mix frameworks within a process.

Each framework family contains:

```text
framework-bindings/<framework>/
  openworkflow-<framework>-binding
  openworkflow-<framework>-service
  openworkflow-studio-<framework>
```

The binding module may contain only host integration:

- bean/component registration and portable adapter discovery;
- configuration binding and validation;
- framework security-principal/JWT adaptation into trusted tenant/actor
  context;
- the corresponding `forwardmeasure-jpa-<framework>` integration;
- transaction/persistence composition hooks;
- health/metrics integration;
- engine lifecycle startup/shutdown wiring; and
- native-image/AOT metadata where applicable.

The service module is the executable assembly. It chooses enabled capabilities,
engine/persistence profiles, ports, and image packaging. It contains no
workflow semantics, repository queries, authorization rules, or duplicate REST
implementations.

The Studio host serves the same built webapp artifact and supplies only
framework-specific static-resource/runtime configuration. Studio is a Maven
module, not an untracked frontend outside the reactor.

All three hosts:

- consume the same generated JAX-RS interfaces and portable implementations;
- use the same domain/JPA repositories and service implementations;
- use their official `forwardmeasure-jpa` adapter;
- use YAML configuration;
- expose the same paths, status codes, media types, and problem documents;
- pass the same black-box framework acceptance suite; and
- are built into images by Maven.

No host-specific endpoint, Spring Data implementation, Panache implementation,
Micronaut Data implementation, or host-only business feature is allowed.

### 3.11 Pekko is the Pekko engine's finite-state machine

Each Pekko execution is one sharded, tenant-qualified event-sourced entity and
single writer. Its persistent behavior uses Pekko's documented event-sourced
FSM pattern, sealed durable state types, enforced replies, and state-specific
command/event handlers.

Durable state represents new, running, waiting, pausing, paused, cancelling,
cancelled, completed, and failed states as required. It contains the compiled
definition cursor, nested frames, branch state, retries, pending effects, and
deadlines needed for deterministic recovery.

Commands produce persisted events. Event handlers reconstruct state during
normal operation and replay. External side effects occur only after a durable
effect intent. Deadlines are persisted before an in-memory timer is scheduled
and are recreated after recovery.

The project will not implement a second generic workflow FSM, scheduler,
mailbox, journal, recovery loop, or actor-independent `decide/apply` engine
beside Pekko.

### 3.12 Pause and cancellation are workflow guarantees

A workflow can be paused, resumed, and cancelled through durable commands.

| Operation | Durable behavior |
|---|---|
| Pause | Stop admitting new work at a defined safe boundary, persist `PAUSING` then `PAUSED`, and retain the exact cursor/pending work. |
| Resume | Continue from the retained durable cursor without repeating accepted effects or completed tasks. |
| Cancel | Persist `CANCELLING`, stop admitting work, request best-effort cancellation of cancellable effects, resolve late observations deterministically, then persist irreversible `CANCELLED`. |

Acknowledgement means the relevant transition is durable. A restarted or
relocated execution remains paused/cancelled. Late timers, retries, responses,
duplicates, or projections cannot resurrect a cancelled workflow.

Kafka Streams must meet the same observable contract using Kafka-native
mechanisms; it does not imitate Pekko's internal representation.

### 3.13 Canonical queries do not own engine persistence

`openworkflow-execution-query` owns canonical query contracts, projections,
query persistence services, pagination/filtering, and public query/history
APIs.

It does not configure Pekko journals/snapshots or Kafka state stores. Pekko
persistence configuration belongs to `openworkflow-engine-pekko`; Kafka
topology/storage configuration belongs to
`openworkflow-engine-kafka-streams`.

Engine projection adapters translate engine-native facts into idempotent,
ordered canonical updates. Studio and public clients query this common plane
and never read engine stores directly.

### 3.14 Studio is a first-class product capability

Studio provides:

- lossless YAML/JSON authoring;
- schema and semantic validation diagnostics;
- revision comparison and governance actions;
- role/decision-aware controls using AuthZEN batch evaluation;
- definition visualization;
- execution list, detail, state, timeline, errors, retries, effects, and audit;
- start, pause, resume, and cancel controls where authorized; and
- consistent behavior against either engine.

Hiding a button is not authorization; every operation is enforced server-side.
The editor must preserve the complete `1.0.3` document and cannot silently
discard constructs it does not visualize.

### 3.15 Deployment is application-owned and automated

Maven builds service, Studio, operation-adapter, and migration images through
the selected container plugin/profile. Operators do not hand-run `docker
build` for standard deployment.

The repository owns:

- namespace `forwardmeasure-openworkflow`;
- application charts;
- Helmfile release orchestration;
- Kustomize environment overlays where useful;
- Keycloak/AuthZEN configuration reconciliation;
- migration Jobs;
- framework, engine, persistence, and Studio profiles;
- application service accounts/RBAC/network policies; and
- optional ingress, disabled by default.

`openworkflow-k8s-setup` is cloud infrastructure only. It creates managed
Kubernetes/network/IAM, managed PostgreSQL, and GCS/S3/Blob resources and
exports metadata. It does not create namespaces, Keycloak, cert-manager,
Istio, External Secrets, Kafka, Cassandra, monitoring, application workloads,
Helm releases, or application Kubernetes resources.

Production PostgreSQL and Cassandra profiles are real but optional. They
require production values only when deliberately selected. There is no
OpenShift-specific deployment model.

### 3.16 Build and code conventions are centrally enforced

The root parent owns all library, plugin, framework-platform, and internal
module versions. Child POMs do not select versions independently. The
`openworkflow-bom` exports supported public artifacts without making consumers
inherit the project parent.

The Java baseline is Java 25 unless an approved dependency compatibility issue
requires an explicit amendment. Maven wrapper and toolchains make that baseline
reproducible.

Spotless with `google-java-format` formats production and test Java and
organizes imports. Generated sources are excluded from reformatting and are
reproduced by their generator. Compiler warnings, dependency convergence,
duplicate classes, license policy, architecture tests, unit/integration split,
and aggregate coverage are configured once in the parent.

Package names begin with `com.forwardmeasure.openworkflow`. API, application,
domain, persistence, engine, transport, and framework packages reflect their
architectural role rather than the host framework. Configuration property
names remain common across hosts wherever the frameworks permit it, and all
checked-in application configuration uses YAML.

### 3.17 Human-task management is a committed product capability

Human-task management is a first-class ForwardMeasure extension to the pinned
Open Workflow `1.0.3` product. It is not optional future scope and is required
before Milestone 4 or 100-percent completion may be claimed.

The capability exists exactly once in the common product plane. It owns:

- the contract-first human-task management API and generated Java,
  TypeScript, and Python bindings;
- the portable task aggregate, application services, query model, audit model,
  and presentation contract;
- tenant-scoped persistence, optimistic concurrency, idempotent commands, and
  durable task history;
- assignment, claim, release, delegation, approval, rejection, rework,
  reopening, escalation, expiry, and cancellation semantics;
- multi-stage approval policy and actor/role eligibility;
- AuthZEN resources/actions and fail-closed authorization; and
- Studio work queues, task detail, history, actionable controls, and approved
  presentation extensions such as A2UI.

The durable lifecycle includes `OPEN`, `ASSIGNED`, `CLAIMED`, `APPROVED`,
`REJECTED`, `REWORK_REQUESTED`, `EXPIRED`, and `CANCELLED`. Exact transition
rules belong to the shared human-task domain contract, not to either workflow
engine or a framework host.

Both Kafka Streams and Pekko integrate through the same engine-neutral
human-task ports. A workflow persists its human-task effect intent before task
creation is dispatched. Task creation and terminal/rework observations are
idempotently correlated to the originating execution and branch. Pause,
resume, cancellation, retry, late observations, engine restart, and task
service restart cannot lose a task, repeat a completed decision, or resurrect
a cancelled workflow.

Human-task management is an independently deployable and scalable capability.
Quarkus, Spring Boot, and Micronaut host the same portable implementation and
contract; a platform installation deploys only the selected framework image.
The workflow-engine images do not absorb the task-management API, persistence,
or user work queue.

The approved implementation design, state machine, lease semantics, API shape,
and Studio experience are specified in
[`docs/human-task-design.md`](docs/human-task-design.md).

## 4. Common relational product model

Each tenant schema contains common application-owned tables for:

- stable workflow definitions;
- immutable workflow revisions and source/resolved digests;
- validation results;
- review/approval/rejection history;
- publications and deprecation;
- canonical execution identity and selected engine;
- canonical execution state/history projections;
- human-task current state, assignment, decisions, and immutable history;
- idempotent command receipts; and
- authorization/audit correlation.

These are implemented as explicit ForwardMeasure JPA entities, repositories,
persistence service interfaces/implementations, mappers, migrations, and
repository contract tests.

Engine-specific relational tables are installed conditionally into the same
tenant schema. Pekko plugin-owned tables are not exposed through business JPA
repositories. Cassandra and Kafka data remain logically tenant-qualified in
their native stores.

An execution always records its immutable definition revision digest and
selected engine. Later definition edits/publications cannot change an existing
execution's meaning.

## 5. Entity Intelligence extension

Introducing Entity Intelligence extends the same tenant Organization through
a separately versioned capability pack. It does not change the meaning of
OpenWorkflow roles.

Tenant provisioning exposes idempotent reconciliation such as:

```text
provisionOrganization(...)
reconcileCapabilityPack(OPENWORKFLOW)
reconcileCapabilityPack(ENTITY_INTELLIGENCE)
```

The Entity Intelligence pack defines reusable client roles for dossier,
entity, evidence, information extraction, ingestion, investigation,
resolution, screening, and reference-population approval. Enabling the pack
creates empty Organization role groups and policies; it never assigns users or
elevates privileges automatically.

AuthZEN maps broad product roles to fine-grained resources/actions and applies
maker-checker where required, including reference-population approval.
`workflow-internal` is not a human Organization role; cross-product automation
uses a narrow workload identity such as
`entity-intelligence-workflow-invoker`.

The Organization may carry the stable tenant identifier/DID association used
by trusted provisioning. Tokens and clients never receive a database schema
selector. No dependency on the retiring Data Fabric persistence or embedded
RBAC code is introduced.

## 6. Reuse, migration, and retirement laws

Code may be copied from OKS and OAE with product-owner permission, but every
adopted source receives a provenance entry and is judged by the new boundaries.
Existing code volume is not evidence of correctness.

Priority reuse is:

- official SDK boundary, reviewed compiler, validation, and jq behavior;
- deterministic semantic and conformance tests;
- OAE Pekko persistent actor behavior;
- OKS Kafka Streams topology/runtime behavior;
- API specifications and generated-binding patterns;
- PostgreSQL repository contracts refactored onto `forwardmeasure-jpa`;
- useful event, operation, human-task, query, and Studio capabilities; and
- deployment/runbook lessons.

The following are removed rather than unified:

- duplicate tenant/security/public API models;
- embedded home-grown RBAC;
- scattered `EntityManager` and framework-specific repositories;
- per-framework business implementations;
- home-grown Pekko-adjacent FSMs;
- duplicated Studio applications;
- manual image/namespace deployment instructions; and
- unsupported completion claims.

Existing live executions ordinarily complete in their original engine. They
are not translated between Kafka and Pekko unless a separate, executable
migration proof exists. Definitions and publication history may be imported as
immutable records with verified digests.

OKS and OAE stop publishing only after the unified replacement passes its
contract/conformance gates and consumers migrate. The old repositories become
read-only historical/provenance sources and are archived only after rollback
windows close.

## 7. Testing and evidence law

Testing is layered to preserve delivery speed:

1. inner-loop formatting, affected compilation, focused unit/behavior tests,
   and generated-contract drift;
2. work-package contract tests, relevant real Testcontainers, three-framework
   acceptance, and the incremental Kubernetes checkpoint;
3. milestone product journeys; and
4. one complete suite at Milestone 4 and one final release-candidate run when
   required by final shared changes.

The full regression matrix is not run after every edit. Focused tests are not
skipped when they help design correct code.

Architecture tests enforce at least:

- dependency direction and absence of module cycles;
- no framework dependencies in domain/application modules;
- no engine dependencies in common product modules;
- no `EntityManager` or repositories in resources/application/engine code;
- no alternate Spring Data/Panache/Micronaut Data domain persistence;
- no handwritten modification of generated API source;
- no API DTOs used as JPA entities or engine durable events;
- no untrusted tenant/actor/schema authority;
- no authorization fallback around AuthZEN;
- no custom FSM beside Pekko; and
- equivalent framework composition and YAML configuration.

Contract suites establish:

- ForwardMeasure JPA repository/service and tenant isolation behavior;
- definition governance and maker-checker;
- API compatibility and generated bindings;
- engine semantics and canonical projections;
- PostgreSQL/Cassandra Pekko recovery parity;
- Kafka restart/rebalance behavior;
- pause/resume/cancel under process disruption;
- human-task lifecycle, authorization, multi-stage decisions, workflow
  correlation, restart recovery, and cross-engine parity;
- AuthZEN active-Organization isolation; and
- Kubernetes definition-to-execution journeys.

## 8. Milestone contract

### Milestone 1 — Governed end-to-end execution

Accept, validate, version, independently approve, publish, start, execute,
recover, and query a workflow through the common APIs with both engines, all
three framework hosts, PostgreSQL product persistence, and both Pekko
persistence profiles.

### Milestone 2 — Studio

Author, validate, review, approve/publish, visualize definitions, and observe
executions through one Studio webapp and all three Studio hosts.

### Milestone 3 — Workflow control

Start, pause, resume, and cancel through common APIs and Studio with durable,
recoverable, equivalent behavior from both engines.

### Milestone 4 — Complete Open Workflow `1.0.3` and committed extensions

Complete every `1.0.3` semantic capability, adapter, conformance fixture,
recovery path, query representation, API/framework binding, and Studio view;
complete the committed human-task capability through both engines, all three
framework hosts, its management API, persistence, authorization, query, and
Studio journeys; pass the complete acceptance matrix; and establish 100
percent.

## 9. Prohibited shortcuts

The implementation must not:

- create a shared tenant table or use a shared tenant data schema;
- treat a tenant ID from an HTTP body as trusted routing context;
- scatter `EntityManager` or repository access through services/resources;
- copy ForwardMeasure JPA functionality into local utility classes;
- use ORM schema auto-generation or application-startup migrations;
- make JPA entities double as public DTOs, AuthZEN resources, or durable engine
  events;
- handwrite generated API interfaces/models or expose framework-specific API
  contracts;
- implement only one framework and label the portable modules complete;
- implement only one engine and call the unified milestone complete;
- omit human-task management, leave it unreachable behind engine capability
  rejection, or mark it implemented from compiler/internal-state evidence
  alone;
- share engine-native persistence under a misleading common abstraction;
- write a generic custom FSM beside Pekko;
- authorize from merged top-level Keycloak roles;
- allow an author or administrator to self-approve/self-publish a revision;
- make AuthZEN optional or fail open;
- require ingress, production credentials, image attestations, security-service
  API keys, live URLs, or scale-target approvals before ordinary development;
- make the cloud-infrastructure repository deploy applications; or
- run the full product regression suite after every small change.

## 10. Decision and change control

This manifesto is intentionally explicit so a new implementation session can
begin without reconstructing decisions from conversation history.

Routine implementation choices that comply with these laws proceed without
additional approval. A material change to the pinned specification, engines,
persistence profiles, tenancy, `forwardmeasure-jpa` boundary, API-generation
model, framework parity, AuthZEN, maker-checker, deployment ownership,
human-task capability, or milestone acceptance contract requires an amendment
before code silently diverges.
