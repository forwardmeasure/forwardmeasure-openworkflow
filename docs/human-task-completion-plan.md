# Human Task Management Completion Plan

**Purpose:** implementation handoff for completing the approved Human Task capability
**Status:** remaining-work plan; it is not a completion claim
**Architectural authorities:** `PROJECT_MANIFESTO.md`, `IMPLEMENTATION_PLAN.md`, and `docs/human-task-design.md`
**Prepared:** 2026-09-02

## 1. Completion standard

This plan is complete only when the completion rule in section 16 of `docs/human-task-design.md` has executable evidence. Compilation, generated interfaces, pod readiness, and “it exists in a repo” are not completion evidence by themselves.

The implementation is complete only when a real workflow can:

- durably create an arbitrary-shaped task;
- acquire, release, recover, and expire a review lease;
- preserve every correction and decision immutably;
- advance or resolve the review plan according to policy;
- resume the exact workflow branch through both Kafka Streams and Pekko;
- satisfy the API, persistence, authorization, framework-host, and Kubernetes evidence gates.

## 2. Operating rules for the implementing agent

1. Implement in order. Do not skip ahead to framework, infrastructure, or Studio work before the portable domain and persistence gates are green.
2. Preserve unrelated worktree changes. Do not rewrite or delete uncommitted code merely because it is inconvenient.
3. Read the exact files you are about to change and inspect the current diff before editing.
4. Do not invent a second Human Task state machine or a second API contract. The design document is the source of truth.
5. Use focused tests after each change and do not run the full multi-repo reactor until the relevant gate is green.
6. If a failure is caused by a topology problem, fix the topology problem before forcing a code workaround.
7. Treat stale target outputs as non-authoritative evidence. Re-run the gate with fresh output.

## 3. Repository ownership map

The implementation is cross-repo and must be traced to the repo that owns the behavior.

- `forwardmeasure-platform`
  - owns the shared parent, compatibility reactor, BOM, build policy, and cross-repo validation orchestration.
  - entry point for local compatibility verification via `reactor.xml`.
  - also owns the shared cluster deployment layer in `forwardmeasure-platform/deploy`.

- `forwardmeasure-jpa`
  - owns portable JPA abstractions, tenant-scoped persistence conventions, repository behavior, and persistence contracts.

- `forwardmeasure-database-migrations`
  - owns schema migrations, tenant provisioning, Liquibase change status, and migration verification.

- `forwardmeasure-openworkflow`
  - owns Human Task domain, compiler normalization, API contract, persistence application, workflow adapters, and engine-neutral ports.
  - also owns the per-application deployment orchestration in `forwardmeasure-openworkflow/deploy`.

- `forwardmeasure-openworkflow/openworkflow-studio`
  - is the current branding and UI authority for OpenWorkflow Studio itself.
  - its webapp theme contract (`openworkflow-studio/webapp/src/main/webapp/src/theme.ts`) is the current source of truth for Studio look-and-feel and must remain the canonical theming surface for any Human Task UI work.
  - theming must be externalized and consistent instead of hardcoded in feature screens.

- `data-fabric/data-fabric-components/java/data-fabric-services/entity-intelligence-framework/entity-intelligence-workbench`
  - owns the concrete product-workbench pattern for tenant-scoped analyst UX, runtime-config wiring, route-style packaging, and a routed UI shell that is separate from the platform dashboard.
  - this is the concrete source to mirror for the general product-workbench composition and runtime-config patterns, while `openworkflow-studio` remains the current OpenWorkflow branding authority.

- `forwardmeasure-object-storage`
  - relevant when content or evidence artifacts are materialized or referenced beyond inline payloads.

- `forwardmeasure-platform-operations`
  - relevant when operational status, monitoring, and deployment health are wired into the Human Task service.

- `forwardmeasure-testcontainers`
  - owns Testcontainers-based persistence verification and reusable infrastructure for PostgreSQL and related contract tests.

- `forwardmeasure-agent-os`, `forwardmeasure-entity-intelligence`, `forwardmeasure-entity-matching`, `forwardmeasure-nlp`, `forwardmeasure-decision-engine`
  - relevant as producers/consumers or domain callers of Human Task behavior, not as the authoritative task implementation.

- `helm-charts` and `openworkflow-k8s-setup`
  - own the deployment packaging, Helm release, service definitions, and Kubernetes acceptance journey.

## 4. Baseline status

The following code should be treated as a starting point, not as completion evidence:

- engine-neutral `DataReference` and shared JSON tests;
- compiler recognition and typed `HumanTaskCallPlan` normalization;
- sealed Human Task state/command/event model and formal state transition implementation;
- application-layer command processing, idempotent command receipts, query service, outbox records, and review credential logic;
- JPA entities, repository implementation, Liquibase change set, and one PostgreSQL repository contract journey;
- Human Task OpenAPI contract plus generated Java, TypeScript, and Python artifacts;
- AuthZEN resources/actions, including dynamic `human-task:decide:<action-code>` authorization;
- portable JAX-RS resource implementing generated interfaces and basic exception mappers;
- Kafka Streams scaffolding and inherited wire/restoration fixtures.

The following are not yet completion evidence:

- exhaustive state/command matrix coverage;
- full concurrency and restart safety evidence;
- complete tenant-isolation and cross-tenant denial evidence;
- comprehensive API/problem/pagination coverage;
- framework-host parity coverage;
- Human Task deployment images and Helm release;
- production bridge from workflow request to task outcome in both Kafka Streams and Pekko;
- Studio blotter and task resolution workspace completion;
- Kubernetes disruption journey.

## 5. Lockdown decisions to preserve

The implementing agent must retain all of the following decisions:

1. Human Task Management is one engine-neutral product-plane capability with one formal FSM.
2. The portable domain, application, JPA, and JAX-RS layers are shared by Quarkus, Spring Boot, and Micronaut. A deployment selects one framework at a time.
3. Kafka Streams and Pekko are adapters to common request/outcome contracts. They do not own Human Task state or persistence.
4. Kafka Streams’ existing Human Task effects, waiting-state, outcome handling, and durable fixtures are migration input and must not be silently broken.
5. Pekko must support Human Tasks with both PostgreSQL and Cassandra persistence profiles.
6. `com.forwardmeasure.openworkflow.human-task` is the only call identifier. Do not restore a `com.forwardmeasure.oks.*` alias.
7. `approvals` is the workflow-authoring shape; immutable `ReviewPlan` is the normalized domain shape; the compiler produces `HumanTaskCallPlan`.
8. `DataReference` has one engine-neutral owner in `openworkflow-data`.
9. Tenant identity comes from the active Keycloak Organization. Every database operation must run under a validated tenant scope; no public fallback is allowed.
10. AuthZEN is authoritative and fails closed. Stage eligibility remains a domain invariant.
11. The original content and all decisions are immutable. Corrections are forward-only and history is append-only.
12. Task expiry is optional and infinite when absent. Review leases are finite, renewable, and independent from assignment and task expiry.
13. The model remains multi-stage, rework, reopening, escalation, and custom-disposition capable even though the initial UI journey uses one stage.
14. The OpenAPI document is the HTTP authority. The portable resource implements generated JAX-RS interfaces and exposes explicit operations, not a generic action endpoint.
15. Human Task Management is independently deployable and scalable with its own release, service, policy, image version, and migration dependency.

## 6. Execution sequence and gates

### HT0 — Trustworthy baseline and repo inventory

Objective:
- ensure the working tree is understood before any edits;
- identify which repo owns which behavior;
- get a clean baseline for the correct scope.

Owner:
- `forwardmeasure-platform` + `forwardmeasure-openworkflow`

Work:
1. Read the three authority docs in full.
2. Inspect git status and all diffs touching Human Task work.
3. Inventory the Human Task modules in the root reactor and BOM.
4. Confirm generated API modules, migration inclusion, and architecture-test allowlists.
5. Run only the focused Human Task tests currently relevant to the slice.
6. Classify failures as implementation, topology, or environment issues.

Verification:
- Run the smallest relevant Maven test set for the module(s) under change.
- Record exact commands, exit codes, and failing tests.

Exit gate:
- the relevant slice compiles, and the current focused Human Task tests pass from a clean invocation;
- the checkpoint lists modified and untracked files and known unrelated failures.

### HT1 — Freeze the portable contracts

Objective:
- make the domain and serialization model authoritative before host and transport work.

Owner:
- `forwardmeasure-openworkflow`

Work:
1. Compare `HumanTaskCallPlan`, domain identifiers, commands, sealed states, events, outcomes, and `DataReference` against sections 3 and 4 of the design.
2. Add serialization golden fixtures for plans, requests, states, commands, events, outcomes, and shared data references.
3. Replace representative state-machine coverage with a table-driven exhaustive legality matrix.
4. Add event-prefix replay tests for every legal transition, including multi-stage, custom disposition, escalate, rework, reopen, cancel, expire, and stale revision/lease cases.
5. Verify injected-clock behavior for task expiry and review lease expiry independence.
6. Add architecture tests proving the portable modules contain no framework, JPA, Kafka Streams, or Pekko dependencies.

Verification:
- Run the domain, data, compiler, and serialization tests for the relevant module(s).
- Add new tests until every legal and illegal state/command pair is explicit and covered.

Exit gate:
- exhaustive domain and serialization tests pass;
- adding a new state, command, or event cannot compile or pass without deliberate matrix and serialization updates.

### HT2 — Complete application, tenancy, persistence, and outbox behavior

Objective:
- lock down the managed task lifecycle in durable storage with correct tenant scoping and atomic writes.

Owner:
- `forwardmeasure-openworkflow` + `forwardmeasure-jpa` + `forwardmeasure-database-migrations` + `forwardmeasure-testcontainers`

Work:
1. Resolve tenant-scope ownership before framework bindings. The transaction must run inside a trusted tenant scope derived from the active organization.
2. Keep framework transaction primitives behind a portable transaction boundary. The repository is a persistence adapter; the FSM remains the sole transition authority.
3. Promote optimistic-conflict and persistence-unavailable failures to portable application exceptions and map them consistently to RFC 9457 responses.
4. Verify atomic writes of snapshot, ordered events, content revisions, command receipt, review session, and outbox message.
5. Complete the outbox lifecycle: message identity, pending selection, claim/lease, successful publication marking, retry handling, and duplicate-safe delivery.
6. Verify immutable event/content history and prohibit arbitrary snapshot mutation.
7. Complete cursor pagination and bounded filtering/sorting for status, task type, source, priority, assignment, reviewer, received time, due time, and promoted blotter fields.
8. Add PostgreSQL Testcontainers cases for tenant separation, concurrent lease acquisition, stale decision rejection, idempotency replay, append-only history, lease expiry without task expiry, transaction rollback, and restart reconstruction.
9. Validate the Liquibase change set via the single migration owner; confirm constraints, indexes, sequence allocation, upgrades, rollback policy, and tenant-provisioning inclusion.

Verification:
- Run the focused persistence, application, and contract tests in the relevant modules.
- Run the PostgreSQL Testcontainers suite for the Human Task repository and migration contract.

Exit gate:
- application and PostgreSQL contract suites pass, including concurrency and tenant-isolation evidence;
- no persistence call runs without trusted tenant scope.

### HT3 — Complete the contract-first API and authorization boundary

Objective:
- make the HTTP contract, authorization, and problem responses trustworthy and host-neutral.

Owner:
- `forwardmeasure-openworkflow`

Work:
1. Validate the single OpenAPI 3.1 source and regenerate Java, TypeScript, and Python artifacts.
2. Confirm all explicit operations exist: reads, review sessions, revisions, comments, release, decision, assignment, reassignment, cancel, expire, and presentation.
3. Enforce `If-Match`, `Idempotency-Key`, review-session token, trusted correlation ID, and trusted actor/tenant context at the correct operations.
4. Add RFC 9457 mappings for malformed input, unauthorized, forbidden, missing, stale revision, lease conflict, illegal transition, validation failure, duplicate command, and dependency-unavailable paths.
5. Verify AuthZEN resource attributes and exact action names for every operation and ensure decision auth resolves to `human-task:decide:<action-code>` before mutation.
6. Verify stage eligibility independently of AuthZEN allow and ensure AuthZEN fail-closed handling.
7. Create a reusable contract-test module for Human Task API behavior rather than host-specific duplication.
8. Add an architecture test proving the resource implements the generated server interfaces and contains no framework-specific persistence code.

Verification:
- Run the generated API contract tests and authorization tests.
- Validate OpenAPI generation drift where the API source is the sole authority.

Exit gate:
- OpenAPI generation drift, JAX-RS implementation, authorization, problem responses, ETag handling, idempotency, and pagination tests all pass.

### HT4 — Add equivalent Quarkus, Spring Boot, and Micronaut hosts

Objective:
- prove the same portable stack behaves identically across three frameworks.

Owner:
- `forwardmeasure-openworkflow` + framework-specific modules

Work:
1. Create the framework-binding subtree used by the design.
2. Wire transaction runner, trusted organization/actor context, tenant scope, repository, application/query services, AuthZEN client, correlation ID, HMAC review credential handling, generated JAX-RS resource, and problem mappers.
3. Validate startup configuration for default lease duration, renewal policy, payload bounds, pagination bounds, and outbox timing.
4. Ensure JAX-RS discovery works in each framework through the required patterns for that framework.
5. Add black-box tests that run the same contract against all hosts and compare status, body, ETag, problem payloads, authorization, tenant routing, and persistence effects.

Verification:
- Run the same contract suite against all framework hosts.

Exit gate:
- Quarkus, Spring Boot, and Micronaut all pass the same black-box contract with no host-specific semantic drift.

### HT5 — Build independently deployable Human Task images and deployment packaging

Objective:
- make the capability independently deployable and observable.

Owner:
- `forwardmeasure-openworkflow` + `helm-charts` + `openworkflow-k8s-setup` + `forwardmeasure-platform`

Work:
1. Add deployment modules under `openworkflow-deployments/human-task-management` for the selected framework host.
2. Ensure the image contains exactly one framework host plus the shared capability and not a workflow engine or another management capability.
3. Follow the established ForwardMeasure Maven image pattern exactly, including labels, non-root user, reproducible tag, registry property, build flag, and push flag.
4. Add focused image-level checks and startup tests for health and one database-backed API request.
5. Create Helm material for service, probes, scaling, policy, network expectations, and migration dependency.

Verification:
- Run the Maven image build and validation tasks for the selected framework(s).
- Start the service and verify health plus a minimal API interaction.

Exit gate:
- the Human Task service builds and runs as an independent deployment unit; image and Helm packaging are validated.

### HT6 — Add engine-neutral request and outcome integration

Objective:
- prove the Human Task service can be invoked from workflow engines without leaking engine-specific types into the domain.

Owner:
- `forwardmeasure-openworkflow`

Work:
1. Define `HumanTaskRequestPort`, `HumanTaskOutcomePort`, acceptance types, request types, and outcome types in an engine-neutral module.
2. Materialize `HumanTaskCallPlan` using the durable workflow request time: evaluate input, normalize approvals into `ReviewPlan`, resolve `dueAfter`, preserve missing expiry as infinity, validate and digest presentation resources, and externalize content according to bounds.
3. Correlate task, execution, workflow branch/path, stage/decision, command, and outbox IDs deterministically.
4. Implement durable request dispatch and outcome publication around the Human Task outbox.
5. Define deterministic cancellation-versus-decision and late/duplicate-outcome rules before continuing to engine adapters.
6. Add engine contract tests that both adapters must satisfy.

Verification:
- Run the request/outcome integration contract tests in the workflow module.

Exit gate:
- portable request/outcome integration is deterministic, idempotent, and suitable for both engine adapters.

### HT7 — Complete Kafka Streams and Pekko production bridges

Objective:
- prove both workflow engines resume the exact workflow branch using the same common contract.

Owner:
- `forwardmeasure-openworkflow` + Kafka Streams and Pekko integrations

Work:
1. Reconcile existing `CREATE_HUMAN_TASK`, `EXPIRE_HUMAN_TASK`, `CANCEL_HUMAN_TASK`, waiting state, and `completeHumanTask()` flow with the common request/outcome contract rather than replacing engine semantics.
2. Implement the Pekko Human Task path with the same common request/outcome behavior and the same durable constraints.
3. Validate duplicate transport records, late decisions, cancellation races, and workflow resumption under restart.
4. Confirm both engines produce equivalent observable behavior for the same task lifecycle.

Verification:
- Run the engine contract suite and workflow restart/recovery tests for Kafka Streams and Pekko.

Exit gate:
- both engines can create, resolve, and resume the same Human Task workflow branch equivalently, with duplicate and late outcomes handled deterministically.

### HT8 — Studio blotter and resolution workspace

Objective:
- complete the product route that a human actually uses to work the task.

Owner:
- `data-fabric/data-fabric-components/java/data-fabric-services/entity-intelligence-framework/entity-intelligence-workbench`
- `forwardmeasure-openworkflow` + Studio-facing integration layer

Work:
1. Treat `forwardmeasure-openworkflow/openworkflow-studio` as the current branding authority for OpenWorkflow UI and keep Human Task screens consistent with that theme contract.
2. Externalize the Human Task screen theming into the same token-driven pattern already used by `openworkflow-studio/webapp/src/main/webapp/src/theme.ts` instead of hardcoding product-specific colors, spacing, and radii.
3. Mirror the proven workbench pattern from the Entity Intelligence Workbench: routed application shell, runtime config injection, tenant-scoped auth, and a separate product surface that is not a platform dashboard.
4. Add the Human Tasks blotter route with dense operational table, server-side pagination, filter/sort, and promoted task-defined fields.
5. Implement the review workspace with immutable source content, schema-aware editing, attachments/evidence, autosave, lease visibility, and before/after comparison.
6. Prove that a second actor sees read-only behavior when a review lease is held.
7. Add terminal decision confirmation, stale revision warnings, and full audit timeline.
8. Verify keyboard operability, screen-reader labels, and raw JSON fallback for unexpected content fields.
9. Reuse the same front-end and deployment shell structure as the existing Studio and workbench patterns rather than inventing a standalone UI product pattern.

Verification:
- Run Playwright or API-backed UI journeys covering task list, lease acquisition, save/release, one-stage decision, and stale revision behavior.
- Validate the workbench route and runtime-config contract against the real Data Fabric workbench package and the current OpenWorkflow Studio theme contract.
- Confirm that the Human Task screens inherit the externalized Studio theme values without hardcoded visual drift.

Exit gate:
- Studio can create, review, correct, and resolve a task end-to-end with persisted audit evidence, using the current OpenWorkflow branding and the same externalized theming contract as the rest of the Studio UI.

### HT9 — Kubernetes acceptance journey

Objective:
- prove the full feature works in deployment conditions, not just in unit tests.

Owner:
- `forwardmeasure-platform/deploy` + `forwardmeasure-openworkflow/deploy`
- `helm-charts` + `openworkflow-k8s-setup` + `forwardmeasure-openworkflow` + `forwardmeasure-platform`

Work:
1. Use the real deployment pattern already defined in `forwardmeasure-platform/deploy/helmfile` and `forwardmeasure-openworkflow/deploy/helmfile`: layered `environments/*.yaml`, chart sources, stage labels, and `helmfile` render/install flow.
2. Add the Human Task image/service release to the same environment and stage model used by the existing OpenWorkflow deployment, rather than introducing a separate bespoke deployment path.
3. Deploy the Human Task image with one workflow engine.
4. Start a workflow that creates a task with non-trivial arbitrary data.
5. Verify it appears in the blotter with correct promoted fields.
6. Open it for review and confirm a second actor receives a read-only lease conflict.
7. Save multiple corrections and release without deciding.
8. Reopen as an eligible actor and verify prior immutable changes/history.
9. Submit approve, decline, and custom outcomes in separate runs.
10. Restart or relocate the task service and engine during the journey.
11. Verify the exact digest and disposition resume the correct workflow branch once.
12. Repeat against both engine profiles and required persistence profiles.

Verification:
- Run the full Kubernetes acceptance journey for the supported deployment profile.
- Validate the actual deployment render/install flow from the existing Helmfile repos before claiming feature readiness.

Exit gate:
- the Human Task capability is green in the deployment environment and behaves identically across the required engine profiles, using the real Helmfile and product-workbench repo structure already present in the monorepo.

## 7. Required evidence before claiming completion

The feature is not complete until all of the following are true:

- exhaustive domain and serialization tests pass;
- persistence and concurrency tests pass under PostgreSQL Testcontainers;
- AuthZEN and stage-eligibility tests pass;
- API generation drift tests pass;
- framework-host parity tests pass;
- engine request/outcome bridge tests pass for Kafka Streams and Pekko;
- Studio review journeys pass with read-only conflict and stale revision handling;
- image and Helm deployment checks pass;
- the Kubernetes disruption journey passes;
- the exact workflow branch resumes once and only once, even under duplicate or late outcomes.

## 8. Final “done” definition

Human Task Management is complete only when the durable task lifecycle is proven in the real system, not merely modeled in the codebase.

The final completion test is not “there are modules and generated code.” The final completion test is:

> a real workflow can create a durable arbitrary-shaped Human Task, a human can acquire/release/recover a finite review lease, every review and correction is preserved immutably, a configured decision completes or advances the review plan, and the exact outcome resumes the correct workflow branch equivalently through Kafka Streams and Pekko, with API, framework, persistence, authorization, Studio, and Kubernetes evidence.

That is the acceptance bar.

## 9. Suggested commit boundaries

Use small reviewable commits after green gates:

1. `Complete human task domain contracts and exhaustive tests`
2. `Complete tenant scoped human task persistence and outbox`
3. `Complete human task API and authorization contracts`
4. `Add human task Quarkus, Spring, and Micronaut bindings`
5. `Add human task framework service images`
6. `Add engine-neutral human task integration ports`
7. `Integrate human tasks with Kafka Streams`
8. `Integrate human tasks with Pekko`
9. `Deploy human task management with Helm`
10. `Add Studio human task blotter and workspace`
11. `Add human task disruption and acceptance evidence`

Do not combine unrelated cleanup or broad formatting into these commits.

## 10. Required checkpoint format

At the end of every `HTn` package, report:

```text
Checkpoint: HTn
Completed:
- exact behavior delivered

Files/modules changed:
- paths and why

Evidence:
- exact command
- tests run / failures / errors
- generated drift, image digest, or Kubernetes evidence where applicable

Known issues:
- defect, owner/module, impact, and whether it blocks the next package

Next:
- the next prescribed HT package and first concrete action
```

Do not proceed past a failed gate unless the failure is proven unrelated, documented, and incapable of invalidating the next layer.
