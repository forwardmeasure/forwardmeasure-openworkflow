# Human Task Management Design

**Status:** Approved direction for implementation planning<br>
**Scope:** ForwardMeasure OpenWorkflow Human-in-the-Loop (HITL) capability<br>
**Specification relationship:** First-class ForwardMeasure extension to Open Workflow `1.0.3`<br>
**Architecture authorities:** `PROJECT_MANIFESTO.md` and `IMPLEMENTATION_PLAN.md`

## 1. Purpose

Human Task Management provides durable, tenant-scoped work that a person can inspect, correct, and
resolve. A task may contain business data of any JSON-compatible shape. ForwardMeasure Studio
presents available work as a trade-blotter-style queue and opens each task in a schema-driven
resolution workspace.

The initial product journey uses one review stage: one eligible human reviews the task and submits
one disposition such as approve, decline, or another task-defined action. The underlying domain,
wire format, persistence model, and state machine are stage-capable from the beginning so sequential
approval, maker-checker, rework, reopening, escalation, and additional reviewers do not require a
future breaking redesign.

Human Task Management is a common product-plane capability. It is not implemented separately by
Kafka Streams and Pekko, and it is not embedded in a framework-specific REST service.

## 2. Locked design decisions

1. The human-task aggregate has one formal, framework-neutral finite-state machine.
2. Commands are decided against a durable state and produce immutable domain events. Events alone
   evolve the state.
3. The original task input is immutable. Every correction creates a new forward-only content
   revision with actor, time, change set, and digest evidence.
4. Every user or domain action is recorded in an append-only history. Mechanical lease heartbeats
   are operational state; acquisition, release, expiry, reassignment, edits, and decisions are audit
   events.
5. Task expiry is optional. An absent `expiresAt` means the task remains open indefinitely.
6. Exclusive review ownership is a finite renewable lease. It is never infinite by default.
7. Assignment and review lease are different concepts. A task may remain assigned to an actor or
   group while no actor currently holds its editing lease.
8. Each occurrence of a review stage in a review round accepts one immutable decision. Operational
   actions such as viewing, claiming, saving, commenting, or releasing are not additional approval
   stages.
9. The first Studio journey configures one review stage. The common model supports a non-empty
   ordered review plan and must be tested with more than one stage before its durable formats are
   declared stable.
10. Terminal actions are task-defined. `APPROVE` and `DECLINE` are standard kinds; `OTHER` carries a
    stable task-defined action code and transition.
11. The API is contract-first and uses explicit operations. A generic `{action}` endpoint is not
    permitted.
12. Quarkus, Spring Boot, and Micronaut host the same application, JAX-RS, persistence, and state
    machine implementation. A deployment selects one framework.
13. Both workflow engines integrate through engine-neutral request and outcome ports. Neither
    engine owns task state or task persistence.
14. Authenticated tenant and actor identity come from trusted platform context. AuthZEN is
    authoritative and fails closed.

## 3. Conceptual model

```text
HumanTask
  ├── immutable identity and workflow/business correlation
  ├── immutable source input
  ├── current forward-only resolution revision
  ├── ReviewPlan
  │     ├── ReviewStage 1
  │     ├── ReviewStage 2 (optional)
  │     └── ReviewStage N (optional)
  ├── current durable state
  ├── optional assignment
  ├── optional active ReviewSession lease
  ├── append-only task events
  └── optional terminal HumanTaskOutcome
```

### 3.1 Human task envelope

The common envelope is stable regardless of the business payload:

| Field | Meaning |
|---|---|
| `taskId` | Stable platform task identifier. |
| `tenantId` | Trusted tenant identifier; never accepted as client routing authority. |
| `taskType` | Stable type used for authorization, presentation, filtering, and reporting. |
| `title`, `description` | Human-readable work summary. |
| `sourceSystem` | Originating capability or external system. |
| `subjectType`, `subjectId` | Optional business subject, such as trade, entity, case, or customer. |
| `businessCorrelationId` | Cross-system correlation distinct from the task identifier. |
| `workflowExecutionId` | Originating execution when workflow-created. |
| `workflowTaskPath` | Exact workflow task/branch awaiting the outcome. |
| `priority` | Stable sortable priority. |
| `receivedAt` | Server-recorded task arrival time. |
| `dueAt` | Optional service target; absence means no due date. |
| `expiresAt` | Optional automatic terminal expiry; absence means infinity. |
| `reviewPlan` | Immutable ordered stage policy pinned when the task is created. |
| `presentation` | Immutable presentation descriptor and resource digests. |
| `sourceContent` | Immutable bounded data or content-addressed artifact reference. |
| `currentRevision` | Latest immutable working-resolution revision. |
| `state` | Current reconstructable FSM state. |
| `aggregateRevision` | Optimistic concurrency and event sequence. |

### 3.2 Arbitrary content and presentation

Arbitrary business data is carried as a `DataReference`:

```text
storage: INLINE | ARTIFACT
mediaType
sizeBytes
sha256
inlineValue | artifactUri
```

Bounded JSON may be inline. Larger documents, attachments, images, and evidence use immutable,
content-addressed artifact storage. The persisted task records both reference and digest.

This is not a new human-task-owned type. The existing
`com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference` is the source behavior and
wire shape, but it currently lives in the Kafka Streams runtime API and therefore has the wrong
architectural owner. Before further Human Task implementation, move its behavior and serialized
shape into a new portable `openworkflow-data` module under an engine-neutral package. Kafka
Streams, Pekko, Human Task Management, operation
adapters, and API mappers consume that one type. `openworkflow-human-task-domain` must replace its
temporary direct `JsonNode` content fields with this shared reference; no duplicate
`DataReference` is permitted. The new module contains no engine, persistence, transport, or
framework dependency.

The presentation descriptor may provide:

- JSON Schema for validation;
- a constrained UI schema for sections, labels, widgets, and read-only fields;
- declared blotter fields;
- attachment/evidence panels;
- task-defined disposition buttons; and
- an approved A2UI document where that presentation mode is enabled.

Task-supplied arbitrary JavaScript is prohibited. Presentation resources are resolved, validated,
digest-pinned, and immutable before a task becomes available.

### 3.3 Review plan and actions

`ReviewPlan` contains one or more ordered `ReviewStage` values. A stage contains:

- stable `stageId` and ordinal;
- display name and instructions;
- eligible actors, groups, roles, or AuthZEN resource attributes;
- assignment policy;
- allowed dispositions;
- optional lease-policy override;
- optional due/expiry policy; and
- transition mapping for each disposition.

Each visible button has a stable action code and one semantic kind:

```text
APPROVE
DECLINE
OTHER
```

An `OTHER` action may represent referral, rework, escalation, exception acceptance, request for
information, or another domain-specific outcome. Its configured transition determines whether the
task advances, returns to an earlier stage, remains open under a new assignment, or resolves.

The initial journey supplies one stage whose selected disposition resolves the task. The model must
not encode that initial policy as a permanent one-stage limitation.

### 3.4 Workflow authoring contract and normalization

The workflow-facing contract already accepted by `OpenWorkflowCompiler` is retained rather than
silently replaced by the domain vocabulary. A workflow author declares a Human Task through the
engine-neutral canonical function name selected during the OKS-identifier cleanup and supplies:

```yaml
call: com.forwardmeasure.openworkflow.human-task
with:
  title: Review extracted evidence
  input: '${ . }'
  approvals:
    makerChecker: true
    distinctApprovers: true
    stages:
      - level: 1
        name: First Review
        requiredApprovals: 1
        candidateRoles: [evidence-reviewer]
  dueAfter: PT4H
  presentation:
    kind: RAW_JSON
```

`approvals` is the public workflow-authoring name. `ReviewPlan` is the normalized domain name; it is
not a competing input property. Compilation/runtime materialization maps them explicitly:

| Authoring field | Normalized Human Task field |
|---|---|
| `title`, optional `description` | immutable task title and description |
| `input`, defaulting to the call task's effective input | immutable `sourceContent` `DataReference` |
| `approvals.makerChecker` | review-plan maker-checker invariant |
| `approvals.distinctApprovers` | cross-stage/round reviewer-distinctness policy |
| `approvals.stages[]` | ordered `ReviewStage` values |
| stage `level`, `name`, `requiredApprovals`, candidate actors/roles | stable stage identity, quorum, and eligibility |
| `dueAt` | absolute service target |
| `dueAfter` | service target resolved from the durable task-request time |
| `presentation` | validated, digest-pinned presentation descriptor |

The existing mutually exclusive `dueAt`/`dueAfter` validation remains. Neither implies terminal
expiry: `dueAt` is an SLA/display target. Automatic expiry requires a separately named
`expiresAt`/`expiresAfter` extension, also mutually exclusive, and absence means infinity. The
compiler must produce a typed immutable `HumanTaskCallPlan` rather than leaving downstream code to
reinterpret raw `CallPlan.arguments()`. That plan preserves unevaluated runtime expressions while
freezing the validated policy shape. The materializer evaluates expressions once, resolves
relative times from the durable request timestamp, and creates the domain `ReviewPlan`.

The call identifier is `com.forwardmeasure.openworkflow.human-task` (`CallPlan.HUMAN_TASK_FUNCTION`).
An earlier `com.forwardmeasure.oks.*`-branded naming convention, carried over from the
`openworkflow-kafka-streams` (OKS) source repo this compiler was originally consolidated from (see
`docs/source-provenance.md`), has been renamed - OKS was never itself deployed, so no real document
anywhere depends on that branding, and no alias/back-compat handling is required.

## 4. Formal state machine

### 4.1 Durable states

Use sealed state types so invalid field combinations are unrepresentable:

```java
sealed interface HumanTaskState {
  record Open(...) implements HumanTaskState {}
  record Assigned(...) implements HumanTaskState {}
  record Claimed(...) implements HumanTaskState {}
  record AwaitingNextStage(...) implements HumanTaskState {}
  record ReworkRequested(...) implements HumanTaskState {}
  record Approved(...) implements HumanTaskState {}
  record Rejected(...) implements HumanTaskState {}
  record Resolved(...) implements HumanTaskState {}
  record Cancelled(...) implements HumanTaskState {}
  record Expired(...) implements HumanTaskState {}
}
```

- `Open` contains the active stage with no durable assignment and no editing lease.
- `Assigned` contains the active stage and durable assignment but no editing lease.
- `Claimed` contains the active stage, reviewer, and a valid `ReviewSession` lease.
- `AwaitingNextStage` is a durable boundary while the next stage is made available or assigned.
- `ReworkRequested` records a completed stage decision and target stage/originator before the new
  review round becomes `Open` or `Assigned`.
- `Approved`, `Rejected`, and `Resolved` contain immutable completed outcomes. `Resolved`
  represents a completed task-defined `OTHER` disposition rather than erasing its stable action
  code. An authorized, policy-valid reopen starts a new review round; it never edits the prior
  outcome.
- `Cancelled` and `Expired` are irreversible terminal states.

The public query projection exposes the manifesto vocabulary `OPEN`, `ASSIGNED`, `CLAIMED`,
`APPROVED`, `REJECTED`, `REWORK_REQUESTED`, `EXPIRED`, and `CANCELLED`, plus
`AWAITING_NEXT_STAGE` and `RESOLVED` where those states apply. Transition logic operates on the
sealed durable types; display labels such as "Available" and "In review" are Studio language, not
alternative persisted states.

### 4.2 Commands

Commands express intent and contain trusted actor context, command/idempotency identifier, expected
aggregate revision, and server-handled time:

```text
CreateTask
AssignTask
UnassignTask
BeginReview
RenewReviewLease
SaveResolutionRevision
AddComment
ReleaseReview
ExpireReviewLease
ReassignReview
SubmitDecision
EscalateTask
ReopenTask
CancelTask
ExpireTask
```

### 4.3 Events

Accepted commands produce immutable events:

```text
TaskCreated
TaskAssigned
TaskUnassigned
ReviewStarted
ReviewLeaseRenewed
ResolutionRevised
CommentAdded
ReviewReleased
ReviewLeaseExpired
ReviewReassigned
DecisionRecorded
ReviewStageAdvanced
TaskReworkRequested
TaskEscalated
TaskApproved
TaskRejected
TaskResolved
TaskReopened
TaskCancelled
TaskExpired
```

The implementation contract is pure:

```java
List<HumanTaskEvent> decide(HumanTaskState state, HumanTaskCommand command);
HumanTaskState evolve(HumanTaskState state, HumanTaskEvent event);
```

Decision code does not call repositories, clocks, HTTP services, Kafka, Pekko, or framework APIs.
Time and trusted identity arrive in commands. Event application performs no I/O.

### 4.4 Principal transition matrix

| Current state | Command | Result |
|---|---|---|
| None | `CreateTask` | `TaskCreated` -> `Open` or `Assigned` according to policy |
| Open | `AssignTask` | `TaskAssigned` -> `Assigned` |
| Assigned | `UnassignTask` | `TaskUnassigned` -> `Open` |
| Open/Assigned | `BeginReview` | `ReviewStarted` -> `Claimed` |
| Claimed | `RenewReviewLease` | `ReviewLeaseRenewed` -> `Claimed` |
| Claimed | `SaveResolutionRevision` | `ResolutionRevised` -> `Claimed` |
| Claimed | `AddComment` | `CommentAdded` -> `Claimed` |
| Claimed | `ReleaseReview` | `ReviewReleased` -> `Open` or `Assigned` |
| Claimed | `ExpireReviewLease` | `ReviewLeaseExpired` -> `Open` or `Assigned` |
| Claimed | `ReassignReview` | release/reassignment events -> `Assigned` or `Claimed` |
| Claimed | `SubmitDecision` | decision plus advance/rework/terminal events |
| Open/Assigned/Claimed | `EscalateTask` | `TaskEscalated` -> configured stage/assignment |
| Approved/Rejected/Resolved | `ReopenTask` | `TaskReopened` -> `Open` or `Assigned` in a new round |
| Non-terminal | `CancelTask` | `TaskCancelled` -> `Cancelled` |
| Non-terminal | `ExpireTask` | `TaskExpired` -> `Expired` |
| Cancelled/Expired | Any mutation | typed illegal-transition failure, no events |

Every state/command pair must be handled explicitly. Adding a state, command, or event must cause
exhaustive compiler/test failures until its behavior is deliberately defined.

### 4.5 Decision processing

`SubmitDecision` requires the active review session, expected task revision, a configured action
code, optional comment, and the exact content revision being decided.

The state machine first records `DecisionRecorded`. It then applies the immutable review-plan
transition:

- resolve with a final outcome;
- advance to the next stage;
- return to a configured prior stage or originator for rework;
- change assignment/eligibility for escalation or referral; or
- remain open only when the selected action explicitly defines that transition.

A decision is never edited. A later stage or rework round adds new events and revisions.
Reopening a completed task likewise creates a new review round linked to the completed outcome. It
does not retract an outcome already delivered to a workflow. Any workflow-level consequence of a
reopen is an explicit, idempotent compensation or follow-up outcome defined by policy.

## 5. Review sessions and leases

Opening a task for review creates an exclusive renewable `ReviewSession`:

```text
reviewSessionId
taskId
stageId
heldBy
acquiredAt
lastRenewedAt
expiresAt
taskRevisionAtAcquisition
leaseTokenDigest
```

Rules:

1. Beginning review atomically checks eligibility, task state, assignment, and lease availability.
2. Only the lease holder may save changes, comment as reviewer, or submit a decision.
3. A second user may inspect an authorized read-only view but cannot modify the task.
4. A finite default lease duration is centrally configurable. The Studio renews it while the
   review workspace is active.
5. Browser loss, network loss, or process death eventually frees the task through lease expiry.
6. Explicit release immediately returns the task to `Open` or `Assigned`, according to its durable
   assignment, while preserving all saved revisions.
7. An authorized supervisor may reassign or release a lease; that action is audited.
8. Assignment may outlive a lease. Lease expiry does not silently unassign the task.
9. Task expiry and lease expiry are independent. A task without `expiresAt` remains open forever,
   but no abandoned editing lease remains held forever.

A read-only `GET` must not secretly mutate state. Studio explicitly begins a review session when
the user chooses **Open for review**. A separate preview may remain read-only. `ReviewStarted`
provides durable evidence of who actually opened the task for action.

Routine heartbeat renewals update the lease safely but need not create one permanent audit row per
heartbeat. The audit records the review interval through start, release, expiry, reassignment, and
decision events.

## 6. Immutable corrections and history

The original input is revision zero and never changes. Each saved correction creates a
`HumanTaskContentRevision` containing:

```text
taskId
contentRevision
basedOnRevision
createdBy
createdAt
reviewSessionId
beforeSha256
afterSha256
jsonPatchReference
resultContentReference
comment
```

The full resulting document is retained or content-addressed so reconstruction does not depend on
an unbounded patch chain. The JSON Patch/change set provides human-readable and machine-verifiable
change evidence.

History must answer:

- when the task arrived;
- which source and workflow created it;
- every assignment and reassignment;
- who opened it for review and when;
- whether the reviewer released it or the lease expired;
- every saved correction and its before/after values;
- every comment;
- every stage decision and its policy context;
- who cancelled or expired it; and
- exactly which content digest was returned to the workflow.

No ordinary operation updates or deletes prior revisions, decisions, or audit events. Administrative
data-retention/purge policy is separate from task lifecycle and must preserve required audit
evidence.

## 7. Persistence model

All application-owned data uses ForwardMeasure JPA in the tenant schema. The minimum logical tables
are:

| Table | Responsibility |
|---|---|
| `human_task` | Current aggregate snapshot/query projection and optimistic version. |
| `human_task_event` | Append-only ordered domain events. |
| `human_task_content_revision` | Immutable source and corrected data revisions. |
| `human_task_review_session` | Current/closed review leases and review intervals. |
| `human_task_command_receipt` | Idempotency key, command status, and response correlation. |
| `human_task_outbox` | Durable task-created and task-outcome delivery. |

Within one tenant-scoped transaction the application service:

1. loads the aggregate and expected version;
2. invokes the pure state machine;
3. appends resulting events;
4. evolves and persists the current snapshot/projection;
5. writes content revisions where applicable;
6. records the command receipt; and
7. writes any integration outcome to the transactional outbox.

Repositories cannot expose arbitrary status mutation methods. The state machine is the only
transition authority. Optimistic locking ensures only one concurrent lease acquisition or decision
wins.

## 8. Contract-first HTTP API

The reviewed OpenAPI `3.1.x` document is the sole HTTP authority. It generates Java models, Apache
HTTP client, Jakarta JAX-RS server interfaces, TypeScript client, and Python client. The portable
JAX-RS implementation implements those generated interfaces and delegates to application services.

Recommended explicit operations:

```text
GET  /v1/human-tasks
GET  /v1/human-tasks/{taskId}
GET  /v1/human-tasks/{taskId}/history
GET  /v1/human-tasks/{taskId}/revisions
GET  /v1/human-tasks/{taskId}/presentation

POST /v1/human-tasks/{taskId}/review-sessions
PUT  /v1/human-tasks/{taskId}/review-sessions/{reviewSessionId}/lease
POST /v1/human-tasks/{taskId}/review-sessions/{reviewSessionId}/revisions
POST /v1/human-tasks/{taskId}/review-sessions/{reviewSessionId}/comments
POST /v1/human-tasks/{taskId}/review-sessions/{reviewSessionId}/release
POST /v1/human-tasks/{taskId}/review-sessions/{reviewSessionId}/decision
```

Administrative assignment, cancellation, expiry, and reassignment operations must also be explicit
and separately authorized; they must not be hidden behind a generic action name.

Mutation requests require:

- `If-Match` using the aggregate revision;
- `Idempotency-Key`;
- the active review-session token where applicable;
- trusted actor and tenant context; and
- an AuthZEN allow decision.

Expected responses include RFC 9457 problem documents for malformed input, unauthorized,
forbidden, not found, stale revision, lease conflict, illegal transition, validation failure,
duplicate command, and unavailable dependency cases.

List operations use cursor pagination rather than an unbounded offset. They support bounded,
authorized filtering and stable sorting by status, task type, source, priority, assignment,
reviewer, received time, due time, and declared blotter fields.

## 9. Authorization

AuthZEN resources/actions include at least:

```text
human-task:list
human-task:read
human-task:read-history
human-task:begin-review
human-task:renew-review
human-task:edit
human-task:comment
human-task:release-review
human-task:decide:<action-code>
human-task:assign
human-task:reassign
human-task:cancel
human-task:expire
```

Eligibility declared by the review stage is a domain invariant in addition to AuthZEN. AuthZEN
allow does not make an actor eligible for a stage, and eligibility does not bypass AuthZEN.

The active Keycloak Organization supplies trusted tenant context. Task payloads, query parameters,
or workflow data cannot select another tenant, actor, database schema, or authorization outcome.

## 10. Workflow-engine integration

The common capability exposes engine-neutral ports such as:

```java
interface HumanTaskRequestPort {
  CompletionStage<HumanTaskAcceptance> request(HumanTaskRequest request);
}

interface HumanTaskOutcomePort {
  CompletionStage<Void> publish(HumanTaskOutcome outcome);
}
```

These ports do not discard the existing Kafka Streams implementation. Kafka Streams already owns
tested `CREATE_HUMAN_TASK`, `EXPIRE_HUMAN_TASK`, and `CANCEL_HUMAN_TASK` effects, Human Task
execution events, pending-interaction state, and `completeHumanTask()` outcome handling. Those
artifacts are the Kafka adapter's migration input. They are reconciled with the common
`HumanTaskRequest`/`HumanTaskOutcome` contract and the shared `DataReference`, not replaced by a
second Kafka state machine. Existing wire-compatibility and restoration fixtures become mandatory
regression tests.

Pekko has no equivalent Human Task path today. It receives a new persisted effect intent, durable
waiting state, cancellation/late-outcome handling, and outcome observation using Pekko Typed
Persistence, but must expose the same common request/outcome behavior and pass the same engine
contract. Kafka's current status enum is mapped deliberately to the richer common disposition and
stage outcome; it does not define the product-wide contract by accident.

The execution sequence is:

1. The workflow engine persists a human-task effect intent.
2. A durable dispatcher submits an idempotent task request.
3. Human Task Management creates or returns the existing correlated task.
4. The workflow enters its durable waiting state.
5. Human Task Management independently executes its review state machine.
6. A workflow-returning decision writes a `HumanTaskOutcome` to the transactional outbox.
7. The selected engine observes that outcome idempotently and resumes the exact execution branch.

The outcome includes task/stage/decision identity, disposition kind and action code, original and
resolved content references/digests, change-summary reference, actor, timestamps, and workflow
correlation.

The transport adapter may initially use durable Kafka command/outcome topics because both engine
deployments already integrate with Kafka transport, but Kafka types must not enter the human-task
domain/application contracts. A later transport replacement must not alter task semantics or the
public API.

Late or duplicate outcomes cannot resume a cancelled/completed workflow or repeat a completed
branch. Workflow cancellation durably requests task cancellation; task cancellation and an
in-flight human decision are resolved deterministically using recorded revisions and command time.

## 11. Studio experience

### 11.1 Trade-blotter work queue

Studio adds a top-level **Human Tasks** route containing a dense operational table:

| Standard column | Purpose |
|---|---|
| Received | Arrival time and stable queue ordering. |
| Age/SLA | Time waiting and overdue indication. |
| Priority | Queue prioritization. |
| Task type | Resolution category. |
| Subject | Trade, customer, entity, case, or other business subject. |
| Summary | Validated task-defined promoted fields. |
| Source | Originating system/workflow. |
| Stage | Current review-plan stage. |
| Status | Available, in review, resolved, cancelled, or expired. |
| Assignment | Durable actor/group assignment. |
| Reviewer/lease | Current reviewer and lease countdown. |
| Last activity | Latest durable user/domain event. |

The blotter provides server-side cursor pagination, filtering, stable sorting, column selection,
refresh/polling, and visible lease changes. Task-defined fields are promoted through a bounded,
validated `blotterFields` projection at task creation; the list page does not interpret arbitrary
payloads on every query.

Bulk navigation/assignment may be added deliberately. Bulk decisions are not part of the initial
journey because each task requires an explicit review of its evidence and resulting content.

### 11.2 Resolution workspace

Opening a task for review atomically acquires the review session and navigates to a workspace with:

- title, task type, priority, age, due/expiry information, source, and subject;
- immutable original content/evidence;
- schema-driven editable resolution content;
- validation diagnostics;
- attachments and supporting evidence;
- a before/after or field-level changes panel;
- append-only timeline of review sessions, edits, comments, and decisions;
- assignment and current lease information;
- autosave status and current aggregate/content revision; and
- configured disposition buttons with required-comment/validation rules.

Studio autosaves immutable revisions while the lease is held. It renews the lease before expiry,
warns when renewal fails, becomes read-only after lease loss, and never submits a decision against a
stale revision. Saved changes survive explicit release or lease expiry and are visible with
attribution to the next reviewer.

Another user opening a leased task sees a read-only workspace, reviewer identity where authorized,
lease expiry, history, and permitted supervisor controls. Studio hiding or disabling a button is
only presentation; the server independently authorizes and validates every operation.

### 11.3 Accessibility and losslessness

The blotter and resolution workspace must be keyboard operable, screen-reader labelled, and usable
without color-only state indicators. Rendering and editing must preserve every field of arbitrary
input/resolution data, including fields the current visual renderer does not understand. A raw JSON
fallback is mandatory for authorized users.

## 12. Module and deployment shape

The capability-oriented target is:

```text
openworkflow-data/
  # shared DataReference, canonical JSON/digest, and bounded materialization contracts

openworkflow-human-task/
  openapi-specifications/
  api-language-bindings/
    java/
      openworkflow-human-task-models/
      openworkflow-human-task-client-apachehttp/
      openworkflow-human-task-server-jaxrs/
    typescript/openworkflow-human-task-client-typescript/
    python/openworkflow-human-task-client-python/
  openworkflow-human-task-domain/
  openworkflow-human-task-application/
  openworkflow-human-task-jpa/
  openworkflow-human-task-jaxrs/
  openworkflow-human-task-contract-tests/
  framework-bindings/
    quarkus/
    spring/
    micronaut/

openworkflow-deployments/human-task-management/
  quarkus/
  spring/
  micronaut/
```

If the repository deliberately retains its current centralized API-specification/language-binding
aggregators, the physical source may be registered there only after reconciling that choice with the
manifesto. There must still be exactly one human-task OpenAPI source and one generated artifact
family.

Each selected-framework image contains the portable domain/application/JPA/JAX-RS capability and
thin framework binding. Human Task Management has its own Helm release, service, scaling, probes,
network policy, migration dependency, and image version. It is not folded into definition
management, execution management, Studio, or either engine image.

## 13. Implementation order

1. Correct the feature ledger: Human Task is committed and partially scaffolded in Kafka Streams,
   but has no reachable production management path and no Pekko equivalent; it is not implemented
   across both engines.
2. Import and review the useful OKS human-task contract/domain/presentation tests through the source
   provenance process.
3. Normalize the existing compiler authoring contract into a typed `HumanTaskCallPlan`; extract the
   existing `DataReference` into `openworkflow-data`; migrate Kafka consumers without changing its
   wire semantics; and freeze the portable identifiers, review plan, commands, sealed states,
   events, outcomes, and serialization fixtures.
4. Implement the pure FSM and exhaustive state/command/replay tests.
5. Define the single OpenAPI contract and generate all language/server bindings.
6. Implement ForwardMeasure JPA entities, repositories, transaction service, tenant migrations,
   outbox, and PostgreSQL contract tests.
7. Implement the portable generated-interface JAX-RS adapter and AuthZEN resources/actions.
8. Implement Quarkus, Spring, and Micronaut composition and black-box API parity tests.
9. Implement the Kafka Streams and Pekko request/outcome bridges behind common ports.
10. Add the framework-specific images and selected-framework Helm release.
11. Implement the Studio blotter, resolution workspace, lease behavior, history, and generated
    TypeScript client integration.
12. Run focused tests after each layer, followed by the human-task capability gate and Kubernetes
    disruption journey.

The first vertical slice uses a single review stage but must exercise arbitrary data, lease
acquisition/release/expiry, immutable corrections, approve/decline/custom disposition, restart,
duplicate delivery, and exact workflow resumption.

## 14. Required tests and acceptance evidence

### Domain and serialization

- exhaustive state/command legality matrix;
- exact event sequences for every legal transition;
- no events for illegal transitions;
- replay from every event prefix;
- serialization golden files for commands, states, events, plans, and outcomes;
- one-stage and multi-stage policy fixtures;
- approve, decline, custom, rework, escalation, and stage-advance fixtures; and
- deterministic task expiry and lease expiry using an injected clock.

### Persistence and concurrency

- PostgreSQL Testcontainers and ForwardMeasure JPA contracts;
- tenant scope required and cross-tenant denial;
- append-only event/content history;
- optimistic locking with two simultaneous review acquisitions and one winner;
- stale edit/decision rejection;
- idempotent duplicate commands;
- atomic snapshot/event/revision/outbox writes;
- lease expiry without task expiry; and
- restart reconstruction from events and snapshots.

### API, authorization, and frameworks

- OpenAPI validation and generation drift;
- generated-interface implementation architecture test;
- consistent ETag, idempotency, pagination, and RFC 9457 problems;
- AuthZEN fail-closed behavior and stage eligibility;
- Quarkus, Spring, and Micronaut black-box parity; and
- no framework, Kafka, or Pekko dependencies in portable modules.

### Engine and disruption

- task request accepted only after a durable workflow effect intent;
- workflow waits through engine restart;
- task service restart while a review lease is held;
- terminal outcome delivered once despite duplicate transport records;
- correct originating branch resumes;
- cancelled workflow cannot be resurrected by a late decision;
- workflow cancellation races deterministically with decision submission; and
- equivalent observable behavior from Kafka Streams and Pekko.

### Studio

- large blotter pagination/filter/sort behavior;
- task-defined column projection;
- schema-driven arbitrary content rendering and raw JSON fallback;
- lease acquisition, renewal, release, expiry, read-only conflict, and supervisor reassignment;
- autosaved immutable corrections and before/after display;
- terminal decision confirmation and stale revision handling;
- complete audit timeline;
- accessibility checks; and
- Playwright journeys against the generated API for all three hosts.

### Kubernetes acceptance journey

1. Deploy the selected framework's human-task image with one workflow engine.
2. Start a workflow that creates a task containing non-trivial arbitrary data.
3. Verify it appears in the blotter with correct promoted fields.
4. Open it for review and prove a second actor receives a read-only lease conflict.
5. Save multiple corrections and release without deciding.
6. Reopen as an eligible actor and verify prior immutable changes/history.
7. Submit approve, decline, and custom outcomes in separate runs.
8. Restart/relocate the task service and engine during the journeys.
9. Verify the exact resulting digest and disposition resume the correct workflow branch once.
10. Repeat against both engines and the required Pekko persistence profiles.

## 15. Initial non-goals

The initial vertical slice does not require bulk approval, offline/mobile synchronization, arbitrary
task-supplied executable UI code, ad hoc cross-tenant queues, or direct reads from engine-native
stores. These exclusions do not remove stage capability, rework, reopening, escalation, or
multi-reviewer policy from the durable model.

## 16. Completion rule

Compiler recognition or an unreachable internal engine state is not Human Task implementation. The
capability is complete only when a real workflow can durably create an arbitrary-shaped task, a
human can acquire/release/recover a review lease, every review and correction is preserved
immutably, a configured decision completes or advances the review plan, and the exact outcome
resumes the correct workflow branch equivalently through Kafka Streams and Pekko, with API,
framework, Studio, persistence, authorization, and Kubernetes evidence.
