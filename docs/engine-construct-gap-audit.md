# OpenWorkflow Engine Construct Gap Audit

Audit of every DSL construct OpenWorkflow's compiler accepts, checked against what each of the two
execution engines (`openworkflow-pekko-engine`, `openworkflow-kafka-streams-engine`) actually
implements, and what the shared `execution-management` REST facade actually exposes. Every finding
below is backed by file:line evidence gathered directly from the code — not general impression.

## Summary

The platform is not systemically broken. All 12 top-level task kinds and all standard `call`/`run`
protocol types work correctly on both engines. Five specific issues were originally identified, all
involving the same two OpenWorkflow-specific extensions (`human-task`, `correlated-worker`) plus one
unrelated bug (subworkflow invocation on Kafka-Streams). **Gaps #1, #3, and #4 are resolved and
independently verified; gap #5 turned out, on direct verification, to have never been real** (see its
section below - the original write-up misread which code path a caller actually reaches); `human-task`
(#2) remains deliberately on hold per explicit direction. **Follow-on work has also landed**: the
state-visibility fix for gap #4 was originally two independently-authored copies of the same rule, one
per engine - that duplication is now closed by a shared policy class both engines consult, plus a new
cross-engine test that would catch it if they ever diverged again. See "Shared execution-state policy"
below.

## Gaps, ranked by severity

### 1. `run: workflow` (subworkflow invocation) crashed with a NullPointerException on Kafka-Streams

**RESOLVED.** Kafka-Streams now implements a real, tested subworkflow-coordination subsystem: the
parent's pending interaction is an `ActiveOperationState` (operation kind `run-workflow`) dispatched
via the new `START_SUBWORKFLOW`/`CONTROL_SUBWORKFLOW` effect types, materialized by
`OksSubworkflowLaunchOutputProcessor` / `OksSubworkflowLaunchProcessor` /
`OksSubworkflowCompletionProcessor` / `OksSubworkflowControlProcessor` in `OksTopology`. Child
completion is routed back to the parent via the same `ObserveOperationCommand` path every other RUN
kind already used, so fork-branch routing, pause buffering and cancellation racing are the existing,
proven machinery — not a parallel implementation. `EngineCapabilities.KAFKA_STREAMS` now declares
`RunPlan.Kind.WORKFLOW` supported.

Verified independently, not just by the implementing pass: full test suites for every touched module
re-run clean from scratch, plus every one of the 5 `OksRestorationIntegrationTest` methods that
failed under full-suite resource contention were individually re-run in isolation afterward and pass
clean (0 failures/errors each) — confirming the original failures were embedded-Kafka-broker resource
contention from heavy concurrent machine load, not a logic regression.

### 2. `human-task` doesn't work in production on either engine — deliberately still on hold

- **Pekko**: rejected at plan-compile time. `MilestoneOneProgram`'s call-kind whitelist
  (`openworkflow-pekko-runtime/.../actor/MilestoneOneProgram.java:290-307`) excludes `HUMAN_TASK`
  entirely — throws `IllegalArgumentException` before execution starts. Fails loud and clean, at least.
- **Kafka-Streams**: has a fully built internal state machine — `ActiveHumanTaskState`,
  `observeHumanTask`/`observeRootHumanTask`/`completeHumanTask` (`WorkflowExecutionEngine.java:2635-2818`),
  events for approve/reject/rework/expire/cancel. But `ObserveHumanTaskCommand` is constructed *only*
  in two test files — zero production code path ever creates one. The state machine is real but
  unreachable.
- **Status**: explicit user direction — do not build this without being asked again, even though the
  other two gaps originally grouped with it (#1, #3) are now resolved.

### 3. `correlated-worker` previously worked only on Kafka-Streams, absent on Pekko

**RESOLVED.** Real support now built on Pekko too. Architecture: three independently-dispatched,
independently-recoverable operations per call (command PUBLISH, events SUBSCRIBE, on-demand
cancellation PUBLISH), each with its own `ProtocolOperationCoordinatorEntity` instance keyed by a
suffixed operation id, while the wire payload sent to the external worker carries the bare lifecycle
id for cross-engine contract consistency with Kafka-Streams' existing worker protocol. The genuinely
new capability — actually dispatching an outbound cancellation instead of dropping a local `Future` —
required a narrow, documented exception in `ProtocolOperationCoordinatorEntity` so a correlated-worker's
own cancellation operation can launch specifically during CANCELLING. `WorkflowEntity.cancel()` now
two-phases cancellation for a pending correlated-worker call: persists `CancellationRequested` +
`CorrelatedWorkerCancellationDispatched` (staying in `Cancelling`, not jumping to `Cancelled`) and only
finalizes once the cancellation operation is acknowledged or the worker's own events channel reports
its outcome first. `EngineCapabilities.PEKKO` now excludes only `HUMAN_TASK`.

Verified independently: 122+ pekko-runtime tests, 23 operation-adapter-core tests, 6 engine-api tests,
all re-run from clean by a separate verification pass, not just the implementing pass's own claim.

**Entity-intelligence rebuild constraint LIFTED**: [[project_entity_intelligence_openworkflow_rebuild]]'s
design counting on `correlated-worker` for external async work is no longer engine-locked to
Kafka-Streams — Pekko is a real option too.

Deliberately not built: `correlated-worker` inside a `fork` branch (compile-rejected via
`MilestoneOneProgram`'s existing fork-slice whitelist, simply not extended — same acceptable gap shape
as other fork+task-kind combinations already in this doc, see #5) and a dedicated grace-period timer
for a lost cancellation ack (relies on the same crash-restart recoverability every other Pekko
operation gets, a deliberate scope reduction versus Kafka-Streams' extra safety net).

### 4. Neither construct was visible through the public REST API, on either engine

**Partially RESOLVED** — state visibility fixed for `correlated-worker` on both engines (`human-task`
intentionally left alone, see #2, since it's still unreachable in production either way).

- The shared `ExecutionCommand` sealed interface behind `execution-management`'s REST facade still
  has exactly 4 variants: `Start, Pause, Resume, Cancel` — human-task and correlated-worker still
  aren't concepts at that abstraction layer, and there's still no dedicated endpoint to complete a
  human task or supply a correlated-worker's result. That part of the gap stands.
- **What's fixed**: a blocked-on-correlated-worker execution's reported `state` no longer stays
  `RUNNING` — it now correctly reports the existing `WAITING` state (same semantic already used
  correctly for timer/retry waits), on both engines:
  - **Kafka-Streams**: `OksKafkaRuntime.mapping()` now maps `CORRELATED_WORKER_STARTED`,
    `CORRELATED_WORKER_COMMAND_PUBLISHED`, `CORRELATED_WORKER_PROGRESS`, and
    `CORRELATED_WORKER_ACCEPTED` to `WAITING` instead of `RUNNING`. Verified:
    `OksKafkaRuntimeMappingTest` (3/3, new).
  - **Pekko**: `WorkflowState.Running.status()` now inspects the task stack for a pending
    `EventExecutionFrame.Kind.CORRELATED_WORKER` frame and returns `WAITING` when one is present,
    instead of the previous unconditional `RUNNING`. Verified: `WorkflowStateTest` (2/2, new).
  - Deliberately reused the existing `WAITING` enum value rather than adding a new granular one
    (e.g. `AWAITING_CORRELATED_WORKER`) — smaller change, no OpenAPI spec/DB/exhaustive-switch
    ripple, and it turned out Studio's frontend (`workflow.ts`'s `canPause()`) already treated
    `WAITING` as pausable, so **zero frontend changes were needed**.
- Error vocabulary is still equally generic on both sides (`ExecutionManagementException.Kind`,
  `EngineCommandException.FailureKind`) — neither has anything correlated-worker/human-task-specific.
  Not addressed; the state-visibility fix above covers the practical "is it stuck" question without
  needing a new error taxonomy.

### 5. Fork nested inside fork (2+ levels deep) — RETRACTED, was never actually reachable

**This gap was never real; the original write-up was wrong.** It claimed `WorkflowEntity.
advanceNestedFork` only handles a subset of task kinds (wait, extension-gate, nested-fork, do,
function, for, set, switch) and that anything else - raise, try, emit, listen, http-call,
protocol-call, subworkflow - throws `IllegalStateException` at runtime when nested two-plus fork
levels deep. That's true of `advanceNestedFork` read in isolation, but wrong about what a caller can
actually trigger: `advanceFork`'s dispatch (`WorkflowEntity.java`, ~line 646) checks the *resolved
leaf instruction* - found via `selectForkLeaf`, which already recurses to any nesting depth - for
emit/listen/subworkflow/http-call/protocol-call/try/raise **before** it ever routes into
`advanceNestedFork`, and the `startForkEmit`/`startForkListen`/`startForkSubworkflow`/
`startForkHttpCall`/`startForkProtocolCall`/`advanceForkFailure` methods those checks call are
already written generically against `selection.path()` (an arbitrary-depth branch path), not a
root-level branch index. `advanceNestedFork` only ever receives the *remaining* instruction kinds
(wait, extension-gate, nested-fork, do, function, for, set, switch) - which is exactly what it
handles. Enumerating `MilestoneOneProgram.Instruction`'s full 24-variant sealed hierarchy confirms
every variant reachable inside a fork (correlated-worker is separately, safely compile-rejected
inside any fork depth - see below) is handled by one path or the other; the `IllegalStateException`
the original write-up pointed to is unreachable defensive code for any currently-compilable plan, not
a live bug.

**Verified directly, not just re-read**: added `nestedForkEmitTwoLevelsDeepPublishesFromTheInnerLane`,
`nestedForkListenTwoLevelsDeepAwaitsFromTheInnerLane`, and
`nestedForkSubworkflowTwoLevelsDeepLaunchesFromTheInnerLane` to `WorkflowEntityTest` - each compiles a
plan with a fork nested inside a fork branch and asserts the inner lane's emit/listen/subworkflow
event fires with a 2-element `branchPath`. All three pass today, joining the pre-existing
`nestedForkHttpCallObeysWorkflowWidePauseResumeAndCancel` and
`nestedForkCatchesNearestErrorAndUncaughtErrorFailsWorkflow`, which already covered http-call and
try/raise at the same depth. `protocol-call` wasn't given its own dedicated nested test but shares the
identical `selection.path()`-generic pattern as the five confirmed constructs, via
`startForkProtocolCall`/`advanceForkProtocolCallIteration`.

Real, separate, and out of scope for this correction: `correlated-worker` inside a fork (any depth,
not just nested) is deliberately compile-rejected via `MilestoneOneProgram`'s fork-slice whitelist,
not silently broken - see gap #3's "Deliberately not built" note above. That's a clean, safe rejection
consistent with Phase 0's fail-fast principle, not a silent-crash gap like this one turned out not to
be.

## What's solid — don't second-guess this

All of the following work correctly on **both** engines, with matching event/state-model coverage:

- All 12 top-level task kinds: `set`, `do`, `switch`, `for`, `fork` (single-level), `emit`, `listen`, `wait`, `raise`, `try`, `call`, `run`.
- All standard `call` protocol kinds: `HTTP`, `GRPC`, `OPEN_API`, `ASYNC_API`, `A2A`, `MCP`, `FUNCTION`, and now `CORRELATED_WORKER` too (see #3).
- All `run` protocol kinds, on both engines now (see #1): `SHELL`, `SCRIPT`, `CONTAINER`, `WORKFLOW`.

## Shared execution-state policy — IMPLEMENTED

Gap #4's state-visibility fix (above) was built as two independently-authored copies of the same
rule - `OksKafkaRuntime.mapping()` on Kafka-Streams, `WorkflowState.Running.status()` on Pekko -
with nothing enforcing they'd stay in agreement, and no test that would have caught it if they
silently diverged. Closed by:

- `BlockingConstructs` (new, `openworkflow-engine-api`): the single source of truth for "does a
  pending interaction of this `CallPlan.Kind` mean WAITING, not RUNNING" - today
  `Set.of(CallPlan.Kind.CORRELATED_WORKER)`. Both engines' status-decision code now calls
  `BlockingConstructs.isBlocking(...)` instead of hardcoding the literal independently.
- `openworkflow-engine-cross-engine-tests` (new module): a genuine regression guard neither engine
  module could host on its own - it deliberately depends on both `openworkflow-pekko-runtime` and
  `openworkflow-kafka-streams-engine` (a dependency shape neither of them may hold in reverse) so one
  test can drive both engines' real production status-decision entry points with the equivalent
  input and assert they agree. This is narrower than a full end-to-end dual-engine execution test
  (that would need a much larger new integration harness - real Pekko actor system plus real
  embedded Kafka broker/topology alive together in one test, which doesn't exist today and wasn't
  needed to close the actual duplication) - flagged as a valid, larger follow-up if ever needed, not
  started.

Verified: `BlockingConstructsTest` (2/2, new), `CorrelatedWorkerStateParityTest` (2/2, new),
`OksKafkaRuntimeMappingTest`/`WorkflowStateTest` unchanged in behavior (3/3, 2/2) confirming the
refactor didn't alter outcomes, full reactor rebuild across all 18 `openworkflow-engine` modules
clean.

## Fail-fast per-engine capability validation — IMPLEMENTED

`EngineCapabilities` (`openworkflow-engine/openworkflow-engine-api/.../EngineCapabilities.java`)
declares exactly what each engine (`PEKKO`, `KAFKA_STREAMS` constants) supports, checked in
`ExecutionManagementService.start()` before dispatch — a plan using an unsupported construct fails at
submission with a clean `ExecutionManagementException.Kind.UNSUPPORTED_CONSTRUCT` (HTTP 422), not a
runtime crash or silent hang. Currently only `HUMAN_TASK` is excluded on either engine, now that #1
and #3 are resolved. Covered by `EngineCapabilitiesTest` (6 tests). Update the two capability
constants if human-task is ever built.
