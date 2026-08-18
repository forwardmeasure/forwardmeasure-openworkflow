# Durable wire-format policy

The journal, snapshot store, cluster remoting, and command replies use
`OpenWorkflowWireSerializer` with Jackson CBOR payloads and explicit string
manifests. Java class names are not durable manifests.

Rules:

1. A published manifest is immutable and is never reused for another type.
2. Backward-compatible optional-field additions retain the manifest version and
   require a golden-fixture backward-read test.
3. Incompatible shape changes receive a new manifest version. The serializer
   must retain the old manifest and migrate its payload into the current domain
   type during reads.
4. Removing a Java type does not remove its reader until every supported
   journal and snapshot has been migrated or replayed past it.
5. Golden fixture changes require an explicit migration explanation. Updating a
   fixture merely to make a test pass is prohibited.
6. Serializer identifier `771003` is reserved permanently for this project.
7. Command and reply compatibility covers rolling cluster upgrades; event and
   state compatibility additionally covers indefinite durable recovery.

The `ow.event.for-entered.v1` event captures an evaluated collection exactly
once. `ow.event.for-advanced.v1` records each subsequent iteration cursor. This
keeps loop recovery deterministic and makes every iteration boundary available
to workflow pause and cancellation commands.

`ow.event.wait-scheduled.v1` persists the deadline before
`ow.command.timer-elapsed.v1` is scheduled. Timer delivery is correlated by
execution, task path, and exact deadline; stale or duplicate deliveries do not
advance the FSM.

`ow.event.deadline-scheduled.v1` persists workflow and task timeout deadlines
before `ow.command.deadline-elapsed.v1` is scheduled. The active-state snapshot
manifests are `v3` because they add the absolute workflow deadline; their `v2`
and `v1` forms remain readable with a missing deadline interpreted as no
timeout. Task deadlines are stored on durable task frames. Paused and terminal
states ignore timer deliveries, and recovery re-arms timers from these absolute
deadlines.

The active-state snapshot manifests advance to `v4` for the durable nested
CloudEvents `until` consumption window carried by event task frames. `v1`,
`v2`, and `v3` frames remain readable; an absent window means no partial
termination correlation has yet been accepted.

`ow.event.try-entered.v1`, `ow.event.error-raised.v1`,
`ow.event.error-caught.v1`, `ow.event.retry-scheduled.v1`, and
`ow.event.retry-started.v1` form the durable structured-failure protocol.
Retry state records the attempt number, cumulative attempt execution time,
first-attempt instant, exact backoff/jitter deadline, structured error, and
guarded cursor before `ow.command.retry-elapsed.v1` is installed. Recovery and
resume re-arm that deadline; cancellation makes late retry wakeups inert. The
additional optional task-frame members remain backward compatible with the
`v3` active-state manifests.

`ow.event.fork-entered.v1` persists the complete declaration-ordered lane
layout and each lane's initial input before branch work begins.
`ow.event.fork-branch-advanced.v1` persists one bounded lane transition, its
next cursor, the next round-robin lane, and any competing winner. A normal join
therefore produces an array in declaration order, while a competing join uses
the first durably observed completed lane. The optional fork member on active
task frames is backward compatible with the `v3` state manifests.
`ow.event.fork-branch-task-entered.v1` and
`ow.event.fork-branch-task-completed.v1` preserve branch-local nested task
stacks without projecting them as a false single global stack.
`ow.event.fork-branch-for-entered.v1` captures a lane-local collection once;
`ow.event.fork-branch-for-advanced.v1` persists each subsequent index and lane
cursor. They apply the same replay and workflow-control cutpoint rules as a
top-level durable iteration.
`ow.event.fork-nested-entered.v1`,
`ow.event.fork-nested-branch-advanced.v1`, and
`ow.event.fork-nested-completed.v1` address a branch by its declaration-index
coordinate path, preserving an arbitrary-depth parallel tree without encoding
ephemeral actor identities. `ow.event.fork-nested-task-entered.v1`,
`ow.event.fork-nested-task-completed.v1`,
`ow.event.fork-nested-for-entered.v1`, and
`ow.event.fork-nested-for-advanced.v1` use the same coordinates for durable
nested `do` and `for` frames. Each command advances only one selected leaf, so
replay retains deterministic round-robin order at every enclosing fork.
`ow.event.fork-branch-wait-scheduled.v1` stores the declaration-index
coordinate and absolute deadline before installing a timer. Its blocked flag
records whether this transition exhausts every runnable leaf, which makes the
workflow-level `RUNNING` to `WAITING` projection deterministic.
`ow.event.fork-branch-wait-completed.v1` removes exactly the matching durable
lane frame; task path and deadline correlation make stale timer deliveries
inert while other lanes continue independently.
`ow.event.fork-branches-waiting.v1` is appended when a non-wait lane transition
leaves every unfinished leaf blocked. It carries the earliest absolute wakeup,
preventing the durable actor state and read-side status from diverging merely
because the last runnable task happened not to be a wait task.
The wakeup is absent when every unfinished lane is blocked on an external
effect such as an event, emission acknowledgement, or awaited child workflow;
recovery then waits for that persist-confirmed observation rather than arming a
timer.
`ow.event.fork-branch-context-updated.v1` persists a complete isolated context
snapshot at a declaration-index coordinate. At a normal join, changes relative
to the common parent snapshot are applied in branch declaration order (later
branches deterministically resolve conflicts); a competing join uses only the
winner's context. Nested forks apply the same rule recursively.
`ow.event.fork-branch-try-entered.v1`,
`ow.event.fork-branch-try-completed.v1`,
`ow.event.fork-branch-error-caught.v1`,
`ow.event.fork-branch-retry-scheduled.v1`, and
`ow.event.fork-branch-retry-started.v1` extend the structured-failure protocol
with a branch coordinate. Retry limits, elapsed-attempt accounting, backoff,
jitter, and absolute deadlines remain identical to top-level policy; recovery
locates and resumes only the owning lane while workflow pause and cancellation
govern the entire fork tree.
`ow.event.emit-requested.v1` is an outbox intent containing a fully materialized
CloudEvents 1.0 envelope and deterministic operation ID; the FSM does not
complete the task until `ow.command.effect-acknowledged.v2` produces
`ow.event.emit-acknowledged.v1`. `ow.event.listen-started.v2` persists the
subscription boundary. Each accepted, deduplicated inbound event, correlation
map, matched-filter set, and ordered accumulation is captured by
`ow.event.listen-event-accepted.v1`, allowing one/all/any and until decisions to
recover without re-evaluating previously consumed events.
`ow.event.listen-until-advanced.v1` persists partial multi-event termination
windows. `ow.event.listen-iteration-started.v1` and
`ow.event.listen-iteration-advanced.v1` retain the collected FIFO sequence and
each `foreach` cursor/result. Reply-bearing inbound commands use
`ow.command.cloud-event-received.v2` and
`ow.schedule.command.event-received.v2`; their fire-and-forget `v1` frames
remain readable and acquire deterministic command IDs during deserialization.
Paused executions reject persist-confirmed ingress for retry, while terminal
executions acknowledge it without changing state.

Fork-lane effects use the coordinate-qualified
`ow.event.fork-branch-emit-requested.v1`,
`ow.event.fork-branch-emit-acknowledged.v1`,
`ow.event.fork-branch-listen-started.v1`,
`ow.event.fork-branch-listen-accepted.v1`,
`ow.event.fork-branch-listen-iteration-advanced.v1`, and
`ow.event.fork-branch-effect-skipped.v1` family. Branch paths address the
owning leaf at arbitrary fork depth. One listen-accepted fact contains every
lane update caused by the same inbound CloudEvent, so parallel consumption is
atomic under replay. Each event also captures whether the remaining fork tree
is blocked and whether any active listeners remain, allowing the persistent
FSM, query projection, and subscription projection to agree without
re-evaluating workflow expressions.

The `ow.schedule.*.v1` family is a separate tenant-qualified persistent FSM.
It stores the next `every` and `cron` instants, every pending `after` instant,
and launch requests awaiting acknowledgement. A deterministic execution ID
makes redelivery at-least-once and idempotent. Recovery re-arms temporal
triggers and redelivers unacknowledged launches; `every` advances from its
previous scheduled instant, while `after` is anchored to a persisted workflow
completion notification.
`ow.schedule.event.event-accepted.v1` stores event correlation progress,
source/id deduplication, and any resulting tenant-qualified launch request.
The active schedule snapshot is `ow.schedule.state.active.v2`; `v1` remains
readable with an empty event window.

The `ow.subflow.*.v1` family is the durable parent/child handshake. A launch
event stores the tenant-qualified parent and deterministic child execution,
the admission-pinned child plan, input, actor, operation identifier, and await
mode before the child is started. The coordinator then durably records the
child's terminal observation before notifying an awaited parent, and records
that notification before becoming quiescent. Recovery retries an active launch
or an unconfirmed parent notification. Parent pause and cancellation are
workflow-wide controls: an active child is paused or cancelled respectively,
while a late terminal child result is acknowledged without mutating a terminal
parent. Explicit command, event, reply, and state manifests cover both cluster
remoting and indefinite journal/snapshot recovery.
`ow.subflow.command.child-observed.v2` distinguishes pause, cancel, and normal
progress observations so a recovered coordinator can start and then control a
child that was still `NEW`. The `v1` reader remains registered and defaults
both control flags to false.
`ow.event.fork-branch-subworkflow-requested.v1` and
`ow.event.fork-branch-subworkflow-completed.v1` add the owning root fork and
arbitrary-depth branch coordinate to the same protocol. Their persisted blocked
flag keeps workflow status deterministic while sibling lanes remain runnable.

Reusable-function entry uses `ow.event.function-entered.v1`,
`ow.event.fork-branch-function-entered.v1`, and
`ow.event.fork-nested-function-entered.v1`. Each fact separates the caller's
raw/transformed input from the evaluated function arguments and carries an
immutable operation descriptor. That descriptor has a deterministic operation
ID, function name, defensive argument snapshot, and an optional digest-pinned
`FUNCTION_DEFINITION` resource reference. Inline functions omit the resource;
catalogued functions must retain the exact admitted URI and SHA-256. Recovery
therefore resumes the compiled child scope without consulting a catalog or
re-evaluating arguments. Existing task-completion facts close the scope, and
the fork variants use declaration-index coordinates at their respective depth.

HTTP and OpenAPI calls use `ow.event.http-call-requested.v1` and
`ow.event.fork-branch-http-call-requested.v1` as persist-before-dispatch
outbox intents. The embedded descriptor fixes the deterministic operation ID,
method, absolute URI, evaluated headers/body, output mode, redirect policy,
authentication plan, and—for OpenAPI—the admitted document URI/SHA-256 plus
operation ID. Expression-backed authentication may additionally carry a
credential-free runtime scope; tenant secrets are joined to that scope only at
the authorised adapter edge and never appear in the journal. The adapter
returns `ow.command.http-call-completed.v1`; successful observations become
the corresponding top-level or coordinate-qualified completion event, while
RFC 9457 failures enter the same catch/retry protocol as other task failures.
Paused workflows reject observations for projection retry. Cancelled and other
terminal workflows acknowledge late observations without persisting or
reviving execution, including calls owned by arbitrary-depth fork lanes.
