package com.forwardmeasure.openworkflow.workflow.runtime.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.durableprocessing.api.DurableAggregate;
import com.forwardmeasure.durableprocessing.api.DurableDecision;
import com.forwardmeasure.durableprocessing.api.DurableDisposition;
import com.forwardmeasure.durableprocessing.api.DurableProcessContext;
import com.forwardmeasure.durableprocessing.core.DurableProcessingKernel;
import com.forwardmeasure.openworkflow.data.DataReference;
import com.forwardmeasure.openworkflow.data.DataReferenceJson;
import com.forwardmeasure.openworkflow.data.DataReferences;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.AdvanceExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.BusinessCorrelationId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ControlExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionControlAction;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionEventType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionHistoryEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionPurgePolicyDecision;
import com.forwardmeasure.openworkflow.workflow.runtime.api.FireTimerCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.HumanTaskObservation;
import com.forwardmeasure.openworkflow.workflow.runtime.api.HumanTaskObservationStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ObserveHumanTaskCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ObserveOperationCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ObserveWorkflowComputationCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ObserveWorkflowComputationFailureCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OksTenantId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservation;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservationStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.PurgeExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ReapplyExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ReceiveAsyncApiMessageCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ReceiveEventCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.StartExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.SwitchDecision;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionBundle;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffectType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowError;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowExecutionId;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class WorkflowExecutionEngineTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Instant NOW = Instant.parse("2026-07-28T20:00:00Z");
  private static final OksTenantId TENANT = OksTenantId.parse("did:web:tenant.example.com");
  private static final ExecutionKey KEY =
      new ExecutionKey(TENANT, new WorkflowExecutionId("execution-1"));
  private static final ActorContext USER =
      new ActorContext(
          TENANT,
          ActorId.parse(
              "did:web:tenant.example.com:actors:" + "2ab3aea3-0972-4eac-8a9d-bcd4a5f0cc45"),
          ActorType.HUMAN,
          "Prashanth Nandavanam",
          "ssb-public",
          BusinessCorrelationId.parse("workflow-cancellation-test"),
          Set.of("evidence-control"),
          null,
          NOW,
          "https://auth.example.com/realms/forwardmeasure",
          "2ab3aea3-0972-4eac-8a9d-bcd4a5f0cc45");
  private static final ActorId RUNTIME_ACTOR_ID =
      ActorId.parse("did:web:runtime.example.com:actors:runtime");
  private static final ActorContext RUNTIME =
      new ActorContext(
          TENANT,
          RUNTIME_ACTOR_ID,
          ActorType.SYSTEM,
          "OKS Runtime",
          "oks-workflow-runtime",
          Set.of(),
          null,
          NOW);
  private static final ActorContext REVIEWER =
      new ActorContext(
          TENANT,
          ActorId.parse("did:web:tenant.example.com:actors:evidence-reviewer"),
          ActorType.HUMAN,
          "Evidence Reviewer",
          "ssb-public",
          Set.of("evidence-reviewer"),
          null,
          NOW);
  private static final ActorContext PURGER =
      new ActorContext(
          TENANT,
          ActorId.parse("did:web:tenant.example.com:actors:records-admin"),
          ActorType.HUMAN,
          "Records Administrator",
          "ssb-public",
          Set.of(PurgeExecutionCommand.REQUIRED_ROLE),
          null,
          NOW);
  private static final byte[] SOURCE =
      """
      document:
        dsl: '1.0.3'
        namespace: evidence
        name: extraction
        version: '1.0.0'
      do:
        - prepare:
            do:
              - initialize:
                  set:
                    status: ready
              - preserve:
                  set:
                    preserveArtifacts: true
      """
          .getBytes(StandardCharsets.UTF_8);

  @Test
  void failsClosedWithABoundedTerminalRecordWhenAggregateStateIsTooLarge() throws Exception {
    WorkflowDefinitionBundle definition = bundle();
    DataReference boundedValue =
        DataReferences.inline(JSON.createObjectNode().put("blob", "x".repeat(30_000)));
    ExecutionCursor oversizedCursor = ExecutionCursor.start(boundedValue);
    for (int index = 0; index < 10; index++) {
      oversizedCursor = oversizedCursor.enter("/do/0/do/" + index, boundedValue, boundedValue);
    }
    var oversized =
        new ExecutionSnapshot(
            KEY,
            definition.reference(),
            definition.plan(),
            USER,
            NOW,
            ExecutionPhase.RUNNING,
            oversizedCursor,
            boundedValue,
            boundedValue,
            boundedValue,
            7);
    var mapper = new ObjectMapper().findAndRegisterModules();
    assertTrue(
        mapper.writeValueAsBytes(oversized).length
            > WorkflowExecutionEngine.MAX_DURABLE_STATE_BYTES,
        "The fixture must exceed the aggregate state envelope");

    var engine =
        new WorkflowExecutionEngine(
            ignored -> definition, RUNTIME_ACTOR_ID, "oks-state-envelope-test");
    var command =
        new ControlExecutionCommand(
            "pause-oversized-state", KEY, ExecutionControlAction.PAUSE, USER, NOW.plusSeconds(1));
    var transition =
        engine.decide(
            new DurableProcessContext(
                KEY.canonical(), command.commandId(), 1, 2, command.requestedAt()),
            oversized,
            command);

    assertEquals(ExecutionPhase.FAILED, transition.state().phase());
    assertEquals(413, transition.state().failure().status());
    assertEquals("Workflow state is too large", transition.state().failure().title());
    assertTrue(transition.state().cursor().complete());
    assertEquals(1, transition.events().size());
    assertEquals(ExecutionEventType.EXECUTION_FAILED, transition.events().getFirst().type());
    assertTrue(transition.outbox().isEmpty());
    assertTrue(transition.followUpCommands().isEmpty());
    assertTrue(
        mapper.writeValueAsBytes(transition.state()).length
            <= WorkflowExecutionEngine.MAX_DURABLE_STATE_BYTES,
        "The terminal failure record must fit the state envelope");
  }

  @Test
  void carriesActorDelegationRolesAndCorrelationIntoDurableWork() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: audit
          name: correlated-wait
          version: '1.0.0'
        do:
          - pending:
              wait: PT1S
        """
            .getBytes(StandardCharsets.UTF_8);
    ActorContext actor =
        new ActorContext(
            TENANT,
            USER.actorId(),
            ActorType.HUMAN,
            USER.displayName(),
            USER.clientId(),
            BusinessCorrelationId.parse("correlation-42"),
            Set.of("workflow-start", "evidence-control"),
            ActorId.parse("did:web:tenant.example.com:actors:delegating-service"),
            NOW);
    WorkflowPlan plan = plan(source);
    Harness harness = new Harness(source);
    var started =
        harness.apply(
            new StartExecutionCommand(
                "start-correlated",
                KEY,
                new WorkflowDefinitionReference(
                    new WorkflowDefinitionKey(TENANT, plan.coordinates()),
                    plan.sourceSha256(),
                    plan.definitionSha256()),
                DataReferences.inline(JSON.createObjectNode()),
                actor,
                NOW),
            "1".repeat(64));

    assertEquals(actor, started.aggregate().state().startedBy());
    assertEquals("correlation-42", started.events().getFirst().actor().correlationId().value());
    assertEquals(actor.delegatedBy(), started.events().getFirst().actor().delegatedBy());
    assertEquals(actor.roles(), started.events().getFirst().actor().roles());

    var waiting =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));
    assertTrue(
        waiting.events().stream()
            .allMatch(event -> "correlation-42".equals(event.actor().correlationId().value())));
    assertFalse(waiting.outbox().isEmpty());
    assertTrue(
        waiting.outbox().stream()
            .allMatch(effect -> "correlation-42".equals(effect.actor().correlationId().value())));
  }

  @Test
  void crossesOneDurableBoundaryPerAdvance() throws Exception {
    Harness harness = new Harness();
    DurableDecision<ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
        decision = harness.apply(startCommand(), "1".repeat(64));
    List<ExecutionEventType> eventTypes = new ArrayList<>();
    addEvents(eventTypes, decision);

    assertEquals(1, decision.aggregate().revision());
    assertEquals(1, decision.aggregate().state().cursor().frames().size());
    assertEquals(1, decision.followUpCommands().size());

    while (!decision.aggregate().state().phase().terminal()) {
      ExecutionCommand continuation = decision.followUpCommands().getFirst();
      decision = harness.apply(continuation, fingerprint(decision.aggregate().revision()));
      addEvents(eventTypes, decision);
    }

    assertEquals(
        List.of(
            ExecutionEventType.EXECUTION_STARTED,
            ExecutionEventType.TASK_STARTED,
            ExecutionEventType.TASK_STARTED,
            ExecutionEventType.TASK_COMPLETED,
            ExecutionEventType.TASK_STARTED,
            ExecutionEventType.TASK_COMPLETED,
            ExecutionEventType.TASK_COMPLETED,
            ExecutionEventType.EXECUTION_COMPLETED),
        eventTypes);
    ExecutionSnapshot completed = decision.aggregate().state();
    assertEquals(ExecutionPhase.COMPLETED, completed.phase());
    assertTrue(completed.cursor().complete());
    assertEquals(8, completed.nextSequence());
    assertTrue(completed.data().inlineValue().required("preserveArtifacts").booleanValue());
    assertEquals(ActorType.SYSTEM, decision.events().getFirst().actor().actorType());
  }

  @Test
  void purgesATerminalExecutionOnlyAfterExternalCleanupSucceeds() throws Exception {
    Harness harness = completedHarness();
    var requested = harness.apply(purgeCommand(PURGER), fingerprint(harness.aggregate.revision()));

    assertEquals(ExecutionPhase.PURGING, requested.aggregate().state().phase());
    ActiveExecutionPurgeState purge =
        assertInstanceOf(
            ActiveExecutionPurgeState.class, requested.aggregate().state().pendingInteraction());
    assertEquals(
        ExecutionEventType.EXECUTION_PURGE_REQUESTED, requested.events().getFirst().type());
    assertEquals(WorkflowEffectType.DISPATCH_OPERATION, requested.outbox().getFirst().type());
    assertEquals(
        "executionPurge",
        requested
            .outbox()
            .getFirst()
            .payload()
            .inlineValue()
            .required("operationKind")
            .textValue());

    var removed =
        harness.apply(
            new ObserveOperationCommand(
                "purge-completed",
                KEY,
                purge.purgeId(),
                new OperationObservation(
                    OperationObservationStatus.SUCCEEDED,
                    DataReferences.inline(JSON.createObjectNode().put("deletedRows", 3)),
                    null,
                    null),
                RUNTIME,
                NOW.plusSeconds(1)),
            fingerprint(requested.aggregate().revision()));

    assertTrue(removed.stateRemoved());
    assertNull(removed.aggregate());
    assertEquals(ExecutionEventType.EXECUTION_PURGED, removed.events().getFirst().type());
    assertEquals(
        WorkflowEffectType.PURGE_EXECUTION_PROJECTIONS, removed.outbox().getFirst().type());
  }

  @Test
  void failedExternalPurgeRestoresTheOriginalTerminalExecution() throws Exception {
    Harness harness = completedHarness();
    var requested = harness.apply(purgeCommand(PURGER), fingerprint(harness.aggregate.revision()));
    ActiveExecutionPurgeState purge =
        assertInstanceOf(
            ActiveExecutionPurgeState.class, requested.aggregate().state().pendingInteraction());

    var failed =
        harness.apply(
            new ObserveOperationCommand(
                "purge-failed",
                KEY,
                purge.purgeId(),
                new OperationObservation(
                    OperationObservationStatus.FAILED,
                    null,
                    new WorkflowError(
                        "urn:oks:error:purge",
                        503,
                        purge.purgeId(),
                        "Purge unavailable",
                        "PostgreSQL cleanup failed"),
                    null),
                RUNTIME,
                NOW.plusSeconds(1)),
            fingerprint(requested.aggregate().revision()));

    assertFalse(failed.stateRemoved());
    assertEquals(ExecutionPhase.COMPLETED, failed.aggregate().state().phase());
    assertNull(failed.aggregate().state().pendingInteraction());
    assertEquals(ExecutionEventType.EXECUTION_PURGE_FAILED, failed.events().getFirst().type());
  }

  @Test
  void cancellingAPurgingExecutionIsRejectedCleanlyLikeATerminalExecution() throws Exception {
    // PURGING is deliberately not ExecutionPhase.terminal() (it only starts once the execution
    // already reached a terminal phase, and that phase must be restorable if the purge itself
    // fails - see ActiveExecutionPurgeState), so cancel()'s original terminal-phase guard didn't
    // catch it. Before this fix, that meant cancel() fell all the way through to
    // appendInteractionCancellationEvents and crashed with a confusing, unrelated
    // "Compiled plan does not contain task $purge" error (ActiveExecutionPurgeState.TASK_PATH is
    // a synthetic marker no real compiled plan ever has a step at) - discovered while verifying
    // Phase 11's PendingInteraction switch conversion, not caused by it. Now rejected with the
    // same clear message shape as a genuinely terminal phase.
    Harness harness = completedHarness();
    var requested = harness.apply(purgeCommand(PURGER), fingerprint(harness.aggregate.revision()));
    assertEquals(ExecutionPhase.PURGING, requested.aggregate().state().phase());

    var cancel =
        new ControlExecutionCommand(
            "cancel-during-purge", KEY, ExecutionControlAction.CANCEL, PURGER, NOW.plusSeconds(1));
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> harness.apply(cancel, fingerprint(requested.aggregate().revision())));
    assertEquals("Cannot cancel a purging execution", thrown.getMessage());
  }

  @Test
  void purgeFailsClosedWithoutDedicatedRoleOrEligibility() throws Exception {
    Harness unauthorized = completedHarness();
    assertThrows(
        SecurityException.class,
        () ->
            unauthorized.apply(purgeCommand(USER), fingerprint(unauthorized.aggregate.revision())));

    Harness held = completedHarness();
    PurgeExecutionCommand command =
        new PurgeExecutionCommand(
            "purge-held",
            KEY,
            new ExecutionPurgePolicyDecision(
                "decision-held",
                "records-v1",
                KEY,
                NOW,
                NOW.minusSeconds(1),
                true,
                "investigation"),
            PURGER,
            NOW);
    assertThrows(
        SecurityException.class, () -> held.apply(command, fingerprint(held.aggregate.revision())));
  }

  @Test
  void oversizedIntermediateDataCreatesADurableComputationCutpoint() throws Exception {
    byte[] source =
        ("""
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: bounded-runtime-data
          version: '1.0.0'
        do:
          - expand:
              set:
                payload: '\
        """
                + "x".repeat(DataReferences.MAX_INLINE_BYTES)
                + "'\n")
            .getBytes(StandardCharsets.UTF_8);
    WorkflowPlan plan = plan(source);
    Harness harness = new Harness(source);
    var started =
        harness.apply(
            new StartExecutionCommand(
                "start-bounded-data",
                KEY,
                new WorkflowDefinitionReference(
                    new WorkflowDefinitionKey(TENANT, plan.coordinates()),
                    plan.sourceSha256(),
                    plan.definitionSha256()),
                DataReferences.inline(JSON.createObjectNode()),
                USER,
                NOW),
            "9".repeat(64));

    var deferred =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));

    assertEquals(ExecutionPhase.COMPUTING, deferred.aggregate().state().phase());
    assertNotNull(deferred.aggregate().state().pendingComputation());
    assertEquals(
        WorkflowEffectType.COMPUTE_WORKFLOW_TRANSITION, deferred.outbox().getFirst().type());
    assertTrue(deferred.events().isEmpty());
  }

  @Test
  void disabledComputationFailsOversizedDataInsteadOfHanging() throws Exception {
    byte[] source =
        ("""
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: bounded-runtime-data-disabled
          version: '1.0.0'
        do:
          - expand:
              set:
                payload: '\
        """
                + "x".repeat(DataReferences.MAX_INLINE_BYTES)
                + "'\n")
            .getBytes(StandardCharsets.UTF_8);
    WorkflowPlan plan = plan(source);
    Harness harness = new Harness(source, List.of(), Duration.ofSeconds(30), false);
    var started =
        harness.apply(
            new StartExecutionCommand(
                "start-bounded-data-disabled",
                KEY,
                new WorkflowDefinitionReference(
                    new WorkflowDefinitionKey(TENANT, plan.coordinates()),
                    plan.sourceSha256(),
                    plan.definitionSha256()),
                DataReferences.inline(JSON.createObjectNode()),
                USER,
                NOW),
            "8".repeat(64));

    var failed =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));

    assertEquals(ExecutionPhase.FAILED, failed.aggregate().state().phase());
    assertNull(failed.aggregate().state().pendingComputation());
    assertTrue(failed.outbox().isEmpty());
    assertEquals(ExecutionEventType.EXECUTION_FAILED, failed.events().getLast().type());
    assertEquals(503, failed.aggregate().state().failure().status());
    assertTrue(failed.aggregate().state().failure().message().contains("capability is disabled"));
  }

  @Test
  void disabledComputationFailsArtifactBackedStartDurably() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: artifact-start-disabled
          version: '1.0.0'
        input:
          from: '${ {instruction: .instruction} }'
        do:
          - copy:
              set:
                extracted: '${ .instruction }'
        """
            .getBytes(StandardCharsets.UTF_8);
    WorkflowPlan plan = plan(source);
    DataReference artifact =
        new DataReference(
            DataReference.Storage.ARTIFACT,
            null,
            URI.create("urn:oks:workflow-data:" + "10000000-0000-0000-0000-000000000199"),
            "application/json",
            512 * 1024,
            "d".repeat(64));
    Harness harness = new Harness(source, List.of(), Duration.ofSeconds(30), false);

    var failed =
        harness.apply(
            new StartExecutionCommand(
                "start-artifact-disabled",
                KEY,
                new WorkflowDefinitionReference(
                    new WorkflowDefinitionKey(TENANT, plan.coordinates()),
                    plan.sourceSha256(),
                    plan.definitionSha256()),
                artifact,
                USER,
                NOW),
            "7".repeat(64));

    assertEquals(ExecutionPhase.FAILED, failed.aggregate().state().phase());
    assertNull(failed.aggregate().state().pendingComputation());
    assertTrue(failed.outbox().isEmpty());
    assertEquals(ExecutionEventType.EXECUTION_FAILED, failed.events().getFirst().type());
    assertEquals(artifact, failed.aggregate().state().failure().rejectedData());
    assertEquals(503, failed.aggregate().state().failure().status());
  }

  @Test
  void terminalComputationFailureIsAuditedAndReleasesTheCutpoint() throws Exception {
    byte[] source =
        ("""
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: failed-runtime-computation
          version: '1.0.0'
        do:
          - expand:
              set:
                payload: '\
        """
                + "x".repeat(DataReferences.MAX_INLINE_BYTES)
                + "'\n")
            .getBytes(StandardCharsets.UTF_8);
    WorkflowPlan plan = plan(source);
    Harness harness = new Harness(source);
    var started =
        harness.apply(
            new StartExecutionCommand(
                "start-failed-computation",
                KEY,
                new WorkflowDefinitionReference(
                    new WorkflowDefinitionKey(TENANT, plan.coordinates()),
                    plan.sourceSha256(),
                    plan.definitionSha256()),
                DataReferences.inline(JSON.createObjectNode()),
                USER,
                NOW),
            "2".repeat(64));
    var deferred = harness.apply(started.followUpCommands().getFirst(), "3".repeat(64));
    PendingWorkflowComputation pending = deferred.aggregate().state().pendingComputation();

    var failed =
        harness.apply(
            new ObserveWorkflowComputationFailureCommand(
                pending.computationId() + ":failure",
                KEY,
                pending.computationId(),
                pending.basisRevision(),
                "urn:oks:error:workflow-computation",
                "Workflow data exceeded the configured maximum",
                RUNTIME,
                NOW.plusSeconds(1)),
            "4".repeat(64));

    assertEquals(ExecutionPhase.FAILED, failed.aggregate().state().phase());
    assertNull(failed.aggregate().state().pendingComputation());
    assertEquals(ExecutionEventType.EXECUTION_FAILED, failed.events().getLast().type());
    assertEquals("urn:oks:error:workflow-computation", failed.aggregate().state().failure().type());
    assertEquals(500, failed.aggregate().state().failure().status());
  }

  @Test
  void artifactBackedStartCompletesThroughATrustedComputationResult() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: artifact-start
          version: '1.0.0'
        input:
          schema:
            format: json
            document:
              type: object
              required: [instruction]
          from: '${ {instruction: .instruction} }'
        do:
          - copy:
              set:
                extracted: '${ .instruction }'
        """
            .getBytes(StandardCharsets.UTF_8);
    WorkflowPlan plan = plan(source);
    DataReference artifact =
        new DataReference(
            DataReference.Storage.ARTIFACT,
            null,
            URI.create("urn:oks:workflow-data:" + "10000000-0000-0000-0000-000000000101"),
            "application/json",
            512 * 1024,
            "b".repeat(64));
    StartExecutionCommand start =
        new StartExecutionCommand(
            "start-artifact",
            KEY,
            new WorkflowDefinitionReference(
                new WorkflowDefinitionKey(TENANT, plan.coordinates()),
                plan.sourceSha256(),
                plan.definitionSha256()),
            artifact,
            USER,
            NOW);
    Harness harness = new Harness(source);

    var deferred = harness.apply(start, "a".repeat(64));
    ExecutionSnapshot computing = deferred.aggregate().state();
    assertEquals(ExecutionPhase.COMPUTING, computing.phase());
    assertEquals(1, deferred.aggregate().revision());
    assertEquals(
        WorkflowEffectType.COMPUTE_WORKFLOW_TRANSITION, deferred.outbox().getFirst().type());

    WorkflowRuntimeDataAccess workerData =
        new WorkflowRuntimeDataAccess() {
          @Override
          public JsonNode resolve(DataReference reference) {
            if (reference.equals(artifact)) {
              return JSON.createObjectNode().put("instruction", "Extract named entities");
            }
            return reference.inlineValue().deepCopy();
          }

          @Override
          public DataReference reference(JsonNode value) {
            return DataReferences.inline(value);
          }
        };
    var worker =
        new WorkflowComputationEngine(
            ignored -> bundle(source),
            RUNTIME_ACTOR_ID,
            "oks-computation-test",
            Duration.ofSeconds(30),
            workerData,
            JSON);
    var encoded = worker.compute(computing);
    PendingWorkflowComputation originalPending = computing.pendingComputation();
    var queued =
        harness.apply(
            new ControlExecutionCommand(
                "pause-during-computation",
                KEY,
                ExecutionControlAction.PAUSE,
                USER,
                NOW.plusMillis(500)),
            "b".repeat(64));
    var queuedAgain =
        harness.apply(
            new ControlExecutionCommand(
                "resume-after-queued-pause",
                KEY,
                ExecutionControlAction.RESUME,
                USER,
                NOW.plusMillis(750)),
            "c".repeat(64));
    ExecutionSnapshot superseded = queuedAgain.aggregate().state();
    PendingWorkflowComputation pending = superseded.pendingComputation();
    assertEquals(2, pending.queuedCommands().size());
    assertEquals("pause-during-computation", pending.queuedCommands().getFirst().commandId());
    assertEquals("resume-after-queued-pause", pending.queuedCommands().getLast().commandId());
    assertEquals(originalPending.computationId(), pending.computationId());
    assertEquals(originalPending.basisRevision(), pending.basisRevision());
    assertTrue(
        queued.outbox().isEmpty(),
        "Queuing a command must not supersede or duplicate " + "the in-flight computation effect");

    ObjectNode tampered = encoded.value().deepCopy();
    tampered.put("tampered", true);
    assertThrows(
        SecurityException.class,
        () ->
            harness.apply(
                new ObserveWorkflowComputationCommand(
                    pending.computationId() + ":tampered-result",
                    KEY,
                    pending.computationId(),
                    pending.basisRevision(),
                    encoded.sha256(),
                    tampered,
                    RUNTIME,
                    NOW.plusSeconds(2)),
                "d".repeat(64)));
    assertEquals(superseded, harness.aggregate.state());

    var observed =
        harness.apply(
            new ObserveWorkflowComputationCommand(
                pending.computationId() + ":result",
                KEY,
                pending.computationId(),
                pending.basisRevision(),
                encoded.sha256(),
                encoded.value(),
                RUNTIME,
                NOW.plusSeconds(3)),
            "e".repeat(64));

    assertEquals(ExecutionPhase.RUNNING, observed.aggregate().state().phase());
    assertNull(observed.aggregate().state().pendingComputation());
    assertEquals(
        "Extract named entities",
        observed.aggregate().state().data().inlineValue().required("instruction").textValue());
    assertEquals(ExecutionEventType.EXECUTION_STARTED, observed.events().getFirst().type());
    assertEquals(1, observed.followUpCommands().size());
    ReapplyExecutionCommand replay =
        assertInstanceOf(ReapplyExecutionCommand.class, observed.followUpCommands().getFirst());
    assertEquals(observed.aggregate().revision(), replay.expectedRevision());
    assertEquals("pause-during-computation", replay.commandId());
    assertEquals(2, replay.remainingCommands().size());
    assertEquals("resume-after-queued-pause", replay.remainingCommands().getFirst().commandId());
    assertEquals(
        observed.aggregate().revision(),
        ((AdvanceExecutionCommand) replay.remainingCommands().getLast()).expectedRevision());

    var paused = harness.apply(replay, "f".repeat(64));
    assertEquals(ExecutionPhase.PAUSED, paused.aggregate().state().phase());
    assertEquals(1, paused.followUpCommands().size());
    ReapplyExecutionCommand queuedResume =
        assertInstanceOf(ReapplyExecutionCommand.class, paused.followUpCommands().getFirst());
    assertEquals("resume-after-queued-pause", queuedResume.commandId());
    var resumed = harness.apply(queuedResume, "1".repeat(64));
    assertEquals(DurableDisposition.APPLIED, resumed.disposition());
    assertEquals(ExecutionPhase.RUNNING, resumed.aggregate().state().phase());
    ReapplyExecutionCommand automaticAdvance =
        assertInstanceOf(ReapplyExecutionCommand.class, resumed.followUpCommands().getFirst());
    var advanced = harness.apply(automaticAdvance, "2".repeat(64));
    assertEquals(DurableDisposition.APPLIED, advanced.disposition());
  }

  @Test
  void cancellationPreemptsADeferredStartAndLateResultIsHarmless() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: cancellable-artifact-start
          version: '1.0.0'
        input:
          from: '${ . }'
        do:
          - annotate:
              set:
                processed: true
        """
            .getBytes(StandardCharsets.UTF_8);
    WorkflowPlan plan = plan(source);
    DataReference artifact =
        new DataReference(
            DataReference.Storage.ARTIFACT,
            null,
            URI.create("urn:oks:workflow-data:" + "10000000-0000-0000-0000-000000000102"),
            "application/json",
            512 * 1024,
            "c".repeat(64));
    Harness harness = new Harness(source);
    var deferred =
        harness.apply(
            new StartExecutionCommand(
                "start-cancellable-artifact",
                KEY,
                new WorkflowDefinitionReference(
                    new WorkflowDefinitionKey(TENANT, plan.coordinates()),
                    plan.sourceSha256(),
                    plan.definitionSha256()),
                artifact,
                USER,
                NOW),
            "2".repeat(64));
    ExecutionSnapshot computing = deferred.aggregate().state();
    PendingWorkflowComputation pending = computing.pendingComputation();
    var worker =
        new WorkflowComputationEngine(
            ignored -> bundle(source),
            RUNTIME_ACTOR_ID,
            "oks-computation-test",
            Duration.ofSeconds(30),
            new WorkflowRuntimeDataAccess() {
              @Override
              public JsonNode resolve(DataReference reference) {
                return reference.equals(artifact)
                    ? JSON.createObjectNode().put("documentId", "evidence-123")
                    : reference.inlineValue().deepCopy();
              }

              @Override
              public DataReference reference(JsonNode value) {
                return DataReferences.inline(value);
              }
            },
            JSON);
    var computedBeforeCancellation = worker.compute(computing);

    var cancelled =
        harness.apply(
            new ControlExecutionCommand(
                "cancel-during-computation",
                KEY,
                ExecutionControlAction.CANCEL,
                USER,
                NOW.plusMillis(500)),
            "3".repeat(64));

    assertEquals(ExecutionPhase.CANCELLED, cancelled.aggregate().state().phase());
    assertNull(cancelled.aggregate().state().pendingComputation());
    assertEquals(ExecutionEventType.EXECUTION_CANCELLED, cancelled.events().getLast().type());
    assertTrue(cancelled.followUpCommands().isEmpty());
    assertTrue(cancelled.outbox().isEmpty());

    long cancelledRevision = cancelled.aggregate().revision();
    var late =
        harness.apply(
            new ObserveWorkflowComputationCommand(
                pending.computationId() + ":late-result",
                KEY,
                pending.computationId(),
                pending.basisRevision(),
                computedBeforeCancellation.sha256(),
                computedBeforeCancellation.value(),
                RUNTIME,
                NOW.plusSeconds(1)),
            "4".repeat(64));

    assertFalse(late.stateChanged());
    assertEquals(cancelledRevision, late.aggregate().revision());
    assertEquals(ExecutionPhase.CANCELLED, late.aggregate().state().phase());
  }

  @Test
  void raiseIsCaughtByInnermostMatchingDurableTryScope() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: caught-error
          version: '1.0.0'
        do:
          - guarded:
              try:
                - reject:
                    raise:
                      error:
                        type: https://example.com/errors/unavailable
                        status: 503
                        title: Evidence service unavailable
                        detail: '${ "case \\(.caseId) failed" }'
              catch:
                errors:
                  with:
                    type: https://example.com/errors/unavailable
                    status: 503
                as: problem
                when: .status == 503
                do:
                  - retain:
                      set:
                        caughtType: '${ $problem.type }'
                        caughtDetail: '${ $problem.detail }'
                then: end
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult result = run(source, JSON.readTree("{\"caseId\":\"case-7\"}"));

    assertEquals(ExecutionPhase.COMPLETED, result.snapshot().phase());
    assertEquals(
        "https://example.com/errors/unavailable",
        result.snapshot().data().inlineValue().required("caughtType").textValue());
    assertEquals(
        "case case-7 failed",
        result.snapshot().data().inlineValue().required("caughtDetail").textValue());
    assertEquals(
        1,
        result.history().stream()
            .filter(event -> event.type() == ExecutionEventType.ERROR_RAISED)
            .count());
    assertEquals(
        1,
        result.history().stream()
            .filter(event -> event.type() == ExecutionEventType.ERROR_CAUGHT)
            .count());
  }

  @Test
  void unmatchedRaiseRetainsTheCompleteWorkflowError() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: uncaught-error
          version: '1.0.0'
        do:
          - reject:
              raise:
                error:
                  type: https://example.com/errors/rejected
                  status: 422
                  instance: /evidence/17
                  title: Evidence rejected
                  detail: The evidence cannot be processed
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult result = run(source, JSON.readTree("{}"));

    assertEquals(ExecutionPhase.FAILED, result.snapshot().phase());
    assertEquals("https://example.com/errors/rejected", result.snapshot().failure().type());
    assertEquals(422, result.snapshot().failure().status());
    assertEquals("/evidence/17", result.snapshot().failure().instance());
    assertEquals("Evidence rejected", result.snapshot().failure().title());
    assertEquals("The evidence cannot be processed", result.snapshot().failure().detail());
  }

  @Test
  void retryDelayIsDurableAndExhaustionRethrowsTheCaughtError() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: retried-error
          version: '1.0.0'
        do:
          - guarded:
              try:
                - reject:
                    raise:
                      error:
                        type: https://example.com/errors/unavailable
                        status: 503
              catch:
                errors:
                  with:
                    status: 503
                retry:
                  delay:
                    seconds: 3
                  backoff:
                    exponential: {}
                  limit:
                    attempt:
                      count: 1
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var decision = harness.apply(startCommand(source, JSON.readTree("{}")), "1".repeat(64));
    List<ExecutionHistoryEvent> history = new ArrayList<>(decision.events());
    while (decision.aggregate().state().pendingInteraction() == null) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
      history.addAll(decision.events());
    }

    ActiveRetryState retry = (ActiveRetryState) decision.aggregate().state().pendingInteraction();
    assertEquals(NOW.plusSeconds(3), retry.dueAt());
    assertEquals(WorkflowEffectType.SCHEDULE_TIMER, decision.outbox().getFirst().type());

    decision =
        harness.apply(
            new FireTimerCommand("fire-retry-1", KEY, retry.timerId(), RUNTIME, retry.dueAt()),
            fingerprint(decision.aggregate().revision()));
    history.addAll(decision.events());
    while (!decision.aggregate().state().phase().terminal()) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
      history.addAll(decision.events());
    }

    assertEquals(ExecutionPhase.FAILED, decision.aggregate().state().phase());
    assertEquals(503, decision.aggregate().state().failure().status());
    assertEquals(
        2,
        history.stream().filter(event -> event.type() == ExecutionEventType.ERROR_RAISED).count());
    assertTrue(
        history.stream().anyMatch(event -> event.type() == ExecutionEventType.RETRY_SCHEDULED));
    assertTrue(
        history.stream().anyMatch(event -> event.type() == ExecutionEventType.RETRY_STARTED));
    assertTrue(
        history.stream().anyMatch(event -> event.type() == ExecutionEventType.RETRY_EXHAUSTED));
  }

  @Test
  void retryAttemptDurationExpiresAnInFlightExternalWait() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: retry-attempt-deadline
          version: '1.0.0'
        do:
          - guarded:
              try:
                - failFirst:
                    if: '${ .retried != true }'
                    raise:
                      error:
                        type: https://example.com/errors/unavailable
                        status: 503
                - awaitEvidence:
                    listen:
                      to:
                        one:
                          with:
                            type: evidence.received.v1
              catch:
                retry:
                  limit:
                    attempt:
                      count: 1
                      duration:
                        seconds: 2
                do:
                  - markRetried:
                      set:
                        retried: true
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var decision = harness.apply(startCommand(source, JSON.readTree("{}")), "1".repeat(64));
    while (!(decision.aggregate().state().pendingInteraction() instanceof ActiveRetryState)) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
    }

    ActiveRetryState retry = (ActiveRetryState) decision.aggregate().state().pendingInteraction();
    decision =
        harness.apply(
            new FireTimerCommand(
                "start-deadlined-retry", KEY, retry.timerId(), RUNTIME, retry.dueAt()),
            fingerprint(decision.aggregate().revision()));
    WorkflowEffect scheduledDeadline =
        decision.outbox().stream()
            .filter(effect -> effect.type() == WorkflowEffectType.SCHEDULE_TIMER)
            .filter(
                effect ->
                    "retry-attempt-deadline"
                        .equals(effect.payload().inlineValue().path("purpose").textValue()))
            .findFirst()
            .orElseThrow();
    String deadlineId = scheduledDeadline.payload().inlineValue().required("timerId").textValue();
    Instant deadlineAt =
        Instant.parse(scheduledDeadline.payload().inlineValue().required("dueAt").textValue());

    while (!(decision.aggregate().state().pendingInteraction() instanceof ActiveListenState)) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
    }
    ActiveListenState listen =
        (ActiveListenState) decision.aggregate().state().pendingInteraction();

    var paused =
        harness.apply(
            new ControlExecutionCommand(
                "pause-deadlined-retry",
                KEY,
                ExecutionControlAction.PAUSE,
                USER,
                NOW.plusSeconds(1)),
            fingerprint(decision.aggregate().revision()));
    assertTrue(
        paused.outbox().stream()
            .anyMatch(
                effect ->
                    effect.type() == WorkflowEffectType.CANCEL_TIMER
                        && deadlineId.equals(
                            effect.payload().inlineValue().required("timerId").textValue())));
    var resumed =
        harness.apply(
            new ControlExecutionCommand(
                "resume-deadlined-retry",
                KEY,
                ExecutionControlAction.RESUME,
                USER,
                NOW.plusMillis(1500)),
            fingerprint(paused.aggregate().revision()));
    assertTrue(
        resumed.outbox().stream()
            .anyMatch(
                effect ->
                    effect.type() == WorkflowEffectType.SCHEDULE_TIMER
                        && deadlineId.equals(
                            effect.payload().inlineValue().required("timerId").textValue())));

    decision =
        harness.apply(
            new FireTimerCommand("expire-deadlined-retry", KEY, deadlineId, RUNTIME, deadlineAt),
            fingerprint(resumed.aggregate().revision()));

    assertNull(decision.aggregate().state().pendingInteraction());
    assertTrue(
        decision.outbox().stream()
            .anyMatch(
                effect ->
                    effect.type() == WorkflowEffectType.DELETE_EVENT_SUBSCRIPTION
                        && listen
                            .subscriptionId()
                            .equals(
                                effect
                                    .payload()
                                    .inlineValue()
                                    .required("subscriptionId")
                                    .textValue())));
    assertTrue(
        decision.outbox().stream()
            .anyMatch(
                effect ->
                    effect.type() == WorkflowEffectType.CANCEL_TIMER
                        && deadlineId.equals(
                            effect.payload().inlineValue().required("timerId").textValue())));
    assertTrue(
        decision.events().stream()
            .anyMatch(event -> event.type() == ExecutionEventType.TIMER_FIRED));
    assertTrue(
        decision.events().stream()
            .anyMatch(event -> event.type() == ExecutionEventType.ERROR_CAUGHT));

    while (!decision.aggregate().state().phase().terminal()) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
    }
    assertEquals(ExecutionPhase.FAILED, decision.aggregate().state().phase());
    assertEquals(
        "https://open-workflow-specification.org/spec/1.0.0/errors/timeout",
        decision.aggregate().state().failure().type());
    assertEquals(408, decision.aggregate().state().failure().status());
  }

  @Test
  void tryCatchesRuntimeValidationErrorsNotOnlyExplicitRaises() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: caught-validation
          version: '1.0.0'
        do:
          - guarded:
              try:
                - invalid:
                    set:
                      count: not-a-number
                    output:
                      schema:
                        format: json
                        document:
                          type: object
                          required: [count]
                          properties:
                            count:
                              type: integer
              catch:
                errors:
                  with:
                    type: https://open-workflow-specification.org/spec/1.0.0/errors/validation
                    status: 400
                as: validation
                do:
                  - recover:
                      set:
                        recovered: '${ $validation.detail }'
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult result = run(source, JSON.readTree("{}"));

    assertEquals(ExecutionPhase.COMPLETED, result.snapshot().phase());
    assertTrue(
        result.snapshot().data().inlineValue().required("recovered").textValue().length() > 10);
    assertTrue(
        result.history().stream()
            .anyMatch(event -> event.type() == ExecutionEventType.ERROR_CAUGHT));
  }

  @Test
  void callUsesADurableOperationBoundaryWithProgressPauseAndResume() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: durable-call
          version: '1.0.0'
        do:
          - extract:
              call: http
              with:
                method: POST
                endpoint: https://extractor.test/v1/extract
                headers:
                  x-tenant: '${ $context.tenant }'
                body:
                  instruction: '${ .instruction }'
                output: content
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var started =
        harness.apply(
            startCommand(
                source,
                JSON.readTree(
                    """
                    {
                      "instruction": "extract persons"
                    }
                    """)),
            "1".repeat(64));
    var dispatched =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));

    ActiveOperationState operation =
        assertInstanceOf(
            ActiveOperationState.class, dispatched.aggregate().state().pendingInteraction());
    assertEquals(WorkflowEffectType.DISPATCH_OPERATION, dispatched.outbox().getFirst().type());
    assertEquals("HTTP", operation.descriptor().inlineValue().required("callKind").textValue());
    assertEquals(
        "extract persons",
        operation
            .descriptor()
            .inlineValue()
            .required("arguments")
            .required("body")
            .required("instruction")
            .textValue());

    var progress =
        harness.apply(
            new ObserveOperationCommand(
                "operation-progress-1",
                KEY,
                operation.operationId(),
                new OperationObservation(
                    OperationObservationStatus.PROGRESS,
                    null,
                    null,
                    DataReferences.inline(
                        JSON.readTree(
                            """
                            {"stage":"document-loaded"}
                            """))),
                RUNTIME,
                NOW.plusSeconds(1)),
            fingerprint(dispatched.aggregate().revision()));
    assertEquals(operation, progress.aggregate().state().pendingInteraction());
    assertTrue(
        progress.events().stream()
            .anyMatch(event -> event.type() == ExecutionEventType.OPERATION_PROGRESS));

    var paused =
        harness.apply(
            new ControlExecutionCommand(
                "pause-call", KEY, ExecutionControlAction.PAUSE, USER, NOW.plusSeconds(2)),
            fingerprint(progress.aggregate().revision()));
    assertTrue(
        paused.outbox().stream()
            .noneMatch(effect -> effect.type() == WorkflowEffectType.CANCEL_OPERATION),
        "Pause must not turn an in-flight operation into a terminal cancellation");
    var resumed =
        harness.apply(
            new ControlExecutionCommand(
                "resume-call", KEY, ExecutionControlAction.RESUME, USER, NOW.plusSeconds(3)),
            fingerprint(paused.aggregate().revision()));
    assertEquals(WorkflowEffectType.DISPATCH_OPERATION, resumed.outbox().getFirst().type());
    assertEquals(
        operation.operationId(),
        resumed.outbox().getFirst().payload().inlineValue().required("operationId").textValue());

    var succeeded =
        harness.apply(
            new ObserveOperationCommand(
                "operation-success-1",
                KEY,
                operation.operationId(),
                new OperationObservation(
                    OperationObservationStatus.SUCCEEDED,
                    DataReferences.inline(
                        JSON.readTree(
                            """
                            {
                              "persons": ["Alice", "Bob"]
                            }
                            """)),
                    null,
                    null),
                RUNTIME,
                NOW.plusSeconds(4)),
            fingerprint(resumed.aggregate().revision()));
    while (!succeeded.aggregate().state().phase().terminal()) {
      succeeded =
          harness.apply(
              succeeded.followUpCommands().getFirst(),
              fingerprint(succeeded.aggregate().revision()));
    }
    assertEquals(ExecutionPhase.COMPLETED, succeeded.aggregate().state().phase());
    assertEquals(2, succeeded.aggregate().state().data().inlineValue().required("persons").size());

    var late =
        harness.apply(
            new ObserveOperationCommand(
                "operation-late-progress",
                KEY,
                operation.operationId(),
                new OperationObservation(
                    OperationObservationStatus.PROGRESS,
                    null,
                    null,
                    DataReferences.inline(JSON.createObjectNode())),
                RUNTIME,
                NOW.plusSeconds(5)),
            fingerprint(succeeded.aggregate().revision()));
    assertFalse(late.stateChanged(), "A late adapter observation is not a poison command");
  }

  @Test
  void humanTaskSurvivesPauseAndResumesFromAnAuditedApproval() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: governed-review
          version: '1.0.0'
        do:
          - approve:
              call: com.forwardmeasure.openworkflow.human-task
              with:
                title: Review extracted evidence
                description: Confirm the extracted people
                input: '${ .extraction }'
                presentation:
                  kind: RAW_JSON
                approvals:
                  makerChecker: true
                  distinctApprovers: true
                  stages:
                    - level: 1
                      name: Evidence Review
                      requiredApprovals: 1
                      candidateRoles: [evidence-reviewer]
                dueAfter: PT4H
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var started =
        harness.apply(
            startCommand(
                source,
                JSON.readTree(
                    """
                    {
                      "extraction": {
                        "people": ["Alice", "Bob"]
                      }
                    }
                    """)),
            "1".repeat(64));
    var waiting =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));

    ActiveHumanTaskState task =
        assertInstanceOf(
            ActiveHumanTaskState.class, waiting.aggregate().state().pendingInteraction());
    assertEquals(
        List.of(WorkflowEffectType.CREATE_HUMAN_TASK, WorkflowEffectType.SCHEDULE_TIMER),
        waiting.outbox().stream().map(WorkflowEffect::type).toList());
    JsonNode descriptor = task.descriptor().inlineValue();
    assertEquals("Review extracted evidence", descriptor.required("title").textValue());
    assertEquals("Alice", descriptor.required("input").required("people").get(0).textValue());
    assertEquals(NOW.plus(Duration.ofHours(4)), task.dueAt());

    var paused =
        harness.apply(
            new ControlExecutionCommand(
                "pause-human-task", KEY, ExecutionControlAction.PAUSE, USER, NOW.plusSeconds(1)),
            fingerprint(waiting.aggregate().revision()));
    assertEquals(ExecutionPhase.PAUSED, paused.aggregate().state().phase());
    assertTrue(
        paused.outbox().stream()
            .noneMatch(effect -> effect.type() == WorkflowEffectType.CANCEL_HUMAN_TASK));
    assertTrue(
        paused.outbox().stream()
            .anyMatch(effect -> effect.type() == WorkflowEffectType.CANCEL_TIMER));

    var buffered =
        harness.apply(
            humanTaskOutcome(
                task,
                HumanTaskObservationStatus.APPROVED,
                JSON.readTree(
                    """
                    {
                      "approved": true,
                      "people": ["Alice", "Bob"]
                    }
                    """),
                NOW.plusSeconds(2)),
            fingerprint(paused.aggregate().revision()));
    ActiveHumanTaskState ready =
        assertInstanceOf(
            ActiveHumanTaskState.class, buffered.aggregate().state().pendingInteraction());
    assertTrue(ready.completionReady());
    assertTrue(
        buffered.events().stream()
            .anyMatch(
                event ->
                    event.type() == ExecutionEventType.HUMAN_TASK_OUTCOME_BUFFERED
                        && event.actor().sameActor(REVIEWER)));

    var completed =
        harness.apply(
            new ControlExecutionCommand(
                "resume-human-task", KEY, ExecutionControlAction.RESUME, USER, NOW.plusSeconds(3)),
            fingerprint(buffered.aggregate().revision()));
    assertTrue(
        completed.events().stream()
            .anyMatch(
                event ->
                    event.type() == ExecutionEventType.HUMAN_TASK_APPROVED
                        && event.actor().sameActor(REVIEWER)),
        completed.events().toString());
    assertTrue(
        completed.events().stream()
            .anyMatch(event -> event.type() == ExecutionEventType.EXECUTION_RESUMED));
    while (!completed.aggregate().state().phase().terminal()) {
      completed =
          harness.apply(
              completed.followUpCommands().getFirst(),
              fingerprint(completed.aggregate().revision()));
    }
    assertEquals(ExecutionPhase.COMPLETED, completed.aggregate().state().phase());
    assertTrue(
        completed.aggregate().state().data().inlineValue().required("approved").booleanValue());
  }

  @Test
  void humanTaskDeadlineRequestsDurableExpiry() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: expiring-review
          version: '1.0.0'
        do:
          - approve:
              call: com.forwardmeasure.openworkflow.human-task
              with:
                title: Review extraction
                approvals:
                  stages:
                    - level: 1
                      name: Review
                      requiredApprovals: 1
                      candidateRoles: [evidence-reviewer]
                dueAfter: PT5M
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var started = harness.apply(startCommand(source, JSON.createObjectNode()), "1".repeat(64));
    var waiting =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));
    ActiveHumanTaskState task =
        assertInstanceOf(
            ActiveHumanTaskState.class, waiting.aggregate().state().pendingInteraction());

    var due =
        harness.apply(
            new FireTimerCommand(
                "fire-human-task-due", KEY, task.dueTimerId(), RUNTIME, task.dueAt()),
            fingerprint(waiting.aggregate().revision()));
    assertEquals(WorkflowEffectType.EXPIRE_HUMAN_TASK, due.outbox().getFirst().type());
    assertEquals(
        task.humanTaskId(),
        due.outbox().getFirst().payload().inlineValue().required("humanTaskId").textValue());
  }

  @Test
  void taskRuntimeDescriptorExposesNormativeTaskMembers() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: task-descriptor
          version: '1.0.0'
        do:
          - inspect:
              call: http
              with:
                method: POST
                endpoint: https://extractor.test/v1/extract
                body:
                  callKind: '${ $task.call }'
                  endpoint: '${ $task.with.endpoint }'
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var started = harness.apply(startCommand(source, JSON.createObjectNode()), "1".repeat(64));
    var dispatched =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));

    ActiveOperationState operation =
        assertInstanceOf(
            ActiveOperationState.class, dispatched.aggregate().state().pendingInteraction());
    JsonNode body = operation.descriptor().inlineValue().required("arguments").required("body");
    assertEquals("http", body.required("callKind").textValue());
    assertEquals("https://extractor.test/v1/extract", body.required("endpoint").textValue());
  }

  @Test
  void mcpStdioDescriptorDurablyCarriesOnlyItsDeclaredSecretReference() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: secured-mcp-stdio
          version: '1.0.0'
        use:
          secrets:
            - evidence-mcp-environment
        do:
          - inspect:
              call: mcp
              with:
                method: tools/list
                transport:
                  stdio:
                    command: /opt/mcp/bin/server
                  options:
                    environmentSecret: evidence-mcp-environment
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var started = harness.apply(startCommand(source, JSON.createObjectNode()), "1".repeat(64));
    var dispatched =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));

    ActiveOperationState operation =
        assertInstanceOf(
            ActiveOperationState.class, dispatched.aggregate().state().pendingInteraction());
    JsonNode descriptor = operation.descriptor().inlineValue();
    assertEquals(
        "evidence-mcp-environment", descriptor.required("secretReferences").get(0).textValue());
    JsonNode protocol = descriptor.required("protocolOperation");
    assertEquals("MCP", protocol.required("kind").textValue());
    assertEquals("mcp-stdio", protocol.required("protocol").textValue());
    assertEquals("tools/list", protocol.required("operation").textValue());
    assertEquals(
        "evidence-mcp-environment",
        descriptor.required("arguments").at("/transport/options/environmentSecret").textValue());
    assertFalse(descriptor.toString().contains("secret-value"));
  }

  @Test
  void extensionCanInterceptAndExitAnExtendedTaskDurably() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extension-interception
          version: '1.0.0'
        use:
          extensions:
            - mock-service:
                extend: call
                when: '${ $task.call == "http" and $task.with.endpoint == "https://mocked.service.test/evidence" }'
                before:
                  - mock-response:
                      set:
                        statusCode: 200
                        content:
                          evidenceId: e-17
                      then: exit
        do:
          - invoke:
              call: http
              with:
                method: GET
                endpoint: https://mocked.service.test/evidence
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult result = run(source, JSON.createObjectNode());

    assertEquals(ExecutionPhase.COMPLETED, result.snapshot().phase());
    assertEquals(
        "e-17",
        result
            .snapshot()
            .data()
            .inlineValue()
            .required("content")
            .required("evidenceId")
            .textValue());
    assertTrue(
        result.history().stream()
            .noneMatch(event -> event.type() == ExecutionEventType.OPERATION_DISPATCHED));
    assertTrue(
        result.history().stream()
            .anyMatch(
                event ->
                    event.taskPath() != null
                        && event.taskPath().contains("/$extensions/0/mock-service/before")));
  }

  @Test
  void extensionConditionIsSelectedOnceAndAfterSeesTheExtendedTask() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extension-after
          version: '1.0.0'
        use:
          extensions:
            - observe:
                extend: all
                when: '${ $task.name == "target" }'
                before:
                  - before:
                      set:
                        phase: before
                after:
                  - after:
                      set:
                        phase: after
                        extendedTask: '${ $task.name }'
                        ready: '${ .ready }'
        do:
          - target:
              set:
                ready: true
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult result = run(source, JSON.createObjectNode());

    assertEquals(ExecutionPhase.COMPLETED, result.snapshot().phase());
    JsonNode output = result.snapshot().data().inlineValue();
    assertEquals("after", output.required("phase").textValue());
    assertEquals("target", output.required("extendedTask").textValue());
    assertTrue(output.required("ready").booleanValue());
  }

  @Test
  void falseExtensionConditionStillExecutesTheTarget() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extension-condition-false
          version: '1.0.0'
        use:
          extensions:
            - skipped:
                extend: all
                when: '${ false }'
                before:
                  - must-not-run:
                      raise:
                        error:
                          type: https://errors.test/extension-ran
                          status: 500
        do:
          - target:
              set:
                ready: true
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult result = run(source, JSON.createObjectNode());

    assertEquals(ExecutionPhase.COMPLETED, result.snapshot().phase());
    assertTrue(result.snapshot().data().inlineValue().required("ready").booleanValue());
  }

  @Test
  void extensionDefersSwitchFlowUntilAfterMiddlewareCompletes() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: extension-switch-flow
          version: '1.0.0'
        use:
          extensions:
            - observe-switch:
                extend: switch
                after:
                  - observed:
                      set:
                        middlewareCompleted: true
        do:
          - choose:
              switch:
                - selected:
                    when: '${ true }'
                    then: finish
          - wrong:
              raise:
                error:
                  type: https://errors.test/wrong-flow
                  status: 500
          - finish:
              set:
                done: true
                middlewareCompleted:
                  '${ .middlewareCompleted }'
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult result = run(source, JSON.createObjectNode());

    assertEquals(ExecutionPhase.COMPLETED, result.snapshot().phase());
    JsonNode output = result.snapshot().data().inlineValue();
    assertTrue(output.required("done").booleanValue());
    assertTrue(output.required("middlewareCompleted").booleanValue());
  }

  @Test
  void asyncApiSubscriptionOwnsFilteringDeduplicationPauseAndResume() throws Exception {
    byte[] source =
        asyncApiWorkflow(
            """
                      filter: '${ .payload.accepted == true }'
                      consume:
                        amount: 2
            """);
    Harness harness = new Harness(source, asyncApiResources());
    var started =
        harness.apply(
            startCommand(source, asyncApiResources(), JSON.createObjectNode()), "1".repeat(64));
    var waiting =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));
    ActiveAsyncApiSubscriptionState subscription =
        assertInstanceOf(
            ActiveAsyncApiSubscriptionState.class,
            waiting.aggregate().state().pendingInteraction());
    assertEquals(
        WorkflowEffectType.UPSERT_ASYNC_API_SUBSCRIPTION, waiting.outbox().getFirst().type());
    assertEquals(
        USER,
        waiting.outbox().getFirst().actor(),
        "The workflow initiator owns captured subscription data; "
            + "an internal continuation actor must not become its "
            + "database owner");

    var ignored =
        harness.apply(
            asyncApiMessage(subscription, "source:0", false, "ignored", 1),
            fingerprint(waiting.aggregate().revision()));
    assertEquals(
        0,
        ((ActiveAsyncApiSubscriptionState) ignored.aggregate().state().pendingInteraction())
            .messages()
            .size());
    assertEquals(ExecutionEventType.ASYNC_API_MESSAGE_FILTERED, ignored.events().getFirst().type());

    var first =
        harness.apply(
            asyncApiMessage(subscription, "source:1", true, "first", 2),
            fingerprint(ignored.aggregate().revision()));
    long revisionAfterFirst = first.aggregate().revision();
    var duplicate =
        harness.apply(
            asyncApiMessage(subscription, "source:1", true, "first", 3),
            fingerprint(revisionAfterFirst));
    assertEquals(
        1,
        ((ActiveAsyncApiSubscriptionState) duplicate.aggregate().state().pendingInteraction())
            .messages()
            .size());
    assertTrue(duplicate.events().isEmpty());
    assertEquals(WorkflowEffectType.ACK_ASYNC_API_MESSAGE, duplicate.outbox().getFirst().type());

    var paused =
        harness.apply(
            new ControlExecutionCommand(
                "pause-asyncapi", KEY, ExecutionControlAction.PAUSE, USER, NOW.plusSeconds(4)),
            fingerprint(duplicate.aggregate().revision()));
    assertEquals(
        WorkflowEffectType.DELETE_ASYNC_API_SUBSCRIPTION, paused.outbox().getFirst().type());

    var completedWhilePaused =
        harness.apply(
            asyncApiMessage(subscription, "source:2", true, "second", 5),
            fingerprint(paused.aggregate().revision()));
    ActiveAsyncApiSubscriptionState ready =
        assertInstanceOf(
            ActiveAsyncApiSubscriptionState.class,
            completedWhilePaused.aggregate().state().pendingInteraction());
    assertTrue(ready.completionReady());
    assertEquals(ExecutionPhase.PAUSED, completedWhilePaused.aggregate().state().phase());

    var resumed =
        harness.apply(
            new ControlExecutionCommand(
                "resume-asyncapi", KEY, ExecutionControlAction.RESUME, USER, NOW.plusSeconds(6)),
            fingerprint(completedWhilePaused.aggregate().revision()));
    while (!resumed.aggregate().state().phase().terminal()) {
      resumed =
          harness.apply(
              resumed.followUpCommands().getFirst(), fingerprint(resumed.aggregate().revision()));
    }
    assertEquals(ExecutionPhase.COMPLETED, resumed.aggregate().state().phase());
    assertEquals(
        List.of("first", "second"),
        List.of(
            resumed
                .aggregate()
                .state()
                .data()
                .inlineValue()
                .get(0)
                .required("payload")
                .required("value")
                .textValue(),
            resumed
                .aggregate()
                .state()
                .data()
                .inlineValue()
                .get(1)
                .required("payload")
                .required("value")
                .textValue()));
  }

  @Test
  void correlatedWorkerOwnsCommandProgressAndTerminalResult() throws Exception {
    byte[] source = correlatedWorkerWorkflow();
    List<ResolvedWorkflowResource> resources = correlatedWorkerResources();
    Harness harness = new Harness(source, resources);
    var started =
        harness.apply(
            startCommand(
                source,
                resources,
                JSON.readTree(
                    """
                    {"populationId":"population-17"}
                    """)),
            "1".repeat(64));
    var waiting =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));
    ActiveCorrelatedWorkerState worker =
        assertInstanceOf(
            ActiveCorrelatedWorkerState.class, waiting.aggregate().state().pendingInteraction());
    assertEquals(
        List.of(
            WorkflowEffectType.UPSERT_ASYNC_API_SUBSCRIPTION,
            WorkflowEffectType.DISPATCH_OPERATION,
            WorkflowEffectType.SCHEDULE_TIMER),
        waiting.outbox().stream().map(WorkflowEffect::type).toList());
    assertEquals(USER, waiting.outbox().get(0).actor());
    assertEquals(USER, waiting.outbox().get(1).actor());
    assertEquals(
        worker.lifecycleId(),
        worker
            .commandDescriptor()
            .inlineValue()
            .required("arguments")
            .required("message")
            .required("payload")
            .required("operationId")
            .textValue());

    var published =
        harness.apply(
            new ObserveOperationCommand(
                "worker-published",
                KEY,
                worker.lifecycleId(),
                new OperationObservation(
                    OperationObservationStatus.SUCCEEDED,
                    DataReferences.inline(JSON.createObjectNode().put("published", true)),
                    null,
                    null),
                RUNTIME,
                NOW.plusSeconds(1)),
            fingerprint(waiting.aggregate().revision()));
    worker =
        assertInstanceOf(
            ActiveCorrelatedWorkerState.class, published.aggregate().state().pendingInteraction());
    assertTrue(worker.commandPublished());

    var progress =
        harness.apply(
            correlatedWorkerMessage(
                worker,
                "workers.events:0:11",
                "PROGRESS",
                JSON.readTree(
                    """
                    {"stage":"indexing","percent":40}
                    """),
                2),
            fingerprint(published.aggregate().revision()));
    assertEquals(
        ExecutionEventType.CORRELATED_WORKER_PROGRESS, progress.events().getFirst().type());

    worker =
        assertInstanceOf(
            ActiveCorrelatedWorkerState.class, progress.aggregate().state().pendingInteraction());
    var succeeded =
        harness.apply(
            correlatedWorkerMessage(
                worker,
                "workers.events:0:12",
                "SUCCEEDED",
                JSON.readTree(
                    """
                    {"revisionId":"revision-42","indexed":901}
                    """),
                3),
            fingerprint(progress.aggregate().revision()));
    assertTrue(
        succeeded.events().stream()
            .anyMatch(event -> event.type() == ExecutionEventType.CORRELATED_WORKER_COMPLETED));
    while (!succeeded.aggregate().state().phase().terminal()) {
      succeeded =
          harness.apply(
              succeeded.followUpCommands().getFirst(),
              fingerprint(succeeded.aggregate().revision()));
    }
    assertEquals(ExecutionPhase.COMPLETED, succeeded.aggregate().state().phase());
    assertEquals(
        "revision-42",
        succeeded.aggregate().state().data().inlineValue().required("revisionId").textValue());
  }

  @Test
  void correlatedWorkerResolvesAnArtifactBackedAsyncApiMessage() throws Exception {
    byte[] source = correlatedWorkerWorkflow();
    List<ResolvedWorkflowResource> resources = correlatedWorkerResources();
    ExternalizingDataAccess dataAccess = new ExternalizingDataAccess();
    Harness harness = new Harness(source, resources, Duration.ofSeconds(30), false, dataAccess);
    var started =
        harness.apply(startCommand(source, resources, JSON.createObjectNode()), "1".repeat(64));
    var waiting =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));
    ActiveCorrelatedWorkerState worker =
        assertInstanceOf(
            ActiveCorrelatedWorkerState.class, waiting.aggregate().state().pendingInteraction());
    ReceiveAsyncApiMessageCommand inline =
        correlatedWorkerMessage(
            worker,
            "workers.events:0:42",
            "SUCCEEDED",
            JSON.createObjectNode().put("revisionId", "revision-42"),
            2);
    ReceiveAsyncApiMessageCommand artifactBacked =
        new ReceiveAsyncApiMessageCommand(
            inline.commandId(),
            inline.key(),
            inline.subscriptionId(),
            inline.sourcePosition(),
            dataAccess.reference(inline.message().inlineValue()),
            inline.actor(),
            inline.requestedAt());

    var succeeded = harness.apply(artifactBacked, fingerprint(waiting.aggregate().revision()));
    while (!succeeded.aggregate().state().phase().terminal()) {
      succeeded =
          harness.apply(
              succeeded.followUpCommands().getFirst(),
              fingerprint(succeeded.aggregate().revision()));
    }

    assertEquals(ExecutionPhase.COMPLETED, succeeded.aggregate().state().phase());
    assertEquals(
        "revision-42",
        dataAccess
            .resolve(succeeded.aggregate().state().data())
            .required("revisionId")
            .textValue());
  }

  @Test
  void correlatedWorkerCancellationWaitsForRemoteTerminalSignal() throws Exception {
    byte[] source = correlatedWorkerWorkflow();
    List<ResolvedWorkflowResource> resources = correlatedWorkerResources();
    Harness harness = new Harness(source, resources);
    var started =
        harness.apply(startCommand(source, resources, JSON.createObjectNode()), "1".repeat(64));
    var waiting =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));
    ActiveCorrelatedWorkerState worker =
        assertInstanceOf(
            ActiveCorrelatedWorkerState.class, waiting.aggregate().state().pendingInteraction());

    var requested =
        harness.apply(
            new ControlExecutionCommand(
                "cancel-worker", KEY, ExecutionControlAction.CANCEL, USER, NOW.plusSeconds(5)),
            fingerprint(waiting.aggregate().revision()));
    assertEquals(ExecutionPhase.CANCEL_REQUESTED, requested.aggregate().state().phase());
    assertEquals(
        USER,
        requested.aggregate().state().cancellation().requestedBy(),
        "Durable cancellation state must retain the initiating actor");
    assertTrue(
        requested.outbox().stream()
            .anyMatch(
                effect ->
                    effect.type() == WorkflowEffectType.DISPATCH_OPERATION
                        && effect
                            .payload()
                            .inlineValue()
                            .required("operationId")
                            .textValue()
                            .endsWith(":cancel")));

    worker =
        assertInstanceOf(
            ActiveCorrelatedWorkerState.class, requested.aggregate().state().pendingInteraction());
    var cancelled =
        harness.apply(
            correlatedWorkerMessage(
                worker, "workers.events:0:19", "CANCELLED", JSON.createObjectNode(), 6),
            fingerprint(requested.aggregate().revision()));
    assertEquals(ExecutionPhase.CANCELLED, cancelled.aggregate().state().phase());
    assertNull(cancelled.aggregate().state().pendingInteraction());
    assertEquals(
        USER,
        cancelled.events().getLast().actor(),
        "The worker acknowledgement must not replace the cancellation initiator in audit history");
  }

  @Test
  void correlatedWorkerTimeoutFailsDurablyAndRequestsRemoteCancellation() throws Exception {
    byte[] source = correlatedWorkerWorkflow();
    List<ResolvedWorkflowResource> resources = correlatedWorkerResources();
    Harness harness = new Harness(source, resources);
    var started =
        harness.apply(startCommand(source, resources, JSON.createObjectNode()), "1".repeat(64));
    var waiting =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));
    ActiveCorrelatedWorkerState worker =
        assertInstanceOf(
            ActiveCorrelatedWorkerState.class, waiting.aggregate().state().pendingInteraction());

    var timedOut =
        harness.apply(
            new FireTimerCommand(
                "fire-worker-deadline",
                KEY,
                worker.deadlineTimerId(),
                RUNTIME,
                worker.deadlineAt()),
            fingerprint(waiting.aggregate().revision()));
    assertEquals(ExecutionPhase.FAILED, timedOut.aggregate().state().phase());
    assertTrue(
        timedOut.events().stream()
            .anyMatch(
                event -> event.type() == ExecutionEventType.CORRELATED_WORKER_OUTCOME_UNKNOWN));
    assertTrue(
        timedOut.outbox().stream()
            .anyMatch(
                effect ->
                    effect.type() == WorkflowEffectType.DISPATCH_OPERATION
                        && effect
                            .payload()
                            .inlineValue()
                            .required("operationId")
                            .textValue()
                            .endsWith(":cancel")));
  }

  @Test
  void asyncApiUntilAndWhileUseTheNormativeEvaluationOrder() throws Exception {
    byte[] untilSource =
        asyncApiWorkflow(
            """
                      consume:
                        until: '${ .payload.stop == true }'
            """);
    Harness untilHarness = new Harness(untilSource, asyncApiResources());
    var untilStarted =
        untilHarness.apply(
            startCommand(untilSource, asyncApiResources(), JSON.createObjectNode()),
            "1".repeat(64));
    var untilWaiting =
        untilHarness.apply(
            untilStarted.followUpCommands().getFirst(),
            fingerprint(untilStarted.aggregate().revision()));
    ActiveAsyncApiSubscriptionState untilSubscription =
        (ActiveAsyncApiSubscriptionState) untilWaiting.aggregate().state().pendingInteraction();
    var accepted =
        untilHarness.apply(
            asyncApiMessage(untilSubscription, "until:0", true, "kept", 1, false),
            fingerprint(untilWaiting.aggregate().revision()));
    var untilCompleted =
        untilHarness.apply(
            asyncApiMessage(untilSubscription, "until:1", true, "sentinel", 2, true),
            fingerprint(accepted.aggregate().revision()));
    while (!untilCompleted.aggregate().state().phase().terminal()) {
      untilCompleted =
          untilHarness.apply(
              untilCompleted.followUpCommands().getFirst(),
              fingerprint(untilCompleted.aggregate().revision()));
    }
    assertEquals(
        1,
        untilCompleted.aggregate().state().data().inlineValue().size(),
        "UNTIL is evaluated before and excludes the sentinel message");

    byte[] whileSource =
        asyncApiWorkflow(
            """
                      consume:
                        while: '${ .payload.keepGoing == true }'
            """);
    Harness whileHarness = new Harness(whileSource, asyncApiResources());
    var whileStarted =
        whileHarness.apply(
            startCommand(whileSource, asyncApiResources(), JSON.createObjectNode()),
            "1".repeat(64));
    var whileWaiting =
        whileHarness.apply(
            whileStarted.followUpCommands().getFirst(),
            fingerprint(whileStarted.aggregate().revision()));
    ActiveAsyncApiSubscriptionState whileSubscription =
        (ActiveAsyncApiSubscriptionState) whileWaiting.aggregate().state().pendingInteraction();
    var whileAccepted =
        whileHarness.apply(
            asyncApiMessage(whileSubscription, "while:0", true, "first", 1, true),
            fingerprint(whileWaiting.aggregate().revision()));
    var whileCompleted =
        whileHarness.apply(
            asyncApiMessage(whileSubscription, "while:1", true, "last", 2, false),
            fingerprint(whileAccepted.aggregate().revision()));
    while (!whileCompleted.aggregate().state().phase().terminal()) {
      whileCompleted =
          whileHarness.apply(
              whileCompleted.followUpCommands().getFirst(),
              fingerprint(whileCompleted.aggregate().revision()));
    }
    assertEquals(
        2,
        whileCompleted.aggregate().state().data().inlineValue().size(),
        "WHILE is evaluated after and includes the terminating message");
  }

  @Test
  void runUsesTheExternalOperationBoundaryAndHonoursAwait() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: durable-run
          version: '1.0.0'
        do:
          - execute:
              run:
                await: true
                return: all
                shell:
                  command: printf
                  arguments:
                    - '${ .instruction }'
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var started =
        harness.apply(
            startCommand(source, JSON.readTree("{\"instruction\":\"extract persons\"}")),
            "1".repeat(64));
    var dispatched =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));
    ActiveOperationState operation =
        assertInstanceOf(
            ActiveOperationState.class, dispatched.aggregate().state().pendingInteraction());
    JsonNode descriptor = operation.descriptor().inlineValue();
    assertEquals("run", descriptor.required("operationKind").textValue());
    assertEquals("SHELL", descriptor.required("runKind").textValue());
    assertEquals("ALL", descriptor.required("return").textValue());
    assertEquals(
        "extract persons",
        descriptor.required("configuration").required("arguments").get(0).textValue());

    var completed =
        harness.apply(
            new ObserveOperationCommand(
                "run-success",
                KEY,
                operation.operationId(),
                new OperationObservation(
                    OperationObservationStatus.SUCCEEDED,
                    DataReferences.inline(
                        JSON.readTree(
                            """
                            {
                              "stdout": "extract persons",
                              "stderr": "",
                              "code": 0
                            }
                            """)),
                    null,
                    null),
                RUNTIME,
                NOW.plusSeconds(1)),
            fingerprint(dispatched.aggregate().revision()));
    while (!completed.aggregate().state().phase().terminal()) {
      completed =
          harness.apply(
              completed.followUpCommands().getFirst(),
              fingerprint(completed.aggregate().revision()));
    }
    assertEquals(0, completed.aggregate().state().data().inlineValue().required("code").intValue());
  }

  @Test
  void opaqueArtifactOperationOutputCompletesWithoutStreamsThreadIo() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: artifact-output
          version: '1.0.0'
        do:
          - execute:
              run:
                shell:
                  command: printf
                  arguments: [ready]
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var started = harness.apply(startCommand(source, JSON.createObjectNode()), "1".repeat(64));
    var dispatched =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));
    ActiveOperationState operation =
        assertInstanceOf(
            ActiveOperationState.class, dispatched.aggregate().state().pendingInteraction());
    var artifact =
        new DataReference(
            DataReference.Storage.ARTIFACT,
            null,
            URI.create("urn:oks:workflow-data:" + "10000000-0000-0000-0000-000000000001"),
            "application/json",
            300 * 1024,
            "a".repeat(64));

    var completed =
        harness.apply(
            new ObserveOperationCommand(
                "artifact-success",
                KEY,
                operation.operationId(),
                new OperationObservation(
                    OperationObservationStatus.SUCCEEDED, artifact, null, null),
                RUNTIME,
                NOW.plusSeconds(1)),
            fingerprint(dispatched.aggregate().revision()));
    while (!completed.aggregate().state().phase().terminal()) {
      completed =
          harness.apply(
              completed.followUpCommands().getFirst(),
              fingerprint(completed.aggregate().revision()));
    }

    assertEquals(DataReference.Storage.ARTIFACT, completed.aggregate().state().data().storage());
    assertEquals(artifact.artifactUri(), completed.aggregate().state().data().artifactUri());
  }

  @Test
  void artifactOutputCanFlowIntoAStaticHumanTaskWithoutMaterialization() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: artifact-human-review
          version: '1.0.0'
        do:
          - extract:
              run:
                shell:
                  command: printf
                  arguments: [ready]
          - review:
              call: com.forwardmeasure.openworkflow.human-task
              with:
                title: Review extracted evidence
                presentation:
                  kind: RAW_JSON
                approvals:
                  stages:
                    - level: 1
                      name: Evidence Review
                      requiredApprovals: 1
                      candidateRoles: [evidence-reviewer]
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var started = harness.apply(startCommand(source, JSON.createObjectNode()), "1".repeat(64));
    var dispatched =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));
    ActiveOperationState operation =
        assertInstanceOf(
            ActiveOperationState.class, dispatched.aggregate().state().pendingInteraction());
    var artifact =
        new DataReference(
            DataReference.Storage.ARTIFACT,
            null,
            URI.create("urn:oks:workflow-data:" + "10000000-0000-0000-0000-000000000002"),
            "application/json",
            300 * 1024,
            "b".repeat(64));

    var progressed =
        harness.apply(
            new ObserveOperationCommand(
                "artifact-human-success",
                KEY,
                operation.operationId(),
                new OperationObservation(
                    OperationObservationStatus.SUCCEEDED, artifact, null, null),
                RUNTIME,
                NOW.plusSeconds(1)),
            fingerprint(dispatched.aggregate().revision()));
    while (!(progressed.aggregate().state().pendingInteraction() instanceof ActiveHumanTaskState)) {
      progressed =
          harness.apply(
              progressed.followUpCommands().getFirst(),
              fingerprint(progressed.aggregate().revision()));
    }

    ActiveHumanTaskState humanTask =
        assertInstanceOf(
            ActiveHumanTaskState.class, progressed.aggregate().state().pendingInteraction());
    JsonNode descriptor = humanTask.descriptor().inlineValue();
    assertEquals(artifact, DataReferenceJson.decode(descriptor.required("inputReference")));
    assertEquals(WorkflowEffectType.CREATE_HUMAN_TASK, progressed.outbox().getFirst().type());
  }

  @Test
  void operationCompletionRacingPauseIsBufferedAndConsumedOnce() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: paused-operation
          version: '1.0.0'
        do:
          - execute:
              run:
                shell:
                  command: printf
                  arguments: [ready]
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var started = harness.apply(startCommand(source, JSON.createObjectNode()), "1".repeat(64));
    var dispatched =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));
    ActiveOperationState operation =
        assertInstanceOf(
            ActiveOperationState.class, dispatched.aggregate().state().pendingInteraction());

    var paused =
        harness.apply(
            new ControlExecutionCommand(
                "pause-operation", KEY, ExecutionControlAction.PAUSE, USER, NOW.plusSeconds(1)),
            fingerprint(dispatched.aggregate().revision()));
    assertTrue(
        paused.outbox().stream()
            .noneMatch(effect -> effect.type() == WorkflowEffectType.CANCEL_OPERATION));

    OperationObservation success =
        new OperationObservation(
            OperationObservationStatus.SUCCEEDED,
            DataReferences.inline(JSON.getNodeFactory().textNode("ready")),
            null,
            null);
    var buffered =
        harness.apply(
            new ObserveOperationCommand(
                "late-operation-result",
                KEY,
                operation.operationId(),
                success,
                RUNTIME,
                NOW.plusSeconds(2)),
            fingerprint(paused.aggregate().revision()));
    assertEquals(ExecutionPhase.PAUSED, buffered.aggregate().state().phase());
    ActiveOperationState ready =
        assertInstanceOf(
            ActiveOperationState.class, buffered.aggregate().state().pendingInteraction());
    assertTrue(ready.completionReady());
    assertEquals(ExecutionEventType.OPERATION_RESULT_BUFFERED, buffered.events().getFirst().type());

    var resumed =
        harness.apply(
            new ControlExecutionCommand(
                "resume-operation", KEY, ExecutionControlAction.RESUME, USER, NOW.plusSeconds(3)),
            fingerprint(buffered.aggregate().revision()));
    assertTrue(
        resumed.outbox().stream()
            .noneMatch(effect -> effect.type() == WorkflowEffectType.DISPATCH_OPERATION));
    while (!resumed.aggregate().state().phase().terminal()) {
      resumed =
          harness.apply(
              resumed.followUpCommands().getFirst(), fingerprint(resumed.aggregate().revision()));
    }
    assertEquals(ExecutionPhase.COMPLETED, resumed.aggregate().state().phase());
    assertEquals("ready", resumed.aggregate().state().data().inlineValue().textValue());
  }

  @Test
  void operationCompletionRacingPauseInsideForkIsNotRedispatched() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: paused-fork-operation
          version: '1.0.0'
        do:
          - parallel:
              fork:
                branches:
                  - external:
                      run:
                        shell:
                          command: printf
                          arguments: [done]
                  - timer:
                      wait: PT1H
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var decision = harness.apply(startCommand(source, JSON.createObjectNode()), "1".repeat(64));
    while (!decision.followUpCommands().isEmpty()) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
    }
    ActiveOperationState operation = findOperation(decision.aggregate().state().activeFork());
    assertNotNull(operation);

    var paused =
        harness.apply(
            new ControlExecutionCommand(
                "pause-fork-operation",
                KEY,
                ExecutionControlAction.PAUSE,
                USER,
                NOW.plusSeconds(1)),
            fingerprint(decision.aggregate().revision()));
    var buffered =
        harness.apply(
            new ObserveOperationCommand(
                "late-fork-operation-result",
                KEY,
                operation.operationId(),
                new OperationObservation(
                    OperationObservationStatus.SUCCEEDED,
                    DataReferences.inline(JSON.getNodeFactory().textNode("done")),
                    null,
                    null),
                RUNTIME,
                NOW.plusSeconds(2)),
            fingerprint(paused.aggregate().revision()));
    assertEquals(ExecutionPhase.PAUSED, buffered.aggregate().state().phase());
    assertTrue(findOperation(buffered.aggregate().state().activeFork()).completionReady());

    var resumed =
        harness.apply(
            new ControlExecutionCommand(
                "resume-fork-operation",
                KEY,
                ExecutionControlAction.RESUME,
                USER,
                NOW.plusSeconds(3)),
            fingerprint(buffered.aggregate().revision()));
    assertTrue(
        resumed.outbox().stream()
            .noneMatch(effect -> effect.type() == WorkflowEffectType.DISPATCH_OPERATION));
    var finalized =
        harness.apply(
            resumed.followUpCommands().getFirst(), fingerprint(resumed.aggregate().revision()));
    assertTrue(
        finalized.outbox().stream()
            .noneMatch(effect -> effect.type() == WorkflowEffectType.DISPATCH_OPERATION));
    assertNull(findOperation(finalized.aggregate().state().activeFork()));
    assertEquals(ExecutionPhase.RUNNING, finalized.aggregate().state().phase());
  }

  @Test
  void nonAwaitedRunDispatchesOnceAndContinuesImmediately() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: detached-run
          version: '1.0.0'
        do:
          - execute:
              run:
                await: false
                return: none
                shell:
                  command: notify
          - continued:
              set:
                continued: true
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var decision = harness.apply(startCommand(source, JSON.createObjectNode()), "1".repeat(64));
    List<WorkflowEffect> effects = new ArrayList<>();
    while (!decision.aggregate().state().phase().terminal()) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
      effects.addAll(decision.outbox());
    }
    assertEquals(ExecutionPhase.COMPLETED, decision.aggregate().state().phase());
    assertTrue(
        decision.aggregate().state().data().inlineValue().required("continued").booleanValue());
    assertEquals(
        1,
        effects.stream()
            .filter(effect -> effect.type() == WorkflowEffectType.DISPATCH_OPERATION)
            .count());
  }

  @Test
  void asyncApiDeadlineAndForeachAreDurableRuntimeSemantics() throws Exception {
    byte[] deadlineSource =
        asyncApiWorkflow(
            """
                      consume:
                        amount: 10
                        for: PT5S
            """);
    Harness deadlineHarness = new Harness(deadlineSource, asyncApiResources());
    var deadlineStarted =
        deadlineHarness.apply(
            startCommand(deadlineSource, asyncApiResources(), JSON.createObjectNode()),
            "1".repeat(64));
    var deadlineWaiting =
        deadlineHarness.apply(
            deadlineStarted.followUpCommands().getFirst(),
            fingerprint(deadlineStarted.aggregate().revision()));
    ActiveAsyncApiSubscriptionState deadlineSubscription =
        (ActiveAsyncApiSubscriptionState) deadlineWaiting.aggregate().state().pendingInteraction();
    assertNotNull(deadlineSubscription.deadlineTimerId());
    assertEquals(
        2, deadlineWaiting.outbox().size(), "Subscription and deadline are committed together");
    var oneMessage =
        deadlineHarness.apply(
            asyncApiMessage(deadlineSubscription, "deadline:0", true, "one", 1),
            fingerprint(deadlineWaiting.aggregate().revision()));
    var deadlineCompleted =
        deadlineHarness.apply(
            new FireTimerCommand(
                "fire-asyncapi-deadline",
                KEY,
                deadlineSubscription.deadlineTimerId(),
                RUNTIME,
                NOW.plusSeconds(5)),
            fingerprint(oneMessage.aggregate().revision()));
    while (!deadlineCompleted.aggregate().state().phase().terminal()) {
      deadlineCompleted =
          deadlineHarness.apply(
              deadlineCompleted.followUpCommands().getFirst(),
              fingerprint(deadlineCompleted.aggregate().revision()));
    }
    assertEquals(1, deadlineCompleted.aggregate().state().data().inlineValue().size());

    byte[] foreachSource =
        asyncApiWorkflow(
            """
                      consume:
                        amount: 2
                      foreach:
                        item: message
                        at: position
                        do:
                          - project:
                              set:
                                value: '${ $message.payload.value }'
                                position: '${ $position }'
            """);
    Harness foreachHarness = new Harness(foreachSource, asyncApiResources());
    var foreachStarted =
        foreachHarness.apply(
            startCommand(foreachSource, asyncApiResources(), JSON.createObjectNode()),
            "1".repeat(64));
    var foreachWaiting =
        foreachHarness.apply(
            foreachStarted.followUpCommands().getFirst(),
            fingerprint(foreachStarted.aggregate().revision()));
    ActiveAsyncApiSubscriptionState foreachSubscription =
        (ActiveAsyncApiSubscriptionState) foreachWaiting.aggregate().state().pendingInteraction();
    var foreachFirst =
        foreachHarness.apply(
            asyncApiMessage(foreachSubscription, "foreach:0", true, "first", 1),
            fingerprint(foreachWaiting.aggregate().revision()));
    var foreachDecision =
        foreachHarness.apply(
            asyncApiMessage(foreachSubscription, "foreach:1", true, "second", 2),
            fingerprint(foreachFirst.aggregate().revision()));
    while (!foreachDecision.aggregate().state().phase().terminal()) {
      foreachDecision =
          foreachHarness.apply(
              foreachDecision.followUpCommands().getFirst(),
              fingerprint(foreachDecision.aggregate().revision()));
    }
    JsonNode foreachOutput = foreachDecision.aggregate().state().data().inlineValue();
    assertEquals(2, foreachOutput.size());
    assertEquals("first", foreachOutput.get(0).required("value").textValue());
    assertEquals(0, foreachOutput.get(0).required("position").intValue());
    assertEquals("second", foreachOutput.get(1).required("value").textValue());
    assertEquals(1, foreachOutput.get(1).required("position").intValue());
  }

  @Test
  void asyncApiMessageRacingPauseInsideForkCompletesAfterResume() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: forked-asyncapi
          version: '1.0.0'
        do:
          - parallel:
              fork:
                branches:
                  - receive:
                      call: asyncapi
                      with:
                        document:
                          endpoint:
                            uri: https://contracts.test/evidence-asyncapi.yaml
                        channel: evidence.messages
                        subscription:
                          consume:
                            amount: 1
                  - delay:
                      wait: PT1S
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source, asyncApiResources());
    var decision =
        harness.apply(
            startCommand(source, asyncApiResources(), JSON.createObjectNode()), "1".repeat(64));
    while (!decision.followUpCommands().isEmpty()) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
    }
    ActiveAsyncApiSubscriptionState subscription =
        findAsyncApiSubscription(decision.aggregate().state().activeFork());
    ActiveTimerState timer = findTimer(decision.aggregate().state().activeFork());
    assertNotNull(subscription);
    assertNotNull(timer);

    var paused =
        harness.apply(
            new ControlExecutionCommand(
                "pause-fork-asyncapi", KEY, ExecutionControlAction.PAUSE, USER, NOW.plusSeconds(1)),
            fingerprint(decision.aggregate().revision()));
    var raced =
        harness.apply(
            asyncApiMessage(subscription, "fork:0", true, "accepted-while-paused", 2),
            fingerprint(paused.aggregate().revision()));
    assertEquals(ExecutionPhase.PAUSED, raced.aggregate().state().phase());
    assertTrue(findAsyncApiSubscription(raced.aggregate().state().activeFork()).completionReady());

    var resumed =
        harness.apply(
            new ControlExecutionCommand(
                "resume-fork-asyncapi",
                KEY,
                ExecutionControlAction.RESUME,
                USER,
                NOW.plusSeconds(3)),
            fingerprint(raced.aggregate().revision()));
    while (!resumed.followUpCommands().isEmpty()) {
      resumed =
          harness.apply(
              resumed.followUpCommands().getFirst(), fingerprint(resumed.aggregate().revision()));
    }
    assertNull(findAsyncApiSubscription(resumed.aggregate().state().activeFork()));

    var fired =
        harness.apply(
            new FireTimerCommand(
                "fire-fork-wait", KEY, timer.timerId(), RUNTIME, NOW.plusSeconds(4)),
            fingerprint(resumed.aggregate().revision()));
    while (!fired.aggregate().state().phase().terminal()) {
      fired =
          harness.apply(
              fired.followUpCommands().getFirst(), fingerprint(fired.aggregate().revision()));
    }
    assertEquals(ExecutionPhase.COMPLETED, fired.aggregate().state().phase());
  }

  @Test
  void inlineFunctionRunsAsDurableNestedTaskWithoutExternalDispatch() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: inline-function
          version: '1.0.0'
        use:
          functions:
            enrich:
              input:
                schema:
                  document:
                    type: object
                    required: [instruction]
              set:
                extracted: '${ .instruction }'
                source: inline-function
        do:
          - invoke:
              call: enrich
              with:
                instruction: '${ .request }'
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult result =
        run(
            source,
            JSON.readTree(
                """
                {"request":"extract persons"}
                """));

    assertEquals(ExecutionPhase.COMPLETED, result.snapshot().phase());
    assertEquals(
        "extract persons",
        result.snapshot().data().inlineValue().required("extracted").textValue());
    assertEquals(
        "inline-function", result.snapshot().data().inlineValue().required("source").textValue());
    assertTrue(
        result.history().stream()
            .noneMatch(event -> event.type() == ExecutionEventType.OPERATION_DISPATCHED));
    assertTrue(
        result.history().stream()
            .anyMatch(
                event -> event.taskPath() != null && event.taskPath().contains("/function/")));
  }

  @Test
  void cataloguedFunctionRunsItsPinnedDefinitionAndTransformsItsOutput() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: catalogued-function-runtime
          version: '1.0.0'
        use:
          catalogs:
            evidence:
              endpoint:
                uri: https://catalog.example.test/
        do:
          - normalize:
              call: normalize:1.2.3@evidence
              with:
                value: '${ .name }'
        """
            .getBytes(StandardCharsets.UTF_8);
    URI functionUri =
        URI.create("https://catalog.example.test/functions/" + "normalize/1.2.3/function.yaml");
    List<ResolvedWorkflowResource> resources =
        List.of(
            ResolvedWorkflowResource.of(
                functionUri,
                "application/yaml",
                """
                input:
                  schema:
                    document:
                      type: object
                      required: [value]
                run:
                  await: true
                  return: all
                  shell:
                    command: normalize
                    arguments:
                      - '${ .value }'
                output:
                  as: '${ .stdout }'
                """));
    Harness harness = new Harness(source, resources);
    var decision =
        harness.apply(
            startCommand(source, resources, JSON.readTree("{\"name\":\"Prashanth\"}")),
            "1".repeat(64));
    while (!(decision.aggregate().state().pendingInteraction() instanceof ActiveOperationState)) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
    }

    ActiveOperationState operation =
        assertInstanceOf(
            ActiveOperationState.class, decision.aggregate().state().pendingInteraction());
    JsonNode descriptor = operation.descriptor().inlineValue();
    assertEquals("run", operation.operationKind());
    assertEquals("SHELL", descriptor.required("runKind").textValue());
    assertEquals(
        "Prashanth", descriptor.required("configuration").required("arguments").get(0).textValue());

    decision =
        harness.apply(
            new ObserveOperationCommand(
                "catalogued-function-success",
                KEY,
                operation.operationId(),
                new OperationObservation(
                    OperationObservationStatus.SUCCEEDED,
                    DataReferences.inline(
                        JSON.readTree(
                            """
                            {
                              "stdout": "PRASHANTH",
                              "stderr": "",
                              "code": 0
                            }
                            """)),
                    null,
                    null),
                RUNTIME,
                NOW.plusSeconds(1)),
            fingerprint(decision.aggregate().revision()));
    while (!decision.aggregate().state().phase().terminal()) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
    }

    assertEquals(ExecutionPhase.COMPLETED, decision.aggregate().state().phase());
    assertEquals("PRASHANTH", decision.aggregate().state().data().inlineValue().textValue());
  }

  @Test
  void failedCallRoutesItsStructuredErrorThroughTryCatch() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: caught-call
          version: '1.0.0'
        do:
          - guarded:
              try:
                - invoke:
                    call: http
                    with:
                      method: POST
                      endpoint: https://extractor.test/evidence
                      body:
                        evidenceId: '${ .evidenceId }'
              catch:
                errors:
                  with:
                    status: 503
                as: problem
                do:
                  - recover:
                      set:
                        recovered: '${ $problem.detail }'
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var decision =
        harness.apply(
            startCommand(source, JSON.readTree("{\"evidenceId\":\"e-17\"}")), "1".repeat(64));
    while (!(decision.aggregate().state().pendingInteraction() instanceof ActiveOperationState)) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
    }
    ActiveOperationState operation =
        (ActiveOperationState) decision.aggregate().state().pendingInteraction();
    decision =
        harness.apply(
            new ObserveOperationCommand(
                "operation-failure-1",
                KEY,
                operation.operationId(),
                new OperationObservation(
                    OperationObservationStatus.FAILED,
                    null,
                    new WorkflowError(
                        "https://example.com/errors/unavailable",
                        503,
                        "/extractor",
                        "Extractor unavailable",
                        "No worker accepted the operation"),
                    null),
                RUNTIME,
                NOW.plusSeconds(1)),
            fingerprint(decision.aggregate().revision()));
    while (!decision.aggregate().state().phase().terminal()) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
    }

    assertEquals(ExecutionPhase.COMPLETED, decision.aggregate().state().phase());
    assertEquals(
        "No worker accepted the operation",
        decision.aggregate().state().data().inlineValue().required("recovered").textValue());
  }

  @Test
  void pauseInvalidatesQueuedContinuationAndResumeUsesCursor() throws Exception {
    Harness harness = new Harness();
    var started = harness.apply(startCommand(), "1".repeat(64));
    ExecutionCommand queuedContinuation = started.followUpCommands().getFirst();
    var pause =
        new ControlExecutionCommand(
            "pause-1", KEY, ExecutionControlAction.PAUSE, USER, NOW.plusSeconds(1));

    var paused = harness.apply(pause, "2".repeat(64));
    assertEquals(ExecutionPhase.PAUSED, paused.aggregate().state().phase());
    assertEquals(2, paused.aggregate().revision());
    assertEquals(0, paused.aggregate().state().cursor().current().nextChildIndex());

    var stale = harness.apply(queuedContinuation, "3".repeat(64));
    assertEquals(DurableDisposition.STALE, stale.disposition());
    assertFalse(stale.stateChanged());
    assertEquals(2, stale.aggregate().revision());

    var resume =
        new ControlExecutionCommand(
            "resume-1", KEY, ExecutionControlAction.RESUME, USER, NOW.plusSeconds(2));
    var resumed = harness.apply(resume, "4".repeat(64));

    assertEquals(ExecutionPhase.RUNNING, resumed.aggregate().state().phase());
    assertEquals(3, resumed.aggregate().revision());
    assertEquals(1, resumed.followUpCommands().size());
    assertEquals(0, resumed.aggregate().state().cursor().current().nextChildIndex());
  }

  @Test
  void cancellationIsTerminalAtCurrentCutpoint() throws Exception {
    Harness harness = new Harness();
    harness.apply(startCommand(), "1".repeat(64));
    var cancel =
        new ControlExecutionCommand(
            "cancel-1", KEY, ExecutionControlAction.CANCEL, USER, NOW.plusSeconds(1));

    var cancelled = harness.apply(cancel, "2".repeat(64));

    assertEquals(ExecutionPhase.CANCELLED, cancelled.aggregate().state().phase());
    assertTrue(cancelled.aggregate().state().cursor().complete());
    assertTrue(cancelled.followUpCommands().isEmpty());
    assertEquals(ExecutionEventType.EXECUTION_CANCELLED, cancelled.events().getFirst().type());
    assertEquals(USER, cancelled.events().getFirst().actor());
  }

  @Test
  void externalOperationCancellationWaitsForAcknowledgement() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: graceful-cancellation
          version: '1.0.0'
        do:
          - extract:
              call: http
              with:
                method: POST
                endpoint: https://extractor.test/v1/extract
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source, List.of(), Duration.ofSeconds(5));
    var started = harness.apply(startCommand(source, JSON.createObjectNode()), "1".repeat(64));
    var dispatched =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));
    ActiveOperationState operation =
        assertInstanceOf(
            ActiveOperationState.class, dispatched.aggregate().state().pendingInteraction());

    var requested =
        harness.apply(
            new ControlExecutionCommand(
                "cancel-external", KEY, ExecutionControlAction.CANCEL, USER, NOW.plusSeconds(1)),
            fingerprint(dispatched.aggregate().revision()));

    assertEquals(ExecutionPhase.CANCEL_REQUESTED, requested.aggregate().state().phase());
    assertEquals(NOW.plusSeconds(6), requested.aggregate().state().cancellation().dueAt());
    assertTrue(
        requested.events().stream()
            .anyMatch(
                event -> event.type() == ExecutionEventType.OPERATION_CANCELLATION_REQUESTED));
    assertTrue(
        requested.events().stream()
            .anyMatch(event -> event.type() == ExecutionEventType.EXECUTION_CANCEL_REQUESTED));
    assertTrue(
        requested.outbox().stream()
            .anyMatch(effect -> effect.type() == WorkflowEffectType.CANCEL_OPERATION));
    assertTrue(
        requested.outbox().stream()
            .anyMatch(
                effect ->
                    effect.type() == WorkflowEffectType.SCHEDULE_TIMER
                        && "cancellation-deadline"
                            .equals(
                                effect.payload().inlineValue().required("purpose").textValue())));

    var acknowledged =
        harness.apply(
            new ObserveOperationCommand(
                "cancel-acknowledged",
                KEY,
                operation.operationId(),
                new OperationObservation(
                    OperationObservationStatus.CANCELLED,
                    null,
                    new WorkflowError(
                        "https://open-workflow-specification.org/spec/1.0.0/errors/runtime",
                        499,
                        operation.operationId(),
                        "Operation cancelled",
                        "Adapter acknowledged cancellation"),
                    null),
                RUNTIME,
                NOW.plusSeconds(2)),
            fingerprint(requested.aggregate().revision()));

    assertEquals(ExecutionPhase.CANCELLED, acknowledged.aggregate().state().phase());
    assertTrue(
        acknowledged.events().stream()
            .anyMatch(event -> event.type() == ExecutionEventType.OPERATION_CANCELLED));
    assertEquals(ExecutionEventType.EXECUTION_CANCELLED, acknowledged.events().getLast().type());
    assertEquals(
        USER,
        acknowledged.events().getLast().actor(),
        "Terminal cancellation must be attributed to the requesting actor, not the adapter"
            + " acknowledgement actor");
    assertTrue(
        acknowledged.outbox().stream()
            .anyMatch(effect -> effect.type() == WorkflowEffectType.CANCEL_TIMER));
  }

  @Test
  void cancellationDeadlineAuditsUnknownExternalOutcome() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: cancellation-deadline
          version: '1.0.0'
        do:
          - extract:
              call: http
              with:
                method: POST
                endpoint: https://extractor.test/v1/extract
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source, List.of(), Duration.ofSeconds(5));
    var started = harness.apply(startCommand(source, JSON.createObjectNode()), "1".repeat(64));
    var dispatched =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));
    var requested =
        harness.apply(
            new ControlExecutionCommand(
                "cancel-timeout", KEY, ExecutionControlAction.CANCEL, USER, NOW.plusSeconds(1)),
            fingerprint(dispatched.aggregate().revision()));
    CancellationState cancellation = requested.aggregate().state().cancellation();

    var expired =
        harness.apply(
            new FireTimerCommand(
                "fire-cancellation-deadline",
                KEY,
                cancellation.timerId(),
                RUNTIME,
                cancellation.dueAt()),
            fingerprint(requested.aggregate().revision()));

    assertEquals(ExecutionPhase.CANCELLED, expired.aggregate().state().phase());
    assertTrue(
        expired.events().stream()
            .anyMatch(event -> event.type() == ExecutionEventType.OPERATION_OUTCOME_UNKNOWN));
    assertEquals(ExecutionEventType.EXECUTION_CANCELLED, expired.events().getLast().type());
    assertEquals(
        USER,
        expired.events().getLast().actor(),
        "Deadline cancellation must retain the authenticated requester rather than the timer"
            + " actor");
    assertEquals(
        "https://auth.example.com/realms/forwardmeasure",
        expired.events().getLast().actor().identityProvider());
    assertEquals(
        "2ab3aea3-0972-4eac-8a9d-bcd4a5f0cc45",
        expired.events().getLast().actor().subjectIdentifier());
  }

  @Test
  void appliesDataFlowSkipsTasksAndHonoursEnd() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: data-flow
          version: '1.0.0'
        input:
          from: '${ {instruction: .instruction, enabled: .enabled} }'
        do:
          - skipped:
              if: '${ .enabled | not }'
              set:
                mustNotRun: '${ error("skipped task executed") }'
          - transform:
              input:
                from: '${ .instruction }'
              set:
                copied: '${ . }'
              output:
                as:
                  result: '${ .copied }'
              export:
                as: '${ $context + {last: .result} }'
              then: end
          - unreachable:
              set:
                mustNotRun: '${ error("end directive ignored") }'
        output:
          as: '${ {result: .result, contextValue: $context.last} }'
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var decision =
        harness.apply(
            startCommand(
                source,
                JSON.readTree(
                    "{\"instruction\":\"Extract entities\","
                        + "\"enabled\":true,"
                        + "\"discard\":\"value\"}")),
            "1".repeat(64));
    List<ExecutionHistoryEvent> history = new ArrayList<>();
    history.addAll(decision.events());

    while (!decision.aggregate().state().phase().terminal()) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
      history.addAll(decision.events());
    }

    assertEquals(
        List.of(
            ExecutionEventType.EXECUTION_STARTED,
            ExecutionEventType.TASK_SKIPPED,
            ExecutionEventType.TASK_STARTED,
            ExecutionEventType.TASK_COMPLETED,
            ExecutionEventType.EXECUTION_COMPLETED),
        history.stream().map(ExecutionHistoryEvent::type).toList());
    ExecutionSnapshot completed = decision.aggregate().state();
    assertEquals("Extract entities", completed.data().inlineValue().required("result").textValue());
    assertEquals(
        "Extract entities", completed.data().inlineValue().required("contextValue").textValue());
    assertEquals(
        "Extract entities", completed.context().inlineValue().required("last").textValue());
    assertNotNull(history.get(1).input());
    assertEquals(
        history.get(1).input(),
        history.get(1).output(),
        "A skipped task must pass its raw input through unchanged");
  }

  @Test
  void namedAndExitFlowDirectivesStayWithinTheirScope() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: flow-directives
          version: '1.0.0'
        do:
          - first:
              set:
                visited: first
              then: third
          - mustBeSkippedByJump:
              set:
                failure: '${ error("named jump was ignored") }'
          - third:
              set:
                visited: third
              then: exit
          - mustBeSkippedByExit:
              set:
                failure: '${ error("exit was ignored") }'
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var decision = harness.apply(startCommand(source, JSON.readTree("{}")), "1".repeat(64));
    List<ExecutionHistoryEvent> history = new ArrayList<>();
    history.addAll(decision.events());

    while (!decision.aggregate().state().phase().terminal()) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
      history.addAll(decision.events());
    }

    assertEquals(
        List.of("first", "third"),
        history.stream()
            .filter(event -> event.type() == ExecutionEventType.TASK_COMPLETED)
            .map(ExecutionHistoryEvent::taskName)
            .toList());
    assertEquals(
        "third", decision.aggregate().state().data().inlineValue().required("visited").textValue());
  }

  @Test
  void switchSelectsFirstConditionalMatchAndAuditsEveryCase() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: switch-first-match
          version: '1.0.0'
        do:
          - choose:
              input:
                from: '${ {color: .color, evaluatedOn: "task-input"} }'
              switch:
                - fallback:
                    then: fallback
                - red:
                    when: '${ .color == "red" }'
                    then: selected
                - mustNotEvaluate:
                    when: '${ error("case after first match was evaluated") }'
                    then: unreachable
              output:
                as: '${ . + {decisionRecorded: true} }'
              export:
                as: '${ $context + {switchSeen: .decisionRecorded} }'
          - selected:
              set:
                result: red
              then: end
          - fallback:
              set:
                result: fallback
              then: end
          - unreachable:
              set:
                result: unreachable
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult run = run(source, JSON.readTree("{\"color\":\"red\",\"discarded\":true}"));
    ExecutionHistoryEvent switchCompleted =
        run.history().stream()
            .filter(event -> event.type() == ExecutionEventType.TASK_COMPLETED)
            .filter(event -> "choose".equals(event.taskName()))
            .findFirst()
            .orElseThrow();
    SwitchDecision decision = switchCompleted.switchDecision();

    assertEquals(ExecutionPhase.COMPLETED, run.snapshot().phase());
    assertEquals("red", run.snapshot().data().inlineValue().required("result").textValue());
    assertTrue(run.snapshot().context().inlineValue().required("switchSeen").booleanValue());
    assertEquals(
        "task-input", switchCompleted.input().inlineValue().required("evaluatedOn").textValue());
    assertTrue(switchCompleted.output().inlineValue().required("decisionRecorded").booleanValue());
    assertEquals("red", decision.selectedCase());
    assertEquals("selected", decision.flowDirective());
    assertEquals(false, decision.cases().get(0).result());
    assertEquals(true, decision.cases().get(1).result());
    assertEquals(null, decision.cases().get(2).result());
  }

  @Test
  void switchUsesExplicitDefaultOnlyAfterConditionsFail() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: switch-explicit-default
          version: '1.0.0'
        do:
          - choose:
              switch:
                - blue:
                    when: '${ .color == "blue" }'
                    then: selected
                - fallback:
                    then: fallback
          - selected:
              set:
                result: selected
              then: end
          - fallback:
              set:
                result: fallback
              then: end
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult run = run(source, JSON.readTree("{\"color\":\"red\"}"));
    SwitchDecision decision =
        run.history().stream()
            .map(ExecutionHistoryEvent::switchDecision)
            .filter(java.util.Objects::nonNull)
            .findFirst()
            .orElseThrow();

    assertEquals("fallback", run.snapshot().data().inlineValue().required("result").textValue());
    assertEquals("fallback", decision.selectedCase());
    assertEquals("fallback", decision.flowDirective());
    assertEquals(
        List.of(false, true), decision.cases().stream().map(value -> value.result()).toList());
  }

  @Test
  void switchUsesCommonThenAsImplicitDefault() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: switch-implicit-default
          version: '1.0.0'
        do:
          - choose:
              switch:
                - blue:
                    when: '${ .color == "blue" }'
                    then: selected
              then: fallback
          - selected:
              set:
                result: selected
              then: end
          - fallback:
              set:
                result: implicit-default
              then: end
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult run = run(source, JSON.readTree("{\"color\":\"red\"}"));
    SwitchDecision decision =
        run.history().stream()
            .map(ExecutionHistoryEvent::switchDecision)
            .filter(java.util.Objects::nonNull)
            .findFirst()
            .orElseThrow();

    assertEquals(
        "implicit-default", run.snapshot().data().inlineValue().required("result").textValue());
    assertEquals(null, decision.selectedCase());
    assertEquals("fallback", decision.flowDirective());
    assertEquals(false, decision.cases().getFirst().result());
  }

  @Test
  void forMatchesConformanceKitDataFlowAndRecordsEveryIteration() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: for-ctk
          version: '1.0.0'
        do:
          - loopColors:
              for:
                each: color
                in: '${ .colors }'
              do:
                - markProcessed:
                    set:
                      processed: '${ {colors: (.processed.colors + [$color]), indexes: (.processed.indexes + [$index])} }'
              output:
                as: '${ . + {loopComplete: true} }'
              export:
                as: '${ $context + {iterations: (.processed.indexes | length)} }'
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult run = run(source, JSON.readTree("{\"colors\":[\"red\",\"green\",\"blue\"]}"));

    assertEquals(
        JSON.readTree("[\"red\",\"green\",\"blue\"]"),
        run.snapshot().data().inlineValue().required("processed").required("colors"));
    assertEquals(
        JSON.readTree("[0,1,2]"),
        run.snapshot().data().inlineValue().required("processed").required("indexes"));
    assertTrue(run.snapshot().data().inlineValue().required("loopComplete").booleanValue());
    assertEquals(3, run.snapshot().context().inlineValue().required("iterations").intValue());

    List<ExecutionHistoryEvent> iterationStarts =
        run.history().stream()
            .filter(event -> event.type() == ExecutionEventType.ITERATION_STARTED)
            .toList();
    assertEquals(3, iterationStarts.size());
    assertEquals(
        List.of(0, 1, 2),
        iterationStarts.stream().map(event -> event.iterations().getLast().index()).toList());
    assertEquals(
        List.of("red", "green", "blue"),
        iterationStarts.stream()
            .map(event -> event.iterations().getLast().item().inlineValue().textValue())
            .toList());
    assertEquals(
        3,
        run.history().stream()
            .filter(event -> "markProcessed".equals(event.taskName()))
            .filter(event -> event.type() == ExecutionEventType.TASK_COMPLETED)
            .peek(event -> assertEquals(1, event.iterations().size()))
            .count());
  }

  @Test
  void longRunningWorkflowRetainsEveryOneOfOneThousandIterationCursors() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: durability
          name: thousand-iteration-run
          version: '1.0.0'
        do:
          - boundedWork:
              for:
                each: item
                in: '${ .items }'
              do:
                - retainCursor:
                    set:
                      lastItem: '${ $item }'
                      lastIndex: '${ $index }'
        """
            .getBytes(StandardCharsets.UTF_8);
    var items = JSON.createArrayNode();
    for (int index = 0; index < 1_000; index++) {
      items.add(index);
    }
    var input = JSON.createObjectNode();
    input.set("items", items);

    RunResult run = run(source, input);

    assertEquals(ExecutionPhase.COMPLETED, run.snapshot().phase());
    assertEquals(999, run.snapshot().data().inlineValue().required("lastItem").intValue());
    assertEquals(999, run.snapshot().data().inlineValue().required("lastIndex").intValue());
    List<ExecutionHistoryEvent> starts =
        run.history().stream()
            .filter(event -> event.type() == ExecutionEventType.ITERATION_STARTED)
            .toList();
    assertEquals(1_000, starts.size());
    assertEquals(999, starts.getLast().iterations().getLast().index());
  }

  @Test
  void forWhileChecksFlowingOutputBeforeEachCandidateIteration() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: for-while
          version: '1.0.0'
        do:
          - loopValues:
              for:
                each: value
                in: '${ .values }'
                at: position
              while: '${ (.processed | length) < 2 }'
              do:
                - append:
                    set:
                      processed: '${ .processed + [{value: $value, position: $position}] }'
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult run = run(source, JSON.readTree("{\"values\":[\"a\",\"b\",\"c\",\"d\"]}"));

    assertEquals(2, run.snapshot().data().inlineValue().required("processed").size());
    assertEquals(
        "b",
        run.snapshot()
            .data()
            .inlineValue()
            .required("processed")
            .get(1)
            .required("value")
            .textValue());
    assertEquals(
        List.of(0, 1),
        run.history().stream()
            .filter(event -> event.type() == ExecutionEventType.ITERATION_STARTED)
            .map(event -> event.iterations().getLast().index())
            .toList());
  }

  @Test
  void nestedForRetainsOuterAndInnerBindingsAndPositions() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: nested-for
          version: '1.0.0'
        do:
          - groups:
              for:
                each: group
                in: '${ .groups }'
                at: groupIndex
              do:
                - members:
                    for:
                      each: member
                      in: '${ $group.items }'
                      at: memberIndex
                    do:
                      - append:
                          set:
                            processed: '${ .processed + [{group: $group.name, groupIndex: $groupIndex, member: $member, memberIndex: $memberIndex}] }'
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult run =
        run(
            source,
            JSON.readTree(
                """
                {
                  "groups": [
                    {"name": "alpha", "items": [10, 11]},
                    {"name": "beta", "items": [20]}
                  ]
                }
                """));
    JsonNode processed = run.snapshot().data().inlineValue().required("processed");

    assertEquals(3, processed.size());
    assertEquals("alpha", processed.get(1).required("group").textValue());
    assertEquals(1, processed.get(1).required("memberIndex").intValue());
    assertEquals("beta", processed.get(2).required("group").textValue());
    assertEquals(1, processed.get(2).required("groupIndex").intValue());

    List<ExecutionHistoryEvent> appended =
        run.history().stream()
            .filter(event -> event.type() == ExecutionEventType.TASK_COMPLETED)
            .filter(event -> "append".equals(event.taskName()))
            .toList();
    assertEquals(3, appended.size());
    appended.forEach(event -> assertEquals(2, event.iterations().size()));
    assertEquals(
        List.of(0, 1),
        appended.get(1).iterations().stream().map(position -> position.index()).toList());
    assertEquals(
        List.of(1, 0),
        appended.get(2).iterations().stream().map(position -> position.index()).toList());
  }

  @Test
  void emptyForPassesTransformedInputWithoutCreatingIterations() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: empty-for
          version: '1.0.0'
        do:
          - loop:
              input:
                from: '${ {values: .values, retained: .retained} }'
              for:
                in: '${ .values }'
              do:
                - mustNotRun:
                    set:
                      failure: '${ error("empty loop executed") }'
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult run =
        run(source, JSON.readTree("{\"values\":[],\"retained\":\"yes\",\"discarded\":true}"));

    assertEquals("yes", run.snapshot().data().inlineValue().required("retained").textValue());
    assertTrue(!run.snapshot().data().inlineValue().has("discarded"));
    assertTrue(
        run.history().stream()
            .noneMatch(event -> event.type() == ExecutionEventType.ITERATION_STARTED));
  }

  @Test
  void forkAdvancesIndependentBranchesAndJoinsInDeclarationOrder() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: fork-join
          version: '1.0.0'
        do:
          - parallel:
              fork:
                branches:
                  - people:
                      set:
                        extracted: people
                  - organisations:
                      do:
                        - extract:
                            set:
                              extracted: organisations
              output:
                as:
                  branchResults: '${ . }'
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult run = run(source, JSON.readTree("{\"instruction\":\"extract\"}"));

    assertEquals(
        JSON.readTree(
            """
            [
              {"people":{"extracted":"people"}},
              {"organisations":{"extracted":"organisations"}}
            ]
            """),
        run.snapshot().data().inlineValue().required("branchResults"));
    assertEquals(
        List.of("people", "organisations"),
        run.history().stream()
            .filter(event -> event.type() == ExecutionEventType.FORK_BRANCH_STARTED)
            .map(event -> event.forks().getLast().branchName())
            .toList());
    assertEquals(
        List.of("people", "organisations"),
        run.history().stream()
            .filter(event -> event.type() == ExecutionEventType.FORK_BRANCH_COMPLETED)
            .map(event -> event.forks().getLast().branchName())
            .toList());
    run.history().stream()
        .filter(
            event ->
                event.taskName() != null
                    && Set.of("people", "organisations", "extract").contains(event.taskName()))
        .forEach(event -> assertEquals(1, event.forks().size()));
  }

  @Test
  void competingForkUsesFirstDurablyCompletedBranchAndAbandonsLosers() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: fork-compete
          version: '1.0.0'
        do:
          - race:
              fork:
                compete: true
                branches:
                  - immediate:
                      set:
                        winner: immediate
                  - nested:
                      do:
                        - slower:
                            set:
                              winner: nested
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult run = run(source, JSON.readTree("{}"));

    assertEquals("immediate", run.snapshot().data().inlineValue().required("winner").textValue());
    ExecutionHistoryEvent abandoned =
        run.history().stream()
            .filter(event -> event.type() == ExecutionEventType.FORK_BRANCH_ABANDONED)
            .findFirst()
            .orElseThrow();
    assertEquals("nested", abandoned.forks().getLast().branchName());
    assertTrue(
        run.history().stream()
            .noneMatch(
                event ->
                    "slower".equals(event.taskName())
                        && event.type() == ExecutionEventType.TASK_COMPLETED));
  }

  @Test
  void nestedForksRetainTheirCompleteBranchCoordinateStack() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: nested-fork
          version: '1.0.0'
        do:
          - outer:
              fork:
                branches:
                  - nested:
                      fork:
                        branches:
                          - first:
                              set:
                                value: first
                          - second:
                              set:
                                value: second
                  - peer:
                      set:
                        value: peer
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult run = run(source, JSON.readTree("{}"));

    List<ExecutionHistoryEvent> innerTasks =
        run.history().stream()
            .filter(event -> event.type() == ExecutionEventType.TASK_COMPLETED)
            .filter(event -> Set.of("first", "second").contains(event.taskName()))
            .toList();
    assertEquals(2, innerTasks.size());
    innerTasks.forEach(
        event ->
            assertEquals(
                List.of("nested", event.taskName()),
                event.forks().stream().map(position -> position.branchName()).toList()));
  }

  @Test
  void endInsideForkEndsTheWorkflowAndCancellationAbandonsActiveLanes() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: fork-end
          version: '1.0.0'
        do:
          - parallel:
              fork:
                branches:
                  - ending:
                      set:
                        result: ended
                      then: end
                  - other:
                      do:
                        - unreachable:
                            set:
                              failure: '${ error("end was ignored") }'
          - alsoUnreachable:
              set:
                failure: '${ error("fork end did not end workflow") }'
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult ended = run(source, JSON.readTree("{}"));
    assertEquals("ended", ended.snapshot().data().inlineValue().required("result").textValue());
    assertTrue(
        ended.history().stream()
            .noneMatch(
                event ->
                    "unreachable".equals(event.taskName())
                        || "alsoUnreachable".equals(event.taskName())));
    assertEquals(
        1,
        ended.history().stream()
            .filter(event -> event.type() == ExecutionEventType.FORK_BRANCH_ABANDONED)
            .count());

    Harness harness = new Harness(source);
    var started = harness.apply(startCommand(source, JSON.createObjectNode()), "1".repeat(64));
    var forkEntered =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));
    assertNotNull(forkEntered.aggregate().state().activeFork());
    var cancelled =
        harness.apply(
            new ControlExecutionCommand(
                "cancel-fork", KEY, ExecutionControlAction.CANCEL, USER, NOW.plusSeconds(1)),
            fingerprint(forkEntered.aggregate().revision()));
    assertEquals(ExecutionPhase.CANCELLED, cancelled.aggregate().state().phase());
    assertEquals(
        2,
        cancelled.events().stream()
            .filter(event -> event.type() == ExecutionEventType.FORK_BRANCH_ABANDONED)
            .count());
  }

  @Test
  void emitMatchesTheConformanceKitCloudEventOutput() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: default
          name: emit
          version: '1.0.0'
        do:
          - emitEvent:
              emit:
                event:
                  with:
                    source: https://fake-source.com
                    type: com.fake-source.user.greeted.v1
                    data:
                      greetings: '${ "Hello \\(.user.firstName) \\(.user.lastName)!" }'
        """
            .getBytes(StandardCharsets.UTF_8);

    RunResult run =
        run(
            source,
            JSON.readTree(
                """
                {
                  "user": {
                    "firstName": "John",
                    "lastName": "Doe"
                  }
                }
                """));
    JsonNode event = run.snapshot().data().inlineValue();

    assertEquals("1.0", event.required("specversion").textValue());
    assertEquals("https://fake-source.com", event.required("source").textValue());
    assertEquals("com.fake-source.user.greeted.v1", event.required("type").textValue());
    assertEquals("Hello John Doe!", event.required("data").required("greetings").textValue());
    assertTrue(event.required("id").isTextual());
    assertEquals(NOW.toString(), event.required("time").textValue());
    ExecutionHistoryEvent emitted =
        run.history().stream()
            .filter(candidate -> candidate.type() == ExecutionEventType.EVENT_EMITTED)
            .findFirst()
            .orElseThrow();
    assertEquals(event, emitted.output().inlineValue());
    assertEquals(
        event,
        run.history().stream()
            .filter(candidate -> candidate.type() == ExecutionEventType.TASK_COMPLETED)
            .filter(candidate -> "emitEvent".equals(candidate.taskName()))
            .findFirst()
            .orElseThrow()
            .output()
            .inlineValue());
  }

  @Test
  void listenCreatesADurableSubscriptionAndResumesFromCloudEvent() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: listen
          version: '1.0.0'
        do:
          - awaitEvidence:
              listen:
                to:
                  one:
                    with:
                      type: evidence.received.v1
                read: data
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var started = harness.apply(startCommand(source, JSON.readTree("{}")), "1".repeat(64));
    var waiting =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));

    assertTrue(waiting.followUpCommands().isEmpty());
    assertTrue(waiting.aggregate().state().pendingInteraction() instanceof ActiveListenState);
    assertEquals(WorkflowEffectType.UPSERT_EVENT_SUBSCRIPTION, waiting.outbox().getFirst().type());
    ActiveListenState subscription =
        (ActiveListenState) waiting.aggregate().state().pendingInteraction();
    var ignored =
        harness.apply(
            received(
                subscription.subscriptionId(),
                "unrelated-1",
                "unrelated.event.v1",
                "{\"ignored\":true}",
                NOW.plusSeconds(1)),
            fingerprint(waiting.aggregate().revision()));
    assertFalse(ignored.stateChanged(), "A legitimate non-matching tenant event is not poison");
    assertTrue(ignored.events().isEmpty());
    var resumed =
        harness.apply(
            new ReceiveEventCommand(
                "event:https://events.test:evidence-1",
                KEY,
                subscription.subscriptionId(),
                DataReferences.inline(
                    JSON.readTree(
                        """
                        {
                          "specversion": "1.0",
                          "id": "evidence-1",
                          "source": "https://events.test",
                          "type": "evidence.received.v1",
                          "data": {"evidenceId": "e-42"}
                        }
                        """)),
                RUNTIME,
                NOW.plusSeconds(2)),
            fingerprint(ignored.aggregate().revision()));

    assertEquals(WorkflowEffectType.DELETE_EVENT_SUBSCRIPTION, resumed.outbox().getFirst().type());
    assertTrue(
        resumed.events().stream()
            .anyMatch(event -> event.type() == ExecutionEventType.EVENT_RECEIVED));
    assertTrue(
        resumed.events().stream()
            .anyMatch(event -> event.type() == ExecutionEventType.SUBSCRIPTION_COMPLETED));
    var completed = harness.apply(resumed.followUpCommands().getFirst(), "3".repeat(64));
    assertEquals(ExecutionPhase.COMPLETED, completed.aggregate().state().phase());
    assertEquals(
        "e-42",
        completed.aggregate().state().data().inlineValue().required("evidenceId").textValue());
  }

  @Test
  void listenAnyUntilForeachProcessesEveryConsumedItem() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: listen-foreach
          version: '1.0.0'
        do:
          - collect:
              listen:
                to:
                  any: []
                  until: ( . | length ) >= 2
              foreach:
                item: event
                at: eventIndex
                do:
                  - retain:
                      set:
                        evidenceId: '${ $event.evidenceId }'
                        index: '${ $eventIndex }'
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var started = harness.apply(startCommand(source, JSON.createObjectNode()), "1".repeat(64));
    var waiting =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));
    ActiveListenState subscription =
        (ActiveListenState) waiting.aggregate().state().pendingInteraction();
    var first =
        harness.apply(
            received(
                subscription.subscriptionId(),
                "event-1",
                "{\"evidenceId\":\"e-1\"}",
                NOW.plusSeconds(1)),
            "2".repeat(64));
    assertTrue(first.followUpCommands().isEmpty());
    assertEquals(
        1,
        ((ActiveListenState) first.aggregate().state().pendingInteraction())
            .consumedEvents()
            .size());

    var decision =
        harness.apply(
            received(
                subscription.subscriptionId(),
                "event-2",
                "{\"evidenceId\":\"e-2\"}",
                NOW.plusSeconds(2)),
            "3".repeat(64));
    while (!decision.aggregate().state().phase().terminal()) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
    }
    JsonNode output = decision.aggregate().state().data().inlineValue();
    assertEquals(2, output.size());
    assertEquals("e-1", output.get(0).required("evidenceId").textValue());
    assertEquals(0, output.get(0).required("index").intValue());
    assertEquals("e-2", output.get(1).required("evidenceId").textValue());
    assertEquals(1, output.get(1).required("index").intValue());
  }

  @Test
  void waitSchedulesAPersistentTimerAndResumesAtItsOriginalDueTime() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: wait
          version: '1.0.0'
        do:
          - delay:
              wait: '${ .delay }'
          - complete:
              set:
                status: complete
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var started =
        harness.apply(startCommand(source, JSON.readTree("{\"delay\":\"PT30S\"}")), "1".repeat(64));
    var waiting =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));
    ActiveTimerState timer = (ActiveTimerState) waiting.aggregate().state().pendingInteraction();

    assertEquals(NOW.plusSeconds(30), timer.dueAt());
    assertEquals(WorkflowEffectType.SCHEDULE_TIMER, waiting.outbox().getFirst().type());
    assertEquals(
        NOW.plusSeconds(30).toString(),
        waiting.outbox().getFirst().payload().inlineValue().required("dueAt").textValue());

    var paused =
        harness.apply(
            new ControlExecutionCommand(
                "pause-wait", KEY, ExecutionControlAction.PAUSE, USER, NOW.plusSeconds(10)),
            fingerprint(waiting.aggregate().revision()));
    assertEquals(WorkflowEffectType.CANCEL_TIMER, paused.outbox().getFirst().type());
    var resumed =
        harness.apply(
            new ControlExecutionCommand(
                "resume-wait", KEY, ExecutionControlAction.RESUME, USER, NOW.plusSeconds(20)),
            fingerprint(paused.aggregate().revision()));
    assertEquals(WorkflowEffectType.SCHEDULE_TIMER, resumed.outbox().getFirst().type());
    assertEquals(
        timer.dueAt().toString(),
        resumed.outbox().getFirst().payload().inlineValue().required("dueAt").textValue());

    var fired =
        harness.apply(
            new FireTimerCommand(
                "timer:" + timer.timerId(), KEY, timer.timerId(), RUNTIME, timer.dueAt()),
            fingerprint(resumed.aggregate().revision()));
    assertEquals(WorkflowEffectType.CANCEL_TIMER, fired.outbox().getFirst().type());
    assertTrue(
        fired.events().stream().anyMatch(event -> event.type() == ExecutionEventType.TIMER_FIRED));
    while (!fired.aggregate().state().phase().terminal()) {
      fired =
          harness.apply(
              fired.followUpCommands().getFirst(), fingerprint(fired.aggregate().revision()));
    }
    assertEquals(
        "complete", fired.aggregate().state().data().inlineValue().required("status").textValue());
  }

  @Test
  void timerControlEnvelopeRemainsInlineWhenWorkflowDataIsExternalized() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: externalized-wait
          version: '1.0.0'
        do:
          - delay:
              wait: PT30S
        """
            .getBytes(StandardCharsets.UTF_8);
    ExternalizingDataAccess dataAccess = new ExternalizingDataAccess();
    Harness harness = new Harness(source, List.of(), Duration.ofSeconds(30), false, dataAccess);

    StartExecutionCommand inlineStart = startCommand(source, JSON.createObjectNode());
    var waiting =
        harness.apply(
            new StartExecutionCommand(
                inlineStart.commandId(),
                inlineStart.key(),
                inlineStart.definition(),
                dataAccess.reference(JSON.createObjectNode()),
                inlineStart.actor(),
                inlineStart.requestedAt()),
            "1".repeat(64));
    while (waiting.outbox().isEmpty() && !waiting.followUpCommands().isEmpty()) {
      waiting =
          harness.apply(
              waiting.followUpCommands().getFirst(), fingerprint(waiting.aggregate().revision()));
    }

    assertEquals(DataReference.Storage.INLINE, waiting.outbox().getFirst().payload().storage());
    assertEquals(
        NOW.plusSeconds(30).toString(),
        waiting.outbox().getFirst().payload().inlineValue().required("dueAt").textValue());
    assertEquals(DataReference.Storage.ARTIFACT, waiting.aggregate().state().data().storage());
  }

  @Test
  void operationDescriptorReferencesExternalizedArgumentsWithoutReinlining() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: externalized-call-arguments
          version: '1.0.0'
        do:
          - extract:
              call: http
              with:
                method: POST
                endpoint: https://extractor.test/v1/extract
                body:
                  documentText: '${ .documentText }'
                output: content
        """
            .getBytes(StandardCharsets.UTF_8);
    String sensitive = "business-payload-must-not-enter-kafka-history";
    ExternalizingDataAccess dataAccess = new ExternalizingDataAccess();
    Harness harness = new Harness(source, List.of(), Duration.ofSeconds(30), false, dataAccess);

    StartExecutionCommand inlineStart =
        startCommand(source, JSON.createObjectNode().put("documentText", sensitive));
    var started =
        harness.apply(
            new StartExecutionCommand(
                inlineStart.commandId(),
                inlineStart.key(),
                inlineStart.definition(),
                dataAccess.reference(inlineStart.input().inlineValue()),
                inlineStart.actor(),
                inlineStart.requestedAt()),
            "1".repeat(64));
    var dispatched =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));

    ActiveOperationState operation =
        assertInstanceOf(
            ActiveOperationState.class, dispatched.aggregate().state().pendingInteraction());
    JsonNode descriptor = operation.descriptor().inlineValue();
    assertFalse(descriptor.toString().contains(sensitive));
    assertFalse(descriptor.has("arguments"));
    DataReference arguments = DataReferenceJson.decode(descriptor.required("argumentsReference"));
    assertEquals(DataReference.Storage.ARTIFACT, arguments.storage());
    assertEquals(sensitive, dataAccess.resolve(arguments).at("/body/documentText").textValue());
    assertFalse(
        dispatched.events().stream().anyMatch(event -> event.toString().contains(sensitive)));
    assertFalse(
        dispatched.outbox().stream().anyMatch(effect -> effect.toString().contains(sensitive)));
  }

  @Test
  void humanTaskDescriptorReferencesExternalizedBusinessInput() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: externalized-human-task-input
          version: '1.0.0'
        do:
          - approve:
              call: com.forwardmeasure.openworkflow.human-task
              with:
                title: Review Evidence
                input: '${ .caseMaterial }'
                approvals:
                  stages:
                    - level: 1
                      name: Evidence Review
                      requiredApprovals: 1
                      candidateRoles: [evidence-reviewer]
        """
            .getBytes(StandardCharsets.UTF_8);
    String sensitive = "human-task-payload-must-not-enter-kafka-history";
    ExternalizingDataAccess dataAccess = new ExternalizingDataAccess();
    Harness harness = new Harness(source, List.of(), Duration.ofSeconds(30), false, dataAccess);
    StartExecutionCommand inlineStart =
        startCommand(source, JSON.createObjectNode().put("caseMaterial", sensitive));

    var started =
        harness.apply(
            new StartExecutionCommand(
                inlineStart.commandId(),
                inlineStart.key(),
                inlineStart.definition(),
                dataAccess.reference(inlineStart.input().inlineValue()),
                inlineStart.actor(),
                inlineStart.requestedAt()),
            "1".repeat(64));
    var waiting =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));

    ActiveHumanTaskState task =
        assertInstanceOf(
            ActiveHumanTaskState.class, waiting.aggregate().state().pendingInteraction());
    JsonNode descriptor = task.descriptor().inlineValue();
    assertFalse(descriptor.toString().contains(sensitive));
    assertFalse(descriptor.has("input"));
    DataReference input = DataReferenceJson.decode(descriptor.required("inputReference"));
    assertEquals(DataReference.Storage.ARTIFACT, input.storage());
    assertEquals(sensitive, dataAccess.resolve(input).textValue());
    assertFalse(waiting.events().stream().anyMatch(event -> event.toString().contains(sensitive)));
    assertFalse(
        waiting.outbox().stream().anyMatch(effect -> effect.toString().contains(sensitive)));
  }

  @Test
  void waitAcceptsTheSpecificationsCalendarDurationLiterals() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: calendar-wait
          version: '1.0.0'
        do:
          - delay:
              wait: P1M
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var started = harness.apply(startCommand(source, JSON.createObjectNode()), "1".repeat(64));
    var waiting =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));

    ActiveTimerState timer =
        assertInstanceOf(ActiveTimerState.class, waiting.aggregate().state().pendingInteraction());
    assertEquals(Instant.parse("2026-08-28T20:00:00Z"), timer.dueAt());
  }

  @Test
  void workflowTimeoutIsDurableAndFailsTheWholeExecution() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: workflow-timeout
          version: '1.0.0'
        timeout:
          after: PT30S
        do:
          - delay:
              wait: PT1H
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var started = harness.apply(startCommand(source, JSON.createObjectNode()), "1".repeat(64));

    ActiveTimeoutState timeout = started.aggregate().state().activeTimeouts().getFirst();
    assertTrue(timeout.workflowTimeout());
    assertEquals(NOW.plusSeconds(30), timeout.dueAt());
    assertTrue(
        started.events().stream()
            .anyMatch(event -> event.type() == ExecutionEventType.TIMER_SCHEDULED));
    assertTrue(
        started.outbox().stream()
            .anyMatch(
                effect ->
                    effect.type() == WorkflowEffectType.SCHEDULE_TIMER
                        && effect
                            .payload()
                            .inlineValue()
                            .path("purpose")
                            .asText()
                            .equals("workflow-timeout")));

    var waiting =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));
    var failed =
        harness.apply(
            new FireTimerCommand(
                "timer:" + timeout.timerId(), KEY, timeout.timerId(), RUNTIME, timeout.dueAt()),
            fingerprint(waiting.aggregate().revision()));

    assertEquals(ExecutionPhase.FAILED, failed.aggregate().state().phase());
    assertTrue(failed.aggregate().state().activeTimeouts().isEmpty());
    assertEquals(408, failed.aggregate().state().failure().status());
    assertTrue(
        failed.events().stream().anyMatch(event -> event.type() == ExecutionEventType.TIMER_FIRED));
    assertTrue(
        failed.events().stream()
            .anyMatch(event -> event.type() == ExecutionEventType.EXECUTION_FAILED));
    assertTrue(
        failed.outbox().stream()
            .anyMatch(
                effect ->
                    effect.type() == WorkflowEffectType.CANCEL_TIMER
                        && effect
                            .payload()
                            .inlineValue()
                            .path("purpose")
                            .asText()
                            .equals("workflow-timeout")));
  }

  @Test
  void successfulCompletionClearsAndCancelsTheWorkflowTimeout() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: completes-before-workflow-timeout
          version: '1.0.0'
        timeout:
          after: PT30S
        do:
          - finish:
              set:
                status: complete
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var decision = harness.apply(startCommand(source, JSON.createObjectNode()), "1".repeat(64));

    assertEquals(1, decision.aggregate().state().activeTimeouts().size());
    while (!decision.aggregate().state().phase().terminal()) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
    }

    assertEquals(ExecutionPhase.COMPLETED, decision.aggregate().state().phase());
    assertTrue(decision.aggregate().state().activeTimeouts().isEmpty());
    assertTrue(
        decision.outbox().stream()
            .anyMatch(
                effect ->
                    effect.type() == WorkflowEffectType.CANCEL_TIMER
                        && effect
                            .payload()
                            .inlineValue()
                            .path("purpose")
                            .asText()
                            .equals("workflow-timeout")));
  }

  @Test
  void taskTimeoutCancelsItsWorkAndCanBeCaught() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: task-timeout
          version: '1.0.0'
        do:
          - guarded:
              try:
                - slow:
                    timeout:
                      after: PT10S
                    wait: PT1H
              catch:
                errors:
                  with:
                    status: 408
                do:
                  - recovered:
                      set:
                        status: recovered
                then: end
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var decision = harness.apply(startCommand(source, JSON.createObjectNode()), "1".repeat(64));
    while (decision.aggregate().state().activeTimeouts().isEmpty()) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
    }

    ActiveTimeoutState timeout = decision.aggregate().state().activeTimeouts().getFirst();
    assertEquals("/do/0/guarded/try/0/slow", timeout.taskPath());
    assertEquals(NOW.plusSeconds(10), timeout.dueAt());
    assertInstanceOf(ActiveTimerState.class, decision.aggregate().state().pendingInteraction());

    var timedOut =
        harness.apply(
            new FireTimerCommand(
                "timer:" + timeout.timerId(), KEY, timeout.timerId(), RUNTIME, timeout.dueAt()),
            fingerprint(decision.aggregate().revision()));
    assertTrue(
        timedOut.events().stream()
            .anyMatch(event -> event.type() == ExecutionEventType.ERROR_CAUGHT));
    assertTrue(timedOut.aggregate().state().activeTimeouts().isEmpty());
    assertEquals(
        2,
        timedOut.outbox().stream()
            .filter(effect -> effect.type() == WorkflowEffectType.CANCEL_TIMER)
            .count(),
        "the wait timer and its enclosing task timeout are cancelled");

    while (!timedOut.aggregate().state().phase().terminal()) {
      timedOut =
          harness.apply(
              timedOut.followUpCommands().getFirst(), fingerprint(timedOut.aggregate().revision()));
    }
    assertEquals(ExecutionPhase.COMPLETED, timedOut.aggregate().state().phase());
    assertEquals(
        "recovered",
        timedOut.aggregate().state().data().inlineValue().required("status").textValue());
  }

  @Test
  void listenSubscriptionsInsideForksPauseResumeAndRecoverIndependently() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: fork-listen
          version: '1.0.0'
        do:
          - parallel:
              fork:
                branches:
                  - left:
                      listen:
                        to:
                          one:
                            with:
                              type: evidence.left.v1
                  - right:
                      listen:
                        to:
                          one:
                            with:
                              type: evidence.right.v1
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var decision = harness.apply(startCommand(source, JSON.createObjectNode()), "1".repeat(64));
    List<WorkflowEffect> entryEffects = new ArrayList<>();
    while (!decision.followUpCommands().isEmpty()) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
      entryEffects.addAll(decision.outbox());
    }

    assertEquals(2, activeListens(decision.aggregate().state()).size());
    assertEquals(
        2,
        entryEffects.stream()
            .filter(effect -> effect.type() == WorkflowEffectType.UPSERT_EVENT_SUBSCRIPTION)
            .count());

    var paused =
        harness.apply(
            new ControlExecutionCommand(
                "pause-fork-listen", KEY, ExecutionControlAction.PAUSE, USER, NOW.plusSeconds(3)),
            fingerprint(decision.aggregate().revision()));
    assertEquals(
        2,
        paused.outbox().stream()
            .filter(effect -> effect.type() == WorkflowEffectType.DELETE_EVENT_SUBSCRIPTION)
            .count());

    var resumed =
        harness.apply(
            new ControlExecutionCommand(
                "resume-fork-listen", KEY, ExecutionControlAction.RESUME, USER, NOW.plusSeconds(4)),
            fingerprint(paused.aggregate().revision()));
    assertEquals(
        2,
        resumed.outbox().stream()
            .filter(effect -> effect.type() == WorkflowEffectType.UPSERT_EVENT_SUBSCRIPTION)
            .count());
    assertTrue(
        resumed.followUpCommands().isEmpty(),
        "A fully waiting fork must not be spuriously advanced");

    decision = resumed;
    for (String type : List.of("evidence.left.v1", "evidence.right.v1")) {
      ActiveListenState subscription =
          activeListens(decision.aggregate().state()).stream()
              .filter(
                  candidate ->
                      candidate.taskPath().contains(type.contains("left") ? "left" : "right"))
              .findFirst()
              .orElseThrow();
      decision =
          harness.apply(
              received(
                  subscription.subscriptionId(),
                  type,
                  type,
                  "{\"side\":\"" + type + "\"}",
                  NOW.plusSeconds(5)),
              fingerprint(decision.aggregate().revision()));
      while (!decision.followUpCommands().isEmpty()) {
        decision =
            harness.apply(
                decision.followUpCommands().getFirst(),
                fingerprint(decision.aggregate().revision()));
      }
    }
    assertEquals(ExecutionPhase.COMPLETED, decision.aggregate().state().phase());
  }

  @Test
  void eventResumesAListenNestedInsideNestedForks() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: nested-fork-listen
          version: '1.0.0'
        do:
          - outer:
              fork:
                branches:
                  - nested:
                      fork:
                        branches:
                          - waiting:
                              listen:
                                to:
                                  one:
                                    with:
                                      type: evidence.nested.v1
                  - immediate:
                      set:
                        peer: complete
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var decision = harness.apply(startCommand(source, JSON.createObjectNode()), "1".repeat(64));
    while (!decision.followUpCommands().isEmpty()) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
    }
    ActiveListenState nested = activeListens(decision.aggregate().state()).getFirst();

    decision =
        harness.apply(
            received(
                nested.subscriptionId(),
                "nested-1",
                "evidence.nested.v1",
                "{\"nested\":true}",
                NOW.plusSeconds(1)),
            fingerprint(decision.aggregate().revision()));
    while (!decision.aggregate().state().phase().terminal()) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
    }
    assertEquals(ExecutionPhase.COMPLETED, decision.aggregate().state().phase());
    assertTrue(decision.aggregate().state().data().inlineValue().toString().contains("nested"));
  }

  private static ReceiveEventCommand received(
      String subscriptionId, String eventId, String data, Instant receivedAt) throws Exception {
    return received(subscriptionId, eventId, "evidence.received.v1", data, receivedAt);
  }

  private static ReceiveEventCommand received(
      String subscriptionId, String eventId, String type, String data, Instant receivedAt)
      throws Exception {
    return new ReceiveEventCommand(
        "event:" + eventId,
        KEY,
        subscriptionId,
        DataReferences.inline(
            JSON.readTree(
                """
                {
                  "specversion": "1.0",
                  "id": "%s",
                  "source": "https://events.test",
                  "type": "%s",
                  "data": %s
                }
                """
                    .formatted(eventId, type, data))),
        RUNTIME,
        receivedAt);
  }

  private static List<ActiveListenState> activeListens(ExecutionSnapshot snapshot) {
    List<ActiveListenState> result = new ArrayList<>();
    if (snapshot.pendingInteraction() instanceof ActiveListenState listen) {
      result.add(listen);
    }
    activeListens(snapshot.activeFork(), result);
    return result;
  }

  private static void activeListens(ForkRuntimeState fork, List<ActiveListenState> result) {
    if (fork == null) return;
    for (ForkBranchState branch : fork.branches()) {
      if (branch.pendingInteraction() instanceof ActiveListenState listen) {
        result.add(listen);
      }
      activeListens(branch.activeFork(), result);
    }
  }

  @Test
  void invalidForCollectionBecomesADurableExpressionFailure() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: invalid-for-collection
          version: '1.0.0'
        do:
          - loop:
              for:
                in: '${ .values }'
              do:
                - record:
                    set:
                      value: '${ $item }'
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var started =
        harness.apply(
            startCommand(source, JSON.readTree("{\"values\":\"not-an-array\"}")), "1".repeat(64));

    var failed =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));

    assertEquals(ExecutionPhase.FAILED, failed.aggregate().state().phase());
    assertEquals(
        com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionFailure.EXPRESSION_ERROR,
        failed.events().getFirst().failure().type());
    assertEquals("/do/0/loop", failed.events().getFirst().failure().definitionPath());
    assertTrue(
        failed.events().getFirst().failure().message().contains("must evaluate to an array"));
    assertTrue(failed.followUpCommands().isEmpty());
  }

  @Test
  void validatesEveryNormativeDataFlowBoundaryInOrder() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: schema-flow
          version: '1.0.0'
        input:
          schema:
            format: json
            document:
              type: object
              required: [instruction]
          from: '${ {instruction: .instruction} }'
        do:
          - transform:
              input:
                schema:
                  format: json
                  document:
                    type: object
                    required: [instruction]
                from: '${ .instruction }'
              set:
                copied: '${ . }'
              output:
                as: '${ {result: .copied} }'
                schema:
                  format: json
                  document:
                    type: object
                    required: [result]
                    properties:
                      result:
                        type: string
              export:
                as: '${ {last: .result} }'
                schema:
                  format: json
                  document:
                    type: object
                    required: [last]
              then: end
        output:
          as: '${ {answer: .result} }'
          schema:
            format: json
            document:
              type: object
              required: [answer]
              properties:
                answer:
                  type: string
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var decision =
        harness.apply(
            startCommand(
                source,
                JSON.readTree("{\"instruction\":\"Extract entities\"," + "\"rawOnly\":true}")),
            "1".repeat(64));

    while (!decision.aggregate().state().phase().terminal()) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
    }

    assertEquals(ExecutionPhase.COMPLETED, decision.aggregate().state().phase());
    assertEquals(
        "Extract entities",
        decision.aggregate().state().data().inlineValue().required("answer").textValue());
    assertEquals(
        "Extract entities",
        decision.aggregate().state().context().inlineValue().required("last").textValue());
  }

  @Test
  void invalidWorkflowInputBecomesDurableFailedExecution() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: invalid-input
          version: '1.0.0'
        input:
          schema:
            format: json
            document:
              type: object
              required: [instruction]
        do:
          - initialize:
              set:
                status: ready
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);

    var decision = harness.apply(startCommand(source, JSON.readTree("{}")), "1".repeat(64));

    assertEquals(ExecutionPhase.FAILED, decision.aggregate().state().phase());
    assertTrue(decision.followUpCommands().isEmpty());
    assertEquals(ExecutionEventType.EXECUTION_FAILED, decision.events().getFirst().type());
    assertEquals("/input/schema", decision.events().getFirst().failure().definitionPath());
    assertEquals(decision.events().getFirst().failure(), decision.aggregate().state().failure());
  }

  @Test
  void invalidTransformedTaskOutputFailsAtExactTaskCutpoint() throws Exception {
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: invalid-output
          version: '1.0.0'
        do:
          - initialize:
              set:
                result: wrong-type
              output:
                schema:
                  format: json
                  document:
                    type: object
                    required: [result]
                    properties:
                      result:
                        type: integer
        """
            .getBytes(StandardCharsets.UTF_8);
    Harness harness = new Harness(source);
    var started = harness.apply(startCommand(source, JSON.readTree("{}")), "1".repeat(64));

    var failed =
        harness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));

    assertEquals(ExecutionPhase.FAILED, failed.aggregate().state().phase());
    assertEquals("/do/0/initialize", failed.events().getFirst().taskPath());
    assertEquals(
        "/do/0/initialize/output/schema", failed.events().getFirst().failure().definitionPath());
    assertEquals(
        "wrong-type",
        failed.events().getFirst().output().inlineValue().required("result").textValue());
    assertEquals(
        failed.events().getFirst().output(), failed.aggregate().state().failure().rejectedData());
    assertTrue(
        !failed.aggregate().state().cursor().complete(),
        "Failure retains the precise cursor for inspection/recovery");
  }

  private static void addEvents(
      List<ExecutionEventType> types,
      DurableDecision<ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
          decision) {
    decision.events().forEach(event -> types.add(event.type()));
  }

  private static RunResult run(byte[] source, JsonNode input) {
    Harness harness = new Harness(source);
    var decision = harness.apply(startCommand(source, input), "1".repeat(64));
    List<ExecutionHistoryEvent> history = new ArrayList<>(decision.events());
    while (!decision.aggregate().state().phase().terminal()) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
      history.addAll(decision.events());
    }
    return new RunResult(decision.aggregate().state(), history);
  }

  private static String fingerprint(long revision) {
    char digit = (char) ('0' + (revision % 10));
    return String.valueOf(digit).repeat(64);
  }

  private static StartExecutionCommand startCommand() throws Exception {
    return startCommand(SOURCE, JSON.readTree("{\"instruction\":\"Extract entities\"}"));
  }

  private static StartExecutionCommand startCommand(byte[] source, JsonNode input) {
    return startCommand(source, List.of(), input);
  }

  private static StartExecutionCommand startCommand(
      byte[] source, List<ResolvedWorkflowResource> resources, JsonNode input) {
    WorkflowPlan plan = plan(source, resources);
    return new StartExecutionCommand(
        "start-1",
        KEY,
        new WorkflowDefinitionReference(
            new WorkflowDefinitionKey(TENANT, plan.coordinates()),
            plan.sourceSha256(),
            plan.definitionSha256()),
        DataReferences.inline(input),
        USER,
        NOW);
  }

  private static WorkflowPlan plan() {
    return plan(SOURCE);
  }

  private static WorkflowPlan plan(byte[] source) {
    return plan(source, List.of());
  }

  private static WorkflowPlan plan(byte[] source, List<ResolvedWorkflowResource> resources) {
    return new OpenWorkflowCompiler().compile(source, resources);
  }

  private static WorkflowDefinitionBundle bundle() {
    return bundle(SOURCE);
  }

  private static WorkflowDefinitionBundle bundle(byte[] source) {
    return bundle(source, List.of());
  }

  private static WorkflowDefinitionBundle bundle(
      byte[] source, List<ResolvedWorkflowResource> resources) {
    WorkflowPlan plan = plan(source, resources);
    return new WorkflowDefinitionBundle(
        new WorkflowDefinitionKey(TENANT, plan.coordinates()),
        new String(source, StandardCharsets.UTF_8),
        plan,
        OpenWorkflowCompiler.COMPILER_SHA256,
        "admission-1",
        USER,
        NOW);
  }

  private static byte[] asyncApiWorkflow(String subscription) throws Exception {
    return ("""
    document:
      dsl: '1.0.3'
      namespace: evidence
      name: asyncapi-runtime
      version: '1.0.0'
    do:
      - receive:
          call: asyncapi
          with:
            document:
              endpoint:
                uri: https://contracts.test/evidence-asyncapi.yaml
            channel: evidence.messages
            subscription:
    """
            + subscription)
        .getBytes(StandardCharsets.UTF_8);
  }

  private static List<ResolvedWorkflowResource> asyncApiResources() {
    return List.of(
        ResolvedWorkflowResource.of(
            URI.create("https://contracts.test/evidence-asyncapi.yaml"),
            "application/yaml",
            """
            asyncapi: 2.6.0
            info:
              title: Evidence messages
              version: 1.0.0
            servers:
              test:
                url: kafka.test:9092
                protocol: kafka
            channels:
              evidence.messages:
                servers: [test]
                subscribe:
                  message:
                    name: EvidenceMessage
            """));
  }

  private static byte[] correlatedWorkerWorkflow() {
    return """
    document:
      dsl: '1.0.3'
      namespace: workers
      name: correlated-runtime
      version: '1.0.0'
    do:
      - execute:
          call: com.forwardmeasure.openworkflow.correlated-worker
          with:
            document:
              endpoint:
                uri: https://contracts.test/workers.yaml
            command:
              channel: workers.commands
              message:
                payload:
                  request: '${ . }'
            events:
              channel: workers.events
              subscription:
                consume:
                  until: '${ .payload.status == "SUCCEEDED" }'
                  for: PT30M
            cancellation:
              channel: workers.cancellations
              message:
                payload: {}
    """
        .getBytes(StandardCharsets.UTF_8);
  }

  private static List<ResolvedWorkflowResource> correlatedWorkerResources() {
    return List.of(
        ResolvedWorkflowResource.of(
            URI.create("https://contracts.test/workers.yaml"),
            "application/yaml",
            """
            asyncapi: 2.6.0
            info:
              title: Workers
              version: 1.0.0
            servers:
              test:
                url: kafka.test:9092
                protocol: kafka
            channels:
              workers.commands:
                servers: [test]
                publish:
                  message:
                    name: WorkerCommand
              workers.events:
                servers: [test]
                subscribe:
                  message:
                    name: WorkerEvent
              workers.cancellations:
                servers: [test]
                publish:
                  message:
                    name: WorkerCancellation
            """));
  }

  private static ReceiveAsyncApiMessageCommand correlatedWorkerMessage(
      ActiveCorrelatedWorkerState worker,
      String sourcePosition,
      String status,
      JsonNode value,
      long seconds) {
    ObjectNode payload = JSON.createObjectNode();
    payload.put("operationId", worker.lifecycleId());
    payload.put("status", status);
    if ("SUCCEEDED".equals(status)) {
      payload.set("output", value);
    } else {
      payload.set("metadata", value);
    }
    ObjectNode message = JSON.createObjectNode();
    message.set("payload", payload);
    message.set("headers", JSON.createObjectNode());
    return new ReceiveAsyncApiMessageCommand(
        "worker-message-" + sourcePosition,
        KEY,
        worker.lifecycleId(),
        sourcePosition,
        DataReferences.inline(message),
        RUNTIME,
        NOW.plusSeconds(seconds));
  }

  private static ReceiveAsyncApiMessageCommand asyncApiMessage(
      ActiveAsyncApiSubscriptionState subscription,
      String sourcePosition,
      boolean accepted,
      String value,
      long seconds)
      throws Exception {
    return asyncApiMessage(subscription, sourcePosition, accepted, value, seconds, false);
  }

  private static ReceiveAsyncApiMessageCommand asyncApiMessage(
      ActiveAsyncApiSubscriptionState subscription,
      String sourcePosition,
      boolean accepted,
      String value,
      long seconds,
      boolean condition)
      throws Exception {
    return new ReceiveAsyncApiMessageCommand(
        "asyncapi-" + sourcePosition.replace(':', '-'),
        KEY,
        subscription.subscriptionId(),
        sourcePosition,
        DataReferences.inline(
            JSON.readTree(
                """
                {
                  "payload": {
                    "accepted": %s,
                    "value": "%s",
                    "stop": %s,
                    "keepGoing": %s
                  }
                }
                """
                    .formatted(accepted, value, condition, condition))),
        RUNTIME,
        NOW.plusSeconds(seconds));
  }

  private static ObserveHumanTaskCommand humanTaskOutcome(
      ActiveHumanTaskState task,
      HumanTaskObservationStatus status,
      JsonNode data,
      Instant occurredAt) {
    return new ObserveHumanTaskCommand(
        "human-outcome-" + status.name().toLowerCase(),
        KEY,
        task.humanTaskId(),
        task.correlationId(),
        new HumanTaskObservation(
            "outcome-" + status.name().toLowerCase(),
            status,
            data == null ? null : DataReferences.inline(data),
            REVIEWER,
            occurredAt),
        new ActorContext(
            TENANT,
            RUNTIME_ACTOR_ID,
            ActorType.SYSTEM,
            "OKS Human Task Bridge",
            "oks-human-task-bridge",
            Set.of(),
            null,
            occurredAt),
        occurredAt);
  }

  private static ActiveAsyncApiSubscriptionState findAsyncApiSubscription(ForkRuntimeState fork) {
    if (fork == null) return null;
    for (ForkBranchState branch : fork.branches()) {
      if (branch.pendingInteraction() instanceof ActiveAsyncApiSubscriptionState subscription) {
        return subscription;
      }
      ActiveAsyncApiSubscriptionState nested = findAsyncApiSubscription(branch.activeFork());
      if (nested != null) return nested;
    }
    return null;
  }

  private static ActiveTimerState findTimer(ForkRuntimeState fork) {
    if (fork == null) return null;
    for (ForkBranchState branch : fork.branches()) {
      if (branch.pendingInteraction() instanceof ActiveTimerState timer) {
        return timer;
      }
      ActiveTimerState nested = findTimer(branch.activeFork());
      if (nested != null) return nested;
    }
    return null;
  }

  private static ActiveOperationState findOperation(ForkRuntimeState fork) {
    if (fork == null) return null;
    for (ForkBranchState branch : fork.branches()) {
      if (branch.pendingInteraction() instanceof ActiveOperationState operation) {
        return operation;
      }
      ActiveOperationState nested = findOperation(branch.activeFork());
      if (nested != null) return nested;
    }
    return null;
  }

  // ---------------------------------------------------------------------------------------
  // Subworkflow (run: workflow:) support.
  //
  // WorkflowExecutionEngine never talks to a child execution directly - it only ever emits
  // durable WorkflowEffects (START_SUBWORKFLOW / CONTROL_SUBWORKFLOW) and later consumes
  // ExecutionCommands (StartExecutionCommand / ControlExecutionCommand / ObserveOperationCommand)
  // that some topology routes back in. These tests stand in for that topology exactly the way
  // OksSubworkflowLaunchProcessor / OksSubworkflowCompletionProcessor /
  // OksSubworkflowControlProcessor
  // do in production - reading the same descriptor fields those processors read - and drive a real,
  // separate engine instance for the child so the round trip is genuine, not simulated.
  // ---------------------------------------------------------------------------------------

  private static final byte[] SUBWORKFLOW_CHILD_SOURCE =
      """
      document:
        dsl: '1.0.3'
        namespace: evidence
        name: subworkflow-child
        version: '2.0.0'
      do:
        - compute:
            set:
              childSaw: '${ .seed }'
      """
          .getBytes(StandardCharsets.UTF_8);

  private static WorkflowDefinitionBundle subworkflowChildBundle() {
    return bundle(SUBWORKFLOW_CHILD_SOURCE);
  }

  private static WorkflowDefinitionBundle subworkflowParentBundle(
      byte[] parentSource, WorkflowDefinitionBundle child) {
    var subflow =
        new com.forwardmeasure.openworkflow.definition.ResolvedSubflow(
            child.plan().coordinates(),
            child.plan().sourceSha256(),
            child.plan().definitionSha256());
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                parentSource,
                List.of(),
                (namespace, name, version) ->
                    namespace.equals(child.plan().coordinates().namespace())
                            && name.equals(child.plan().coordinates().name())
                            && version.equals(child.plan().coordinates().version())
                        ? java.util.Optional.of(subflow)
                        : java.util.Optional.empty());
    return new WorkflowDefinitionBundle(
        new WorkflowDefinitionKey(TENANT, plan.coordinates()),
        new String(parentSource, StandardCharsets.UTF_8),
        plan,
        OpenWorkflowCompiler.COMPILER_SHA256,
        "admission-subworkflow-parent",
        USER,
        NOW);
  }

  private static WorkflowDefinitionResolver twoBundleResolver(
      WorkflowDefinitionBundle first, WorkflowDefinitionBundle second) {
    return reference -> {
      if (first.reference().equals(reference)) return first;
      if (second.reference().equals(reference)) return second;
      return null;
    };
  }

  /**
   * Mirrors {@code OksSubworkflowLaunchProcessor}: builds the child's start command from the
   * parent's {@code START_SUBWORKFLOW} effect descriptor, the way the real topology routing does.
   */
  private static StartExecutionCommand subworkflowChildStartCommand(WorkflowEffect launch) {
    JsonNode descriptor = launch.payload().inlineValue();
    ExecutionKey childKey =
        ExecutionKey.parse(descriptor.required("childExecutionKey").textValue());
    WorkflowDefinitionReference childDefinition =
        new WorkflowDefinitionReference(
            new WorkflowDefinitionKey(
                childKey.tenantId(),
                new com.forwardmeasure.openworkflow.definition.WorkflowCoordinates(
                    descriptor.required("childNamespace").textValue(),
                    descriptor.required("childName").textValue(),
                    descriptor.required("childVersion").textValue(),
                    descriptor.required("childDsl").textValue())),
            descriptor.required("childSourceSha256").textValue(),
            descriptor.required("childDefinitionSha256").textValue());
    DataReference childInput = DataReferenceJson.decode(descriptor.required("childInput"));
    return new StartExecutionCommand(
        "subworkflow-start:" + descriptor.required("operationId").textValue(),
        childKey,
        childDefinition,
        childInput,
        launch.actor(),
        launch.requestedAt());
  }

  /**
   * Mirrors {@code OksSubworkflowCompletionProcessor}: builds the parent's resume command once the
   * child it is waiting on reaches a terminal state.
   */
  private static ObserveOperationCommand subworkflowParentResumeCommand(
      WorkflowEffect launch, OperationObservation observation, Instant occurredAt) {
    JsonNode descriptor = launch.payload().inlineValue();
    ExecutionKey parentKey =
        ExecutionKey.parse(descriptor.required("parentExecutionKey").textValue());
    return new ObserveOperationCommand(
        "subworkflow-complete:" + descriptor.required("operationId").textValue(),
        parentKey,
        descriptor.required("operationId").textValue(),
        observation,
        RUNTIME,
        occurredAt);
  }

  /**
   * Mirrors {@code OksSubworkflowControlProcessor}: builds the child control command that
   * propagates a parent's pause, resume or cancellation.
   */
  private static ControlExecutionCommand subworkflowChildControlCommand(WorkflowEffect control) {
    JsonNode payload = control.payload().inlineValue();
    ExecutionKey childKey = ExecutionKey.parse(payload.required("childExecutionKey").textValue());
    ExecutionControlAction action =
        ExecutionControlAction.valueOf(payload.required("action").textValue());
    return new ControlExecutionCommand(
        control.effectId(), childKey, action, control.actor(), control.requestedAt());
  }

  private static WorkflowEffect onlySubworkflowEffect(
      List<WorkflowEffect> effects, WorkflowEffectType type, ExecutionControlAction action) {
    return effects.stream()
        .filter(effect -> effect.type() == type)
        .filter(
            effect ->
                action == null
                    || action
                        .name()
                        .equals(effect.payload().inlineValue().path("action").asText(null)))
        .findFirst()
        .orElseThrow(
            () -> new AssertionError("No " + type + " (" + action + ") effect was emitted"));
  }

  private static DurableDecision<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      runToTerminal(
          Harness harness,
          DurableDecision<
                  ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
              decision) {
    while (!decision.aggregate().state().phase().terminal()) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
    }
    return decision;
  }

  @Test
  void subworkflowRunStartsChildAndResumesParentWithItsOutput() throws Exception {
    WorkflowDefinitionBundle child = subworkflowChildBundle();
    byte[] parentSource =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: subworkflow-parent
          version: '1.0.0'
        do:
          - invoke:
              run:
                workflow:
                  namespace: evidence
                  name: subworkflow-child
                  version: '2.0.0'
                  input:
                    seed: '${ .seed }'
        """
            .getBytes(StandardCharsets.UTF_8);
    WorkflowDefinitionBundle parent = subworkflowParentBundle(parentSource, child);
    ExecutionKey parentKey =
        new ExecutionKey(TENANT, new WorkflowExecutionId("subworkflow-parent-completion"));
    Harness parentHarness = new Harness(parentKey, twoBundleResolver(parent, child));

    var started =
        parentHarness.apply(
            new StartExecutionCommand(
                "start-subworkflow-parent",
                parentKey,
                parent.reference(),
                DataReferences.inline(JSON.readTree("{\"seed\":\"abc-123\"}")),
                USER,
                NOW),
            "1".repeat(64));
    var dispatched =
        parentHarness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));

    ActiveOperationState operation =
        assertInstanceOf(
            ActiveOperationState.class, dispatched.aggregate().state().pendingInteraction());
    assertEquals("run-workflow", operation.operationKind());
    WorkflowEffect launch = dispatched.outbox().getFirst();
    assertEquals(WorkflowEffectType.START_SUBWORKFLOW, launch.type());
    JsonNode descriptor = launch.payload().inlineValue();
    assertEquals("subworkflow-child", descriptor.required("childName").textValue());
    assertEquals(parentKey.canonical(), descriptor.required("parentExecutionKey").textValue());
    assertTrue(descriptor.required("awaitParent").booleanValue());
    assertEquals(
        "abc-123",
        DataReferenceJson.decode(descriptor.required("childInput"))
            .inlineValue()
            .required("seed")
            .textValue());
    assertTrue(
        dispatched.events().stream()
            .anyMatch(event -> event.type() == ExecutionEventType.OPERATION_DISPATCHED));

    StartExecutionCommand childStart = subworkflowChildStartCommand(launch);
    Harness childHarness = new Harness(childStart.key(), twoBundleResolver(parent, child));
    var childFinal = runToTerminal(childHarness, childHarness.apply(childStart, "1".repeat(64)));
    assertEquals(ExecutionPhase.COMPLETED, childFinal.aggregate().state().phase());
    assertEquals(
        "abc-123",
        childFinal.aggregate().state().data().inlineValue().required("childSaw").textValue());

    var resumed =
        parentHarness.apply(
            subworkflowParentResumeCommand(
                launch,
                new OperationObservation(
                    OperationObservationStatus.SUCCEEDED,
                    childFinal.aggregate().state().data(),
                    null,
                    null),
                NOW.plusSeconds(5)),
            fingerprint(dispatched.aggregate().revision()));
    assertTrue(
        resumed.events().stream()
            .anyMatch(event -> event.type() == ExecutionEventType.OPERATION_COMPLETED));
    var parentFinal = runToTerminal(parentHarness, resumed);
    assertEquals(ExecutionPhase.COMPLETED, parentFinal.aggregate().state().phase());
    assertEquals(
        "abc-123",
        parentFinal.aggregate().state().data().inlineValue().required("childSaw").textValue());
  }

  @Test
  void pausingOrCancellingTheParentPropagatesToTheChildInsteadOfOrphaningIt() throws Exception {
    WorkflowDefinitionBundle child = subworkflowChildBundle();
    byte[] parentSource =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: subworkflow-parent-control
          version: '1.0.0'
        do:
          - invoke:
              run:
                workflow:
                  namespace: evidence
                  name: subworkflow-child
                  version: '2.0.0'
                  input:
                    seed: '${ .seed }'
        """
            .getBytes(StandardCharsets.UTF_8);
    WorkflowDefinitionBundle parent = subworkflowParentBundle(parentSource, child);
    ExecutionKey parentKey =
        new ExecutionKey(TENANT, new WorkflowExecutionId("subworkflow-parent-control"));
    Harness parentHarness = new Harness(parentKey, twoBundleResolver(parent, child));

    var started =
        parentHarness.apply(
            new StartExecutionCommand(
                "start-subworkflow-parent-control",
                parentKey,
                parent.reference(),
                DataReferences.inline(JSON.readTree("{\"seed\":\"do-not-orphan-me\"}")),
                USER,
                NOW),
            "1".repeat(64));
    var dispatched =
        parentHarness.apply(
            started.followUpCommands().getFirst(), fingerprint(started.aggregate().revision()));
    WorkflowEffect launch = dispatched.outbox().getFirst();
    assertEquals(WorkflowEffectType.START_SUBWORKFLOW, launch.type());

    StartExecutionCommand childStart = subworkflowChildStartCommand(launch);
    Harness childHarness = new Harness(childStart.key(), twoBundleResolver(parent, child));
    var childStarted = childHarness.apply(childStart, "1".repeat(64));
    assertEquals(ExecutionPhase.RUNNING, childStarted.aggregate().state().phase());

    // Pausing the parent must actually pause the child, not merely stop watching it.
    var paused =
        parentHarness.apply(
            new ControlExecutionCommand(
                "pause-subworkflow-parent",
                parentKey,
                ExecutionControlAction.PAUSE,
                USER,
                NOW.plusSeconds(1)),
            fingerprint(dispatched.aggregate().revision()));
    assertEquals(ExecutionPhase.PAUSED, paused.aggregate().state().phase());
    WorkflowEffect pauseControl =
        onlySubworkflowEffect(
            paused.outbox(), WorkflowEffectType.CONTROL_SUBWORKFLOW, ExecutionControlAction.PAUSE);
    var childPaused =
        childHarness.apply(
            subworkflowChildControlCommand(pauseControl),
            fingerprint(childStarted.aggregate().revision()));
    assertEquals(ExecutionPhase.PAUSED, childPaused.aggregate().state().phase());

    // Resuming the parent must resume the child too, not redispatch (start) it again.
    var resumedParent =
        parentHarness.apply(
            new ControlExecutionCommand(
                "resume-subworkflow-parent",
                parentKey,
                ExecutionControlAction.RESUME,
                USER,
                NOW.plusSeconds(2)),
            fingerprint(paused.aggregate().revision()));
    assertEquals(ExecutionPhase.RUNNING, resumedParent.aggregate().state().phase());
    assertTrue(
        resumedParent.outbox().stream()
            .noneMatch(effect -> effect.type() == WorkflowEffectType.START_SUBWORKFLOW),
        "Resuming a waiting parent must not re-launch its child");
    WorkflowEffect resumeControl =
        onlySubworkflowEffect(
            resumedParent.outbox(),
            WorkflowEffectType.CONTROL_SUBWORKFLOW,
            ExecutionControlAction.RESUME);
    var childResumed =
        childHarness.apply(
            subworkflowChildControlCommand(resumeControl),
            fingerprint(childPaused.aggregate().revision()));
    assertEquals(ExecutionPhase.RUNNING, childResumed.aggregate().state().phase());

    // Cancelling the parent must cancel the child - the whole point of propagation.
    var cancelRequested =
        parentHarness.apply(
            new ControlExecutionCommand(
                "cancel-subworkflow-parent",
                parentKey,
                ExecutionControlAction.CANCEL,
                USER,
                NOW.plusSeconds(3)),
            fingerprint(resumedParent.aggregate().revision()));
    assertEquals(ExecutionPhase.CANCEL_REQUESTED, cancelRequested.aggregate().state().phase());
    WorkflowEffect cancelControl =
        onlySubworkflowEffect(
            cancelRequested.outbox(),
            WorkflowEffectType.CONTROL_SUBWORKFLOW,
            ExecutionControlAction.CANCEL);
    var childCancelled =
        childHarness.apply(
            subworkflowChildControlCommand(cancelControl),
            fingerprint(childResumed.aggregate().revision()));
    assertEquals(ExecutionPhase.CANCELLED, childCancelled.aggregate().state().phase());

    var cancelAcknowledged =
        parentHarness.apply(
            subworkflowParentResumeCommand(
                launch,
                new OperationObservation(
                    OperationObservationStatus.CANCELLED,
                    null,
                    new WorkflowError(
                        "https://forwardmeasure.com/oks/errors/subworkflow/cancelled",
                        499,
                        childStart.key().canonical(),
                        "Subworkflow cancelled",
                        "Child workflow execution "
                            + childStart.key().canonical()
                            + " was cancelled"),
                    null),
                NOW.plusSeconds(4)),
            fingerprint(cancelRequested.aggregate().revision()));
    var parentFinal = runToTerminal(parentHarness, cancelAcknowledged);
    assertEquals(ExecutionPhase.CANCELLED, parentFinal.aggregate().state().phase());
  }

  @Test
  void subworkflowInsideAForkBranchWaitsWhileTheOtherBranchProceeds() throws Exception {
    WorkflowDefinitionBundle child = subworkflowChildBundle();
    byte[] parentSource =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: subworkflow-fork-parent
          version: '1.0.0'
        do:
          - parallel:
              fork:
                branches:
                  - invoke:
                      run:
                        workflow:
                          namespace: evidence
                          name: subworkflow-child
                          version: '2.0.0'
                          input:
                            seed: '${ .seed }'
                  - immediate:
                      set:
                        immediateDone: true
        """
            .getBytes(StandardCharsets.UTF_8);
    WorkflowDefinitionBundle parent = subworkflowParentBundle(parentSource, child);
    ExecutionKey parentKey =
        new ExecutionKey(TENANT, new WorkflowExecutionId("subworkflow-fork-parent"));
    Harness parentHarness = new Harness(parentKey, twoBundleResolver(parent, child));

    var decision =
        parentHarness.apply(
            new StartExecutionCommand(
                "start-subworkflow-fork-parent",
                parentKey,
                parent.reference(),
                DataReferences.inline(JSON.readTree("{\"seed\":\"fork-seed\"}")),
                USER,
                NOW),
            "1".repeat(64));
    List<WorkflowEffect> effects = new ArrayList<>(decision.outbox());
    while (!decision.followUpCommands().isEmpty()) {
      decision =
          parentHarness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
      effects.addAll(decision.outbox());
    }

    ActiveOperationState operation = findOperation(decision.aggregate().state().activeFork());
    assertNotNull(operation, "The invoke branch must be waiting on its subworkflow");
    assertEquals("run-workflow", operation.operationKind());
    ForkBranchState immediateBranch =
        decision.aggregate().state().activeFork().branches().stream()
            .filter(branch -> branch.name().equals("immediate"))
            .findFirst()
            .orElseThrow();
    assertEquals(
        ForkBranchPhase.COMPLETED,
        immediateBranch.phase(),
        "The plain branch must not be blocked by its sibling's subworkflow");

    WorkflowEffect launch =
        effects.stream()
            .filter(effect -> effect.type() == WorkflowEffectType.START_SUBWORKFLOW)
            .findFirst()
            .orElseThrow();
    JsonNode descriptor = launch.payload().inlineValue();
    assertEquals(
        parentKey.canonical(),
        descriptor.required("parentExecutionKey").textValue(),
        "A fork-branch subworkflow must still record the OUTER execution as its parent");

    StartExecutionCommand childStart = subworkflowChildStartCommand(launch);
    Harness childHarness = new Harness(childStart.key(), twoBundleResolver(parent, child));
    var childFinal = runToTerminal(childHarness, childHarness.apply(childStart, "1".repeat(64)));
    assertEquals(ExecutionPhase.COMPLETED, childFinal.aggregate().state().phase());
    assertEquals(
        "fork-seed",
        childFinal.aggregate().state().data().inlineValue().required("childSaw").textValue());

    var resumed =
        parentHarness.apply(
            subworkflowParentResumeCommand(
                launch,
                new OperationObservation(
                    OperationObservationStatus.SUCCEEDED,
                    childFinal.aggregate().state().data(),
                    null,
                    null),
                NOW.plusSeconds(5)),
            fingerprint(decision.aggregate().revision()));
    var parentFinal = runToTerminal(parentHarness, resumed);
    assertEquals(ExecutionPhase.COMPLETED, parentFinal.aggregate().state().phase());
  }

  private static Harness completedHarness() throws Exception {
    Harness harness = new Harness();
    var decision = harness.apply(startCommand(), "1".repeat(64));
    while (!decision.aggregate().state().phase().terminal()) {
      decision =
          harness.apply(
              decision.followUpCommands().getFirst(), fingerprint(decision.aggregate().revision()));
    }
    assertEquals(ExecutionPhase.COMPLETED, decision.aggregate().state().phase());
    return harness;
  }

  private static PurgeExecutionCommand purgeCommand(ActorContext actor) {
    return new PurgeExecutionCommand(
        "purge-execution-1",
        KEY,
        new ExecutionPurgePolicyDecision(
            "decision-execution-1",
            "records-v1",
            KEY,
            NOW,
            NOW.minusSeconds(1),
            false,
            "investigation"),
        actor,
        NOW);
  }

  private static final class Harness {
    private final DurableProcessingKernel<
            ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
        kernel;
    private final ExecutionKey key;
    private DurableAggregate<ExecutionSnapshot> aggregate;

    Harness() {
      this(SOURCE);
    }

    Harness(byte[] source) {
      this(source, List.of());
    }

    Harness(byte[] source, List<ResolvedWorkflowResource> resources) {
      this(source, resources, Duration.ofSeconds(30));
    }

    Harness(
        byte[] source, List<ResolvedWorkflowResource> resources, Duration cancellationGracePeriod) {
      this(source, resources, cancellationGracePeriod, true);
    }

    Harness(
        byte[] source,
        List<ResolvedWorkflowResource> resources,
        Duration cancellationGracePeriod,
        boolean deferredComputationEnabled) {
      this(
          source,
          resources,
          cancellationGracePeriod,
          deferredComputationEnabled,
          WorkflowRuntimeDataAccess.inlineOnly());
    }

    Harness(
        byte[] source,
        List<ResolvedWorkflowResource> resources,
        Duration cancellationGracePeriod,
        boolean deferredComputationEnabled,
        WorkflowRuntimeDataAccess dataAccess) {
      this(
          KEY,
          ignored -> bundle(source, resources),
          cancellationGracePeriod,
          deferredComputationEnabled,
          dataAccess);
    }

    /**
     * Drives one execution keyed at {@code key}, resolving any definition (its own, or - for a
     * {@code run: workflow:} step - a pinned child's) through {@code definitions}. Used to run a
     * parent and a child subworkflow execution side by side against one shared definition
     * catalogue, the same way the real {@code OksTopology} routing does.
     */
    Harness(ExecutionKey key, WorkflowDefinitionResolver definitions) {
      this(key, definitions, Duration.ofSeconds(30), true, WorkflowRuntimeDataAccess.inlineOnly());
    }

    Harness(
        ExecutionKey key,
        WorkflowDefinitionResolver definitions,
        Duration cancellationGracePeriod,
        boolean deferredComputationEnabled,
        WorkflowRuntimeDataAccess dataAccess) {
      this.key = key;
      kernel =
          new DurableProcessingKernel<>(
              new OpenWorkflowCommandMetadata(),
              new WorkflowExecutionEngine(
                  definitions,
                  RUNTIME_ACTOR_ID,
                  "oks-test",
                  cancellationGracePeriod,
                  dataAccess,
                  deferredComputationEnabled));
    }

    DurableDecision<ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
        apply(ExecutionCommand command, String fingerprint) {
      var decision = kernel.decide(key.canonical(), aggregate, command, fingerprint, null);
      aggregate = decision.aggregate();
      return decision;
    }
  }

  private static final class ExternalizingDataAccess implements WorkflowRuntimeDataAccess {
    private final java.util.Map<String, JsonNode> values = new java.util.HashMap<>();

    @Override
    public JsonNode resolve(DataReference reference) {
      if (reference.storage() == DataReference.Storage.INLINE) {
        return reference.inlineValue().deepCopy();
      }
      return values.get(reference.sha256()).deepCopy();
    }

    @Override
    public DataReference reference(JsonNode value) {
      DataReference inline = DataReferences.inline(value);
      values.put(inline.sha256(), value.deepCopy());
      return new DataReference(
          DataReference.Storage.ARTIFACT,
          null,
          URI.create("urn:test:" + inline.sha256()),
          inline.mediaType(),
          inline.sizeBytes(),
          inline.sha256());
    }
  }

  private record RunResult(ExecutionSnapshot snapshot, List<ExecutionHistoryEvent> history) {}
}
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
