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
package com.forwardmeasure.openworkflow.actor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.DeadlineScope;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionStatus;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.persistence.testkit.javadsl.EventSourcedBehaviorTestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class WorkflowEntityTest {
  private static final Instant REQUESTED_AT = Instant.parse("2099-08-15T12:00:00Z");
  private static ActorTestKit actorTestKit;

  @BeforeAll
  static void startActorSystem() {
    actorTestKit = ActorTestKit.create(EventSourcedBehaviorTestKit.config());
  }

  @AfterAll
  static void stopActorSystem() {
    actorTestKit.shutdownTestKit();
  }

  @Test
  void pekkoFsmPersistsToCompletedStateRepliesAndRecovers() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:acme");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var commandId = UUID.randomUUID();
    var testKit = testKit(executionId);

    var result =
        testKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Start(
                    commandId,
                    executionId,
                    actor(tenant, "alice"),
                    setPlan("durable-set"),
                    JsonNodeFactory.instance.objectNode().put("requested", true),
                    REQUESTED_AT,
                    replyTo));

    assertEquals(1, result.events().size());
    assertInstanceOf(EngineEvent.Started.class, result.events().get(0));
    var running = assertInstanceOf(WorkflowState.Running.class, result.state());
    assertEquals(1, running.revision());
    assertEquals(
        ExecutionStatus.RUNNING, result.replyOfType(WorkflowReply.Accepted.class).status());

    var advanced =
        testKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.RunNext(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "engine"),
                    REQUESTED_AT.plusSeconds(1),
                    replyTo));
    assertEquals(3, advanced.events().size());
    assertInstanceOf(EngineEvent.TaskEntered.class, advanced.events().get(0));
    assertInstanceOf(EngineEvent.TaskCompleted.class, advanced.events().get(1));
    assertInstanceOf(EngineEvent.Completed.class, advanced.events().get(2));
    var completed = assertInstanceOf(WorkflowState.Completed.class, advanced.state());
    assertTrue(completed.data().required("accepted").booleanValue());
    assertEquals(4, completed.revision());
    assertEquals(ExecutionStatus.COMPLETED, completed.status());

    var accepted = advanced.replyOfType(WorkflowReply.Accepted.class);
    assertEquals(4, accepted.revision());
    assertEquals(ExecutionStatus.COMPLETED, accepted.status());

    WorkflowState recovered = testKit.restart().state();
    assertEquals(completed, recovered);
    assertEquals(executionId, recovered.executionId());
  }

  @Test
  void pekkoFsmRejectsCrossTenantRoutingWithoutPersisting() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:acme");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var other =
        new ExecutionId(
            com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
                "did:web:forwardmeasure.com:tenant:other"),
            UUID.randomUUID());
    var testKit = testKit(executionId);

    var result =
        testKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Start(
                    UUID.randomUUID(),
                    other,
                    actor(other.tenantId(), "mallory"),
                    setPlan("tenant-isolation"),
                    JsonNodeFactory.instance.objectNode(),
                    REQUESTED_AT,
                    replyTo));

    assertTrue(result.hasNoEvents());
    assertInstanceOf(WorkflowState.New.class, result.state());
    var rejected = result.replyOfType(WorkflowReply.Rejected.class);
    assertEquals("wrong_execution", rejected.code());
    assertEquals(executionId, rejected.executionId());
  }

  @Test
  void duplicateStartReturnsThePersistedReceiptWithoutNewEvents() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:acme");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var commandId = UUID.randomUUID();
    var plan = setPlan("idempotent-start");
    var testKit = testKit(executionId);

    testKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                commandId,
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    var runningDuplicate =
        testKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Start(
                    commandId,
                    executionId,
                    actor(tenant, "alice"),
                    plan,
                    JsonNodeFactory.instance.objectNode(),
                    REQUESTED_AT,
                    replyTo));
    assertTrue(runningDuplicate.hasNoEvents());
    assertEquals(
        ExecutionStatus.RUNNING,
        runningDuplicate.replyOfType(WorkflowReply.Accepted.class).status());
    testKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.RunNext(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "engine"),
                REQUESTED_AT.plusSeconds(1),
                replyTo));
    var duplicate =
        testKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Start(
                    commandId,
                    executionId,
                    actor(tenant, "alice"),
                    plan,
                    JsonNodeFactory.instance.objectNode(),
                    REQUESTED_AT,
                    replyTo));

    assertTrue(duplicate.hasNoEvents());
    var accepted = duplicate.replyOfType(WorkflowReply.Accepted.class);
    assertEquals(4, accepted.revision());
    assertEquals(ExecutionStatus.COMPLETED, accepted.status());
  }

  @Test
  void getStateUsesTheCommonPekkoFsmQueryHandler() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:acme");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var testKit = testKit(executionId);

    var result =
        testKit.<WorkflowReply>runCommand(
            replyTo -> new WorkflowCommand.GetState(executionId, replyTo));

    assertTrue(result.hasNoEvents());
    var snapshot = result.replyOfType(WorkflowReply.StateSnapshot.class);
    assertEquals(executionId, snapshot.executionId());
    assertEquals(0, snapshot.revision());
    assertEquals(ExecutionStatus.NEW, snapshot.status());
  }

  @Test
  void pauseAndResumeAreDurablePekkoFsmTransitions() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:controls");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var testKit = testKit(executionId);
    testKit.initialize(
        new WorkflowState.Running(
            executionId,
            setPlan("pause-resume"),
            JsonNodeFactory.instance.objectNode().put("position", 1),
            0,
            1,
            Set.of()));
    UUID pauseId = UUID.randomUUID();

    var pausedResult =
        testKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Pause(
                    pauseId, executionId, actor(tenant, "operator"), REQUESTED_AT, replyTo));

    assertEquals(2, pausedResult.events().size());
    assertInstanceOf(EngineEvent.PauseRequested.class, pausedResult.events().get(0));
    assertInstanceOf(EngineEvent.Paused.class, pausedResult.events().get(1));
    var paused = assertInstanceOf(WorkflowState.Paused.class, pausedResult.state());
    assertEquals(ExecutionStatus.PAUSED, paused.status());
    assertTrue(paused.processedCommands().contains(pauseId));

    UUID resumeId = UUID.randomUUID();
    var resumedResult =
        testKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Resume(
                    resumeId,
                    executionId,
                    actor(tenant, "operator"),
                    REQUESTED_AT.plusSeconds(1),
                    replyTo));

    assertEquals(1, resumedResult.events().size());
    assertInstanceOf(EngineEvent.Resumed.class, resumedResult.events().getFirst());
    var running = assertInstanceOf(WorkflowState.Running.class, resumedResult.state());
    assertEquals(ExecutionStatus.RUNNING, running.status());
    assertEquals(running, testKit.restart().state());
  }

  @Test
  void cancelIsDurableFromPausedAndIdempotentAfterCancellation() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:cancel");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var testKit = testKit(executionId);
    testKit.initialize(
        new WorkflowState.Paused(
            executionId,
            setPlan("cancel-paused"),
            JsonNodeFactory.instance.objectNode().put("position", 1),
            0,
            5,
            Set.of()));
    UUID cancelId = UUID.randomUUID();
    var cancelledResult =
        testKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Cancel(
                    cancelId, executionId, actor(tenant, "operator"), REQUESTED_AT, replyTo));

    assertEquals(2, cancelledResult.events().size());
    assertInstanceOf(EngineEvent.CancellationRequested.class, cancelledResult.events().get(0));
    assertInstanceOf(EngineEvent.Cancelled.class, cancelledResult.events().get(1));
    var cancelled = assertInstanceOf(WorkflowState.Cancelled.class, cancelledResult.state());
    assertEquals(ExecutionStatus.CANCELLED, cancelled.status());

    var duplicate =
        testKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Cancel(
                    cancelId, executionId, actor(tenant, "operator"), REQUESTED_AT, replyTo));
    assertTrue(duplicate.hasNoEvents());
    assertEquals(
        ExecutionStatus.CANCELLED, duplicate.replyOfType(WorkflowReply.Accepted.class).status());
    assertEquals(cancelled, testKit.restart().state());
  }

  @Test
  void controlsRejectIllegalNewAndTerminalTransitionsWithoutEvents() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:illegal-controls");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var testKit = testKit(executionId);

    var pauseNew =
        testKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Pause(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "operator"),
                    REQUESTED_AT,
                    replyTo));

    assertTrue(pauseNew.hasNoEvents());
    assertEquals("not_running", pauseNew.replyOfType(WorkflowReply.Rejected.class).code());
  }

  @Test
  void durableCommandReceiptsRetainTheNewestBoundedWindow() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:receipts");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var receipts = new LinkedHashSet<UUID>();
    UUID oldest = null;
    UUID newest = null;
    for (int index = 0; index < WorkflowState.MAX_PROCESSED_COMMANDS + 3; index++) {
      UUID receipt = new UUID(0, index + 1L);
      if (oldest == null) oldest = receipt;
      newest = receipt;
      receipts.add(receipt);
    }

    var state =
        new WorkflowState.Running(
            executionId,
            setPlan("bounded-receipts"),
            JsonNodeFactory.instance.objectNode(),
            0,
            1,
            receipts);

    assertEquals(WorkflowState.MAX_PROCESSED_COMMANDS, state.processedCommands().size());
    assertTrue(!state.processedCommands().contains(oldest));
    assertTrue(state.processedCommands().contains(newest));
  }

  @Test
  void sequentialTasksCanPauseAtADurableBoundaryThenResumeAndComplete() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:interleaving");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var testKit = testKit(executionId);
    testKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                sequentialSetPlan(),
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));

    var first =
        testKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.RunNext(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "engine"),
                    REQUESTED_AT.plusSeconds(1),
                    replyTo));
    var running = assertInstanceOf(WorkflowState.Running.class, first.state());
    assertEquals(1, running.nextStep());
    assertEquals(3, running.revision());
    assertEquals(2, first.events().size());

    testKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Pause(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "operator"),
                REQUESTED_AT.plusSeconds(2),
                replyTo));
    assertInstanceOf(WorkflowState.Paused.class, testKit.restart().state());
    testKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Resume(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "operator"),
                REQUESTED_AT.plusSeconds(3),
                replyTo));

    var second =
        testKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.RunNext(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "engine"),
                    REQUESTED_AT.plusSeconds(4),
                    replyTo));
    var completed = assertInstanceOf(WorkflowState.Completed.class, second.state());
    assertEquals(2, completed.data().required("second").intValue());
    assertEquals(ExecutionStatus.COMPLETED, completed.status());
  }

  @Test
  void executesNestedDoAndNormativeDataFlowThroughDurableFrames() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:data-flow");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: nested-data-flow
                  version: '1.0.0'
                input:
                  schema:
                    format: json
                    document:
                      type: object
                      required: [value, enabled]
                  from: '${ {value: .value, enabled: .enabled} }'
                do:
                  - nested:
                      do:
                        - derive:
                            input:
                              from: '${ {value: .value} }'
                            set:
                              value: '${ .value + 1 }'
                            export:
                              as: '${ $context + {seen: .value} }'
                        - skipped:
                            if: '${ false }'
                            set:
                              forbidden: true
                      output:
                        as: '${ {nested: .value} }'
                      export:
                        as: '${ $context + {outer: .nested} }'
                  - finish:
                      input:
                        from: '${ {value: .nested, seen: $context.seen, outer: $context.outer} }'
                      set:
                        result: '${ .value + .seen }'
                        outer: '${ .outer }'
                      then: end
                output:
                  as: '${ {answer: .result, outer: .outer} }'
                  schema:
                    format: json
                    document:
                      type: object
                      required: [answer, outer]
                """
                    .getBytes(StandardCharsets.UTF_8));
    var testKit = testKit(executionId);
    var input =
        JsonNodeFactory.instance
            .objectNode()
            .put("value", 1)
            .put("enabled", true)
            .put("discard", true);

    var started =
        testKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Start(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "alice"),
                    plan,
                    input,
                    REQUESTED_AT,
                    replyTo));
    var initial = assertInstanceOf(WorkflowState.Running.class, started.state());
    assertTrue(!initial.data().has("discard"));
    assertEquals(input, initial.rawWorkflowInput());

    WorkflowState state = initial;
    for (int step = 0; step < 5; step++) {
      long offset = step + 1L;
      var advanced =
          testKit.<WorkflowReply>runCommand(
              replyTo ->
                  new WorkflowCommand.RunNext(
                      UUID.randomUUID(),
                      executionId,
                      actor(tenant, "engine"),
                      REQUESTED_AT.plusSeconds(offset),
                      replyTo));
      state = advanced.state();
    }

    var completed = assertInstanceOf(WorkflowState.Completed.class, state);
    assertEquals(4, completed.data().required("answer").intValue());
    assertEquals(2, completed.data().required("outer").intValue());
    assertEquals(completed, testKit.restart().state());
  }

  @Test
  void falseDoConditionSkipsItsEntireNestedScope() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:skip-do");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: skip-do
                  version: '1.0.0'
                do:
                  - skipped:
                      if: '${ false }'
                      do:
                        - forbidden:
                            set:
                              forbidden: true
                  - finish:
                      set:
                        accepted: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    WorkflowState.Running skipped =
        assertInstanceOf(
            WorkflowState.Running.class,
            runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1)).state());
    assertTrue(!skipped.data().has("forbidden"));
    WorkflowState.Completed completed =
        assertInstanceOf(
            WorkflowState.Completed.class,
            runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(2)).state());
    assertTrue(completed.data().required("accepted").booleanValue());
    assertTrue(!completed.data().has("forbidden"));
  }

  @Test
  void rejectsInvalidWorkflowInputWithoutMisclassifyingTheDefinition() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:input-validation");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: input-validation
                  version: '1.0.0'
                input:
                  schema:
                    format: json
                    document:
                      type: object
                      required: [requiredValue]
                do:
                  - accepted:
                      set:
                        accepted: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var testKit = testKit(executionId);

    var rejected =
        testKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Start(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "alice"),
                    plan,
                    JsonNodeFactory.instance.objectNode(),
                    REQUESTED_AT,
                    replyTo));

    assertTrue(rejected.hasNoEvents());
    assertInstanceOf(WorkflowState.New.class, rejected.state());
    assertEquals("invalid_input", rejected.replyOfType(WorkflowReply.Rejected.class).code());
  }

  @Test
  void pauseResumeAndCancelPreserveAnActiveNestedTaskFrame() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:nested-controls");
    var plan = nestedControlPlan();

    var resumableId = new ExecutionId(tenant, UUID.randomUUID());
    var resumable = testKit(resumableId);
    resumable.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                resumableId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    var entered =
        resumable.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.RunNext(
                    UUID.randomUUID(),
                    resumableId,
                    actor(tenant, "engine"),
                    REQUESTED_AT.plusSeconds(1),
                    replyTo));
    var runningInsideDo = assertInstanceOf(WorkflowState.Running.class, entered.state());
    assertEquals(1, runningInsideDo.taskStack().size());

    var pauseResult =
        resumable.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Pause(
                    UUID.randomUUID(),
                    resumableId,
                    actor(tenant, "operator"),
                    REQUESTED_AT.plusSeconds(2),
                    replyTo));
    assertEquals(
        List.of("/do/0/nested"),
        assertInstanceOf(EngineEvent.Paused.class, pauseResult.events().get(1)).activeTaskPaths());
    var recoveredPaused = assertInstanceOf(WorkflowState.Paused.class, resumable.restart().state());
    assertEquals(runningInsideDo.taskStack(), recoveredPaused.taskStack());
    var resumeResult =
        resumable.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Resume(
                    UUID.randomUUID(),
                    resumableId,
                    actor(tenant, "operator"),
                    REQUESTED_AT.plusSeconds(3),
                    replyTo));
    assertEquals(
        List.of("/do/0/nested"),
        assertInstanceOf(EngineEvent.Resumed.class, resumeResult.events().getFirst())
            .activeTaskPaths());
    WorkflowState resumedState = recoveredPaused;
    for (int index = 0; index < 2; index++) {
      long offset = 4L + index;
      var advanced =
          resumable.<WorkflowReply>runCommand(
              replyTo ->
                  new WorkflowCommand.RunNext(
                      UUID.randomUUID(),
                      resumableId,
                      actor(tenant, "engine"),
                      REQUESTED_AT.plusSeconds(offset),
                      replyTo));
      resumedState = advanced.state();
    }
    var completed = assertInstanceOf(WorkflowState.Completed.class, resumedState);
    assertEquals(1, completed.data().required("inside").intValue());

    var cancellableId = new ExecutionId(tenant, UUID.randomUUID());
    var cancellable = testKit(cancellableId);
    cancellable.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                cancellableId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    cancellable.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.RunNext(
                UUID.randomUUID(),
                cancellableId,
                actor(tenant, "engine"),
                REQUESTED_AT.plusSeconds(1),
                replyTo));
    var cancelled =
        cancellable.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Cancel(
                    UUID.randomUUID(),
                    cancellableId,
                    actor(tenant, "operator"),
                    REQUESTED_AT.plusSeconds(2),
                    replyTo));
    assertEquals(
        List.of("/do/0/nested"),
        assertInstanceOf(EngineEvent.Cancelled.class, cancelled.events().get(1)).activeTaskPaths());
    assertInstanceOf(WorkflowState.Cancelled.class, cancelled.state());
    assertEquals(cancelled.state(), cancellable.restart().state());
  }

  @Test
  void namedThenSupportsDeclarationIndependentFlowAndCancellableBackwardLoops() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:then-flow");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: named-then
                  version: '1.0.0'
                do:
                  - route:
                      set:
                        route: true
                      then: target
                  - bypassed:
                      set:
                        bypassed: true
                  - target:
                      set:
                        target: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var testKit = testKit(executionId);
    testKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    testKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.RunNext(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "engine"),
                REQUESTED_AT.plusSeconds(1),
                replyTo));
    var finished =
        testKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.RunNext(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "engine"),
                    REQUESTED_AT.plusSeconds(2),
                    replyTo));
    var completed = assertInstanceOf(WorkflowState.Completed.class, finished.state());
    assertTrue(completed.data().required("target").booleanValue());
    assertTrue(!completed.data().has("bypassed"));

    var declarationIndependentId = new ExecutionId(tenant, UUID.randomUUID());
    var declarationIndependentPlan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: declaration-independent-then
                  version: '1.0.0'
                do:
                  - red:
                      set: { colors: '${ .colors + ["red"] }' }
                      then: green
                  - blue:
                      set: { colors: '${ .colors + ["blue"] }' }
                      then: end
                  - green:
                      set: { colors: '${ .colors + ["green"] }' }
                      then: blue
                """
                    .getBytes(StandardCharsets.UTF_8));
    var declarationIndependent = testKit(declarationIndependentId);
    declarationIndependent.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                declarationIndependentId,
                actor(tenant, "alice"),
                declarationIndependentPlan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    WorkflowState declarationIndependentState = declarationIndependent.getState();
    for (int index = 0; index < 3; index++) {
      int offset = index;
      declarationIndependentState =
          declarationIndependent
              .<WorkflowReply>runCommand(
                  replyTo ->
                      new WorkflowCommand.RunNext(
                          UUID.randomUUID(),
                          declarationIndependentId,
                          actor(tenant, "engine"),
                          REQUESTED_AT.plusSeconds(offset + 1),
                          replyTo))
              .state();
    }
    assertEquals(
        List.of("red", "green", "blue"),
        java.util.stream.StreamSupport.stream(
                assertInstanceOf(WorkflowState.Completed.class, declarationIndependentState)
                    .data()
                    .required("colors")
                    .spliterator(),
                false)
            .map(com.fasterxml.jackson.databind.JsonNode::asText)
            .toList());

    var loopingId = new ExecutionId(tenant, UUID.randomUUID());
    var loopingPlan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: backward-loop
                  version: '1.0.0'
                do:
                  - first:
                      set:
                        count: 1
                  - repeat:
                      set:
                        count: '${ .count + 1 }'
                      then: first
                """
                    .getBytes(StandardCharsets.UTF_8));
    var looping = testKit(loopingId);
    looping.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                loopingId,
                actor(tenant, "alice"),
                loopingPlan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    for (int index = 0; index < 4; index++) {
      int offset = index;
      looping.<WorkflowReply>runCommand(
          replyTo ->
              new WorkflowCommand.RunNext(
                  UUID.randomUUID(),
                  loopingId,
                  actor(tenant, "engine"),
                  REQUESTED_AT.plusSeconds(offset + 1),
                  replyTo));
    }
    assertInstanceOf(WorkflowState.Running.class, looping.getState());
    assertEquals(2, looping.getState().data().required("count").asInt());
    var cancelled =
        looping.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Cancel(
                    UUID.randomUUID(),
                    loopingId,
                    actor(tenant, "alice"),
                    REQUESTED_AT.plusSeconds(5),
                    replyTo));
    assertInstanceOf(WorkflowState.Cancelled.class, cancelled.state());
  }

  @Test
  void replaysEveryPersistedPrefixOfANestedMilestoneOneExecution() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:prefix-replay");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var testKit = testKit(executionId);
    var events = new java.util.ArrayList<EngineEvent>();

    events.addAll(
        testKit
            .<WorkflowReply>runCommand(
                replyTo ->
                    new WorkflowCommand.Start(
                        UUID.randomUUID(),
                        executionId,
                        actor(tenant, "alice"),
                        nestedControlPlan(),
                        JsonNodeFactory.instance.objectNode(),
                        REQUESTED_AT,
                        replyTo))
            .events());
    for (int index = 0; index < 3; index++) {
      long offset = index + 1L;
      events.addAll(
          testKit
              .<WorkflowReply>runCommand(
                  replyTo ->
                      new WorkflowCommand.RunNext(
                          UUID.randomUUID(),
                          executionId,
                          actor(tenant, "engine"),
                          REQUESTED_AT.plusSeconds(offset),
                          replyTo))
              .events());
    }
    assertEquals(6, events.size());

    for (int prefix = 1; prefix <= events.size(); prefix++) {
      testKit.clear();
      testKit.initialize(events.subList(0, prefix).toArray());
      WorkflowState recovered = testKit.restart().state();
      assertEquals(prefix, recovered.revision());
      assertEquals(
          prefix == events.size() ? ExecutionStatus.COMPLETED : ExecutionStatus.RUNNING,
          recovered.status());
    }
  }

  @Test
  void shardingEntityIdentityRoundTripsTenantAndExecution() {
    var executionId =
        new ExecutionId(
            com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
                "did:web:forwardmeasure.com:tenant:sharding:with:colons"),
            UUID.fromString("01234567-89ab-cdef-0123-456789abcdef"));

    assertEquals(executionId, ExecutionId.fromEntityId(executionId.entityId()));
  }

  @Test
  void switchSelectsConditionalBeforeDefaultAndRecoversChosenCursor() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:switch");
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: durable-switch
                  version: '1.0.0'
                do:
                  - choose:
                      switch:
                        - fallback:
                            then: other
                        - red:
                            when: '${ .color == "red" }'
                            then: selected
                      then: end
                  - selected:
                      set:
                        result: selected
                      then: end
                  - other:
                      set:
                        result: other
                      then: end
                """
                    .getBytes(StandardCharsets.UTF_8));

    var redId = new ExecutionId(tenant, UUID.randomUUID());
    var red = testKit(redId);
    red.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                redId,
                actor(tenant, "switch-red"),
                plan,
                JsonNodeFactory.instance.objectNode().put("color", "red"),
                REQUESTED_AT,
                replyTo));
    var selected =
        red.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.RunNext(
                    UUID.randomUUID(),
                    redId,
                    actor(tenant, "engine"),
                    REQUESTED_AT.plusSeconds(1),
                    replyTo));
    assertEquals(
        ExecutionStatus.RUNNING, selected.replyOfType(WorkflowReply.Accepted.class).status());
    assertEquals(selected.state(), red.restart().state());
    var redCompleted =
        red.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.RunNext(
                    UUID.randomUUID(),
                    redId,
                    actor(tenant, "engine"),
                    REQUESTED_AT.plusSeconds(2),
                    replyTo));
    assertEquals("selected", redCompleted.state().data().required("result").textValue());

    var blueId = new ExecutionId(tenant, UUID.randomUUID());
    var blue = testKit(blueId);
    blue.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                blueId,
                actor(tenant, "switch-blue"),
                plan,
                JsonNodeFactory.instance.objectNode().put("color", "blue"),
                REQUESTED_AT,
                replyTo));
    blue.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.RunNext(
                UUID.randomUUID(),
                blueId,
                actor(tenant, "engine"),
                REQUESTED_AT.plusSeconds(1),
                replyTo));
    var blueCompleted =
        blue.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.RunNext(
                    UUID.randomUUID(),
                    blueId,
                    actor(tenant, "engine"),
                    REQUESTED_AT.plusSeconds(2),
                    replyTo));
    assertEquals("other", blueCompleted.state().data().required("result").textValue());
  }

  @Test
  void forIterationsAreDurablePausableAndCancellableWorkflowBoundaries() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:for-controls");
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: durable-for
                  version: '1.0.0'
                do:
                  - accumulate:
                      for:
                        each: value
                        in: '${ .values }'
                        at: position
                      while: '${ $position < 2 }'
                      do:
                        - add:
                            set:
                              values: []
                              sum: '${ .sum + $value }'
                              lastPosition: '${ $position }'
                """
                    .getBytes(StandardCharsets.UTF_8));
    var input = JsonNodeFactory.instance.objectNode();
    input.set("values", JsonNodeFactory.instance.arrayNode().add(2).add(4).add(8));
    input.put("sum", 0);

    var resumableId = new ExecutionId(tenant, UUID.randomUUID());
    var resumable = testKit(resumableId);
    resumable.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                resumableId,
                actor(tenant, "alice"),
                plan,
                input,
                REQUESTED_AT,
                replyTo));
    var entered =
        resumable.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.RunNext(
                    UUID.randomUUID(),
                    resumableId,
                    actor(tenant, "engine"),
                    REQUESTED_AT.plusSeconds(1),
                    replyTo));
    assertInstanceOf(EngineEvent.ForEntered.class, entered.events().getFirst());
    var insideFirst = assertInstanceOf(WorkflowState.Running.class, entered.state());
    assertEquals(0, insideFirst.taskStack().getLast().iterationIndex());
    assertEquals(3, insideFirst.taskStack().getLast().collection().size());

    resumable.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Pause(
                UUID.randomUUID(),
                resumableId,
                actor(tenant, "operator"),
                REQUESTED_AT.plusSeconds(2),
                replyTo));
    var paused = assertInstanceOf(WorkflowState.Paused.class, resumable.restart().state());
    assertEquals(insideFirst.taskStack(), paused.taskStack());
    resumable.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Resume(
                UUID.randomUUID(),
                resumableId,
                actor(tenant, "operator"),
                REQUESTED_AT.plusSeconds(3),
                replyTo));

    resumable.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.RunNext(
                UUID.randomUUID(),
                resumableId,
                actor(tenant, "engine"),
                REQUESTED_AT.plusSeconds(4),
                replyTo));
    var advanced =
        resumable.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.RunNext(
                    UUID.randomUUID(),
                    resumableId,
                    actor(tenant, "engine"),
                    REQUESTED_AT.plusSeconds(5),
                    replyTo));
    assertInstanceOf(EngineEvent.ForIterationAdvanced.class, advanced.events().getFirst());
    var insideSecond = assertInstanceOf(WorkflowState.Running.class, resumable.restart().state());
    assertEquals(1, insideSecond.taskStack().getLast().iterationIndex());
    assertEquals(3, insideSecond.taskStack().getLast().collection().size());

    resumable.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.RunNext(
                UUID.randomUUID(),
                resumableId,
                actor(tenant, "engine"),
                REQUESTED_AT.plusSeconds(6),
                replyTo));
    var completedResult =
        resumable.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.RunNext(
                    UUID.randomUUID(),
                    resumableId,
                    actor(tenant, "engine"),
                    REQUESTED_AT.plusSeconds(7),
                    replyTo));
    var completed = assertInstanceOf(WorkflowState.Completed.class, completedResult.state());
    assertEquals(6, completed.data().required("sum").intValue());
    assertEquals(1, completed.data().required("lastPosition").intValue());
    assertEquals(completed, resumable.restart().state());

    var cancellableId = new ExecutionId(tenant, UUID.randomUUID());
    var cancellable = testKit(cancellableId);
    cancellable.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                cancellableId,
                actor(tenant, "alice"),
                plan,
                input,
                REQUESTED_AT,
                replyTo));
    cancellable.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.RunNext(
                UUID.randomUUID(),
                cancellableId,
                actor(tenant, "engine"),
                REQUESTED_AT.plusSeconds(1),
                replyTo));
    var cancelled =
        cancellable.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Cancel(
                    UUID.randomUUID(),
                    cancellableId,
                    actor(tenant, "operator"),
                    REQUESTED_AT.plusSeconds(2),
                    replyTo));
    assertInstanceOf(WorkflowState.Cancelled.class, cancelled.state());
    assertEquals(cancelled.state(), cancellable.restart().state());
  }

  @Test
  void nestedForScopesBindVariablesAndEmptyCollectionsContinue() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:nested-for");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: nested-for
                  version: '1.0.0'
                do:
                  - outerLoop:
                      for:
                        each: outer
                        at: outerIndex
                        in:
                          - name: a
                          - name: b
                      do:
                        - innerLoop:
                            for:
                              each: inner
                              at: innerIndex
                              in:
                                - number: 1
                                - number: 2
                            do:
                              - capture:
                                  set:
                                    outer: '${ $outer.name }'
                                    outerIndex: '${ $outerIndex }'
                                    inner: '${ $inner.number }'
                                    innerIndex: '${ $innerIndex }'
                  - emptyLoop:
                      for:
                        in: []
                      do:
                        - forbidden:
                            set:
                              forbidden: true
                  - finish:
                      set: '${ . + {finished: true} }'
                """
                    .getBytes(StandardCharsets.UTF_8));
    var testKit = testKit(executionId);
    testKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));

    WorkflowState state = testKit.getState();
    for (int boundary = 0; boundary < 30 && state.status() == ExecutionStatus.RUNNING; boundary++) {
      long offset = boundary + 1L;
      state =
          testKit
              .<WorkflowReply>runCommand(
                  replyTo ->
                      new WorkflowCommand.RunNext(
                          UUID.randomUUID(),
                          executionId,
                          actor(tenant, "engine"),
                          REQUESTED_AT.plusSeconds(offset),
                          replyTo))
              .state();
    }

    var completed = assertInstanceOf(WorkflowState.Completed.class, state);
    assertEquals("b", completed.data().required("outer").textValue());
    assertEquals(1, completed.data().required("outerIndex").intValue());
    assertEquals(2, completed.data().required("inner").intValue());
    assertEquals(1, completed.data().required("innerIndex").intValue());
    assertTrue(completed.data().required("finished").booleanValue());
    assertTrue(!completed.data().has("forbidden"));
    assertEquals(completed, testKit.restart().state());
  }

  @Test
  void durableWaitCanPauseResumeRecoverElapseAndCancel() throws Exception {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:wait-controls");
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: durable-wait
                  version: '1.0.0'
                do:
                  - delay:
                      wait:
                        minutes: 1
                        milliseconds: 250
                  - finish:
                      set:
                        done: true
                """
                    .getBytes(StandardCharsets.UTF_8));

    var resumableId = new ExecutionId(tenant, UUID.randomUUID());
    var resumable = automaticTestKit(resumableId);
    resumable.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                resumableId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    Instant waitStarted = REQUESTED_AT.plusSeconds(86_400);
    var scheduled =
        resumable.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.RunNext(
                    UUID.randomUUID(), resumableId, actor(tenant, "engine"), waitStarted, replyTo));
    var waiting = assertInstanceOf(WorkflowState.Waiting.class, scheduled.state());
    assertEquals(waitStarted.plusSeconds(60).plusMillis(250), waiting.deadline());
    assertTrue(waiting.taskStack().getLast().waiting());
    assertInstanceOf(EngineEvent.WaitScheduled.class, scheduled.events().getFirst());

    var stale =
        resumable.runCommand(
            new WorkflowCommand.TimerElapsed(
                resumableId,
                waiting.taskStack().getLast().taskPath(),
                waiting.deadline().plusSeconds(1)));
    assertTrue(stale.hasNoEvents());
    resumable.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Pause(
                UUID.randomUUID(),
                resumableId,
                actor(tenant, "operator"),
                waitStarted.plusSeconds(2),
                replyTo));
    var paused = assertInstanceOf(WorkflowState.Paused.class, resumable.restart().state());
    assertTrue(paused.taskStack().getLast().waiting());
    var resumed =
        resumable.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Resume(
                    UUID.randomUUID(),
                    resumableId,
                    actor(tenant, "operator"),
                    waitStarted.plusSeconds(3),
                    replyTo));
    var waitingAgain = assertInstanceOf(WorkflowState.Waiting.class, resumed.state());
    assertEquals(waiting.deadline(), waitingAgain.deadline());

    var elapsed =
        resumable.runCommand(
            new WorkflowCommand.TimerElapsed(
                resumableId, waiting.taskStack().getLast().taskPath(), waiting.deadline()));
    assertInstanceOf(EngineEvent.TaskCompleted.class, elapsed.events().getFirst());
    assertInstanceOf(WorkflowState.Running.class, elapsed.state());
    long completionTimeout = System.nanoTime() + Duration.ofSeconds(2).toNanos();
    WorkflowState completed = resumable.getState();
    while (completed.status() != ExecutionStatus.COMPLETED
        && System.nanoTime() < completionTimeout) {
      Thread.sleep(10);
      completed = resumable.getState();
    }
    assertInstanceOf(WorkflowState.Completed.class, completed);
    assertTrue(completed.data().required("done").booleanValue());
    assertEquals(completed, resumable.restart().state());

    var cancellableId = new ExecutionId(tenant, UUID.randomUUID());
    var cancellable = testKit(cancellableId);
    cancellable.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                cancellableId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    cancellable.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.RunNext(
                UUID.randomUUID(), cancellableId, actor(tenant, "engine"), waitStarted, replyTo));
    var cancelled =
        cancellable.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Cancel(
                    UUID.randomUUID(),
                    cancellableId,
                    actor(tenant, "operator"),
                    waitStarted.plusSeconds(2),
                    replyTo));
    assertInstanceOf(WorkflowState.Cancelled.class, cancelled.state());
    assertEquals(cancelled.state(), cancellable.restart().state());
  }

  @Test
  void executesLiteralExpressionAndInlineWaitDurations() throws Exception {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:wait-forms");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: wait-forms
                  version: '1.0.0'
                do:
                  - literal:
                      wait: PT30S
                  - expression:
                      wait: '${ .delay }'
                  - inline:
                      wait:
                        seconds: 1
                """
                    .getBytes(StandardCharsets.UTF_8));
    var input = JsonNodeFactory.instance.objectNode().put("delay", "PT2M");
    var kit = automaticTestKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                input,
                REQUESTED_AT,
                replyTo));

    Instant anchor = REQUESTED_AT.plusSeconds(172_800);
    long[] expectedSeconds = {30, 120, 1};
    WorkflowState state =
        kit.<WorkflowReply>runCommand(
                replyTo ->
                    new WorkflowCommand.RunNext(
                        UUID.randomUUID(), executionId, actor(tenant, "engine"), anchor, replyTo))
            .state();
    Instant requestedAt = anchor;
    for (int index = 0; index < expectedSeconds.length; index++) {
      var waiting = assertInstanceOf(WorkflowState.Waiting.class, state);
      assertEquals(requestedAt.plusSeconds(expectedSeconds[index]), waiting.deadline());
      requestedAt = waiting.deadline();
      state =
          kit.runCommand(
                  new WorkflowCommand.TimerElapsed(
                      executionId, waiting.taskStack().getLast().taskPath(), waiting.deadline()))
              .state();
      if (index + 1 < expectedSeconds.length) {
        long waitTimeout = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (state.status() != ExecutionStatus.WAITING && System.nanoTime() < waitTimeout) {
          Thread.sleep(10);
          state = kit.getState();
        }
      }
    }
    long completionTimeout = System.nanoTime() + Duration.ofSeconds(2).toNanos();
    while (state.status() != ExecutionStatus.COMPLETED && System.nanoTime() < completionTimeout) {
      Thread.sleep(10);
      state = kit.getState();
    }
    assertInstanceOf(WorkflowState.Completed.class, state);
    assertEquals(input, state.data());
    assertEquals(state, kit.restart().state());
  }

  @Test
  void pekkoTimerAutomaticallyCompletesADurableWait() throws Exception {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:live-timer");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: live-timer
                  version: '1.0.0'
                do:
                  - delay:
                      wait: PT0.05S
                  - after-wait:
                      set:
                        continued: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var kit = automaticTestKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                Instant.now(),
                replyTo));
    var scheduled =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.RunNext(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "engine"),
                    Instant.now(),
                    replyTo));
    assertInstanceOf(EngineEvent.WaitScheduled.class, scheduled.events().getFirst());

    long timeout = System.nanoTime() + Duration.ofSeconds(2).toNanos();
    WorkflowState state = kit.getState();
    while (state.status() != ExecutionStatus.COMPLETED && System.nanoTime() < timeout) {
      Thread.sleep(10);
      state = kit.getState();
    }
    assertInstanceOf(WorkflowState.Completed.class, state);
    assertTrue(state.data().required("continued").booleanValue());
    assertEquals(state, kit.restart().state());
  }

  @Test
  void workflowTimeoutSurvivesPauseAndRecoveryButCannotReviveCancellation() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:workflow-timeout");
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: workflow-timeout-controls
                  version: '1.0.0'
                timeout:
                  after: PT24H
                do:
                  - work:
                      set:
                        started: true
                """
                    .getBytes(StandardCharsets.UTF_8));

    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    var started =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Start(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "alice"),
                    plan,
                    JsonNodeFactory.instance.objectNode(),
                    REQUESTED_AT,
                    replyTo));
    assertEquals(2, started.events().size());
    assertInstanceOf(EngineEvent.DeadlineScheduled.class, started.events().get(1));
    Instant deadline =
        assertInstanceOf(WorkflowState.Running.class, started.state()).workflowDeadline();
    assertEquals(REQUESTED_AT.plus(Duration.ofHours(24)), deadline);

    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Pause(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "operator"),
                REQUESTED_AT.plusSeconds(1),
                replyTo));
    var paused = assertInstanceOf(WorkflowState.Paused.class, kit.restart().state());
    assertEquals(deadline, paused.workflowDeadline());
    var ignoredWhilePaused =
        kit.runCommand(
            new WorkflowCommand.DeadlineElapsed(
                executionId, DeadlineScope.WORKFLOW, null, deadline));
    assertTrue(ignoredWhilePaused.hasNoEvents());

    var resumed =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Resume(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "operator"),
                    REQUESTED_AT.plusSeconds(2),
                    replyTo));
    assertEquals(
        deadline,
        assertInstanceOf(WorkflowState.Running.class, resumed.state()).workflowDeadline());
    var expired =
        kit.runCommand(
            new WorkflowCommand.DeadlineElapsed(
                executionId, DeadlineScope.WORKFLOW, null, deadline));
    assertInstanceOf(EngineEvent.Failed.class, expired.events().getFirst());
    assertInstanceOf(WorkflowState.Failed.class, expired.state());
    assertEquals(expired.state(), kit.restart().state());

    var cancelledId = new ExecutionId(tenant, UUID.randomUUID());
    var cancelledKit = testKit(cancelledId);
    var cancellable =
        cancelledKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Start(
                    UUID.randomUUID(),
                    cancelledId,
                    actor(tenant, "alice"),
                    plan,
                    JsonNodeFactory.instance.objectNode(),
                    REQUESTED_AT,
                    replyTo));
    Instant cancelledDeadline =
        assertInstanceOf(WorkflowState.Running.class, cancellable.state()).workflowDeadline();
    cancelledKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Cancel(
                UUID.randomUUID(),
                cancelledId,
                actor(tenant, "operator"),
                REQUESTED_AT.plusSeconds(1),
                replyTo));
    var late =
        cancelledKit.runCommand(
            new WorkflowCommand.DeadlineElapsed(
                cancelledId, DeadlineScope.WORKFLOW, null, cancelledDeadline));
    assertTrue(late.hasNoEvents());
    assertInstanceOf(WorkflowState.Cancelled.class, late.state());
  }

  @Test
  void taskTimeoutIsPersistedOnTheDurableNestedTaskFrame() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:task-timeout");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: task-timeout
                  version: '1.0.0'
                do:
                  - bounded:
                      timeout:
                        after: PT24H
                      do:
                        - inner:
                            set:
                              entered: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));

    Instant enteredAt = REQUESTED_AT.plusSeconds(10);
    var entered =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.RunNext(
                    UUID.randomUUID(), executionId, actor(tenant, "engine"), enteredAt, replyTo));
    assertEquals(2, entered.events().size());
    assertInstanceOf(EngineEvent.TaskEntered.class, entered.events().getFirst());
    assertInstanceOf(EngineEvent.DeadlineScheduled.class, entered.events().get(1));
    TaskExecutionFrame frame =
        assertInstanceOf(WorkflowState.Running.class, entered.state()).taskStack().getLast();
    assertEquals(enteredAt.plus(Duration.ofHours(24)), frame.timeoutDeadline());
    assertEquals(
        frame,
        assertInstanceOf(WorkflowState.Running.class, kit.restart().state()).taskStack().getLast());

    var expired =
        kit.runCommand(
            new WorkflowCommand.DeadlineElapsed(
                executionId, DeadlineScope.TASK, frame.taskPath(), frame.timeoutDeadline()));
    assertInstanceOf(EngineEvent.Failed.class, expired.events().getFirst());
    assertInstanceOf(WorkflowState.Failed.class, expired.state());
  }

  @Test
  void pekkoTimerAutomaticallyEnforcesAWorkflowTimeout() throws Exception {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:live-timeout");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: live-workflow-timeout
                  version: '1.0.0'
                timeout:
                  after: PT0.05S
                do:
                  - work:
                      wait: PT1H
                """
                    .getBytes(StandardCharsets.UTF_8));
    var kit = automaticTestKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                Instant.now(),
                replyTo));

    long timeout = System.nanoTime() + Duration.ofSeconds(2).toNanos();
    WorkflowState state = kit.getState();
    while (state.status() != ExecutionStatus.FAILED && System.nanoTime() < timeout) {
      Thread.sleep(10);
      state = kit.getState();
    }
    assertInstanceOf(WorkflowState.Failed.class, state);
    assertEquals(state, kit.restart().state());
  }

  @Test
  void longWorkflowTimeoutUsesBoundedWakeupsWithoutLosingItsAbsoluteDeadline() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:long-timeout");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: long-workflow-timeout
                  version: '1.0.0'
                timeout:
                  after: PT100000H
                do:
                  - work:
                      set:
                        accepted: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var kit = testKit(executionId);
    var started =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Start(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "alice"),
                    plan,
                    JsonNodeFactory.instance.objectNode(),
                    REQUESTED_AT,
                    replyTo));

    assertEquals(
        REQUESTED_AT.plus(Duration.ofHours(100000)),
        assertInstanceOf(WorkflowState.Running.class, started.state()).workflowDeadline());
    assertEquals(started.state(), kit.restart().state());
  }

  @Test
  void structuredRaiseIsCaughtAndExposedToCatchTasks() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:catch");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: structured-catch
                  version: '1.0.0'
                do:
                  - guarded:
                      try:
                        - reject:
                            raise:
                              error:
                                type: https://errors.example/unavailable
                                status: 503
                                title: Temporarily unavailable
                                detail: Please retry
                      catch:
                        errors:
                          with:
                            status: 503
                        as: problem
                        when: $problem.status == 503
                        do:
                          - record:
                              set:
                                caught: ${ $problem.title }
                """
                    .getBytes(StandardCharsets.UTF_8));
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));

    var raised = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(2));
    EngineEvent.ErrorRaised errorRaised =
        assertInstanceOf(EngineEvent.ErrorRaised.class, raised.events().getFirst());
    assertEquals(
        "/do/0/guarded/try/0/reject", errorRaised.error().required("instance").textValue());
    assertInstanceOf(EngineEvent.ErrorCaught.class, raised.events().get(1));
    WorkflowState.Running caught = assertInstanceOf(WorkflowState.Running.class, raised.state());
    assertEquals(TaskExecutionFrame.TryPhase.CATCH, caught.taskStack().getLast().tryPhase());
    assertEquals(caught, kit.restart().state());

    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(3));
    var completed = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(4));
    WorkflowState.Completed state =
        assertInstanceOf(WorkflowState.Completed.class, completed.state());
    assertEquals("Temporarily unavailable", state.data().required("caught").textValue());
  }

  @Test
  void retryDelayRecoversPausesResumesAndEventuallyRunsCatch() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:retry");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: durable-retry
                  version: '1.0.0'
                do:
                  - guarded:
                      try:
                        - reject:
                            raise:
                              error:
                                type: https://errors.example/unavailable
                                status: 503
                      catch:
                        retry:
                          delay: PT24H
                          backoff:
                            exponential: {}
                          limit:
                            attempt:
                              count: 2
                        do:
                          - exhausted:
                              set:
                                attempts: 2
                """
                    .getBytes(StandardCharsets.UTF_8));
    var kit = testKit(executionId);
    Instant startedAt = Instant.now();
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                startedAt,
                replyTo));
    runNext(kit, executionId, tenant, startedAt.plusSeconds(1));
    var scheduled = runNext(kit, executionId, tenant, startedAt.plusSeconds(2));
    assertInstanceOf(EngineEvent.ErrorRaised.class, scheduled.events().getFirst());
    EngineEvent.RetryScheduled retry =
        assertInstanceOf(EngineEvent.RetryScheduled.class, scheduled.events().get(1));
    WorkflowState.Waiting waiting =
        assertInstanceOf(WorkflowState.Waiting.class, scheduled.state());
    assertEquals(2, waiting.taskStack().getLast().attempt());
    assertEquals(waiting, kit.restart().state());

    var paused =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Pause(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "operator"),
                    startedAt.plusSeconds(3),
                    replyTo));
    assertInstanceOf(WorkflowState.Paused.class, paused.state());
    var resumed =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Resume(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "operator"),
                    startedAt.plusSeconds(4),
                    replyTo));
    assertInstanceOf(WorkflowState.Waiting.class, resumed.state());

    var began =
        kit.runCommand(
            new WorkflowCommand.RetryElapsed(executionId, "/do/0/guarded", retry.deadline()));
    assertInstanceOf(EngineEvent.RetryStarted.class, began.events().getFirst());
    WorkflowState state = began.state();
    for (int i = 0; i < 5 && !(state instanceof WorkflowState.Completed); i++) {
      state = runNext(kit, executionId, tenant, retry.deadline().plusSeconds(1 + i)).state();
    }
    var completed = assertInstanceOf(WorkflowState.Completed.class, state);
    assertEquals(2, completed.data().required("attempts").intValue());
    assertEquals(completed, kit.restart().state());
  }

  @Test
  void cancellationDuringRetryIsTerminalAndMakesLateWakeupInert() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:retry-cancel");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: cancel-retry
                  version: '1.0.0'
                do:
                  - guarded:
                      try:
                        - reject:
                            raise:
                              error:
                                type: https://errors.example/retryable
                                status: 503
                      catch:
                        retry:
                          delay: PT24H
                          limit:
                            attempt:
                              count: 3
                """
                    .getBytes(StandardCharsets.UTF_8));
    var kit = testKit(executionId);
    Instant now = Instant.now();
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                now,
                replyTo));
    runNext(kit, executionId, tenant, now.plusSeconds(1));
    EngineEvent.RetryScheduled retry =
        assertInstanceOf(
            EngineEvent.RetryScheduled.class,
            runNext(kit, executionId, tenant, now.plusSeconds(2)).events().get(1));
    var cancelled =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Cancel(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "operator"),
                    now.plusSeconds(3),
                    replyTo));
    assertInstanceOf(WorkflowState.Cancelled.class, cancelled.state());

    var late =
        kit.runCommand(
            new WorkflowCommand.RetryElapsed(executionId, "/do/0/guarded", retry.deadline()));
    assertTrue(late.hasNoEvents());
    assertInstanceOf(WorkflowState.Cancelled.class, late.state());
    assertEquals(late.state(), kit.restart().state());
  }

  @Test
  void retryBackoffStrategiesProduceDurableAbsoluteDeadlines() {
    assertRetryDelays("constant", 10, 10, 10);
    assertRetryDelays("linear", 10, 20, 30);
    assertRetryDelays("exponential", 10, 20, 40);
  }

  @Test
  void nestedRaiseSelectsTheNearestMatchingCatchAndUnwindsInnerFrames() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:nested-catch");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: nested-catch
                  version: '1.0.0'
                do:
                  - outer:
                      try:
                        - inner:
                            try:
                              - reject:
                                  raise:
                                    error:
                                      type: https://errors.example/unavailable
                                      status: 503
                            catch:
                              errors:
                                with:
                                  status: 404
                      catch:
                        errors:
                          with:
                            status: 503
                        as: outerError
                        do:
                          - record:
                              set:
                                caughtBy: ${ $outerError.status }
                """
                    .getBytes(StandardCharsets.UTF_8));
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(2));
    var caught = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(3));
    EngineEvent.ErrorCaught event =
        assertInstanceOf(EngineEvent.ErrorCaught.class, caught.events().get(1));
    assertEquals("/do/0/outer", event.tryTaskPath());
    WorkflowState.Running state = assertInstanceOf(WorkflowState.Running.class, caught.state());
    assertEquals(1, state.taskStack().size());
    assertEquals(TaskExecutionFrame.TryPhase.CATCH, state.taskStack().getLast().tryPhase());
    assertEquals(state, kit.restart().state());

    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(4));
    var complete = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(5));
    assertEquals(
        503,
        assertInstanceOf(WorkflowState.Completed.class, complete.state())
            .data()
            .required("caughtBy")
            .intValue());
  }

  @Test
  void durableForkJoinsBranchOutputsInDeclarationOrderAndRecovers() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:fork-join");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    WorkflowPlan plan = forkPlan("ordered-fork", false);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode().put("seed", true),
                REQUESTED_AT,
                replyTo));

    var entered = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));
    assertInstanceOf(EngineEvent.ForkEntered.class, entered.events().getFirst());
    WorkflowState.Running forked = assertInstanceOf(WorkflowState.Running.class, entered.state());
    assertTrue(forked.taskStack().getLast().forking());

    var firstLane = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(2));
    EngineEvent.ForkBranchAdvanced first =
        assertInstanceOf(EngineEvent.ForkBranchAdvanced.class, firstLane.events().getFirst());
    assertEquals(0, first.branchIndex());
    assertEquals(firstLane.state(), kit.restart().state());

    var joined = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(3));
    assertInstanceOf(EngineEvent.ForkBranchAdvanced.class, joined.events().get(0));
    assertInstanceOf(EngineEvent.TaskCompleted.class, joined.events().get(1));
    assertInstanceOf(EngineEvent.Completed.class, joined.events().get(2));
    WorkflowState.Completed completed =
        assertInstanceOf(WorkflowState.Completed.class, joined.state());
    assertEquals("people", completed.data().get(0).required("kind").textValue());
    assertEquals("organisations", completed.data().get(1).required("kind").textValue());
    assertEquals(completed, kit.restart().state());
  }

  @Test
  void competingForkCompletesWithTheFirstDurablyObservedBranch() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:fork-compete");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                forkPlan("competing-fork", true),
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));

    var won = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(2));
    EngineEvent.ForkBranchAdvanced advanced =
        assertInstanceOf(EngineEvent.ForkBranchAdvanced.class, won.events().get(0));
    assertEquals(0, advanced.winner());
    WorkflowState.Completed completed =
        assertInstanceOf(WorkflowState.Completed.class, won.state());
    assertEquals("people", completed.data().required("kind").textValue());
  }

  @Test
  void workflowPauseResumeAndCancelApplyWhileForkLanesAreActive() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:fork-controls");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                forkPlan("controlled-fork", false),
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));

    var paused =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Pause(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "operator"),
                    REQUESTED_AT.plusSeconds(2),
                    replyTo));
    assertInstanceOf(WorkflowState.Paused.class, paused.state());
    assertEquals(paused.state(), kit.restart().state());

    var resumed =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Resume(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "operator"),
                    REQUESTED_AT.plusSeconds(3),
                    replyTo));
    WorkflowState.Running running = assertInstanceOf(WorkflowState.Running.class, resumed.state());
    assertTrue(running.taskStack().getLast().forking());

    var cancelled =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Cancel(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "operator"),
                    REQUESTED_AT.plusSeconds(4),
                    replyTo));
    assertInstanceOf(WorkflowState.Cancelled.class, cancelled.state());
    assertEquals(cancelled.state(), kit.restart().state());
  }

  @Test
  void forkLanePersistsAndRecoversItsOwnNestedDoStack() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:fork-do");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: fork-nested-do
                  version: '1.0.0'
                do:
                  - parallel:
                      fork:
                        branches:
                          - left:
                              do:
                                - first:
                                    set:
                                      left: 1
                                - second:
                                    set:
                                      left: '${ .left }'
                                      second: 2
                          - right:
                              set:
                                right: 3
                """
                    .getBytes(StandardCharsets.UTF_8));
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));

    var enteredDo = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(2));
    assertInstanceOf(EngineEvent.ForkBranchTaskEntered.class, enteredDo.events().getFirst());
    WorkflowState.Running enteredState =
        assertInstanceOf(WorkflowState.Running.class, enteredDo.state());
    assertEquals(
        1, enteredState.taskStack().getLast().fork().branches().getFirst().taskStack().size());
    assertEquals(enteredState, kit.restart().state());

    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(3));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(4));
    WorkflowState.Running beforeExit =
        assertInstanceOf(
            WorkflowState.Running.class,
            runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(5)).state());
    assertEquals(
        2,
        beforeExit
            .taskStack()
            .getLast()
            .fork()
            .branches()
            .getFirst()
            .data()
            .required("second")
            .intValue());

    var joined = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(6));
    assertInstanceOf(EngineEvent.ForkBranchTaskCompleted.class, joined.events().getFirst());
    WorkflowState.Completed completed =
        assertInstanceOf(WorkflowState.Completed.class, joined.state());
    assertEquals(1, completed.data().get(0).required("left").intValue());
    assertEquals(2, completed.data().get(0).required("second").intValue());
    assertEquals(3, completed.data().get(1).required("right").intValue());
    assertEquals(completed, kit.restart().state());
  }

  @Test
  void forkLanePersistsIterationCollectionIndexAndRecovery() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:fork-for");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: fork-for
                  version: '1.0.0'
                do:
                  - parallel:
                      fork:
                        branches:
                          - accumulate:
                              for:
                                each: value
                                at: position
                                in: '${ .values }'
                              do:
                                - add:
                                    set:
                                      values: '${ .values }'
                                      sum: '${ .sum + $value }'
                                      position: '${ $position }'
                          - right:
                              set:
                                right: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var input = JsonNodeFactory.instance.objectNode();
    input.set("values", JsonNodeFactory.instance.arrayNode().add(1).add(2));
    input.put("sum", 0);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                input,
                REQUESTED_AT,
                replyTo));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));
    var entered = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(2));
    assertInstanceOf(EngineEvent.ForkBranchForEntered.class, entered.events().getFirst());
    assertEquals(entered.state(), kit.restart().state());

    WorkflowState state = entered.state();
    boolean advancedIteration = false;
    for (int boundary = 0; boundary < 12 && state.status() == ExecutionStatus.RUNNING; boundary++) {
      var result = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(3L + boundary));
      advancedIteration |=
          result.events().stream().anyMatch(EngineEvent.ForkBranchForAdvanced.class::isInstance);
      state = result.state();
    }
    assertTrue(advancedIteration);
    WorkflowState.Completed completed = assertInstanceOf(WorkflowState.Completed.class, state);
    assertEquals(3, completed.data().get(0).required("sum").intValue());
    assertEquals(1, completed.data().get(0).required("position").intValue());
    assertTrue(completed.data().get(1).required("right").booleanValue());
    assertEquals(completed, kit.restart().state());
  }

  @Test
  void nestedForkTreeAdvancesDurablyAndJoinsAtEachLevel() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:nested-fork");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
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
                          - third:
                              set:
                                value: third
                """
                    .getBytes(StandardCharsets.UTF_8));
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));

    boolean nestedEntered = false;
    boolean nestedAdvanced = false;
    boolean nestedCompleted = false;
    WorkflowState state = kit.getState();
    for (int boundary = 0; boundary < 12 && state.status() == ExecutionStatus.RUNNING; boundary++) {
      var result = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(2L + boundary));
      nestedEntered |=
          result.events().stream().anyMatch(EngineEvent.ForkNestedEntered.class::isInstance);
      nestedAdvanced |=
          result.events().stream().anyMatch(EngineEvent.ForkNestedBranchAdvanced.class::isInstance);
      nestedCompleted |=
          result.events().stream().anyMatch(EngineEvent.ForkNestedCompleted.class::isInstance);
      state = result.state();
      assertEquals(state, kit.restart().state());
    }
    assertTrue(nestedEntered);
    assertTrue(nestedAdvanced);
    assertTrue(nestedCompleted);
    WorkflowState.Completed completed = assertInstanceOf(WorkflowState.Completed.class, state);
    assertEquals("first", completed.data().get(0).get(0).required("value").textValue());
    assertEquals("second", completed.data().get(0).get(1).required("value").textValue());
    assertEquals("third", completed.data().get(1).required("value").textValue());
  }

  @Test
  void nestedCompetingForkPersistsItsFirstObservedWinner() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:nested-compete");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: nested-compete
                  version: '1.0.0'
                do:
                  - outer:
                      fork:
                        branches:
                          - race:
                              fork:
                                compete: true
                                branches:
                                  - winner:
                                      set:
                                        winner: true
                                  - loser:
                                      set:
                                        winner: false
                          - sibling:
                              set:
                                sibling: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    WorkflowState state = kit.getState();
    for (int boundary = 0; boundary < 12 && state.status() == ExecutionStatus.RUNNING; boundary++) {
      state = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(boundary + 1L)).state();
    }
    WorkflowState.Completed completed = assertInstanceOf(WorkflowState.Completed.class, state);
    assertTrue(completed.data().get(0).required("winner").booleanValue());
    assertTrue(completed.data().get(1).required("sibling").booleanValue());
    assertEquals(completed, kit.restart().state());
  }

  @Test
  void nestedForkBranchesPersistDoAndForFramesAtArbitraryDepth() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:nested-mixed");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: nested-mixed
                  version: '1.0.0'
                do:
                  - outer:
                      fork:
                        branches:
                          - nested:
                              fork:
                                branches:
                                  - sequence:
                                      do:
                                        - write:
                                            set:
                                              sequence: true
                                  - accumulate:
                                      for:
                                        each: value
                                        at: position
                                        in: '${ .values }'
                                      do:
                                        - add:
                                            set:
                                              values: '${ .values }'
                                              sum: '${ .sum + $value }'
                                              position: '${ $position }'
                          - sibling:
                              set:
                                sibling: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var input = JsonNodeFactory.instance.objectNode();
    input.set("values", JsonNodeFactory.instance.arrayNode().add(2).add(3));
    input.put("sum", 0);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                input,
                REQUESTED_AT,
                replyTo));
    WorkflowState state = kit.getState();
    boolean taskFrame = false;
    boolean forFrame = false;
    boolean forAdvanced = false;
    for (int boundary = 0; boundary < 30 && state.status() == ExecutionStatus.RUNNING; boundary++) {
      var result = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(boundary + 1L));
      taskFrame |=
          result.events().stream().anyMatch(EngineEvent.ForkNestedTaskEntered.class::isInstance);
      forFrame |=
          result.events().stream().anyMatch(EngineEvent.ForkNestedForEntered.class::isInstance);
      forAdvanced |=
          result.events().stream().anyMatch(EngineEvent.ForkNestedForAdvanced.class::isInstance);
      state = result.state();
      assertEquals(state, kit.restart().state());
    }
    assertTrue(taskFrame);
    assertTrue(forFrame);
    assertTrue(forAdvanced);
    WorkflowState.Completed completed = assertInstanceOf(WorkflowState.Completed.class, state);
    assertTrue(completed.data().get(0).get(0).required("sequence").booleanValue());
    assertEquals(5, completed.data().get(0).get(1).required("sum").intValue());
    assertEquals(1, completed.data().get(0).get(1).required("position").intValue());
    assertTrue(completed.data().get(1).required("sibling").booleanValue());
  }

  @Test
  void forkWaitsPersistIndependentlyAndOnlyBlockWhenEveryLaneWaits() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:fork-waits");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    Instant waitAnchor = REQUESTED_AT.plusSeconds(86_400);
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: fork-waits
                  version: '1.0.0'
                do:
                  - parallel:
                      fork:
                        branches:
                          - short:
                              wait: PT10S
                          - long:
                              wait: PT20S
                """
                    .getBytes(StandardCharsets.UTF_8));
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(kit, executionId, tenant, waitAnchor.plusSeconds(1));

    var first = runNext(kit, executionId, tenant, waitAnchor.plusSeconds(2));
    EngineEvent.ForkBranchWaitScheduled shortWait =
        assertInstanceOf(EngineEvent.ForkBranchWaitScheduled.class, first.events().getFirst());
    assertTrue(!shortWait.allBranchesWaiting());
    assertInstanceOf(WorkflowState.Running.class, first.state());
    assertEquals(first.state(), kit.restart().state());

    var second = runNext(kit, executionId, tenant, waitAnchor.plusSeconds(3));
    EngineEvent.ForkBranchWaitScheduled longWait =
        assertInstanceOf(EngineEvent.ForkBranchWaitScheduled.class, second.events().getFirst());
    assertTrue(longWait.allBranchesWaiting());
    assertInstanceOf(WorkflowState.Waiting.class, second.state());
    assertEquals(second.state(), kit.restart().state());

    var paused =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Pause(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "operator"),
                    waitAnchor.plusSeconds(4),
                    replyTo));
    assertInstanceOf(WorkflowState.Paused.class, paused.state());
    var resumed =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Resume(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "operator"),
                    waitAnchor.plusSeconds(5),
                    replyTo));
    assertInstanceOf(WorkflowState.Waiting.class, resumed.state());

    var shortDone =
        kit.runCommand(
            new WorkflowCommand.TimerElapsed(
                executionId, shortWait.taskPath(), shortWait.deadline()));
    assertInstanceOf(EngineEvent.ForkBranchWaitCompleted.class, shortDone.events().getFirst());
    assertInstanceOf(WorkflowState.Waiting.class, shortDone.state());
    assertEquals(shortDone.state(), kit.restart().state());

    var longDone =
        kit.runCommand(
            new WorkflowCommand.TimerElapsed(
                executionId, longWait.taskPath(), longWait.deadline()));
    assertInstanceOf(EngineEvent.ForkBranchWaitCompleted.class, longDone.events().getFirst());
    assertInstanceOf(WorkflowState.Running.class, longDone.state());
    WorkflowState.Completed completed =
        assertInstanceOf(
            WorkflowState.Completed.class,
            runNext(kit, executionId, tenant, waitAnchor.plusSeconds(30)).state());
    assertEquals(2, completed.data().size());
    assertEquals(completed, kit.restart().state());
  }

  @Test
  void nestedForkWaitsUseDurableCoordinatesAndRecoverAtEveryDeadline() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:nested-fork-waits");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = automaticTestKit(executionId);
    Instant waitAnchor = REQUESTED_AT.plusSeconds(86_400);
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: nested-fork-waits
                  version: '1.0.0'
                do:
                  - outer:
                      fork:
                        branches:
                          - nested:
                              fork:
                                branches:
                                  - first:
                                      wait: PT10S
                                  - second:
                                      wait: PT20S
                          - sibling:
                              wait: PT30S
                """
                    .getBytes(StandardCharsets.UTF_8));
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));

    var waits = new java.util.ArrayList<EngineEvent.ForkBranchWaitScheduled>();
    WorkflowState state = kit.getState();
    for (int boundary = 0; boundary < 10 && state.status() == ExecutionStatus.RUNNING; boundary++) {
      var result = runNext(kit, executionId, tenant, waitAnchor.plusSeconds(boundary));
      result.events().stream()
          .filter(EngineEvent.ForkBranchWaitScheduled.class::isInstance)
          .map(EngineEvent.ForkBranchWaitScheduled.class::cast)
          .forEach(waits::add);
      state = result.state();
      assertEquals(state, kit.restart().state());
    }
    assertEquals(3, waits.size());
    assertEquals(1, waits.stream().filter(event -> event.branchPath().size() == 1).count());
    assertEquals(2, waits.stream().filter(event -> event.branchPath().size() == 2).count());
    assertInstanceOf(WorkflowState.Waiting.class, state);

    var nestedWaits = waits.stream().filter(event -> event.branchPath().size() == 2).toList();
    for (EngineEvent.ForkBranchWaitScheduled wait : nestedWaits) {
      var elapsed =
          kit.runCommand(
              new WorkflowCommand.TimerElapsed(executionId, wait.taskPath(), wait.deadline()));
      assertInstanceOf(EngineEvent.ForkBranchWaitCompleted.class, elapsed.events().getFirst());
      state = kit.restart().state();
    }
    assertInstanceOf(WorkflowState.Waiting.class, state);

    EngineEvent.ForkBranchWaitScheduled sibling =
        waits.stream().filter(event -> event.branchPath().size() == 1).findFirst().orElseThrow();
    state =
        kit.runCommand(
                new WorkflowCommand.TimerElapsed(
                    executionId, sibling.taskPath(), sibling.deadline()))
            .state();

    for (int boundary = 0; boundary < 5 && state.status() == ExecutionStatus.RUNNING; boundary++) {
      state = runNext(kit, executionId, tenant, waitAnchor.plusSeconds(40L + boundary)).state();
      assertEquals(state, kit.restart().state());
    }
    WorkflowState.Completed completed = assertInstanceOf(WorkflowState.Completed.class, state);
    assertEquals(2, completed.data().size());
  }

  @Test
  void forkPersistsBlockedStateWhenTheLastRunnableLaneDoesNotWait() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:fork-blocked");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    Instant waitAnchor = REQUESTED_AT.plusSeconds(86_400);
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: fork-blocked
                  version: '1.0.0'
                do:
                  - parallel:
                      fork:
                        branches:
                          - delayed:
                              wait: PT10S
                          - immediate:
                              set:
                                immediate: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(kit, executionId, tenant, waitAnchor);
    EngineEvent.ForkBranchWaitScheduled wait =
        assertInstanceOf(
            EngineEvent.ForkBranchWaitScheduled.class,
            runNext(kit, executionId, tenant, waitAnchor.plusSeconds(1)).events().getFirst());

    var blocked = runNext(kit, executionId, tenant, waitAnchor.plusSeconds(2));
    assertTrue(
        blocked.events().stream().anyMatch(EngineEvent.ForkBranchesWaiting.class::isInstance));
    assertInstanceOf(WorkflowState.Waiting.class, blocked.state());
    assertEquals(blocked.state(), kit.restart().state());

    var awakened =
        kit.runCommand(
            new WorkflowCommand.TimerElapsed(executionId, wait.taskPath(), wait.deadline()));
    assertInstanceOf(WorkflowState.Running.class, awakened.state());
    WorkflowState.Completed completed =
        assertInstanceOf(
            WorkflowState.Completed.class,
            runNext(kit, executionId, tenant, waitAnchor.plusSeconds(20)).state());
    assertTrue(completed.data().get(1).required("immediate").booleanValue());
  }

  @Test
  void nestedForkTaskDeadlinePersistsRecursivelyAndFailsOnlyItsWorkflow() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:fork-timeout");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    Instant anchor = REQUESTED_AT.plusSeconds(86_400);
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: nested-fork-timeout
                  version: '1.0.0'
                do:
                  - outer:
                      fork:
                        branches:
                          - nested:
                              fork:
                                branches:
                                  - bounded:
                                      timeout:
                                        after: PT10S
                                      do:
                                        - work:
                                            wait: PT1H
                                  - peer:
                                      wait: PT1H
                          - sibling:
                              wait: PT1H
                """
                    .getBytes(StandardCharsets.UTF_8));
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));

    EngineEvent.DeadlineScheduled deadline = null;
    for (int boundary = 0; boundary < 10 && deadline == null; boundary++) {
      var result = runNext(kit, executionId, tenant, anchor.plusSeconds(boundary));
      deadline =
          result.events().stream()
              .filter(EngineEvent.DeadlineScheduled.class::isInstance)
              .map(EngineEvent.DeadlineScheduled.class::cast)
              .findFirst()
              .orElse(null);
    }
    assertTrue(deadline != null);
    assertEquals(DeadlineScope.TASK, deadline.scope());
    assertEquals(kit.getState(), kit.restart().state());

    var expired =
        kit.runCommand(
            new WorkflowCommand.DeadlineElapsed(
                executionId, deadline.scope(), deadline.taskPath(), deadline.deadline()));
    assertInstanceOf(EngineEvent.Failed.class, expired.events().getFirst());
    assertInstanceOf(WorkflowState.Failed.class, expired.state());
    assertEquals(expired.state(), kit.restart().state());
  }

  @Test
  void forkContextsAreIsolatedAndMergeInDeclarationOrderAtNestedJoins() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:fork-context");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: fork-context
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
                                      export:
                                        as: '${ $context + {first: true, conflict: "first"} }'
                                  - second:
                                      set:
                                        value: second
                                      export:
                                        as: '${ $context + {second: true, conflict: "second"} }'
                          - sibling:
                              set:
                                value: sibling
                              export:
                                as: '${ $context + {sibling: true} }'
                  - observe:
                      set:
                        context: '${ $context }'
                """
                    .getBytes(StandardCharsets.UTF_8));
    var input = JsonNodeFactory.instance.objectNode().put("base", true);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                input,
                REQUESTED_AT,
                replyTo));

    WorkflowState state = kit.getState();
    for (int boundary = 0; boundary < 20 && state.status() == ExecutionStatus.RUNNING; boundary++) {
      state = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(boundary + 1L)).state();
      assertEquals(state, kit.restart().state());
    }
    WorkflowState.Completed completed = assertInstanceOf(WorkflowState.Completed.class, state);
    var context = completed.data().required("context");
    assertTrue(context.required("base").booleanValue());
    assertTrue(context.required("first").booleanValue());
    assertTrue(context.required("second").booleanValue());
    assertTrue(context.required("sibling").booleanValue());
    assertEquals("second", context.required("conflict").textValue());
  }

  @Test
  void forkTryCatchRetryPersistsAtItsBranchCoordinate() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:fork-retry");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    Instant anchor = REQUESTED_AT.plusSeconds(86_400);
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: fork-retry
                  version: '1.0.0'
                do:
                  - parallel:
                      fork:
                        branches:
                          - guarded:
                              try:
                                - reject:
                                    raise:
                                      error:
                                        type: https://errors.example/retryable
                                        status: 503
                              catch:
                                retry:
                                  delay: PT24H
                                  limit:
                                    attempt:
                                      count: 2
                                do:
                                  - exhausted:
                                      set:
                                        attempts: 2
                          - sibling:
                              wait: PT48H
                """
                    .getBytes(StandardCharsets.UTF_8));
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(kit, executionId, tenant, anchor);
    var entered = runNext(kit, executionId, tenant, anchor.plusSeconds(1));
    assertInstanceOf(EngineEvent.ForkBranchTryEntered.class, entered.events().getFirst());
    EngineEvent.ForkBranchWaitScheduled siblingWait =
        assertInstanceOf(
            EngineEvent.ForkBranchWaitScheduled.class,
            runNext(kit, executionId, tenant, anchor.plusSeconds(2)).events().getFirst());

    var scheduled = runNext(kit, executionId, tenant, anchor.plusSeconds(3));
    assertInstanceOf(EngineEvent.ErrorRaised.class, scheduled.events().getFirst());
    EngineEvent.ForkBranchRetryScheduled retry =
        assertInstanceOf(EngineEvent.ForkBranchRetryScheduled.class, scheduled.events().get(1));
    assertTrue(retry.allBranchesWaiting());
    assertInstanceOf(WorkflowState.Waiting.class, scheduled.state());
    assertEquals(scheduled.state(), kit.restart().state());

    var paused =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Pause(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "operator"),
                    anchor.plusSeconds(4),
                    replyTo));
    assertInstanceOf(WorkflowState.Paused.class, paused.state());
    assertInstanceOf(
        WorkflowState.Waiting.class,
        kit.<WorkflowReply>runCommand(
                replyTo ->
                    new WorkflowCommand.Resume(
                        UUID.randomUUID(),
                        executionId,
                        actor(tenant, "operator"),
                        anchor.plusSeconds(5),
                        replyTo))
            .state());

    var retrying =
        kit.runCommand(
            new WorkflowCommand.RetryElapsed(executionId, retry.tryTaskPath(), retry.deadline()));
    assertInstanceOf(EngineEvent.ForkBranchRetryStarted.class, retrying.events().getFirst());
    assertInstanceOf(WorkflowState.Running.class, retrying.state());
    assertEquals(retrying.state(), kit.restart().state());

    var caught = runNext(kit, executionId, tenant, retry.deadline().plusSeconds(1));
    assertInstanceOf(EngineEvent.ErrorRaised.class, caught.events().getFirst());
    assertInstanceOf(EngineEvent.ForkBranchErrorCaught.class, caught.events().get(1));

    kit.runCommand(
        new WorkflowCommand.TimerElapsed(
            executionId, siblingWait.taskPath(), siblingWait.deadline()));
    WorkflowState state = kit.getState();
    for (int boundary = 0; boundary < 6 && state.status() == ExecutionStatus.RUNNING; boundary++) {
      state =
          runNext(kit, executionId, tenant, retry.deadline().plusSeconds(2L + boundary)).state();
    }
    WorkflowState.Completed completed = assertInstanceOf(WorkflowState.Completed.class, state);
    assertEquals(2, completed.data().get(0).required("attempts").intValue());
    assertEquals(completed, kit.restart().state());
  }

  @Test
  void nestedForkCatchesNearestErrorAndUncaughtErrorFailsWorkflow() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:nested-fork-error");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    WorkflowPlan caughtPlan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: nested-fork-catch
                  version: '1.0.0'
                do:
                  - outer:
                      fork:
                        branches:
                          - nested:
                              fork:
                                branches:
                                  - guarded:
                                      try:
                                        - reject:
                                            raise:
                                              error:
                                                type: https://errors.example/nested
                                                status: 409
                                      catch:
                                        as: problem
                                        do:
                                          - record:
                                              set:
                                                caught: '${ $problem.status }'
                                  - peer:
                                      set:
                                        peer: true
                          - sibling:
                              set:
                                sibling: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                caughtPlan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    WorkflowState state = kit.getState();
    boolean nestedCatch = false;
    for (int boundary = 0; boundary < 24 && state.status() == ExecutionStatus.RUNNING; boundary++) {
      var result = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(boundary + 1L));
      nestedCatch |=
          result.events().stream()
              .filter(EngineEvent.ForkBranchErrorCaught.class::isInstance)
              .map(EngineEvent.ForkBranchErrorCaught.class::cast)
              .anyMatch(event -> event.branchPath().size() == 2);
      state = result.state();
      assertEquals(state, kit.restart().state());
    }
    assertTrue(nestedCatch);
    assertEquals(
        409,
        assertInstanceOf(WorkflowState.Completed.class, state)
            .data()
            .get(0)
            .get(0)
            .required("caught")
            .intValue());

    var failedId = new ExecutionId(tenant, UUID.randomUUID());
    var failedKit = testKit(failedId);
    WorkflowPlan failedPlan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: fork-uncaught
                  version: '1.0.0'
                do:
                  - parallel:
                      fork:
                        branches:
                          - reject:
                              raise:
                                error:
                                  type: https://errors.example/uncaught
                                  status: 500
                                  detail: branch failed
                          - peer:
                              set:
                                peer: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    failedKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                failedId,
                actor(tenant, "alice"),
                failedPlan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(failedKit, failedId, tenant, REQUESTED_AT.plusSeconds(1));
    var failed = runNext(failedKit, failedId, tenant, REQUESTED_AT.plusSeconds(2));
    assertInstanceOf(EngineEvent.ErrorRaised.class, failed.events().getFirst());
    assertInstanceOf(EngineEvent.Failed.class, failed.events().get(1));
    assertInstanceOf(WorkflowState.Failed.class, failed.state());
    assertEquals(failed.state(), failedKit.restart().state());
  }

  @Test
  void emitPersistsCloudEventIntentBeforeIdempotentAcknowledgement() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:emit");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: durable-emit
                  version: '1.0.0'
                do:
                  - publish:
                      emit:
                        event:
                          with:
                            source: https://events.forwardmeasure.com/workflows
                            type: com.forwardmeasure.workflow.result.v1
                            subject: '${ .subject }'
                            tenant: '${ .tenant }'
                            data:
                              value: '${ .value }'
                """
                    .getBytes(StandardCharsets.UTF_8));
    var input =
        JsonNodeFactory.instance
            .objectNode()
            .put("subject", "case-7")
            .put("value", 42)
            .put("tenant", tenant.value().toString());
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                input,
                REQUESTED_AT,
                replyTo));

    var requested = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));
    EngineEvent.EmitRequested emit =
        assertInstanceOf(EngineEvent.EmitRequested.class, requested.events().getFirst());
    assertEquals("1.0", emit.event().specVersion());
    assertEquals("com.forwardmeasure.workflow.result.v1", emit.event().type());
    assertEquals("case-7", emit.event().subject());
    assertEquals(42, emit.event().data().required("value").intValue());
    assertEquals(tenant.value().toString(), emit.event().extensions().get("tenant").textValue());
    assertInstanceOf(WorkflowState.Waiting.class, requested.state());
    assertEquals(requested.state(), kit.restart().state());

    var paused =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Pause(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "operator"),
                    REQUESTED_AT.plusSeconds(2),
                    replyTo));
    assertInstanceOf(WorkflowState.Paused.class, paused.state());
    var pausedAcknowledgement =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.EffectAcknowledged(
                    executionId, emit.operationId(), REQUESTED_AT.plusMillis(2500), replyTo));
    assertEquals(
        "execution_paused", pausedAcknowledgement.replyOfType(WorkflowReply.Rejected.class).code());
    assertInstanceOf(WorkflowState.Paused.class, pausedAcknowledgement.state());
    assertInstanceOf(
        WorkflowState.Waiting.class,
        kit.<WorkflowReply>runCommand(
                replyTo ->
                    new WorkflowCommand.Resume(
                        UUID.randomUUID(),
                        executionId,
                        actor(tenant, "operator"),
                        REQUESTED_AT.plusSeconds(3),
                        replyTo))
            .state());

    var acknowledged =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.EffectAcknowledged(
                    executionId, emit.operationId(), REQUESTED_AT.plusSeconds(4), replyTo));
    assertInstanceOf(EngineEvent.EmitAcknowledged.class, acknowledged.events().getFirst());
    WorkflowState.Completed completed =
        assertInstanceOf(WorkflowState.Completed.class, acknowledged.state());
    assertEquals("1.0", completed.data().required("specversion").textValue());
    assertEquals(emit.event().id(), completed.data().required("id").textValue());
    assertEquals(
        "https://events.forwardmeasure.com/workflows",
        completed.data().required("source").textValue());
    assertEquals(
        "com.forwardmeasure.workflow.result.v1", completed.data().required("type").textValue());
    assertEquals("case-7", completed.data().required("subject").textValue());
    assertEquals(42, completed.data().required("data").required("value").intValue());
    assertEquals(tenant.value().toString(), completed.data().required("tenant").textValue());
    acknowledged.replyOfType(WorkflowReply.Accepted.class);
    assertEquals(completed, kit.restart().state());
    assertTrue(
        kit.runCommand(
                new WorkflowCommand.EffectAcknowledged(
                    executionId, emit.operationId(), REQUESTED_AT.plusSeconds(5)))
            .hasNoEvents());

    var cancelledId = new ExecutionId(tenant, UUID.randomUUID());
    var cancelledKit = testKit(cancelledId);
    cancelledKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                cancelledId,
                actor(tenant, "alice"),
                plan,
                input,
                REQUESTED_AT,
                replyTo));
    EngineEvent.EmitRequested cancelledEmit =
        assertInstanceOf(
            EngineEvent.EmitRequested.class,
            runNext(cancelledKit, cancelledId, tenant, REQUESTED_AT.plusSeconds(1))
                .events()
                .getFirst());
    var cancelled =
        cancelledKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Cancel(
                    UUID.randomUUID(),
                    cancelledId,
                    actor(tenant, "operator"),
                    REQUESTED_AT.plusSeconds(2),
                    replyTo));
    assertInstanceOf(WorkflowState.Cancelled.class, cancelled.state());
    assertInstanceOf(WorkflowState.Cancelled.class, cancelledKit.restart().state());
    assertTrue(
        cancelledKit
            .runCommand(
                new WorkflowCommand.EffectAcknowledged(
                    cancelledId, cancelledEmit.operationId(), REQUESTED_AT.plusSeconds(3)))
            .hasNoEvents());
  }

  @Test
  void forkEmitAndListenRemainDurableAcrossPauseResumeAndCancel() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:fork-events");
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: fork-events
                  version: '1.0.0'
                do:
                  - parallel:
                      fork:
                        branches:
                          - publish:
                              emit:
                                event:
                                  with:
                                    source: https://events.forwardmeasure.com/workflows
                                    type: fork.published.v1
                                    data: { value: 1 }
                          - receive:
                              listen:
                                to:
                                  one:
                                    with: { type: fork.received.v1 }
                """
                    .getBytes(StandardCharsets.UTF_8));
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = automaticTestKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));
    EngineEvent.ForkBranchEmitRequested emit =
        assertInstanceOf(
            EngineEvent.ForkBranchEmitRequested.class,
            runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(2)).events().getFirst());
    var listen = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(3));
    assertInstanceOf(EngineEvent.ForkBranchListenStarted.class, listen.events().getFirst());
    assertInstanceOf(WorkflowState.Waiting.class, listen.state());
    assertEquals(listen.state(), kit.restart().state());

    assertInstanceOf(
        WorkflowState.Paused.class,
        kit.<WorkflowReply>runCommand(
                replyTo ->
                    new WorkflowCommand.Pause(
                        UUID.randomUUID(),
                        executionId,
                        actor(tenant, "operator"),
                        REQUESTED_AT.plusSeconds(4),
                        replyTo))
            .state());
    var event =
        new com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent(
            "1.0",
            "fork-event-1",
            java.net.URI.create("urn:test"),
            "fork.received.v1",
            null,
            REQUESTED_AT,
            "application/json",
            JsonNodeFactory.instance.objectNode().put("accepted", true),
            java.util.Map.of());
    assertEquals(
        "execution_paused",
        kit.<WorkflowReply>runCommand(
                replyTo ->
                    new WorkflowCommand.CloudEventReceived(
                        executionId, event, REQUESTED_AT.plusSeconds(5), replyTo))
            .replyOfType(WorkflowReply.Rejected.class)
            .code());
    assertEquals(
        "execution_paused",
        kit.<WorkflowReply>runCommand(
                replyTo ->
                    new WorkflowCommand.EffectAcknowledged(
                        executionId, emit.operationId(), REQUESTED_AT.plusSeconds(5), replyTo))
            .replyOfType(WorkflowReply.Rejected.class)
            .code());
    assertInstanceOf(
        WorkflowState.Waiting.class,
        kit.<WorkflowReply>runCommand(
                replyTo ->
                    new WorkflowCommand.Resume(
                        UUID.randomUUID(),
                        executionId,
                        actor(tenant, "operator"),
                        REQUESTED_AT.plusSeconds(6),
                        replyTo))
            .state());

    var acknowledged =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.EffectAcknowledged(
                    executionId, emit.operationId(), REQUESTED_AT.plusSeconds(7), replyTo));
    assertInstanceOf(
        EngineEvent.ForkBranchEmitAcknowledged.class, acknowledged.events().getFirst());
    assertInstanceOf(WorkflowState.Waiting.class, acknowledged.state());
    var delivered =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.CloudEventReceived(
                    executionId, event, REQUESTED_AT.plusSeconds(8), replyTo));
    assertInstanceOf(EngineEvent.ForkBranchListenAccepted.class, delivered.events().getFirst());
    assertInstanceOf(WorkflowState.Completed.class, delivered.state());
    assertEquals(delivered.state(), kit.restart().state());

    var cancelledId = new ExecutionId(tenant, UUID.randomUUID());
    var cancelledKit = testKit(cancelledId);
    cancelledKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                cancelledId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(cancelledKit, cancelledId, tenant, REQUESTED_AT.plusSeconds(1));
    EngineEvent.ForkBranchEmitRequested cancelledEmit =
        assertInstanceOf(
            EngineEvent.ForkBranchEmitRequested.class,
            runNext(cancelledKit, cancelledId, tenant, REQUESTED_AT.plusSeconds(2))
                .events()
                .getFirst());
    runNext(cancelledKit, cancelledId, tenant, REQUESTED_AT.plusSeconds(3));
    assertInstanceOf(
        WorkflowState.Cancelled.class,
        cancelledKit
            .<WorkflowReply>runCommand(
                replyTo ->
                    new WorkflowCommand.Cancel(
                        UUID.randomUUID(),
                        cancelledId,
                        actor(tenant, "operator"),
                        REQUESTED_AT.plusSeconds(4),
                        replyTo))
            .state());
    assertTrue(
        cancelledKit
            .runCommand(
                new WorkflowCommand.EffectAcknowledged(
                    cancelledId, cancelledEmit.operationId(), REQUESTED_AT.plusSeconds(5)))
            .hasNoEvents());
    assertTrue(
        cancelledKit
            .runCommand(
                new WorkflowCommand.CloudEventReceived(
                    cancelledId, event, REQUESTED_AT.plusSeconds(5)))
            .hasNoEvents());
    assertInstanceOf(WorkflowState.Cancelled.class, cancelledKit.restart().state());
  }

  @Test
  void awaitedSubworkflowPersistsAcrossPauseResumeCompletionAndCancel() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:subworkflow");
    var child =
        new com.forwardmeasure.openworkflow.definition.ResolvedSubflow(
            new com.forwardmeasure.openworkflow.definition.WorkflowCoordinates(
                "forwardmeasure", "child", "1.0.0", "1.0.3"),
            "a".repeat(64),
            "b".repeat(64));
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: parent
                  version: '1.0.0'
                do:
                  - child:
                      run:
                        await: true
                        workflow:
                          namespace: forwardmeasure
                          name: child
                          version: '1.0.0'
                          input:
                            caseId: '${ .caseId }'
                """
                    .getBytes(StandardCharsets.UTF_8),
                java.util.List.of(),
                (namespace, name, version) -> java.util.Optional.of(child));
    var input = JsonNodeFactory.instance.objectNode().put("caseId", "case-9");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                input,
                REQUESTED_AT,
                replyTo));
    var requested = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));
    EngineEvent.SubworkflowRequested launch =
        assertInstanceOf(EngineEvent.SubworkflowRequested.class, requested.events().getFirst());
    assertEquals(tenant, launch.childExecutionId().tenantId());
    assertEquals("case-9", launch.childInput().required("caseId").textValue());
    assertEquals(child, launch.subflow());
    assertInstanceOf(WorkflowState.Waiting.class, requested.state());
    assertEquals(requested.state(), kit.restart().state());

    assertInstanceOf(
        WorkflowState.Paused.class,
        kit.<WorkflowReply>runCommand(
                replyTo ->
                    new WorkflowCommand.Pause(
                        UUID.randomUUID(),
                        executionId,
                        actor(tenant, "operator"),
                        REQUESTED_AT.plusSeconds(2),
                        replyTo))
            .state());
    var childOutput = JsonNodeFactory.instance.objectNode().put("child", true);
    var paused =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.SubworkflowCompleted(
                    UUID.randomUUID(),
                    executionId,
                    launch.operationId(),
                    launch.childExecutionId(),
                    ExecutionStatus.COMPLETED,
                    childOutput,
                    null,
                    REQUESTED_AT.plusSeconds(3),
                    replyTo));
    assertEquals("execution_paused", paused.replyOfType(WorkflowReply.Rejected.class).code());
    assertTrue(paused.hasNoEvents());
    assertInstanceOf(
        WorkflowState.Waiting.class,
        kit.<WorkflowReply>runCommand(
                replyTo ->
                    new WorkflowCommand.Resume(
                        UUID.randomUUID(),
                        executionId,
                        actor(tenant, "operator"),
                        REQUESTED_AT.plusSeconds(4),
                        replyTo))
            .state());

    var completed =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.SubworkflowCompleted(
                    UUID.randomUUID(),
                    executionId,
                    launch.operationId(),
                    launch.childExecutionId(),
                    ExecutionStatus.COMPLETED,
                    childOutput,
                    null,
                    REQUESTED_AT.plusSeconds(5),
                    replyTo));
    assertInstanceOf(EngineEvent.SubworkflowCompleted.class, completed.events().getFirst());
    assertInstanceOf(WorkflowState.Completed.class, completed.state());
    assertEquals(childOutput, completed.state().data());
    assertEquals(completed.state(), kit.restart().state());

    var cancelledId = new ExecutionId(tenant, UUID.randomUUID());
    var cancelledKit = testKit(cancelledId);
    cancelledKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                cancelledId,
                actor(tenant, "alice"),
                plan,
                input,
                REQUESTED_AT,
                replyTo));
    EngineEvent.SubworkflowRequested cancelledLaunch =
        assertInstanceOf(
            EngineEvent.SubworkflowRequested.class,
            runNext(cancelledKit, cancelledId, tenant, REQUESTED_AT.plusSeconds(1))
                .events()
                .getFirst());
    assertInstanceOf(
        WorkflowState.Cancelled.class,
        cancelledKit
            .<WorkflowReply>runCommand(
                replyTo ->
                    new WorkflowCommand.Cancel(
                        UUID.randomUUID(),
                        cancelledId,
                        actor(tenant, "operator"),
                        REQUESTED_AT.plusSeconds(2),
                        replyTo))
            .state());
    assertTrue(
        cancelledKit
            .runCommand(
                new WorkflowCommand.SubworkflowCompleted(
                    cancelledId,
                    cancelledLaunch.operationId(),
                    cancelledLaunch.childExecutionId(),
                    ExecutionStatus.COMPLETED,
                    childOutput,
                    null,
                    REQUESTED_AT.plusSeconds(3)))
            .hasNoEvents());
    assertInstanceOf(WorkflowState.Cancelled.class, cancelledKit.restart().state());
  }

  @Test
  void awaitedSubworkflowInsideForkPreservesSiblingProgressPauseAndCancellation() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:fork-subworkflow");
    var child =
        new com.forwardmeasure.openworkflow.definition.ResolvedSubflow(
            new com.forwardmeasure.openworkflow.definition.WorkflowCoordinates(
                "forwardmeasure", "child", "1.0.0", "1.0.3"),
            "a".repeat(64),
            "b".repeat(64));
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: fork-parent
                  version: '1.0.0'
                do:
                  - parallel:
                      fork:
                        branches:
                          - child:
                              run:
                                await: true
                                workflow:
                                  namespace: forwardmeasure
                                  name: child
                                  version: '1.0.0'
                                  input:
                                    caseId: '${ .caseId }'
                          - sibling:
                              set:
                                sibling: true
                """
                    .getBytes(StandardCharsets.UTF_8),
                java.util.List.of(),
                (namespace, name, version) -> java.util.Optional.of(child));
    var input = JsonNodeFactory.instance.objectNode().put("caseId", "case-fork");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = automaticTestKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                input,
                REQUESTED_AT,
                replyTo));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));

    var requested = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(2));
    EngineEvent.ForkBranchSubworkflowRequested launch =
        assertInstanceOf(
            EngineEvent.ForkBranchSubworkflowRequested.class, requested.events().getFirst());
    assertEquals(java.util.List.of(0), launch.branchPath());
    assertTrue(!launch.allBranchesBlocked());
    assertEquals("case-fork", launch.childInput().required("caseId").textValue());
    assertInstanceOf(WorkflowState.Running.class, requested.state());

    var sibling = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(3));
    assertInstanceOf(
        EngineEvent.ForkBranchAdvanced.class,
        sibling.events().getFirst(),
        () -> sibling.events().toString());
    assertInstanceOf(WorkflowState.Waiting.class, sibling.state());
    assertEquals(sibling.state(), kit.restart().state());

    assertInstanceOf(
        WorkflowState.Paused.class,
        kit.<WorkflowReply>runCommand(
                replyTo ->
                    new WorkflowCommand.Pause(
                        UUID.randomUUID(),
                        executionId,
                        actor(tenant, "operator"),
                        REQUESTED_AT.plusSeconds(4),
                        replyTo))
            .state());
    var childOutput = JsonNodeFactory.instance.objectNode().put("child", true);
    var pausedCompletion =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.SubworkflowCompleted(
                    UUID.randomUUID(),
                    executionId,
                    launch.operationId(),
                    launch.childExecutionId(),
                    ExecutionStatus.COMPLETED,
                    childOutput,
                    null,
                    REQUESTED_AT.plusSeconds(5),
                    replyTo));
    assertEquals(
        "execution_paused", pausedCompletion.replyOfType(WorkflowReply.Rejected.class).code());
    assertTrue(pausedCompletion.hasNoEvents());
    assertInstanceOf(
        WorkflowState.Waiting.class,
        kit.<WorkflowReply>runCommand(
                replyTo ->
                    new WorkflowCommand.Resume(
                        UUID.randomUUID(),
                        executionId,
                        actor(tenant, "operator"),
                        REQUESTED_AT.plusSeconds(6),
                        replyTo))
            .state());

    var childCompleted =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.SubworkflowCompleted(
                    UUID.randomUUID(),
                    executionId,
                    launch.operationId(),
                    launch.childExecutionId(),
                    ExecutionStatus.COMPLETED,
                    childOutput,
                    null,
                    REQUESTED_AT.plusSeconds(7),
                    replyTo));
    assertInstanceOf(
        EngineEvent.ForkBranchSubworkflowCompleted.class, childCompleted.events().getFirst());
    var completed = assertInstanceOf(WorkflowState.Completed.class, childCompleted.state());
    assertEquals(true, completed.data().get(0).required("child").booleanValue());
    assertEquals(true, completed.data().get(1).required("sibling").booleanValue());
    assertEquals(completed, kit.restart().state());

    var cancelledId = new ExecutionId(tenant, UUID.randomUUID());
    var cancelledKit = testKit(cancelledId);
    cancelledKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                cancelledId,
                actor(tenant, "alice"),
                plan,
                input,
                REQUESTED_AT,
                replyTo));
    runNext(cancelledKit, cancelledId, tenant, REQUESTED_AT.plusSeconds(1));
    var cancelledLaunch =
        assertInstanceOf(
            EngineEvent.ForkBranchSubworkflowRequested.class,
            runNext(cancelledKit, cancelledId, tenant, REQUESTED_AT.plusSeconds(2))
                .events()
                .getFirst());
    assertInstanceOf(
        WorkflowState.Cancelled.class,
        cancelledKit
            .<WorkflowReply>runCommand(
                replyTo ->
                    new WorkflowCommand.Cancel(
                        UUID.randomUUID(),
                        cancelledId,
                        actor(tenant, "operator"),
                        REQUESTED_AT.plusSeconds(3),
                        replyTo))
            .state());
    assertTrue(
        cancelledKit
            .runCommand(
                new WorkflowCommand.SubworkflowCompleted(
                    cancelledId,
                    cancelledLaunch.operationId(),
                    cancelledLaunch.childExecutionId(),
                    ExecutionStatus.COMPLETED,
                    childOutput,
                    null,
                    REQUESTED_AT.plusSeconds(4)))
            .hasNoEvents());
    assertInstanceOf(WorkflowState.Cancelled.class, cancelledKit.restart().state());
  }

  @Test
  void detachedSubworkflowCompletesTheParentWithoutAwaitingItsLateResult() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:detached-subworkflow");
    var child =
        new com.forwardmeasure.openworkflow.definition.ResolvedSubflow(
            new com.forwardmeasure.openworkflow.definition.WorkflowCoordinates(
                "forwardmeasure", "child", "1.0.0", "1.0.3"),
            "a".repeat(64),
            "b".repeat(64));
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: detached-parent
                  version: '1.0.0'
                do:
                  - child:
                      run:
                        await: false
                        workflow:
                          namespace: forwardmeasure
                          name: child
                          version: '1.0.0'
                """
                    .getBytes(StandardCharsets.UTF_8),
                java.util.List.of(),
                (namespace, name, version) -> java.util.Optional.of(child));
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode().put("parent", true),
                REQUESTED_AT,
                replyTo));

    var launched = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));
    var request =
        assertInstanceOf(EngineEvent.SubworkflowRequested.class, launched.events().getFirst());
    assertTrue(!request.await());
    assertEquals(2, launched.events().size());
    assertInstanceOf(EngineEvent.Completed.class, launched.events().get(1));
    assertInstanceOf(WorkflowState.Completed.class, launched.state());
    assertEquals(launched.state(), kit.restart().state());

    var late =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.SubworkflowCompleted(
                    UUID.randomUUID(),
                    executionId,
                    request.operationId(),
                    request.childExecutionId(),
                    ExecutionStatus.COMPLETED,
                    JsonNodeFactory.instance.objectNode().put("late", true),
                    null,
                    REQUESTED_AT.plusSeconds(2),
                    replyTo));
    assertTrue(late.hasNoEvents());
    assertEquals(
        ExecutionStatus.COMPLETED, late.replyOfType(WorkflowReply.Accepted.class).status());
  }

  @Test
  void awaitedSubworkflowFailureAndTaskTimeoutFailDurablyWithoutRevival() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:failed-subworkflow");
    var child =
        new com.forwardmeasure.openworkflow.definition.ResolvedSubflow(
            new com.forwardmeasure.openworkflow.definition.WorkflowCoordinates(
                "forwardmeasure", "child", "1.0.0", "1.0.3"),
            "a".repeat(64),
            "b".repeat(64));
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: bounded-child-parent
                  version: '1.0.0'
                do:
                  - child:
                      timeout:
                        after: PT24H
                      run:
                        await: true
                        workflow:
                          namespace: forwardmeasure
                          name: child
                          version: '1.0.0'
                """
                    .getBytes(StandardCharsets.UTF_8),
                java.util.List.of(),
                (namespace, name, version) -> java.util.Optional.of(child));

    var failedId = new ExecutionId(tenant, UUID.randomUUID());
    var failedKit = testKit(failedId);
    failedKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                failedId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    var failedLaunch =
        assertInstanceOf(
            EngineEvent.SubworkflowRequested.class,
            runNext(failedKit, failedId, tenant, REQUESTED_AT.plusSeconds(1)).events().getFirst());
    var failed =
        failedKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.SubworkflowCompleted(
                    UUID.randomUUID(),
                    failedId,
                    failedLaunch.operationId(),
                    failedLaunch.childExecutionId(),
                    ExecutionStatus.FAILED,
                    JsonNodeFactory.instance.objectNode(),
                    "child rejected its input",
                    REQUESTED_AT.plusSeconds(2),
                    replyTo));
    var failedState = assertInstanceOf(WorkflowState.Failed.class, failed.state());
    assertEquals("child rejected its input", failedState.message());
    assertEquals(failedState, failedKit.restart().state());

    var timedId = new ExecutionId(tenant, UUID.randomUUID());
    var timedKit = testKit(timedId);
    timedKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                timedId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    var launched = runNext(timedKit, timedId, tenant, REQUESTED_AT.plusSeconds(1));
    var timedLaunch =
        assertInstanceOf(EngineEvent.SubworkflowRequested.class, launched.events().getFirst());
    assertInstanceOf(EngineEvent.DeadlineScheduled.class, launched.events().get(1));
    var waiting = assertInstanceOf(WorkflowState.Waiting.class, launched.state());
    TaskExecutionFrame frame = waiting.taskStack().getLast();
    var expired =
        timedKit.runCommand(
            new WorkflowCommand.DeadlineElapsed(
                timedId, DeadlineScope.TASK, frame.taskPath(), frame.timeoutDeadline()));
    assertInstanceOf(WorkflowState.Failed.class, expired.state());
    assertEquals(expired.state(), timedKit.restart().state());
    var late =
        timedKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.SubworkflowCompleted(
                    UUID.randomUUID(),
                    timedId,
                    timedLaunch.operationId(),
                    timedLaunch.childExecutionId(),
                    ExecutionStatus.COMPLETED,
                    JsonNodeFactory.instance.objectNode().put("late", true),
                    null,
                    frame.timeoutDeadline().plusSeconds(1),
                    replyTo));
    assertTrue(late.hasNoEvents());
    assertEquals(ExecutionStatus.FAILED, late.replyOfType(WorkflowReply.Accepted.class).status());
  }

  @Test
  void awaitedChildFailureParticipatesInStructuredTryCatch() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:caught-subworkflow");
    var child =
        new com.forwardmeasure.openworkflow.definition.ResolvedSubflow(
            new com.forwardmeasure.openworkflow.definition.WorkflowCoordinates(
                "forwardmeasure", "child", "1.0.0", "1.0.3"),
            "a".repeat(64),
            "b".repeat(64));
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: caught-child-parent
                  version: '1.0.0'
                do:
                  - guarded:
                      try:
                        - child:
                            run:
                              workflow:
                                namespace: forwardmeasure
                                name: child
                                version: '1.0.0'
                      catch:
                        errors:
                          with:
                            type: urn:openworkflow:subworkflow:failed
                        as: childError
                        do:
                          - recover:
                              set:
                                recoveredStatus: '${ $childError.status }'
                """
                    .getBytes(StandardCharsets.UTF_8),
                java.util.List.of(),
                (namespace, name, version) -> java.util.Optional.of(child));
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));
    var launch =
        assertInstanceOf(
            EngineEvent.SubworkflowRequested.class,
            runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(2)).events().getFirst());

    var caught =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.SubworkflowCompleted(
                    UUID.randomUUID(),
                    executionId,
                    launch.operationId(),
                    launch.childExecutionId(),
                    ExecutionStatus.FAILED,
                    JsonNodeFactory.instance.objectNode(),
                    "child unavailable",
                    REQUESTED_AT.plusSeconds(3),
                    replyTo));
    assertInstanceOf(EngineEvent.ErrorRaised.class, caught.events().get(0));
    assertInstanceOf(EngineEvent.ErrorCaught.class, caught.events().get(1));
    assertInstanceOf(WorkflowState.Running.class, caught.state());
    assertEquals(caught.state(), kit.restart().state());

    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(4));
    var completed =
        assertInstanceOf(
            WorkflowState.Completed.class,
            runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(5)).state());
    assertEquals(500, completed.data().required("recoveredStatus").intValue());
  }

  @Test
  void forkOwnedChildFailureParticipatesInItsLaneTryCatch() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:fork-caught-subworkflow");
    var child =
        new com.forwardmeasure.openworkflow.definition.ResolvedSubflow(
            new com.forwardmeasure.openworkflow.definition.WorkflowCoordinates(
                "forwardmeasure", "child", "1.0.0", "1.0.3"),
            "a".repeat(64),
            "b".repeat(64));
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: fork-caught-child-parent
                  version: '1.0.0'
                do:
                  - parallel:
                      fork:
                        branches:
                          - guarded:
                              try:
                                - child:
                                    run:
                                      workflow:
                                        namespace: forwardmeasure
                                        name: child
                                        version: '1.0.0'
                              catch:
                                errors:
                                  with:
                                    type: urn:openworkflow:subworkflow:failed
                                as: childError
                                do:
                                  - recover:
                                      set:
                                        recoveredStatus: '${ $childError.status }'
                          - sibling:
                              set:
                                sibling: true
                """
                    .getBytes(StandardCharsets.UTF_8),
                java.util.List.of(),
                (namespace, name, version) -> java.util.Optional.of(child));
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(2));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(3));
    var launch =
        assertInstanceOf(
            EngineEvent.ForkBranchSubworkflowRequested.class,
            runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(4)).events().getFirst());

    var caught =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.SubworkflowCompleted(
                    UUID.randomUUID(),
                    executionId,
                    launch.operationId(),
                    launch.childExecutionId(),
                    ExecutionStatus.FAILED,
                    JsonNodeFactory.instance.objectNode(),
                    "fork child unavailable",
                    REQUESTED_AT.plusSeconds(5),
                    replyTo));
    assertInstanceOf(EngineEvent.ErrorRaised.class, caught.events().get(0));
    assertInstanceOf(EngineEvent.ForkBranchErrorCaught.class, caught.events().get(1));
    assertInstanceOf(WorkflowState.Running.class, caught.state());
    assertEquals(caught.state(), kit.restart().state());

    WorkflowState state = caught.state();
    for (int step = 0; step < 4 && state.status() != ExecutionStatus.COMPLETED; step++) {
      state = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(6L + step)).state();
    }
    var completed = assertInstanceOf(WorkflowState.Completed.class, state);
    assertEquals(500, completed.data().get(0).required("recoveredStatus").intValue());
    assertEquals(true, completed.data().get(1).required("sibling").booleanValue());
  }

  @Test
  void listenAllPersistsFilteringCorrelationDeduplicationAndRecovery() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:listen");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: durable-listen
                  version: '1.0.0'
                do:
                  - collect:
                      listen:
                        to:
                          all:
                            - with:
                                type: evidence.first.v1
                              correlate:
                                caseId:
                                  from: .caseId
                            - with:
                                type: evidence.second.v1
                              correlate:
                                caseId:
                                  from: .caseId
                        read: envelope
                """
                    .getBytes(StandardCharsets.UTF_8));
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    var started = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));
    assertInstanceOf(EngineEvent.ListenStarted.class, started.events().getFirst());
    assertInstanceOf(WorkflowState.Waiting.class, started.state());

    var paused =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Pause(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "operator"),
                    REQUESTED_AT.plusMillis(1100),
                    replyTo));
    assertInstanceOf(WorkflowState.Paused.class, paused.state());
    assertInstanceOf(WorkflowState.Paused.class, kit.restart().state());
    var pausedEvent =
        new com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent(
            "1.0",
            "paused-1",
            java.net.URI.create("https://events.test"),
            "evidence.first.v1",
            null,
            REQUESTED_AT,
            "application/json",
            JsonNodeFactory.instance.objectNode().put("caseId", "case-7"),
            java.util.Map.of());
    var pausedDelivery =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.CloudEventReceived(
                    executionId, pausedEvent, REQUESTED_AT.plusMillis(1150), replyTo));
    assertEquals(
        "execution_paused", pausedDelivery.replyOfType(WorkflowReply.Rejected.class).code());
    assertTrue(pausedDelivery.hasNoEvents());
    assertInstanceOf(
        WorkflowState.Waiting.class,
        kit.<WorkflowReply>runCommand(
                replyTo ->
                    new WorkflowCommand.Resume(
                        UUID.randomUUID(),
                        executionId,
                        actor(tenant, "operator"),
                        REQUESTED_AT.plusMillis(1200),
                        replyTo))
            .state());

    var firstData = JsonNodeFactory.instance.objectNode().put("caseId", "case-7").put("value", 1);
    var first =
        new com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent(
            "1.0",
            "first-1",
            java.net.URI.create("https://events.test"),
            "evidence.first.v1",
            null,
            REQUESTED_AT,
            "application/json",
            firstData,
            java.util.Map.of());
    var accepted =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.CloudEventReceived(
                    executionId, first, REQUESTED_AT.plusSeconds(2), replyTo));
    EngineEvent.ListenEventAccepted firstAccepted =
        assertInstanceOf(EngineEvent.ListenEventAccepted.class, accepted.events().getFirst());
    assertTrue(!firstAccepted.completed());
    assertEquals(
        ExecutionStatus.WAITING, accepted.replyOfType(WorkflowReply.Accepted.class).status());
    assertInstanceOf(WorkflowState.Waiting.class, accepted.state());
    assertEquals(accepted.state(), kit.restart().state());
    assertTrue(
        kit.runCommand(
                new WorkflowCommand.CloudEventReceived(
                    executionId, first, REQUESTED_AT.plusSeconds(3)))
            .hasNoEvents());

    var wrongData = JsonNodeFactory.instance.objectNode().put("caseId", "other");
    var wrong =
        new com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent(
            "1.0",
            "second-wrong",
            java.net.URI.create("https://events.test"),
            "evidence.second.v1",
            null,
            REQUESTED_AT,
            "application/json",
            wrongData,
            java.util.Map.of());
    assertTrue(
        kit.runCommand(
                new WorkflowCommand.CloudEventReceived(
                    executionId, wrong, REQUESTED_AT.plusSeconds(4)))
            .hasNoEvents());

    var secondData = JsonNodeFactory.instance.objectNode().put("caseId", "case-7").put("value", 2);
    var second =
        new com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent(
            "1.0",
            "second-1",
            java.net.URI.create("https://events.test"),
            "evidence.second.v1",
            null,
            REQUESTED_AT,
            "application/json",
            secondData,
            java.util.Map.of());
    var completedResult =
        kit.runCommand(
            new WorkflowCommand.CloudEventReceived(
                executionId, second, REQUESTED_AT.plusSeconds(5)));
    EngineEvent.ListenEventAccepted secondAccepted =
        assertInstanceOf(
            EngineEvent.ListenEventAccepted.class, completedResult.events().getFirst());
    assertTrue(secondAccepted.completed());
    WorkflowState.Completed completed =
        assertInstanceOf(WorkflowState.Completed.class, completedResult.state());
    assertEquals(2, completed.data().size());
    assertEquals("evidence.first.v1", completed.data().get(0).required("type").textValue());
    assertEquals("evidence.second.v1", completed.data().get(1).required("type").textValue());
    assertEquals(completed, kit.restart().state());

    var cancelledId = new ExecutionId(tenant, UUID.randomUUID());
    var cancelledKit = testKit(cancelledId);
    cancelledKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                cancelledId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(cancelledKit, cancelledId, tenant, REQUESTED_AT.plusSeconds(1));
    var cancelled =
        cancelledKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Cancel(
                    UUID.randomUUID(),
                    cancelledId,
                    actor(tenant, "operator"),
                    REQUESTED_AT.plusSeconds(2),
                    replyTo));
    assertInstanceOf(WorkflowState.Cancelled.class, cancelled.state());
    assertInstanceOf(WorkflowState.Cancelled.class, cancelledKit.restart().state());
    var terminalDelivery =
        cancelledKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.CloudEventReceived(
                    cancelledId, first, REQUESTED_AT.plusSeconds(3), replyTo));
    assertTrue(terminalDelivery.hasNoEvents());
    assertEquals(
        ExecutionStatus.CANCELLED,
        terminalDelivery.replyOfType(WorkflowReply.Accepted.class).status());
  }

  @Test
  void listenOneSupportsLiteralCorrelationAndAnySupportsBothUntilForms() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:listen-strategies");
    WorkflowPlan onePlan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: listen-one
                  version: '1.0.0'
                do:
                  - receive:
                      listen:
                        to:
                          one:
                            with:
                              type: evidence.one.v1
                            correlate:
                              caseId:
                                from: .caseId
                                expect: case-7
                        read: data
                """
                    .getBytes(StandardCharsets.UTF_8));
    var oneId = new ExecutionId(tenant, UUID.randomUUID());
    var oneKit = testKit(oneId);
    oneKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                oneId,
                actor(tenant, "alice"),
                onePlan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(oneKit, oneId, tenant, REQUESTED_AT.plusSeconds(1));
    var wrong =
        cloudEvent(
            "one-wrong",
            "evidence.one.v1",
            JsonNodeFactory.instance.objectNode().put("caseId", "other"));
    assertTrue(
        oneKit
            .runCommand(
                new WorkflowCommand.CloudEventReceived(oneId, wrong, REQUESTED_AT.plusSeconds(2)))
            .hasNoEvents());
    var expectedData =
        JsonNodeFactory.instance.objectNode().put("caseId", "case-7").put("value", 1);
    var oneCompleted =
        oneKit.runCommand(
            new WorkflowCommand.CloudEventReceived(
                oneId,
                cloudEvent("one", "evidence.one.v1", expectedData),
                REQUESTED_AT.plusSeconds(3)));
    assertEquals(
        expectedData,
        assertInstanceOf(WorkflowState.Completed.class, oneCompleted.state()).data().get(0));

    WorkflowPlan untilConditionPlan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: listen-until-condition
                  version: '1.0.0'
                do:
                  - receive:
                      listen:
                        to:
                          any: []
                          until: ( . | length ) >= 2
                """
                    .getBytes(StandardCharsets.UTF_8));
    var conditionId = new ExecutionId(tenant, UUID.randomUUID());
    var conditionKit = testKit(conditionId);
    conditionKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                conditionId,
                actor(tenant, "alice"),
                untilConditionPlan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(conditionKit, conditionId, tenant, REQUESTED_AT.plusSeconds(1));
    var first =
        conditionKit.runCommand(
            new WorkflowCommand.CloudEventReceived(
                conditionId,
                cloudEvent(
                    "any-1",
                    "anything.v1",
                    JsonNodeFactory.instance.objectNode().put("sequence", 1)),
                REQUESTED_AT.plusSeconds(2)));
    assertInstanceOf(WorkflowState.Waiting.class, first.state());
    var conditionCompleted =
        conditionKit.runCommand(
            new WorkflowCommand.CloudEventReceived(
                conditionId,
                cloudEvent(
                    "any-2", "else.v1", JsonNodeFactory.instance.objectNode().put("sequence", 2)),
                REQUESTED_AT.plusSeconds(3)));
    assertEquals(
        2,
        assertInstanceOf(WorkflowState.Completed.class, conditionCompleted.state()).data().size());

    WorkflowPlan untilEventPlan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: listen-until-event
                  version: '1.0.0'
                do:
                  - receive:
                      listen:
                        to:
                          any:
                            - with:
                                type: evidence.item.v1
                          until:
                            one:
                              with:
                                type: evidence.stop.v1
                        read: envelope
                """
                    .getBytes(StandardCharsets.UTF_8));
    var untilId = new ExecutionId(tenant, UUID.randomUUID());
    var untilKit = testKit(untilId);
    untilKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                untilId,
                actor(tenant, "alice"),
                untilEventPlan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(untilKit, untilId, tenant, REQUESTED_AT.plusSeconds(1));
    untilKit.runCommand(
        new WorkflowCommand.CloudEventReceived(
            untilId,
            cloudEvent(
                "item-1",
                "evidence.item.v1",
                JsonNodeFactory.instance.objectNode().put("value", 1)),
            REQUESTED_AT.plusSeconds(2)));
    var untilCompleted =
        untilKit.runCommand(
            new WorkflowCommand.CloudEventReceived(
                untilId,
                cloudEvent(
                    "stop",
                    "evidence.stop.v1",
                    JsonNodeFactory.instance.objectNode().put("stop", true)),
                REQUESTED_AT.plusSeconds(3)));
    var untilOutput =
        assertInstanceOf(WorkflowState.Completed.class, untilCompleted.state()).data();
    assertEquals(1, untilOutput.size());
    assertEquals("evidence.item.v1", untilOutput.get(0).required("type").textValue());

    WorkflowPlan untilAllPlan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: listen-until-all
                  version: '1.0.0'
                do:
                  - receive:
                      listen:
                        to:
                          any:
                            - with:
                                type: evidence.item.v1
                          until:
                            all:
                              - with:
                                  type: evidence.stop-a.v1
                              - with:
                                  type: evidence.stop-b.v1
                """
                    .getBytes(StandardCharsets.UTF_8));
    var untilAllId = new ExecutionId(tenant, UUID.randomUUID());
    var untilAllKit = testKit(untilAllId);
    untilAllKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                untilAllId,
                actor(tenant, "alice"),
                untilAllPlan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(untilAllKit, untilAllId, tenant, REQUESTED_AT.plusSeconds(1));
    untilAllKit.runCommand(
        new WorkflowCommand.CloudEventReceived(
            untilAllId,
            cloudEvent(
                "kept", "evidence.item.v1", JsonNodeFactory.instance.objectNode().put("value", 7)),
            REQUESTED_AT.plusSeconds(2)));
    var partialUntil =
        untilAllKit.runCommand(
            new WorkflowCommand.CloudEventReceived(
                untilAllId,
                cloudEvent("stop-a", "evidence.stop-a.v1", JsonNodeFactory.instance.objectNode()),
                REQUESTED_AT.plusSeconds(3)));
    assertInstanceOf(EngineEvent.ListenUntilAdvanced.class, partialUntil.events().getFirst());
    assertInstanceOf(WorkflowState.Waiting.class, partialUntil.state());
    assertEquals(partialUntil.state(), untilAllKit.restart().state());
    var untilAllCompleted =
        untilAllKit.runCommand(
            new WorkflowCommand.CloudEventReceived(
                untilAllId,
                cloudEvent("stop-b", "evidence.stop-b.v1", JsonNodeFactory.instance.objectNode()),
                REQUESTED_AT.plusSeconds(4)));
    var untilAllOutput =
        assertInstanceOf(WorkflowState.Completed.class, untilAllCompleted.state()).data();
    assertEquals(1, untilAllOutput.size());
    assertEquals(7, untilAllOutput.get(0).required("value").intValue());
  }

  @Test
  void listenForeachProcessesTheDurableCollectionInFifoOrder() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:listen-foreach");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: listen-foreach
                  version: '1.0.0'
                do:
                  - receive:
                      listen:
                        to:
                          any: []
                          until: ( . | length ) >= 2
                      foreach:
                        item: event
                        at: eventIndex
                        do:
                          - transform:
                              set:
                                value: '${ $event.value * 10 }'
                                index: '${ $eventIndex }'
                        output:
                          as: '${ . }'
                """
                    .getBytes(StandardCharsets.UTF_8));
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));
    var first =
        kit.runCommand(
            new WorkflowCommand.CloudEventReceived(
                executionId,
                cloudEvent(
                    "fifo-1", "item.v1", JsonNodeFactory.instance.objectNode().put("value", 1)),
                REQUESTED_AT.plusSeconds(2)));
    assertInstanceOf(WorkflowState.Waiting.class, first.state());
    var collectionReady =
        kit.runCommand(
            new WorkflowCommand.CloudEventReceived(
                executionId,
                cloudEvent(
                    "fifo-2", "item.v1", JsonNodeFactory.instance.objectNode().put("value", 2)),
                REQUESTED_AT.plusSeconds(3)));
    assertInstanceOf(EngineEvent.ListenIterationStarted.class, collectionReady.events().get(1));
    assertInstanceOf(WorkflowState.Running.class, collectionReady.state());

    var paused =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Pause(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "operator"),
                    REQUESTED_AT.plusSeconds(4),
                    replyTo));
    assertInstanceOf(WorkflowState.Paused.class, paused.state());
    assertInstanceOf(WorkflowState.Paused.class, kit.restart().state());
    WorkflowState state =
        kit.<WorkflowReply>runCommand(
                replyTo ->
                    new WorkflowCommand.Resume(
                        UUID.randomUUID(),
                        executionId,
                        actor(tenant, "operator"),
                        REQUESTED_AT.plusSeconds(5),
                        replyTo))
            .state();
    for (int i = 0; i < 8 && !(state instanceof WorkflowState.Completed); i++) {
      state = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(6 + i)).state();
    }
    var output = assertInstanceOf(WorkflowState.Completed.class, state).data();
    assertEquals(10, output.get(0).required("value").intValue());
    assertEquals(0, output.get(0).required("index").intValue());
    assertEquals(20, output.get(1).required("value").intValue());
    assertEquals(1, output.get(1).required("index").intValue());
    assertEquals(state, kit.restart().state());
  }

  private static com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent cloudEvent(
      String id, String type, com.fasterxml.jackson.databind.JsonNode data) {
    return new com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent(
        "1.0",
        id,
        java.net.URI.create("https://events.test"),
        type,
        null,
        REQUESTED_AT,
        "application/json",
        data,
        java.util.Map.of());
  }

  private static void assertRetryDelays(String backoff, long first, long second, long third) {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:backoff-" + backoff);
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                ("""
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: %s-backoff
                  version: '1.0.0'
                do:
                  - guarded:
                      try:
                        - reject:
                            raise:
                              error:
                                type: https://errors.example/retryable
                                status: 503
                      catch:
                        retry:
                          delay: PT10S
                          backoff:
                            %s: {}
                          limit:
                            attempt:
                              count: 4
                """)
                    .formatted(backoff, backoff)
                    .getBytes(StandardCharsets.UTF_8));
    var kit = testKit(executionId);
    Instant startedAt = Instant.now().plus(Duration.ofDays(1));
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                startedAt,
                replyTo));
    runNext(kit, executionId, tenant, startedAt);
    Instant at = startedAt;
    long[] expected = {first, second, third};
    for (long delay : expected) {
      Instant failedAt = at.plusSeconds(1);
      EngineEvent.RetryScheduled scheduled =
          assertInstanceOf(
              EngineEvent.RetryScheduled.class,
              runNext(kit, executionId, tenant, failedAt).events().get(1));
      assertEquals(failedAt.plusSeconds(delay), scheduled.deadline());
      kit.runCommand(
          new WorkflowCommand.RetryElapsed(executionId, "/do/0/guarded", scheduled.deadline()));
      at = scheduled.deadline();
    }
  }

  @Test
  void reusableFunctionPersistsItsImmutableInvocationAndSurvivesPauseResumeCancel() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:function-controls");
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: durable-function
                  version: '1.0.0'
                use:
                  functions:
                    double:
                      input:
                        schema:
                          format: json
                          document:
                            type: object
                            required: [amount]
                      set:
                        result: '${ .amount * 2 }'
                do:
                  - invoke:
                      input:
                        from: '${ {value: .payload} }'
                      call: double
                      with:
                        amount: '${ .value + 1 }'
                      output:
                        as: '${ {answer: .result, caller: $input.value} }'
                """
                    .getBytes(StandardCharsets.UTF_8));

    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode().put("payload", 3),
                REQUESTED_AT,
                replyTo));

    var entered = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));
    var event = assertInstanceOf(EngineEvent.FunctionEntered.class, entered.events().getFirst());
    assertEquals("double", event.operation().functionName());
    assertEquals(null, event.operation().resource());
    assertEquals(4, event.operation().arguments().required("amount").intValue());
    assertEquals(3, event.input().required("value").intValue());
    assertEquals(entered.state(), kit.restart().state());

    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Pause(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "operator"),
                REQUESTED_AT.plusSeconds(2),
                replyTo));
    var paused = assertInstanceOf(WorkflowState.Paused.class, kit.restart().state());
    assertEquals(4, paused.data().required("amount").intValue());
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Resume(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "operator"),
                REQUESTED_AT.plusSeconds(3),
                replyTo));

    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(4));
    var completed =
        assertInstanceOf(
            WorkflowState.Completed.class,
            runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(5)).state());
    assertEquals(8, completed.data().required("answer").intValue());
    assertEquals(3, completed.data().required("caller").intValue());
    assertEquals(completed, kit.restart().state());

    var cancelledId = new ExecutionId(tenant, UUID.randomUUID());
    var cancelledKit = testKit(cancelledId);
    cancelledKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                cancelledId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode().put("payload", 5),
                REQUESTED_AT,
                replyTo));
    runNext(cancelledKit, cancelledId, tenant, REQUESTED_AT.plusSeconds(1));
    var cancelled =
        cancelledKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Cancel(
                    UUID.randomUUID(),
                    cancelledId,
                    actor(tenant, "operator"),
                    REQUESTED_AT.plusSeconds(2),
                    replyTo));
    assertInstanceOf(WorkflowState.Cancelled.class, cancelled.state());
    assertEquals(cancelled.state(), cancelledKit.restart().state());
  }

  @Test
  void nestedForkFunctionUsesTheSameDurableFrameAndLifecycleBoundary() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:function-fork");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: nested-fork-function
                  version: '1.0.0'
                use:
                  functions:
                    mark:
                      set:
                        value: '${ .value }'
                        invoked: true
                do:
                  - root:
                      fork:
                        branches:
                          - nested:
                              fork:
                                branches:
                                  - invoke:
                                      call: mark
                                      with:
                                        value: 11
                                  - sibling:
                                      set:
                                        sibling: true
                          - other:
                              call: mark
                              with:
                                value: 22
                """
                    .getBytes(StandardCharsets.UTF_8));
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));

    EngineEvent.ForkNestedFunctionEntered functionEntered = null;
    boolean rootFunctionEntered = false;
    for (int attempt = 1; attempt <= 8 && functionEntered == null; attempt++) {
      var advanced = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(attempt));
      for (EngineEvent event : advanced.events()) {
        if (event instanceof EngineEvent.ForkNestedFunctionEntered entered) {
          functionEntered = entered;
        }
        rootFunctionEntered |= event instanceof EngineEvent.ForkBranchFunctionEntered;
      }
    }
    assertTrue(functionEntered != null);
    assertEquals(List.of(0, 0), functionEntered.branchPath());
    assertEquals(11, functionEntered.operation().arguments().required("value").intValue());
    WorkflowState beforePause = kit.restart().state();
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Pause(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "operator"),
                REQUESTED_AT.plusSeconds(10),
                replyTo));
    assertInstanceOf(WorkflowState.Paused.class, kit.restart().state());
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Resume(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "operator"),
                REQUESTED_AT.plusSeconds(11),
                replyTo));
    assertEquals(beforePause.data(), kit.restart().state().data());

    WorkflowState state = kit.restart().state();
    for (int attempt = 12;
        attempt <= 30 && !(state instanceof WorkflowState.Completed);
        attempt++) {
      var advanced = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(attempt));
      rootFunctionEntered |=
          advanced.events().stream()
              .anyMatch(EngineEvent.ForkBranchFunctionEntered.class::isInstance);
      state = advanced.state();
    }
    var completed = assertInstanceOf(WorkflowState.Completed.class, state);
    assertTrue(rootFunctionEntered);
    assertTrue(completed.data().toString().contains("\"invoked\":true"));
    assertEquals(completed, kit.restart().state());
  }

  @Test
  void catalogFunctionDescriptorCarriesTheAdmittedResourceDigest() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:function-catalog");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: forwardmeasure
          name: catalog-function-runtime
          version: '1.0.0'
        use:
          catalogs:
            approved:
              endpoint:
                uri: https://catalog.example.test/
        do:
          - invoke:
              call: normalize:1.2.3@approved
              with:
                value: 7
        """
            .getBytes(StandardCharsets.UTF_8);
    var resources =
        new com.forwardmeasure.openworkflow.definition.WorkflowResourceResolver()
            .resolve(
                source,
                request ->
                    com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource.of(
                        request.uri(),
                        "application/yaml",
                        """
                        set:
                          normalized: '${ .value }'
                        """));
    WorkflowPlan plan = new OpenWorkflowCompiler().compile(source, resources);
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));

    var entered =
        assertInstanceOf(
            EngineEvent.FunctionEntered.class,
            runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1)).events().getFirst());
    assertEquals(resources.getFirst().sha256(), entered.operation().resource().sha256());
    assertEquals(resources.getFirst().uri(), entered.operation().resource().uri());
    assertEquals(entered.operation().resource(), plan.steps().getFirst().callPlan().resource());
    assertEquals(kit.restart().state(), kit.restart().state());
  }

  @Test
  void functionBodyFailureParticipatesInTheContainingStructuredCatch() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:function-catch");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: function-catch
                  version: '1.0.0'
                use:
                  functions:
                    fail:
                      raise:
                        error:
                          type: https://errors.example/function
                          status: 503
                do:
                  - guarded:
                      try:
                        - invoke:
                            call: fail
                      catch:
                        errors:
                          with:
                            type: https://errors.example/function
                        do:
                          - recover:
                              set:
                                recovered: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(2));
    var caught = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(3));
    assertTrue(caught.events().stream().anyMatch(EngineEvent.ErrorCaught.class::isInstance));
    assertEquals(caught.state(), kit.restart().state());
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(4));
    var completed =
        assertInstanceOf(
            WorkflowState.Completed.class,
            runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(5)).state());
    assertTrue(completed.data().required("recovered").booleanValue());
  }

  @Test
  void httpCallIsDurableAndWorkflowWidePauseAndCancelGovernItsResult() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:http-call");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: durable-http
                  version: '1.0.0'
                do:
                  - invoke:
                      call: http
                      with:
                        method: POST
                        endpoint: https://api.example.test/items/{item}
                        headers:
                          X-Request: '${ .request }'
                        body:
                          accepted: '${ .accepted }'
                        query:
                          view: complete
                        output: response
                """
                    .getBytes(StandardCharsets.UTF_8));
    var input =
        JsonNodeFactory.instance
            .objectNode()
            .put("item", "a b")
            .put("request", "r-1")
            .put("accepted", true);
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                input,
                REQUESTED_AT,
                replyTo));

    var requested =
        assertInstanceOf(
            EngineEvent.HttpCallRequested.class,
            runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1)).events().getFirst());
    assertEquals("POST", requested.operation().method());
    assertEquals(
        "https://api.example.test/items/a%20b?view=complete",
        requested.operation().uri().toString());
    assertEquals("r-1", requested.operation().headers().get("X-Request"));
    assertTrue(requested.operation().body().required("accepted").booleanValue());
    assertInstanceOf(WorkflowState.Waiting.class, kit.getState());
    assertEquals(kit.getState(), kit.restart().state());

    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Pause(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                REQUESTED_AT.plusSeconds(2),
                replyTo));
    var pausedResult =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.HttpCallCompleted(
                    executionId,
                    requested.operation().operationId(),
                    JsonNodeFactory.instance.objectNode().put("statusCode", 200),
                    null,
                    REQUESTED_AT.plusSeconds(3),
                    replyTo));
    assertTrue(pausedResult.hasNoEvents());
    assertEquals("execution_paused", pausedResult.replyOfType(WorkflowReply.Rejected.class).code());
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Resume(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                REQUESTED_AT.plusSeconds(4),
                replyTo));
    var completed =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.HttpCallCompleted(
                    executionId,
                    requested.operation().operationId(),
                    JsonNodeFactory.instance.objectNode().put("statusCode", 200),
                    null,
                    REQUESTED_AT.plusSeconds(5),
                    replyTo));
    assertTrue(
        completed.events().stream().anyMatch(EngineEvent.HttpCallCompleted.class::isInstance));
    assertInstanceOf(WorkflowState.Completed.class, completed.state());

    var cancelledExecution = new ExecutionId(tenant, UUID.randomUUID());
    var cancelledKit = testKit(cancelledExecution);
    cancelledKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                cancelledExecution,
                actor(tenant, "alice"),
                plan,
                input,
                REQUESTED_AT,
                replyTo));
    var cancelledRequest =
        assertInstanceOf(
            EngineEvent.HttpCallRequested.class,
            runNext(cancelledKit, cancelledExecution, tenant, REQUESTED_AT.plusSeconds(1))
                .events()
                .getFirst());
    cancelledKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Cancel(
                UUID.randomUUID(),
                cancelledExecution,
                actor(tenant, "alice"),
                REQUESTED_AT.plusSeconds(2),
                replyTo));
    var late =
        cancelledKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.HttpCallCompleted(
                    cancelledExecution,
                    cancelledRequest.operation().operationId(),
                    JsonNodeFactory.instance.objectNode().put("ignored", true),
                    null,
                    REQUESTED_AT.plusSeconds(3),
                    replyTo));
    assertTrue(late.hasNoEvents());
    assertEquals(
        ExecutionStatus.CANCELLED, late.replyOfType(WorkflowReply.Accepted.class).status());
  }

  @Test
  void openApiCallUsesOnlyThePinnedDocumentToMaterializeItsOperation() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:openapi-call");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: forwardmeasure
          name: pinned-openapi
          version: '1.0.0'
        do:
          - invoke:
              call: openapi
              with:
                document:
                  endpoint: https://contracts.example.test/items.yaml
                operationId: createItem
                parameters:
                  account: '${ .account }'
                  verbose: true
                  name: '${ .name }'
                output: content
        """
            .getBytes(StandardCharsets.UTF_8);
    var resources =
        new com.forwardmeasure.openworkflow.definition.WorkflowResourceResolver()
            .resolve(
                source,
                request ->
                    com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource.of(
                        request.uri(),
                        "application/yaml",
                        """
                        openapi: 3.1.0
                        info: {title: Items, version: 1.0.0}
                        servers:
                          - url: https://api.example.test
                        paths:
                          /accounts/{account}/items:
                            post:
                              operationId: createItem
                              parameters:
                                - {name: account, in: path, required: true, schema: {type: string}}
                                - {name: verbose, in: query, schema: {type: boolean}}
                              requestBody:
                                content:
                                  application/json:
                                    schema: {type: object}
                              responses:
                                '200': {description: created}
                        """));
    WorkflowPlan plan = new OpenWorkflowCompiler().compile(source, resources);
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance
                    .objectNode()
                    .put("account", "north west")
                    .put("name", "evidence"),
                REQUESTED_AT,
                replyTo));
    var requested =
        assertInstanceOf(
            EngineEvent.HttpCallRequested.class,
            runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1)).events().getFirst());
    assertEquals(
        com.forwardmeasure.openworkflow.engine.api.HttpOperationDescriptor.Kind.OPEN_API,
        requested.operation().kind());
    assertEquals("POST", requested.operation().method());
    assertEquals(
        "https://api.example.test/accounts/north%20west/items?verbose=true",
        requested.operation().uri().toString());
    assertEquals("evidence", requested.operation().body().required("name").asText());
    assertEquals(resources.getFirst().sha256(), requested.operation().openApiDocument().sha256());
    assertEquals("createItem", requested.operation().openApiOperationId());
    assertEquals(kit.getState(), kit.restart().state());
  }

  @Test
  void swaggerTwoCallUsesPinnedHostBasePathAndParameters() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:swagger-two");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: forwardmeasure
          name: pinned-swagger-two
          version: '1.0.0'
        do:
          - getPet:
              call: openapi
              with:
                document:
                  endpoint: https://contracts.example.test/swagger.json
                operationId: getPetById
                parameters:
                  petId: '${ .petId }'
                  trace: '${ .trace }'
                output: response
        """
            .getBytes(StandardCharsets.UTF_8);
    var resources =
        new com.forwardmeasure.openworkflow.definition.WorkflowResourceResolver()
            .resolve(
                source,
                request ->
                    com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource.of(
                        request.uri(),
                        "application/json",
                        """
                        {
                          "swagger": "2.0",
                          "info": {"title": "Pets", "version": "1.0.0"},
                          "schemes": ["https"],
                          "host": "petstore.example.test",
                          "basePath": "/v2",
                          "paths": {
                            "/pet/{petId}": {
                              "get": {
                                "operationId": "getPetById",
                                "parameters": [
                                  {"name":"petId","in":"path","required":true,"type":"integer"},
                                  {"name":"trace","in":"query","type":"string"}
                                ],
                                "responses": {"200":{"description":"ok"}}
                              }
                            }
                          }
                        }
                        """));
    WorkflowPlan plan = new OpenWorkflowCompiler().compile(source, resources);
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode().put("petId", 1).put("trace", "a b"),
                REQUESTED_AT,
                replyTo));
    EngineEvent.HttpCallRequested requested =
        assertInstanceOf(
            EngineEvent.HttpCallRequested.class,
            runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1)).events().getFirst());
    assertEquals("GET", requested.operation().method());
    assertEquals(
        "https://petstore.example.test/v2/pet/1?trace=a%20b",
        requested.operation().uri().toString());
    assertEquals(resources.getFirst().sha256(), requested.operation().openApiDocument().sha256());
  }

  @Test
  void nestedForkHttpCallObeysWorkflowWidePauseResumeAndCancel() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:http-fork");
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: nested-fork-http
                  version: '1.0.0'
                do:
                  - root:
                      fork:
                        branches:
                          - nested:
                              fork:
                                branches:
                                  - remote:
                                      call: http
                                      with:
                                        method: POST
                                        endpoint: https://api.example.test/items
                                        body:
                                          value: 11
                                  - sibling:
                                      set:
                                        sibling: true
                          - other:
                              set:
                                other: true
                """
                    .getBytes(StandardCharsets.UTF_8));

    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));

    EngineEvent.ForkBranchHttpCallRequested request = null;
    for (int attempt = 1; attempt <= 10 && request == null; attempt++) {
      var advanced = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(attempt));
      request =
          advanced.events().stream()
              .filter(EngineEvent.ForkBranchHttpCallRequested.class::isInstance)
              .map(EngineEvent.ForkBranchHttpCallRequested.class::cast)
              .findFirst()
              .orElse(null);
    }
    assertTrue(request != null);
    assertEquals(List.of(0, 0), request.branchPath());
    assertEquals(kit.getState(), kit.restart().state());

    String operationId = request.operation().operationId();
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Pause(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "operator"),
                REQUESTED_AT.plusSeconds(11),
                replyTo));
    var whilePaused =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.HttpCallCompleted(
                    executionId,
                    operationId,
                    JsonNodeFactory.instance.objectNode().put("accepted", true),
                    null,
                    REQUESTED_AT.plusSeconds(12),
                    replyTo));
    assertEquals("execution_paused", whilePaused.replyOfType(WorkflowReply.Rejected.class).code());
    assertEquals(whilePaused.state(), kit.restart().state());

    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Resume(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "operator"),
                REQUESTED_AT.plusSeconds(13),
                replyTo));
    var observed =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.HttpCallCompleted(
                    executionId,
                    operationId,
                    JsonNodeFactory.instance.objectNode().put("accepted", true),
                    null,
                    REQUESTED_AT.plusSeconds(14),
                    replyTo));
    var completedEvent =
        observed.events().stream()
            .filter(EngineEvent.ForkBranchHttpCallCompleted.class::isInstance)
            .map(EngineEvent.ForkBranchHttpCallCompleted.class::cast)
            .findFirst()
            .orElseThrow();
    assertEquals(List.of(0, 0), completedEvent.branchPath());

    WorkflowState state = observed.state();
    for (int attempt = 15;
        attempt <= 35 && !(state instanceof WorkflowState.Completed);
        attempt++) {
      state = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(attempt)).state();
    }
    assertInstanceOf(WorkflowState.Completed.class, state);

    var cancelledId = new ExecutionId(tenant, UUID.randomUUID());
    var cancelledKit = testKit(cancelledId);
    cancelledKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                cancelledId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    EngineEvent.ForkBranchHttpCallRequested cancelledRequest = null;
    for (int attempt = 1; attempt <= 10 && cancelledRequest == null; attempt++) {
      var advanced = runNext(cancelledKit, cancelledId, tenant, REQUESTED_AT.plusSeconds(attempt));
      cancelledRequest =
          advanced.events().stream()
              .filter(EngineEvent.ForkBranchHttpCallRequested.class::isInstance)
              .map(EngineEvent.ForkBranchHttpCallRequested.class::cast)
              .findFirst()
              .orElse(null);
    }
    assertTrue(cancelledRequest != null);
    String cancelledOperationId = cancelledRequest.operation().operationId();
    cancelledKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Pause(
                UUID.randomUUID(),
                cancelledId,
                actor(tenant, "operator"),
                REQUESTED_AT.plusSeconds(11),
                replyTo));
    assertInstanceOf(WorkflowState.Paused.class, cancelledKit.restart().state());
    cancelledKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Cancel(
                UUID.randomUUID(),
                cancelledId,
                actor(tenant, "operator"),
                REQUESTED_AT.plusSeconds(12),
                replyTo));
    var late =
        cancelledKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.HttpCallCompleted(
                    cancelledId,
                    cancelledOperationId,
                    JsonNodeFactory.instance.objectNode().put("ignored", true),
                    null,
                    REQUESTED_AT.plusSeconds(13),
                    replyTo));
    assertTrue(late.hasNoEvents());
    assertEquals(
        ExecutionStatus.CANCELLED, late.replyOfType(WorkflowReply.Accepted.class).status());
    assertInstanceOf(WorkflowState.Cancelled.class, cancelledKit.restart().state());
  }

  @Test
  void httpFailureParticipatesInTheContainingStructuredCatch() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:http-catch");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: http-catch
                  version: '1.0.0'
                do:
                  - guarded:
                      try:
                        - remote:
                            call: http
                            with:
                              method: GET
                              endpoint: https://api.example.test/unavailable
                      catch:
                        errors:
                          with:
                            type: https://serverlessworkflow.io/dsl/errors/types/communication
                            status: 503
                        do:
                          - recover:
                              set:
                                recovered: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var kit = automaticTestKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));

    EngineEvent.HttpCallRequested request = null;
    for (int attempt = 1; attempt <= 5 && request == null; attempt++) {
      var advanced = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(attempt));
      request =
          advanced.events().stream()
              .filter(EngineEvent.HttpCallRequested.class::isInstance)
              .map(EngineEvent.HttpCallRequested.class::cast)
              .findFirst()
              .orElse(null);
    }
    assertTrue(request != null);
    var problem =
        JsonNodeFactory.instance
            .objectNode()
            .put("type", "urn:openworkflow:http:status:503")
            .put("status", 503)
            .put("title", "Service unavailable")
            .put("detail", "try again later");
    String operationId = request.operation().operationId();
    var failed =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.HttpCallCompleted(
                    executionId, operationId, null, problem, REQUESTED_AT.plusSeconds(6), replyTo));
    assertTrue(failed.events().stream().anyMatch(EngineEvent.ErrorRaised.class::isInstance));
    assertTrue(failed.events().stream().anyMatch(EngineEvent.ErrorCaught.class::isInstance));
    EngineEvent.ErrorRaised raised =
        failed.events().stream()
            .filter(EngineEvent.ErrorRaised.class::isInstance)
            .map(EngineEvent.ErrorRaised.class::cast)
            .findFirst()
            .orElseThrow();
    assertEquals(
        "https://serverlessworkflow.io/dsl/errors/types/communication",
        raised.error().required("type").textValue());
    assertEquals("/do/0/guarded/try/0/remote", raised.error().required("instance").textValue());
    var completed = assertInstanceOf(WorkflowState.Completed.class, kit.getState());
    assertTrue(completed.data().required("recovered").booleanValue());
    assertEquals(completed, kit.restart().state());
  }

  @Test
  void httpAuthenticationPersistsOnlyItsCredentialFreeEvaluationScope() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:http-auth-scope");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: http-auth-scope
                  version: '1.0.0'
                use:
                  secrets:
                    - api-token
                do:
                  - remote:
                      call: http
                      with:
                        method: GET
                        endpoint:
                          uri: https://api.example.test/secured
                          authentication:
                            bearer:
                              token: '${ $secrets["api-token"] }'
                """
                    .getBytes(StandardCharsets.UTF_8));
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode().put("account", "north"),
                REQUESTED_AT,
                replyTo));

    var requested =
        assertInstanceOf(
            EngineEvent.HttpCallRequested.class,
            runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1)).events().getFirst());

    assertEquals(
        java.util.List.of("api-token"), requested.operation().authentication().secretReferences());
    assertEquals(
        "north",
        requested.operation().authenticationContext().input().required("account").asText());
    assertTrue(!requested.operation().toString().contains("edge-only-token"));
    var recovered = assertInstanceOf(WorkflowState.Waiting.class, kit.restart().state());
    assertEquals(
        requested.operation().operationId(), recovered.taskStack().getLast().event().operationId());
  }

  @Test
  void securedRunIntentRecoversAndWorkflowPauseResumeCancelGovernIt() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:durable-run");
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: durable-shell-run
                  version: '1.0.0'
                do:
                  - execute:
                      run:
                        await: true
                        return: all
                        shell:
                          command: approved-command
                          arguments: ['${ .id }']
                """
                    .getBytes(StandardCharsets.UTF_8));
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode().put("id", "ev-42"),
                REQUESTED_AT,
                replyTo));
    var requested =
        assertInstanceOf(
            EngineEvent.ProtocolCallRequested.class,
            runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1)).events().getFirst());
    assertEquals(
        com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.RUN,
        requested.operation().kind());
    assertEquals(
        "ev-42",
        requested
            .operation()
            .request()
            .required("configuration")
            .required("arguments")
            .get(0)
            .asText());
    assertEquals(kit.getState(), kit.restart().state());

    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Pause(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                REQUESTED_AT.plusSeconds(2),
                replyTo));
    var paused =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.ProtocolCallObserved(
                    executionId,
                    requested.operation().operationId(),
                    "run-paused",
                    JsonNodeFactory.instance.objectNode().put("code", 0),
                    false,
                    true,
                    REQUESTED_AT.plusSeconds(3),
                    replyTo));
    assertEquals("execution_paused", paused.replyOfType(WorkflowReply.Rejected.class).code());
    assertEquals(paused.state(), kit.restart().state());
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Resume(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                REQUESTED_AT.plusSeconds(4),
                replyTo));
    var output =
        JsonNodeFactory.instance
            .objectNode()
            .put("stdout", "done")
            .put("stderr", "")
            .put("code", 0);
    var completed =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.ProtocolCallObserved(
                    executionId,
                    requested.operation().operationId(),
                    "run-complete",
                    output,
                    false,
                    true,
                    REQUESTED_AT.plusSeconds(5),
                    replyTo));
    assertEquals(
        "done",
        assertInstanceOf(WorkflowState.Completed.class, completed.state())
            .data()
            .required("stdout")
            .asText());

    var cancelledId = new ExecutionId(tenant, UUID.randomUUID());
    var cancelledKit = testKit(cancelledId);
    cancelledKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                cancelledId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode().put("id", "ev-43"),
                REQUESTED_AT,
                replyTo));
    var cancelledRequest =
        assertInstanceOf(
            EngineEvent.ProtocolCallRequested.class,
            runNext(cancelledKit, cancelledId, tenant, REQUESTED_AT.plusSeconds(1))
                .events()
                .getFirst());
    cancelledKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Cancel(
                UUID.randomUUID(),
                cancelledId,
                actor(tenant, "alice"),
                REQUESTED_AT.plusSeconds(2),
                replyTo));
    var late =
        cancelledKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.ProtocolCallObserved(
                    cancelledId,
                    cancelledRequest.operation().operationId(),
                    "run-late",
                    output,
                    false,
                    true,
                    REQUESTED_AT.plusSeconds(3),
                    replyTo));
    assertTrue(late.hasNoEvents());
    assertEquals(
        ExecutionStatus.CANCELLED, late.replyOfType(WorkflowReply.Accepted.class).status());
    assertInstanceOf(WorkflowState.Cancelled.class, cancelledKit.restart().state());
  }

  @Test
  void a2aAndMcpCallsRecoverAndObeyWorkflowPauseAndCancellation() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:agent-protocols");
    byte[] a2aSource =
        """
        document:
          dsl: '1.0.3'
          namespace: forwardmeasure
          name: durable-a2a
          version: '1.0.0'
        do:
          - delegate:
              call: a2a
              with:
                server: https://agent.example.test/rpc
                method: message/stream
                parameters: {message: {messageId: ev-42}}
        """
            .getBytes(StandardCharsets.UTF_8);
    WorkflowPlan a2a = new OpenWorkflowCompiler().compile(a2aSource, java.util.List.of());
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                a2a,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    var requested =
        assertInstanceOf(
            EngineEvent.ProtocolCallRequested.class,
            runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1)).events().getFirst());
    assertEquals(
        com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.A2A,
        requested.operation().kind());
    assertEquals(kit.getState(), kit.restart().state());
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Pause(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                REQUESTED_AT.plusSeconds(2),
                replyTo));
    var paused =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.ProtocolCallObserved(
                    executionId,
                    requested.operation().operationId(),
                    "a2a-0",
                    JsonNodeFactory.instance.objectNode().put("state", "working"),
                    false,
                    false,
                    REQUESTED_AT.plusSeconds(3),
                    replyTo));
    assertEquals("execution_paused", paused.replyOfType(WorkflowReply.Rejected.class).code());
    assertEquals(paused.state(), kit.restart().state());
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Resume(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                REQUESTED_AT.plusSeconds(4),
                replyTo));
    var completed =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.ProtocolCallObserved(
                    executionId,
                    requested.operation().operationId(),
                    "a2a-1",
                    JsonNodeFactory.instance.objectNode().put("state", "completed"),
                    false,
                    true,
                    REQUESTED_AT.plusSeconds(5),
                    replyTo));
    assertInstanceOf(WorkflowState.Completed.class, completed.state());

    byte[] mcpSource =
        """
        document:
          dsl: '1.0.3'
          namespace: forwardmeasure
          name: durable-mcp
          version: '1.0.0'
        do:
          - tool:
              call: mcp
              with:
                method: tools/call
                parameters: {name: extract}
                transport:
                  stdio: {command: approved-mcp}
        """
            .getBytes(StandardCharsets.UTF_8);
    WorkflowPlan mcp = new OpenWorkflowCompiler().compile(mcpSource, java.util.List.of());
    var cancelledId = new ExecutionId(tenant, UUID.randomUUID());
    var cancelledKit = testKit(cancelledId);
    cancelledKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                cancelledId,
                actor(tenant, "alice"),
                mcp,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    var mcpRequested =
        assertInstanceOf(
            EngineEvent.ProtocolCallRequested.class,
            runNext(cancelledKit, cancelledId, tenant, REQUESTED_AT.plusSeconds(1))
                .events()
                .getFirst());
    assertEquals(
        com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.MCP,
        mcpRequested.operation().kind());
    cancelledKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Cancel(
                UUID.randomUUID(),
                cancelledId,
                actor(tenant, "alice"),
                REQUESTED_AT.plusSeconds(2),
                replyTo));
    var late =
        cancelledKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.ProtocolCallObserved(
                    cancelledId,
                    mcpRequested.operation().operationId(),
                    "mcp-late",
                    JsonNodeFactory.instance.objectNode().put("ignored", true),
                    false,
                    true,
                    REQUESTED_AT.plusSeconds(3),
                    replyTo));
    assertTrue(late.hasNoEvents());
    assertEquals(
        ExecutionStatus.CANCELLED, late.replyOfType(WorkflowReply.Accepted.class).status());
    assertInstanceOf(WorkflowState.Cancelled.class, cancelledKit.restart().state());
  }

  @Test
  void asyncApiStreamIsDurableAndWorkflowPauseResumeCancelGovernObservations() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:asyncapi-stream");
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: forwardmeasure
          name: durable-asyncapi-stream
          version: '1.0.0'
        do:
          - receive:
              call: asyncapi
              with:
                document:
                  endpoint: https://contracts.example.test/events.yaml
                operation: receiveEvidence
                subscription:
                  consume:
                    amount: 2
                    for: PT30S
        """
            .getBytes(StandardCharsets.UTF_8);
    var resources =
        new com.forwardmeasure.openworkflow.definition.WorkflowResourceResolver()
            .resolve(
                source,
                request ->
                    com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource.of(
                        request.uri(),
                        "application/yaml",
                        """
                        asyncapi: 3.0.0
                        info: {title: Evidence, version: 1.0.0}
                        servers:
                          broker:
                            host: broker.example.test:8883
                            protocol: mqtt
                        channels:
                          evidence: {address: evidence}
                        operations:
                          receiveEvidence:
                            action: receive
                            channel: {$ref: '#/channels/evidence'}
                        """));
    WorkflowPlan plan = new OpenWorkflowCompiler().compile(source, resources);
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));

    var requestEvent =
        runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1)).events().getFirst();
    assertTrue(requestEvent instanceof EngineEvent.ProtocolCallRequested, requestEvent::toString);
    var requested = (EngineEvent.ProtocolCallRequested) requestEvent;
    assertEquals("mqtt", requested.operation().protocol());
    assertEquals(
        "mqtt://broker.example.test:8883/evidence", requested.operation().endpoint().toString());
    assertEquals(
        com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Mode.SUBSCRIBE,
        requested.operation().mode());
    assertEquals(REQUESTED_AT.plusSeconds(31), requested.operation().subscriptionDeadline());
    assertEquals(kit.getState(), kit.restart().state());

    var first =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.ProtocolCallObserved(
                    executionId,
                    requested.operation().operationId(),
                    "item-0",
                    JsonNodeFactory.instance.objectNode().put("sequence", 1),
                    false,
                    false,
                    REQUESTED_AT.plusSeconds(2),
                    replyTo));
    assertInstanceOf(EngineEvent.ProtocolCallItemAccepted.class, first.events().getFirst());
    var waiting = assertInstanceOf(WorkflowState.Waiting.class, first.state());
    assertEquals(1, waiting.taskStack().getLast().event().protocolItems().size());
    assertEquals(waiting, kit.restart().state());
    var duplicate =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.ProtocolCallObserved(
                    executionId,
                    requested.operation().operationId(),
                    "item-0",
                    JsonNodeFactory.instance.objectNode().put("sequence", 1),
                    false,
                    false,
                    REQUESTED_AT.plusSeconds(20),
                    replyTo));
    assertTrue(duplicate.hasNoEvents());
    assertEquals(
        1,
        assertInstanceOf(WorkflowState.Waiting.class, duplicate.state())
            .taskStack()
            .getLast()
            .event()
            .protocolItems()
            .size());

    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Pause(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                REQUESTED_AT.plusSeconds(3),
                replyTo));
    var pausedObservation =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.ProtocolCallObserved(
                    executionId,
                    requested.operation().operationId(),
                    JsonNodeFactory.instance.objectNode().put("sequence", 2),
                    false,
                    true,
                    REQUESTED_AT.plusSeconds(4),
                    replyTo));
    assertTrue(pausedObservation.hasNoEvents());
    assertEquals(
        "execution_paused", pausedObservation.replyOfType(WorkflowReply.Rejected.class).code());
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Resume(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                REQUESTED_AT.plusSeconds(5),
                replyTo));
    var completed =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.ProtocolCallObserved(
                    executionId,
                    requested.operation().operationId(),
                    JsonNodeFactory.instance.objectNode().put("sequence", 2),
                    false,
                    false,
                    REQUESTED_AT.plusSeconds(6),
                    replyTo));
    assertTrue(
        completed.events().stream().anyMatch(EngineEvent.ProtocolCallCompleted.class::isInstance));
    var completedState = assertInstanceOf(WorkflowState.Completed.class, completed.state());
    assertEquals(2, completedState.data().size());
    assertEquals(1, completedState.data().get(0).required("sequence").asInt());
    assertEquals(2, completedState.data().get(1).required("sequence").asInt());

    var cancelledExecution = new ExecutionId(tenant, UUID.randomUUID());
    var cancelledKit = testKit(cancelledExecution);
    cancelledKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                cancelledExecution,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    var cancelledRequest =
        assertInstanceOf(
            EngineEvent.ProtocolCallRequested.class,
            runNext(cancelledKit, cancelledExecution, tenant, REQUESTED_AT.plusSeconds(1))
                .events()
                .getFirst());
    cancelledKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Cancel(
                UUID.randomUUID(),
                cancelledExecution,
                actor(tenant, "alice"),
                REQUESTED_AT.plusSeconds(2),
                replyTo));
    var late =
        cancelledKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.ProtocolCallObserved(
                    cancelledExecution,
                    cancelledRequest.operation().operationId(),
                    JsonNodeFactory.instance.objectNode().put("ignored", true),
                    false,
                    true,
                    REQUESTED_AT.plusSeconds(3),
                    replyTo));
    assertTrue(late.hasNoEvents());
    assertEquals(
        ExecutionStatus.CANCELLED, late.replyOfType(WorkflowReply.Accepted.class).status());
    assertInstanceOf(WorkflowState.Cancelled.class, cancelledKit.restart().state());
  }

  @Test
  void asyncApiForeachExecutesItsDurableBodyForEveryAcceptedItem() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:asyncapi-foreach");
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: forwardmeasure
          name: durable-asyncapi-foreach
          version: '1.0.0'
        do:
          - receive:
              call: asyncapi
              with:
                document:
                  endpoint: https://contracts.example.test/events.yaml
                operation: receiveEvidence
                subscription:
                  consume:
                    amount: 2
                  foreach:
                    item: message
                    at: messageIndex
                    do:
                      - map:
                          set:
                            sequence: '${ $message.sequence }'
                            index: '${ $messageIndex }'
        """
            .getBytes(StandardCharsets.UTF_8);
    var resources =
        new com.forwardmeasure.openworkflow.definition.WorkflowResourceResolver()
            .resolve(
                source,
                request ->
                    com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource.of(
                        request.uri(),
                        "application/yaml",
                        """
                        asyncapi: 3.0.0
                        info: {title: Evidence, version: 1.0.0}
                        servers:
                          broker:
                            host: broker.example.test:9092
                            protocol: kafka
                        channels:
                          evidence: {address: evidence}
                        operations:
                          receiveEvidence:
                            action: receive
                            channel: {$ref: '#/channels/evidence'}
                        """));
    WorkflowPlan plan = new OpenWorkflowCompiler().compile(source, resources);
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    var foreachRequestEvent =
        runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1)).events().getFirst();
    assertTrue(
        foreachRequestEvent instanceof EngineEvent.ProtocolCallRequested,
        foreachRequestEvent::toString);
    var requested = (EngineEvent.ProtocolCallRequested) foreachRequestEvent;

    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.ProtocolCallObserved(
                executionId,
                requested.operation().operationId(),
                "item-0",
                JsonNodeFactory.instance.objectNode().put("sequence", 11),
                false,
                false,
                REQUESTED_AT.plusSeconds(2),
                replyTo));
    var terminal =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.ProtocolCallObserved(
                    executionId,
                    requested.operation().operationId(),
                    "item-1",
                    JsonNodeFactory.instance.objectNode().put("sequence", 22),
                    false,
                    false,
                    REQUESTED_AT.plusSeconds(3),
                    replyTo));
    assertTrue(
        terminal.events().stream()
            .anyMatch(EngineEvent.ProtocolCallIterationStarted.class::isInstance));
    assertInstanceOf(WorkflowState.Running.class, terminal.state());
    assertEquals(terminal.state(), kit.restart().state());

    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(4));
    var firstAdvanced = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(5));
    assertInstanceOf(
        EngineEvent.ProtocolCallIterationAdvanced.class, firstAdvanced.events().getFirst());
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(6));
    var completed = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(7));
    assertTrue(completed.events().stream().anyMatch(EngineEvent.Completed.class::isInstance));
    var output = assertInstanceOf(WorkflowState.Completed.class, completed.state()).data();
    assertEquals(11, output.get(0).required("sequence").asInt());
    assertEquals(0, output.get(0).required("index").asInt());
    assertEquals(22, output.get(1).required("sequence").asInt());
    assertEquals(1, output.get(1).required("index").asInt());
    assertEquals(completed.state(), kit.restart().state());
  }

  @Test
  void asyncApiForeachRunsInsideAnArbitraryForkLane() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:asyncapi-fork-foreach");
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: forwardmeasure
          name: fork-asyncapi-foreach
          version: '1.0.0'
        do:
          - parallel:
              fork:
                branches:
                  - receive:
                      call: asyncapi
                      with:
                        document:
                          endpoint: https://contracts.example.test/events.yaml
                        operation: receiveEvidence
                        subscription:
                          consume:
                            amount: 2
                          foreach:
                            item: message
                            at: messageIndex
                            do:
                              - map:
                                  set:
                                    sequence: '${ $message.sequence }'
                                    index: '${ $messageIndex }'
                  - right:
                      set:
                        right: true
        """
            .getBytes(StandardCharsets.UTF_8);
    var resources =
        new com.forwardmeasure.openworkflow.definition.WorkflowResourceResolver()
            .resolve(
                source,
                request ->
                    com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource.of(
                        request.uri(),
                        "application/yaml",
                        """
                        asyncapi: 3.0.0
                        info: {title: Evidence, version: 1.0.0}
                        servers:
                          broker:
                            host: broker.example.test:9092
                            protocol: kafka
                        channels:
                          evidence: {address: evidence}
                        operations:
                          receiveEvidence:
                            action: receive
                            channel: {$ref: '#/channels/evidence'}
                        """));
    WorkflowPlan plan = new OpenWorkflowCompiler().compile(source, resources);
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));
    var request =
        assertInstanceOf(
            EngineEvent.ForkBranchProtocolCallRequested.class,
            runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(2)).events().getFirst());
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.ProtocolCallObserved(
                executionId,
                request.operation().operationId(),
                "fork-item-0",
                JsonNodeFactory.instance.objectNode().put("sequence", 11),
                false,
                false,
                REQUESTED_AT.plusSeconds(3),
                replyTo));
    var terminal =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.ProtocolCallObserved(
                    executionId,
                    request.operation().operationId(),
                    "fork-item-1",
                    JsonNodeFactory.instance.objectNode().put("sequence", 22),
                    false,
                    false,
                    REQUESTED_AT.plusSeconds(4),
                    replyTo));
    assertTrue(
        terminal.events().stream()
            .anyMatch(EngineEvent.ForkBranchProtocolCallIterationStarted.class::isInstance));
    assertEquals(terminal.state(), kit.restart().state());

    WorkflowState state = terminal.state();
    boolean advanced = false;
    for (int boundary = 0;
        boundary < 16 && state.status() != ExecutionStatus.COMPLETED;
        boundary++) {
      var result = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(5L + boundary));
      advanced |=
          result.events().stream()
              .anyMatch(EngineEvent.ForkBranchProtocolCallIterationAdvanced.class::isInstance);
      state = result.state();
      assertEquals(state, kit.restart().state());
    }
    assertTrue(advanced);
    assertInstanceOf(WorkflowState.Completed.class, state);
  }

  @Test
  void securedRunExecutesAtAnArbitraryForkLaneBoundary() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:fork-run");
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: fork-run
                  version: '1.0.0'
                do:
                  - parallel:
                      fork:
                        branches:
                          - process:
                              run:
                                return: stdout
                                shell:
                                  command: approved-command
                                  arguments: [fork]
                          - right:
                              set: {right: true}
                """
                    .getBytes(StandardCharsets.UTF_8));
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));
    var request =
        assertInstanceOf(
            EngineEvent.ForkBranchProtocolCallRequested.class,
            runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(2)).events().getFirst());
    assertEquals(
        com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.RUN,
        request.operation().kind());
    assertEquals(
        request.operation(),
        assertInstanceOf(WorkflowState.Running.class, kit.restart().state())
            .taskStack()
            .getLast()
            .fork()
            .branches()
            .getFirst()
            .taskStack()
            .getLast()
            .event()
            .protocolOperation());
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.ProtocolCallObserved(
                executionId,
                request.operation().operationId(),
                "fork-run-result",
                JsonNodeFactory.instance.textNode("fork-complete"),
                false,
                true,
                REQUESTED_AT.plusSeconds(3),
                replyTo));
    WorkflowState state = kit.getState();
    for (int boundary = 0;
        boundary < 8 && state.status() != ExecutionStatus.COMPLETED;
        boundary++) {
      state = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(4L + boundary)).state();
      assertEquals(state, kit.restart().state());
    }
    assertInstanceOf(WorkflowState.Completed.class, state);
  }

  @Test
  void extensionSelectionIsDurableAndPauseResumeCancelGovernMiddleware() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:extensions");
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: durable-extensions
                  version: '1.0.0'
                use:
                  extensions:
                    - audit:
                        extend: set
                        when: '${ .apply }'
                        before:
                          - before:
                              set: {apply: false, before: true}
                        after:
                          - after:
                              set:
                                apply: '${ .apply }'
                                before: '${ .before }'
                                target: '${ .target }'
                                after: true
                do:
                  - target:
                      set:
                        apply: '${ .apply }'
                        before: '${ .before }'
                        target: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode().put("apply", true),
                REQUESTED_AT,
                replyTo));

    var entered = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1));
    assertInstanceOf(EngineEvent.ExtensionEntered.class, entered.events().getFirst());
    assertEquals(entered.state(), kit.restart().state());

    var paused =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Pause(
                    UUID.randomUUID(),
                    executionId,
                    actor(tenant, "alice"),
                    REQUESTED_AT.plusSeconds(2),
                    replyTo));
    assertInstanceOf(WorkflowState.Paused.class, paused.state());
    assertEquals(paused.state(), kit.restart().state());
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Resume(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                REQUESTED_AT.plusSeconds(3),
                replyTo));

    WorkflowState state = kit.getState();
    for (int boundary = 0;
        boundary < 8 && state.status() != ExecutionStatus.COMPLETED;
        boundary++) {
      state = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(4L + boundary)).state();
      assertEquals(state, kit.restart().state());
    }
    var completed = assertInstanceOf(WorkflowState.Completed.class, state);
    assertTrue(completed.data().required("before").booleanValue());
    assertTrue(completed.data().required("target").booleanValue());
    assertTrue(completed.data().required("after").booleanValue());

    var cancelledId = new ExecutionId(tenant, UUID.randomUUID());
    var cancelledKit = testKit(cancelledId);
    cancelledKit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                cancelledId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode().put("apply", true),
                REQUESTED_AT,
                replyTo));
    runNext(cancelledKit, cancelledId, tenant, REQUESTED_AT.plusSeconds(1));
    var cancelled =
        cancelledKit.<WorkflowReply>runCommand(
            replyTo ->
                new WorkflowCommand.Cancel(
                    UUID.randomUUID(),
                    cancelledId,
                    actor(tenant, "alice"),
                    REQUESTED_AT.plusSeconds(2),
                    replyTo));
    assertInstanceOf(WorkflowState.Cancelled.class, cancelled.state());
    assertInstanceOf(WorkflowState.Cancelled.class, cancelledKit.restart().state());
  }

  @Test
  void extensionExitInterceptsTargetAndMiddlewareRunsInsideForkLanes() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:extension-fork");
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: extension-fork
                  version: '1.0.0'
                use:
                  extensions:
                    - intercept:
                        extend: set
                        when: '${ $task.name == "blocked" }'
                        before:
                          - mock:
                              set: {mocked: true}
                              then: exit
                    - audit:
                        extend: set
                        before:
                          - mark:
                              set: {audited: true}
                do:
                  - parallel:
                      fork:
                        branches:
                          - blocked:
                              set: {targetRan: true}
                          - allowed:
                              set: {allowedRan: true}
                """
                    .getBytes(StandardCharsets.UTF_8));
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    WorkflowState state = kit.getState();
    for (int boundary = 0;
        boundary < 20 && state.status() != ExecutionStatus.COMPLETED;
        boundary++) {
      state = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1L + boundary)).state();
      assertEquals(state, kit.restart().state());
    }
    var completed = assertInstanceOf(WorkflowState.Completed.class, state);
    assertTrue(completed.data().isArray());
    assertTrue(completed.data().get(0).required("mocked").booleanValue());
    assertTrue(!completed.data().get(0).has("targetRan"));
    assertTrue(completed.data().get(1).required("allowedRan").booleanValue());
  }

  @Test
  void exitCompletesOnlyItsCurrentScopeAndMainScopeExitCompletesWorkflow() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:flow-exit");
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: flow-exit
                  version: '1.0.0'
                do:
                  - nested:
                      do:
                        - first:
                            set: {nestedExited: true}
                            then: exit
                        - skippedNested:
                            set: {wrong: nested}
                  - mainExit:
                      set:
                        nestedExited: '${ .nestedExited }'
                        mainExited: true
                      then: exit
                  - skippedMain:
                      set: {wrong: main}
                """
                    .getBytes(StandardCharsets.UTF_8));
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var kit = testKit(executionId);
    kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.Start(
                UUID.randomUUID(),
                executionId,
                actor(tenant, "alice"),
                plan,
                JsonNodeFactory.instance.objectNode(),
                REQUESTED_AT,
                replyTo));
    WorkflowState state = kit.getState();
    for (int boundary = 0;
        boundary < 8 && state.status() != ExecutionStatus.COMPLETED;
        boundary++) {
      state = runNext(kit, executionId, tenant, REQUESTED_AT.plusSeconds(1L + boundary)).state();
    }
    var completed = assertInstanceOf(WorkflowState.Completed.class, state);
    assertTrue(completed.data().required("nestedExited").booleanValue());
    assertTrue(completed.data().required("mainExited").booleanValue());
    assertTrue(!completed.data().has("wrong"));
  }

  @Test
  void everyEntityUsesExactlyOneStableProjectionTag() {
    var executionId =
        new ExecutionId(
            com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
                "did:web:forwardmeasure.com:tenant:projection-tags"),
            UUID.fromString("01234567-89ab-cdef-0123-456789abcdef"));
    String tag = WorkflowEntity.projectionTagFor(executionId);

    assertEquals(tag, WorkflowEntity.projectionTagFor(executionId));
    assertTrue(WorkflowEntity.projectionTags().contains(tag));
    assertEquals(WorkflowEntity.PROJECTION_TAG_COUNT, WorkflowEntity.projectionTags().size());
  }

  private static EventSourcedBehaviorTestKit<WorkflowCommand, EngineEvent, WorkflowState> testKit(
      ExecutionId executionId) {
    return EventSourcedBehaviorTestKit.create(
        actorTestKit.system(),
        WorkflowEntity.create(executionId, false),
        EventSourcedBehaviorTestKit.enabledSerializationSettings());
  }

  private static EventSourcedBehaviorTestKit<WorkflowCommand, EngineEvent, WorkflowState>
      automaticTestKit(ExecutionId executionId) {
    return EventSourcedBehaviorTestKit.create(
        actorTestKit.system(),
        WorkflowEntity.create(executionId),
        EventSourcedBehaviorTestKit.enabledSerializationSettings());
  }

  private static EventSourcedBehaviorTestKit.CommandResult<
          WorkflowCommand, EngineEvent, WorkflowState>
      runNext(
          EventSourcedBehaviorTestKit<WorkflowCommand, EngineEvent, WorkflowState> kit,
          ExecutionId executionId,
          TenantId tenant,
          Instant at) {
    return kit.<WorkflowReply>runCommand(
        replyTo ->
            new WorkflowCommand.RunNext(
                UUID.randomUUID(), executionId, actor(tenant, "engine"), at, replyTo));
  }

  private static ActorIdentity actor(TenantId tenant, String name) {
    return new ActorIdentity(tenant, "did:web:forwardmeasure.com:actor:" + name);
  }

  private static WorkflowPlan setPlan(String name) {
    return new OpenWorkflowCompiler()
        .compile(
            ("""
            document:
              dsl: '1.0.3'
              namespace: forwardmeasure
              name: %s
              version: '1.0.0'
            do:
              - initialize:
                  set:
                    accepted: true
            """)
                .formatted(name)
                .getBytes(StandardCharsets.UTF_8));
  }

  private static WorkflowPlan sequentialSetPlan() {
    return new OpenWorkflowCompiler()
        .compile(
            """
            document:
              dsl: '1.0.3'
              namespace: forwardmeasure
              name: sequential-pause
              version: '1.0.0'
            do:
              - first:
                  set:
                    first: 1
              - second:
                  set:
                    second: 2
            """
                .getBytes(StandardCharsets.UTF_8));
  }

  private static WorkflowPlan nestedControlPlan() {
    return new OpenWorkflowCompiler()
        .compile(
            """
            document:
              dsl: '1.0.3'
              namespace: forwardmeasure
              name: nested-controls
              version: '1.0.0'
            do:
              - nested:
                  do:
                    - inner:
                        set:
                          inside: 1
            """
                .getBytes(StandardCharsets.UTF_8));
  }

  private static WorkflowPlan forkPlan(String name, boolean compete) {
    return new OpenWorkflowCompiler()
        .compile(
            ("""
            document:
              dsl: '1.0.3'
              namespace: forwardmeasure
              name: %s
              version: '1.0.0'
            do:
              - parallel:
                  fork:
                    compete: %s
                    branches:
                      - people:
                          set:
                            kind: people
                      - organisations:
                          set:
                            kind: organisations
            """)
                .formatted(name, compete)
                .getBytes(StandardCharsets.UTF_8));
  }
}
