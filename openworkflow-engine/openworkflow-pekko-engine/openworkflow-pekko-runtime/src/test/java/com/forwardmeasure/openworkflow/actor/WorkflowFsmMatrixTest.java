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

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.persistence.testkit.javadsl.EventSourcedBehaviorTestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Exhaustive M1 state/command legality table for the Pekko persistent FSM. */
class WorkflowFsmMatrixTest {
  private static final Instant AT = Instant.parse("2099-08-15T13:00:00Z");
  private static ActorTestKit actors;

  @BeforeAll
  static void start() {
    actors = ActorTestKit.create(EventSourcedBehaviorTestKit.config());
  }

  @AfterAll
  static void stop() {
    actors.shutdownTestKit();
  }

  @Test
  void everyStateDefinesEveryPublicAndInternalCommand() {
    List<StateCase> states =
        List.of(
            new StateCase("new", EnumSet.of(CommandKind.START)),
            new StateCase(
                "running", EnumSet.of(CommandKind.RUN_NEXT, CommandKind.PAUSE, CommandKind.CANCEL)),
            new StateCase("waiting", EnumSet.of(CommandKind.PAUSE, CommandKind.CANCEL)),
            new StateCase("pausing", EnumSet.of(CommandKind.CANCEL)),
            new StateCase("paused", EnumSet.of(CommandKind.RESUME, CommandKind.CANCEL)),
            new StateCase("cancelling", EnumSet.noneOf(CommandKind.class)),
            new StateCase("cancelled", EnumSet.noneOf(CommandKind.class)),
            new StateCase("completed", EnumSet.noneOf(CommandKind.class)),
            new StateCase("failed", EnumSet.noneOf(CommandKind.class)));

    for (StateCase stateCase : states) {
      for (CommandKind command : CommandKind.values()) {
        verify(stateCase, command);
      }
    }
  }

  private static void verify(StateCase stateCase, CommandKind commandKind) {
    TenantId tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:matrix");
    ExecutionId executionId =
        new ExecutionId(
            tenant,
            UUID.nameUUIDFromBytes(
                (stateCase.name() + "|" + commandKind).getBytes(StandardCharsets.UTF_8)));
    UUID commandId =
        UUID.nameUUIDFromBytes(
            ("command|" + stateCase.name() + "|" + commandKind).getBytes(StandardCharsets.UTF_8));
    WorkflowPlan plan = plan();
    var kit =
        EventSourcedBehaviorTestKit.<WorkflowCommand, EngineEvent, WorkflowState>create(
            actors.system(),
            WorkflowEntity.create(executionId, false),
            EventSourcedBehaviorTestKit.enabledSerializationSettings());
    kit.initialize(state(stateCase.name(), executionId, plan));

    var result =
        kit.<WorkflowReply>runCommand(
            replyTo ->
                switch (commandKind) {
                  case START ->
                      new WorkflowCommand.Start(
                          commandId,
                          executionId,
                          actor(tenant),
                          plan,
                          JsonNodeFactory.instance.objectNode(),
                          AT,
                          replyTo);
                  case RUN_NEXT ->
                      new WorkflowCommand.RunNext(
                          commandId, executionId, actor(tenant), AT, replyTo);
                  case PAUSE ->
                      new WorkflowCommand.Pause(commandId, executionId, actor(tenant), AT, replyTo);
                  case RESUME ->
                      new WorkflowCommand.Resume(
                          commandId, executionId, actor(tenant), AT, replyTo);
                  case CANCEL ->
                      new WorkflowCommand.Cancel(
                          commandId, executionId, actor(tenant), AT, replyTo);
                  case GET_STATE -> new WorkflowCommand.GetState(executionId, replyTo);
                });

    String context = stateCase.name() + " + " + commandKind;
    if (commandKind == CommandKind.GET_STATE) {
      assertInstanceOf(WorkflowReply.StateSnapshot.class, result.reply(), context);
      assertTrue(result.hasNoEvents(), context);
    } else if (stateCase.accepted().contains(commandKind)) {
      assertInstanceOf(WorkflowReply.Accepted.class, result.reply(), context);
      assertTrue(!result.events().isEmpty(), context);
    } else {
      assertInstanceOf(WorkflowReply.Rejected.class, result.reply(), context);
      assertTrue(result.hasNoEvents(), context);
    }
  }

  private static WorkflowState state(String name, ExecutionId executionId, WorkflowPlan plan) {
    var data = JsonNodeFactory.instance.objectNode().put("state", name);
    return switch (name) {
      case "new" -> new WorkflowState.New(executionId);
      case "running" -> new WorkflowState.Running(executionId, plan, data, 0, 1, Set.of());
      case "waiting" ->
          new WorkflowState.Waiting(
              executionId, plan, data, 0, 1, Set.of(), "timer", AT.plusSeconds(60));
      case "pausing" -> new WorkflowState.Pausing(executionId, plan, data, 0, 1, Set.of());
      case "paused" -> new WorkflowState.Paused(executionId, plan, data, 0, 1, Set.of());
      case "cancelling" -> new WorkflowState.Cancelling(executionId, plan, data, 0, 1, Set.of());
      case "cancelled" -> new WorkflowState.Cancelled(executionId, data, 1, Set.of());
      case "completed" -> new WorkflowState.Completed(executionId, data, 1, Set.of());
      case "failed" -> new WorkflowState.Failed(executionId, data, 1, Set.of(), "matrix failure");
      default -> throw new IllegalArgumentException(name);
    };
  }

  private static ActorIdentity actor(TenantId tenant) {
    return new ActorIdentity(tenant, "did:web:forwardmeasure.com:actor:matrix");
  }

  private static WorkflowPlan plan() {
    return new OpenWorkflowCompiler()
        .compile(
            """
            document:
              dsl: '1.0.3'
              namespace: forwardmeasure
              name: matrix
              version: '1.0.0'
            do:
              - task:
                  set:
                    accepted: true
            """
                .getBytes(StandardCharsets.UTF_8));
  }

  private enum CommandKind {
    START,
    RUN_NEXT,
    PAUSE,
    RESUME,
    CANCEL,
    GET_STATE
  }

  private record StateCase(String name, Set<CommandKind> accepted) {}
}
