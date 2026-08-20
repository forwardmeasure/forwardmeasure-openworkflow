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
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionStatus;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.persistence.testkit.javadsl.EventSourcedBehaviorTestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SubworkflowCoordinatorEntityTest {
  private static ActorTestKit actors;
  private static final Instant AT = Instant.parse("2026-08-15T12:00:00Z");

  @BeforeAll
  static void start() {
    actors = ActorTestKit.create(EventSourcedBehaviorTestKit.config());
  }

  @AfterAll
  static void stop() {
    actors.shutdownTestKit();
  }

  @Test
  void persistsLaunchTerminalObservationAndParentDeliveryAcrossRecovery() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:subflow-coordinator");
    var parent = new ExecutionId(tenant, UUID.randomUUID());
    var child = new ExecutionId(tenant, UUID.randomUUID());
    var actor = new ActorIdentity(tenant, "did:web:forwardmeasure.com:actor:engine");
    var plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: child
                  version: '1.0.0'
                do:
                  - finish: { set: { child: true } }
                """
                    .getBytes(StandardCharsets.UTF_8));
    SubworkflowCoordinatorEntity.WorkflowEndpoint endpoint =
        (ignored, command, timeout) -> new CompletableFuture<>();
    var kit =
        EventSourcedBehaviorTestKit.create(
            actors.system(),
            SubworkflowCoordinatorEntity.create(child, endpoint),
            EventSourcedBehaviorTestKit.enabledSerializationSettings());

    var launched =
        kit.<SubworkflowCoordinatorReply>runCommand(
            replyTo ->
                new SubworkflowCoordinatorCommand.Launch(
                    UUID.randomUUID(),
                    parent,
                    child,
                    child.value().toString(),
                    actor,
                    plan,
                    JsonNodeFactory.instance.objectNode(),
                    true,
                    AT,
                    replyTo));
    assertInstanceOf(SubworkflowCoordinatorEvent.Launched.class, launched.events().getFirst());
    var active = assertInstanceOf(SubworkflowCoordinatorState.Active.class, launched.state());
    assertEquals(parent, active.parentExecutionId());
    assertEquals(active, kit.restart().state());

    var output = JsonNodeFactory.instance.objectNode().put("child", true);
    var observed =
        kit.runCommand(
            new SubworkflowCoordinatorCommand.ChildObserved(
                new SubworkflowCoordinatorCommand.WorkflowObservation(
                    child, 4, ExecutionStatus.COMPLETED, output, true, true, null),
                null));
    assertInstanceOf(
        SubworkflowCoordinatorEvent.ChildTerminalObserved.class, observed.events().getFirst());
    var terminal = assertInstanceOf(SubworkflowCoordinatorState.Terminal.class, observed.state());
    assertEquals(output, terminal.output());
    assertEquals(terminal, kit.restart().state());

    var delivered =
        kit.runCommand(new SubworkflowCoordinatorCommand.ParentDeliveryObserved(true, null));
    assertInstanceOf(
        SubworkflowCoordinatorEvent.ParentNotified.class, delivered.events().getFirst());
    assertInstanceOf(SubworkflowCoordinatorState.Delivered.class, delivered.state());
    assertEquals(delivered.state(), kit.restart().state());
  }

  @Test
  void propagatesParentPauseAndCancellationToTheActiveChild() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:subflow-controls");
    var parent = new ExecutionId(tenant, UUID.randomUUID());
    var child = new ExecutionId(tenant, UUID.randomUUID());
    var actor = new ActorIdentity(tenant, "did:web:forwardmeasure.com:actor:engine");
    var plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: controlled-child
                  version: '1.0.0'
                do:
                  - finish: { set: { child: true } }
                """
                    .getBytes(StandardCharsets.UTF_8));
    var commands = new ConcurrentLinkedQueue<WorkflowCommand>();
    SubworkflowCoordinatorEntity.WorkflowEndpoint endpoint =
        (ignored, command, timeout) -> {
          commands.add(command.apply(actors.<WorkflowReply>createTestProbe().ref()));
          return new CompletableFuture<>();
        };
    var kit =
        EventSourcedBehaviorTestKit.create(
            actors.system(),
            SubworkflowCoordinatorEntity.create(child, endpoint),
            EventSourcedBehaviorTestKit.enabledSerializationSettings());

    kit.<SubworkflowCoordinatorReply>runCommand(
        replyTo ->
            new SubworkflowCoordinatorCommand.Launch(
                UUID.randomUUID(),
                parent,
                child,
                child.value().toString(),
                actor,
                plan,
                JsonNodeFactory.instance.objectNode(),
                true,
                AT,
                replyTo));
    assertTrue(commands.stream().anyMatch(WorkflowCommand.Start.class::isInstance));
    commands.clear();

    kit.runCommand(
        new SubworkflowCoordinatorCommand.ParentObserved(
            observation(parent, ExecutionStatus.PAUSED), null));
    assertTrue(commands.stream().anyMatch(WorkflowCommand.Pause.class::isInstance));
    assertInstanceOf(SubworkflowCoordinatorState.Active.class, kit.restart().state());
    commands.clear();
    kit.runCommand(
        new SubworkflowCoordinatorCommand.ChildObserved(
            observation(child, ExecutionStatus.NEW), null, false, true));
    assertTrue(commands.stream().anyMatch(WorkflowCommand.Start.class::isInstance));
    commands.clear();

    kit.runCommand(
        new SubworkflowCoordinatorCommand.ParentObserved(
            observation(parent, ExecutionStatus.CANCELLED), null));
    assertTrue(commands.stream().anyMatch(WorkflowCommand.Cancel.class::isInstance));
    assertInstanceOf(SubworkflowCoordinatorState.Active.class, kit.restart().state());
    commands.clear();
    kit.runCommand(
        new SubworkflowCoordinatorCommand.ChildObserved(
            observation(child, ExecutionStatus.NEW), null, true, false));
    assertTrue(commands.stream().anyMatch(WorkflowCommand.Start.class::isInstance));
    commands.clear();
    kit.runCommand(
        new SubworkflowCoordinatorCommand.ParentObserved(
            observation(parent, ExecutionStatus.FAILED), null));
    assertTrue(commands.stream().anyMatch(WorkflowCommand.Cancel.class::isInstance));
  }

  private static SubworkflowCoordinatorCommand.WorkflowObservation observation(
      ExecutionId executionId, ExecutionStatus status) {
    return new SubworkflowCoordinatorCommand.WorkflowObservation(
        executionId, 3, status, JsonNodeFactory.instance.objectNode(), true, true, null);
  }
}
