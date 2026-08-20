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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.AsyncApiSubscriptionPlan;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceKind;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceReference;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionStatus;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pekko.Done;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

final class ProtocolOperationCoordinatorEntityTest {
  private static ActorTestKit actors;
  private static final Instant AT = Instant.parse("2026-08-15T12:00:00Z");

  @BeforeAll
  static void start() {
    actors = ActorTestKit.create();
  }

  @AfterAll
  static void stop() {
    actors.shutdownTestKit();
  }

  @Test
  void pauseStopsTransportResumeRestartsItAndCancelStopsItPermanently() throws Exception {
    var execution =
        new ExecutionId(
            com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
                "did:web:forwardmeasure.com:tenant:protocol-control"),
            UUID.randomUUID());
    var operation = operation();
    var state = new AtomicReference<WorkflowState>(running(execution, operation, 1));
    var observed = new CopyOnWriteArrayList<WorkflowCommand.ProtocolCallObserved>();
    ProtocolOperationCoordinatorEntity.WorkflowEndpoint endpoint =
        (routed, commandFactory, timeout) -> {
          WorkflowCommand command =
              commandFactory.apply(actors.<WorkflowReply>createTestProbe().ref());
          if (command instanceof WorkflowCommand.GetRuntimeState) {
            return CompletableFuture.completedFuture(
                new WorkflowReply.RuntimeState(WorkflowRuntimeState.from(state.get())));
          }
          var item = (WorkflowCommand.ProtocolCallObserved) command;
          observed.add(item);
          return CompletableFuture.completedFuture(
              new WorkflowReply.Accepted(
                  UUID.randomUUID(), execution, state.get().revision(), state.get().status()));
        };
    var transports = new LinkedBlockingQueue<CompletableFuture<Done>>();
    var sinks = new LinkedBlockingQueue<ProtocolTransport.ObservationSink>();
    ProtocolTransport transport =
        (routed, descriptor, sink) -> {
          assertEquals(execution, routed);
          assertEquals(operation, descriptor);
          var completion = new CompletableFuture<Done>();
          sinks.add(sink);
          transports.add(completion);
          return completion;
        };
    var ref =
        actors.spawn(
            ProtocolOperationCoordinatorEntity.create(
                new ProtocolOperationCoordinatorSharding.Coordinates(
                    execution, operation.operationId()),
                endpoint,
                transport));
    var replies = actors.<ProtocolOperationCoordinatorReply>createTestProbe();
    ref.tell(
        new ProtocolOperationCoordinatorCommand.Start(
            execution, operation.operationId(), replies.ref()));
    assertTrue(replies.receiveMessage().accepted());

    CompletableFuture<Done> first = transports.poll(3, TimeUnit.SECONDS);
    assertTrue(first != null, "transport was launched from durable state");
    sinks
        .take()
        .observe("offset-1", JsonNodeFactory.instance.numberNode(1), false, false, AT)
        .toCompletableFuture()
        .join();
    assertEquals("offset-1", observed.getFirst().observationId());

    state.set(paused(execution, operation, 2));
    eventually(first::isCancelled);

    state.set(running(execution, operation, 3));
    CompletableFuture<Done> resumed = transports.poll(3, TimeUnit.SECONDS);
    assertTrue(resumed != null, "transport was relaunched after resume");

    state.set(
        new WorkflowState.Cancelled(execution, JsonNodeFactory.instance.objectNode(), 4, Set.of()));
    eventually(resumed::isCancelled);
    assertEquals(null, transports.poll(700, TimeUnit.MILLISECONDS));
  }

  @Test
  void expiredSubscriptionCompletesDurablyWithoutLaunchingTransport() throws Exception {
    var execution =
        new ExecutionId(
            com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
                "did:web:forwardmeasure.com:tenant:protocol-duration"),
            UUID.randomUUID());
    var operation = operation(AT.minusSeconds(1));
    var state = new AtomicReference<WorkflowState>(running(execution, operation, 1));
    var deadline = new CompletableFuture<WorkflowCommand.ProtocolCallObserved>();
    ProtocolOperationCoordinatorEntity.WorkflowEndpoint endpoint =
        (routed, commandFactory, timeout) -> {
          WorkflowCommand command =
              commandFactory.apply(actors.<WorkflowReply>createTestProbe().ref());
          if (command instanceof WorkflowCommand.GetRuntimeState) {
            return CompletableFuture.completedFuture(
                new WorkflowReply.RuntimeState(WorkflowRuntimeState.from(state.get())));
          }
          var observed = (WorkflowCommand.ProtocolCallObserved) command;
          deadline.complete(observed);
          state.set(
              new WorkflowState.Completed(
                  execution, JsonNodeFactory.instance.arrayNode(), 2, Set.of()));
          return CompletableFuture.completedFuture(
              new WorkflowReply.Accepted(
                  UUID.randomUUID(), execution, 2, ExecutionStatus.COMPLETED));
        };
    var launches = new java.util.concurrent.atomic.AtomicInteger();
    ProtocolTransport transport =
        (routed, descriptor, sink) -> {
          launches.incrementAndGet();
          return new CompletableFuture<>();
        };
    actors.spawn(
        ProtocolOperationCoordinatorEntity.create(
            new ProtocolOperationCoordinatorSharding.Coordinates(
                execution, operation.operationId()),
            endpoint,
            transport));

    var observed = deadline.get(3, TimeUnit.SECONDS);
    assertTrue(observed.terminal());
    assertEquals("duration-" + operation.subscriptionDeadline(), observed.observationId());
    assertEquals(0, launches.get());
  }

  private static WorkflowState.Running running(
      ExecutionId execution, ProtocolOperationDescriptor operation, long revision) {
    var data = JsonNodeFactory.instance.objectNode();
    return new WorkflowState.Running(
        execution,
        plan(),
        data,
        0,
        revision,
        Set.of(),
        data,
        data,
        List.of(
            TaskExecutionFrame.eventing(
                "/do/0/receive", data, data, EventExecutionFrame.protocolCall(operation))),
        null);
  }

  private static WorkflowState paused(
      ExecutionId execution, ProtocolOperationDescriptor operation, long revision) {
    var running = running(execution, operation, revision);
    return new WorkflowState.Paused(
        execution,
        running.plan(),
        running.data(),
        running.nextStep(),
        revision,
        Set.of(),
        running.context(),
        running.rawWorkflowInput(),
        running.taskStack(),
        null);
  }

  private static com.forwardmeasure.openworkflow.definition.WorkflowPlan plan() {
    return new OpenWorkflowCompiler()
        .compile(
            """
            document:
              dsl: '1.0.3'
              namespace: forwardmeasure
              name: protocol-control
              version: '1.0.0'
            do:
              - finish: { set: { done: true } }
            """
                .getBytes(StandardCharsets.UTF_8));
  }

  private static ProtocolOperationDescriptor operation() {
    return operation(null);
  }

  private static ProtocolOperationDescriptor operation(Instant deadline) {
    return new ProtocolOperationDescriptor(
        "operation-1",
        ProtocolOperationDescriptor.Kind.ASYNC_API,
        deadline == null
            ? ProtocolOperationDescriptor.Mode.PUBLISH
            : ProtocolOperationDescriptor.Mode.SUBSCRIBE,
        new WorkflowResourceReference(
            WorkflowResourceKind.ASYNC_API_DOCUMENT,
            URI.create("https://contracts.example.test/events.yaml"),
            "a".repeat(64)),
        "mqtt",
        URI.create("mqtt://broker.example.test:1883"),
        "receive",
        JsonNodeFactory.instance.objectNode(),
        deadline == null
            ? null
            : new AsyncApiSubscriptionPlan(
                null,
                new AsyncApiSubscriptionPlan.Consumption(
                    AsyncApiSubscriptionPlan.Consumption.Mode.AMOUNT, 10, null, null),
                null,
                null,
                null),
        null,
        null,
        null,
        deadline);
  }

  private static void eventually(java.util.function.BooleanSupplier condition)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(25);
    }
    assertTrue(condition.getAsBoolean());
  }
}
