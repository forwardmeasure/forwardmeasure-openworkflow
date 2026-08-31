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
package com.forwardmeasure.openworkflow.actor.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.actor.ProtocolOperationCoordinatorCommand;
import com.forwardmeasure.openworkflow.actor.ProtocolOperationCoordinatorReply;
import com.forwardmeasure.openworkflow.actor.ScheduleCommand;
import com.forwardmeasure.openworkflow.actor.ScheduleEvent;
import com.forwardmeasure.openworkflow.actor.ScheduleId;
import com.forwardmeasure.openworkflow.actor.ScheduleReply;
import com.forwardmeasure.openworkflow.actor.ScheduleState;
import com.forwardmeasure.openworkflow.actor.ScheduleTriggerKind;
import com.forwardmeasure.openworkflow.actor.ScheduledExecutionRequest;
import com.forwardmeasure.openworkflow.actor.SubworkflowCoordinatorCommand;
import com.forwardmeasure.openworkflow.actor.SubworkflowCoordinatorEvent;
import com.forwardmeasure.openworkflow.actor.SubworkflowCoordinatorReply;
import com.forwardmeasure.openworkflow.actor.SubworkflowCoordinatorState;
import com.forwardmeasure.openworkflow.actor.WorkflowCommand;
import com.forwardmeasure.openworkflow.actor.WorkflowReply;
import com.forwardmeasure.openworkflow.actor.WorkflowRuntimeState;
import com.forwardmeasure.openworkflow.actor.WorkflowState;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.DeadlineScope;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.engine.api.EventConsumptionWindow;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionStatus;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.serialization.SerializationExtension;
import org.apache.pekko.serialization.SerializerWithStringManifest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class WireGoldenFixtureTest {
  private static final TenantId TENANT =
      com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
          "did:web:forwardmeasure.com:tenant:wire");
  private static final ExecutionId EXECUTION =
      new ExecutionId(TENANT, UUID.fromString("11111111-2222-3333-4444-555555555555"));
  private static final ActorIdentity ACTOR =
      new ActorIdentity(TENANT, "did:web:forwardmeasure.com:actor:wire");
  private static final ActorIdentity ORGANIZATION_ACTOR =
      new ActorIdentity(
          TENANT,
          "did:web:forwardmeasure.com:actor:wire",
          "organization-wire",
          Set.of("workflow-execution-controller"),
          "correlation-wire");
  private static final UUID COMMAND = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
  private static final Instant AT = Instant.parse("2026-08-15T12:34:56Z");
  private static ActorTestKit testKit;

  @BeforeAll
  static void start() {
    testKit = ActorTestKit.create("openworkflow-wire-golden");
  }

  @AfterAll
  static void stop() {
    testKit.shutdownTestKit();
  }

  @Test
  void everyPublishedTypeHasAUniqueStableManifestAndGoldenBackwardRead() throws Exception {
    var serialization = SerializationExtension.get(testKit.system());
    var values = values();
    var manifests = OpenWorkflowWireSerializer.manifests();
    assertEquals(manifests.size(), new java.util.HashSet<>(manifests.values()).size());
    assertEquals(
        manifests.keySet(),
        values.stream().map(Object::getClass).collect(java.util.stream.Collectors.toSet()));

    Properties fixtures = fixtures();
    var missing = new ArrayList<String>();
    for (Object original : values) {
      var serializer =
          assertInstanceOf(
              SerializerWithStringManifest.class, serialization.findSerializerFor(original));
      assertInstanceOf(OpenWorkflowWireSerializer.class, serializer);
      String manifest = serializer.manifest(original);
      byte[] current = serialization.serialize(original).get();
      String expected = fixtures.getProperty(manifest);
      if (expected == null) {
        missing.add(manifest + "=" + Base64.getEncoder().encodeToString(current));
        continue;
      }
      byte[] golden = Base64.getDecoder().decode(expected);
      Object restored = serialization.deserialize(golden, serializer.identifier(), manifest).get();
      if (original instanceof WorkflowCommand command) {
        assertEquals(
            original.getClass(),
            restored.getClass(),
            "Golden command type no longer reads for " + manifest);
        assertEquals(command.executionId(), ((WorkflowCommand) restored).executionId());
      } else if (original instanceof ScheduleCommand command) {
        assertEquals(
            original.getClass(),
            restored.getClass(),
            "Golden schedule command type no longer reads for " + manifest);
        assertEquals(command.scheduleId(), ((ScheduleCommand) restored).scheduleId());
      } else if (original instanceof SubworkflowCoordinatorCommand command) {
        assertEquals(
            original.getClass(),
            restored.getClass(),
            "Golden subworkflow command type no longer reads for " + manifest);
        if (command instanceof SubworkflowCoordinatorCommand.Launch launch) {
          var decoded = assertInstanceOf(SubworkflowCoordinatorCommand.Launch.class, restored);
          assertEquals(launch.parentExecutionId(), decoded.parentExecutionId());
          assertEquals(launch.childExecutionId(), decoded.childExecutionId());
          assertEquals(launch.operationId(), decoded.operationId());
        } else {
          assertEquals(original, restored);
        }
      } else if (original instanceof ProtocolOperationCoordinatorCommand command) {
        assertEquals(
            original.getClass(),
            restored.getClass(),
            "Golden protocol command type no longer reads for " + manifest);
        if (command instanceof ProtocolOperationCoordinatorCommand.Start start) {
          var decoded = assertInstanceOf(ProtocolOperationCoordinatorCommand.Start.class, restored);
          assertEquals(start.executionId(), decoded.executionId());
          assertEquals(start.operationId(), decoded.operationId());
        } else {
          assertEquals(original, restored);
        }
      } else if (original instanceof EngineEvent.ForkBranchListenAccepted accepted) {
        var decoded = assertInstanceOf(EngineEvent.ForkBranchListenAccepted.class, restored);
        assertEquals(accepted.commandId(), decoded.commandId());
        assertEquals(accepted.rootForkTaskPath(), decoded.rootForkTaskPath());
        assertEquals(accepted.event(), decoded.event());
        assertEquals(accepted.updates().toString(), decoded.updates().toString());
        assertEquals(accepted.hasActiveListeners(), decoded.hasActiveListeners());
        assertEquals(accepted.allBranchesBlocked(), decoded.allBranchesBlocked());
        assertEquals(accepted.occurredAt(), decoded.occurredAt());
      } else {
        assertEquals(
            original.getClass(),
            restored.getClass(),
            "Golden payload type no longer reads for " + manifest);
      }
    }
    assertTrue(
        missing.isEmpty(), () -> "Add missing golden fixtures:\n" + String.join("\n", missing));
  }

  @Test
  void protocolAndRunDescriptorVariantsRoundTripInsideTheStableEvent() throws Exception {
    var serialization = SerializationExtension.get(testKit.system());
    var data = JsonNodeFactory.instance.objectNode().put("fixture", true);
    var descriptors =
        List.of(
            new com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor(
                    "a2a-wire",
                    com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.A2A,
                    com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Mode
                        .RPC_STREAM,
                    null,
                    "a2a-jsonrpc",
                    java.net.URI.create("https://agent.example.test/rpc"),
                    "message/stream",
                    data,
                    null,
                    null,
                    null)
                .requestedBy(ORGANIZATION_ACTOR),
            new com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor(
                    "mcp-wire",
                    com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.MCP,
                    com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Mode
                        .RPC_UNARY,
                    null,
                    "mcp-stdio",
                    java.net.URI.create("stdio://local"),
                    "tools/call",
                    data,
                    null,
                    null,
                    null)
                .requestedBy(ORGANIZATION_ACTOR));
    var mutable = new java.util.ArrayList<>(descriptors);
    mutable.add(
        new com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor(
                "run-wire",
                com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.RUN,
                com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Mode
                    .RUN_AWAIT,
                null,
                "run-shell",
                java.net.URI.create("runner://local"),
                "/usr/bin/printf",
                data,
                null,
                null,
                null)
            .requestedBy(ORGANIZATION_ACTOR));
    for (var descriptor : mutable) {
      var event =
          new EngineEvent.ProtocolCallRequested(
              COMMAND, "/do/0/protocol", data, data, 1, descriptor, AT);
      byte[] bytes = serialization.serialize(event).get();
      var serializer =
          assertInstanceOf(
              SerializerWithStringManifest.class, serialization.findSerializerFor(event));
      var restored =
          assertInstanceOf(
              EngineEvent.ProtocolCallRequested.class,
              serialization
                  .deserialize(bytes, serializer.identifier(), serializer.manifest(event))
                  .get());
      assertEquals(descriptor, restored.operation());
    }
  }

  @Test
  void currentStartedEventRoundTripsCanonicalExecutionIdentity() throws Exception {
    var serialization = SerializationExtension.get(testKit.system());
    var started =
        values().stream()
            .filter(EngineEvent.Started.class::isInstance)
            .map(EngineEvent.Started.class::cast)
            .findFirst()
            .orElseThrow();
    byte[] bytes = serialization.serialize(started).get();
    var serializer =
        assertInstanceOf(
            SerializerWithStringManifest.class, serialization.findSerializerFor(started));
    var restored =
        assertInstanceOf(
            EngineEvent.Started.class,
            serialization
                .deserialize(bytes, serializer.identifier(), serializer.manifest(started))
                .get());
    assertEquals(started.executionId(), restored.executionId());
  }

  @Test
  void legacyFramesRemainReadableAfterDurableContextAndDeadlinesWereAdded() throws Exception {
    var serialization = SerializationExtension.get(testKit.system());
    Properties fixtures = fixtures();
    for (String manifest :
        List.of(
            "ow.event.task-completed.v1",
            "ow.state.running.v1",
            "ow.state.waiting.v1",
            "ow.state.pausing.v1",
            "ow.state.paused.v1",
            "ow.state.cancelling.v1",
            "ow.state.running.v3",
            "ow.state.waiting.v3",
            "ow.state.pausing.v3",
            "ow.state.paused.v3",
            "ow.state.cancelling.v3",
            "ow.state.running.v2",
            "ow.state.waiting.v2",
            "ow.state.pausing.v2",
            "ow.state.paused.v2",
            "ow.state.cancelling.v2")) {
      Object restored =
          serialization
              .deserialize(
                  Base64.getDecoder().decode(fixtures.getProperty(manifest)),
                  OpenWorkflowWireSerializer.IDENTIFIER,
                  manifest)
              .get();
      if (restored instanceof EngineEvent.TaskCompleted completed) {
        assertEquals(completed.output(), completed.context());
      } else {
        var state = assertInstanceOf(WorkflowState.class, restored);
        assertEquals(state.data(), state.context());
        assertEquals(state.data(), state.rawWorkflowInput());
        assertTrue(state.taskStack().isEmpty());
        if (!manifest.endsWith(".v3")) {
          assertEquals(null, state.workflowDeadline());
        }
      }
    }
  }

  @Test
  void legacyFireAndForgetCloudEventCommandsRemainReadable() throws Exception {
    var serialization = SerializationExtension.get(testKit.system());
    Properties fixtures = fixtures();
    Object workflow =
        serialization
            .deserialize(
                Base64.getDecoder()
                    .decode(fixtures.getProperty("ow.command.cloud-event-received.v1")),
                OpenWorkflowWireSerializer.IDENTIFIER,
                "ow.command.cloud-event-received.v1")
            .get();
    Object schedule =
        serialization
            .deserialize(
                Base64.getDecoder()
                    .decode(fixtures.getProperty("ow.schedule.command.event-received.v1")),
                OpenWorkflowWireSerializer.IDENTIFIER,
                "ow.schedule.command.event-received.v1")
            .get();

    assertInstanceOf(WorkflowCommand.CloudEventReceived.class, workflow);
    assertInstanceOf(ScheduleCommand.EventReceived.class, schedule);
    assertEquals(null, ((WorkflowCommand.CloudEventReceived) workflow).replyTo());
    assertEquals(null, ((ScheduleCommand.EventReceived) schedule).replyTo());

    Object listen =
        serialization
            .deserialize(
                Base64.getDecoder().decode(fixtures.getProperty("ow.event.listen-started.v1")),
                OpenWorkflowWireSerializer.IDENTIFIER,
                "ow.event.listen-started.v1")
            .get();
    assertTrue(assertInstanceOf(EngineEvent.ListenStarted.class, listen).eventTypes().isEmpty());
  }

  @Test
  void legacyChildObservationDefaultsToOrdinaryProgress() throws Exception {
    var serialization = SerializationExtension.get(testKit.system());
    Properties fixtures = fixtures();
    Object restored =
        serialization
            .deserialize(
                Base64.getDecoder()
                    .decode(fixtures.getProperty("ow.subflow.command.child-observed.v1")),
                OpenWorkflowWireSerializer.IDENTIFIER,
                "ow.subflow.command.child-observed.v1")
            .get();
    var observed = assertInstanceOf(SubworkflowCoordinatorCommand.ChildObserved.class, restored);
    assertTrue(!observed.cancellation());
    assertTrue(!observed.pause());
  }

  @Test
  void legacyControlEventsDefaultToAnEmptyActiveTaskSnapshot() throws Exception {
    var serialization = SerializationExtension.get(testKit.system());
    Properties fixtures = fixtures();
    for (String manifest :
        List.of("ow.event.paused.v1", "ow.event.resumed.v1", "ow.event.cancelled.v1")) {
      Object restored =
          serialization
              .deserialize(
                  Base64.getDecoder().decode(fixtures.getProperty(manifest)),
                  OpenWorkflowWireSerializer.IDENTIFIER,
                  manifest)
              .get();
      List<String> paths =
          switch (restored) {
            case EngineEvent.Paused paused -> paused.activeTaskPaths();
            case EngineEvent.Resumed resumed -> resumed.activeTaskPaths();
            case EngineEvent.Cancelled cancelled -> cancelled.activeTaskPaths();
            default -> throw new AssertionError("Unexpected control event for " + manifest);
          };
      assertTrue(paths.isEmpty());
    }
  }

  private static Properties fixtures() throws IOException {
    var properties = new Properties();
    try (var input =
        WireGoldenFixtureTest.class.getResourceAsStream("/wire-golden-v1.properties")) {
      if (input == null) throw new IllegalStateException("Golden fixture resource is missing");
      properties.load(input);
    }
    return properties;
  }

  private static List<Object> values() {
    var plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: wire-fixture
                  version: '1.0.0'
                do:
                  - initialize:
                      set:
                        accepted: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var data = JsonNodeFactory.instance.objectNode().put("value", 7);
    var output = JsonNodeFactory.instance.objectNode().put("result", "ok");
    var error =
        JsonNodeFactory.instance
            .objectNode()
            .put("type", "https://errors.example/fixture")
            .put("status", 503)
            .put("title", "Fixture failure");
    var replyTo = testKit.system().<WorkflowReply>ignoreRef();
    var scheduleReplyTo = testKit.system().<ScheduleReply>ignoreRef();
    var schedulePlan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: wire-schedule
                  version: '1.0.0'
                schedule:
                  every: PT1H
                  after: PT1M
                do:
                  - initialize:
                      set:
                        accepted: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var scheduleId = new ScheduleId(TENANT, schedulePlan.coordinates());
    var scheduledExecution =
        new ExecutionId(TENANT, UUID.fromString("99999999-8888-7777-6666-555555555555"));
    var coordinatorReplyTo = testKit.system().<SubworkflowCoordinatorReply>ignoreRef();
    var subflow =
        new com.forwardmeasure.openworkflow.definition.ResolvedSubflow(
            new com.forwardmeasure.openworkflow.definition.WorkflowCoordinates(
                "forwardmeasure", "child", "1.0.0", "1.0.3"),
            "a".repeat(64),
            "b".repeat(64));
    var request =
        new ScheduledExecutionRequest(
            scheduleId,
            scheduledExecution,
            ACTOR,
            schedulePlan,
            data,
            ScheduleTriggerKind.EVERY,
            AT.plusSeconds(30));
    Set<UUID> receipts = Set.of(COMMAND);
    var coordinatorObservation =
        new SubworkflowCoordinatorCommand.WorkflowObservation(
            scheduledExecution, 4, ExecutionStatus.RUNNING, data, true, true, null);
    var coordinatorActive =
        new SubworkflowCoordinatorState.Active(
            COMMAND,
            EXECUTION,
            scheduledExecution,
            scheduledExecution.value().toString(),
            ACTOR,
            plan,
            data,
            true,
            1);
    var coordinatorTerminal =
        new SubworkflowCoordinatorState.Terminal(
            coordinatorActive, ExecutionStatus.COMPLETED, output, null, 2);
    var functionOperation =
        new com.forwardmeasure.openworkflow.engine.api.FunctionOperationDescriptor(
            "function-fixture", "normalize", null, data);
    var httpOperation =
        new com.forwardmeasure.openworkflow.engine.api.HttpOperationDescriptor(
            "http-fixture",
            com.forwardmeasure.openworkflow.engine.api.HttpOperationDescriptor.Kind.HTTP,
            "POST",
            java.net.URI.create("https://api.example.test/items"),
            java.util.Map.of("X-Fixture", "wire"),
            data,
            com.forwardmeasure.openworkflow.engine.api.HttpOperationDescriptor.Output.RESPONSE,
            true,
            null,
            null,
            null);
    var protocolOperation =
        new com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor(
            "protocol-fixture",
            com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.ASYNC_API,
            com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Mode.PUBLISH,
            new com.forwardmeasure.openworkflow.definition.WorkflowResourceReference(
                com.forwardmeasure.openworkflow.definition.WorkflowResourceKind.ASYNC_API_DOCUMENT,
                java.net.URI.create("https://contracts.example.test/events.yaml"),
                "c".repeat(64)),
            "http",
            java.net.URI.create("https://events.example.test/messages"),
            "publishEvidence",
            data,
            null,
            null,
            null);
    var correlatedWorkerEventsOperation =
        new com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor(
            "correlated-worker-fixture:events",
            com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.ASYNC_API,
            com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Mode.SUBSCRIBE,
            new com.forwardmeasure.openworkflow.definition.WorkflowResourceReference(
                com.forwardmeasure.openworkflow.definition.WorkflowResourceKind.ASYNC_API_DOCUMENT,
                java.net.URI.create("https://contracts.example.test/workers.yaml"),
                "d".repeat(64)),
            "http",
            java.net.URI.create("https://events.example.test/workers"),
            "receiveWorkerEvents",
            data,
            new com.forwardmeasure.openworkflow.definition.AsyncApiSubscriptionPlan(
                null,
                new com.forwardmeasure.openworkflow.definition.AsyncApiSubscriptionPlan.Consumption(
                    com.forwardmeasure.openworkflow.definition.AsyncApiSubscriptionPlan.Consumption
                        .Mode.UNTIL,
                    null,
                    "${ .payload.status == \"SUCCEEDED\" }",
                    null),
                null,
                null,
                null),
            null,
            null);
    var correlatedWorkerCancellationOperation =
        new com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor(
            "correlated-worker-fixture:cancel",
            com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.ASYNC_API,
            com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Mode.PUBLISH,
            new com.forwardmeasure.openworkflow.definition.WorkflowResourceReference(
                com.forwardmeasure.openworkflow.definition.WorkflowResourceKind.ASYNC_API_DOCUMENT,
                java.net.URI.create("https://contracts.example.test/workers.yaml"),
                "d".repeat(64)),
            "http",
            java.net.URI.create("https://events.example.test/workers/cancel"),
            "cancelWorker",
            data,
            null,
            null,
            null);
    var scheduleCloudEvent =
        new com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent(
            "1.0",
            "schedule-fixture",
            java.net.URI.create("https://events.example.test"),
            "example.schedule.v1",
            null,
            AT,
            "application/json",
            data,
            java.util.Map.of());
    return List.of(
        new WorkflowCommand.Start(COMMAND, EXECUTION, ACTOR, plan, data, AT, replyTo),
        new WorkflowCommand.RunNext(COMMAND, EXECUTION, ACTOR, AT, replyTo),
        new WorkflowCommand.Pause(COMMAND, EXECUTION, ACTOR, AT, replyTo),
        new WorkflowCommand.Resume(COMMAND, EXECUTION, ACTOR, AT, replyTo),
        new WorkflowCommand.Cancel(COMMAND, EXECUTION, ACTOR, AT, replyTo),
        new WorkflowCommand.TimerElapsed(EXECUTION, "/do/0/wait", AT.plusSeconds(30)),
        new WorkflowCommand.RetryElapsed(EXECUTION, "/do/0/try", AT.plusSeconds(30)),
        new WorkflowCommand.DeadlineElapsed(
            EXECUTION, DeadlineScope.WORKFLOW, null, AT.plusSeconds(30)),
        new WorkflowCommand.RecheckTimers(EXECUTION),
        new WorkflowCommand.EffectAcknowledged(EXECUTION, "emit-fixture", AT),
        new WorkflowCommand.HttpCallCompleted(EXECUTION, "http-fixture", output, null, AT, replyTo),
        new WorkflowCommand.ProtocolCallObserved(
            EXECUTION, "protocol-fixture", output, false, true, AT, replyTo),
        new WorkflowCommand.SubworkflowCompleted(
            COMMAND,
            EXECUTION,
            scheduledExecution.value().toString(),
            scheduledExecution,
            ExecutionStatus.COMPLETED,
            output,
            null,
            AT,
            replyTo),
        new WorkflowCommand.CloudEventReceived(
            EXECUTION,
            new com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent(
                "1.0",
                "received-fixture",
                java.net.URI.create("https://events.example.test"),
                "example.received.v1",
                null,
                AT,
                "application/json",
                data,
                java.util.Map.of()),
            AT),
        new WorkflowCommand.GetState(EXECUTION, replyTo),
        new WorkflowCommand.GetRuntimeState(EXECUTION, replyTo),
        new EngineEvent.Started(COMMAND, EXECUTION, ACTOR, plan, data, AT),
        new EngineEvent.TaskEntered(COMMAND, "/do/0/initialize", data, data, 0, AT),
        new EngineEvent.ExtensionEntered(
            COMMAND, "/do/0/extended", data, data, List.of(true, false), 1, AT),
        new EngineEvent.FunctionEntered(
            COMMAND, "/do/0/function", data, data, functionOperation, 1, AT),
        new EngineEvent.ForEntered(
            COMMAND,
            "/do/0/loop",
            data,
            data,
            JsonNodeFactory.instance.arrayNode().add(data),
            0,
            "item",
            "index",
            1,
            AT),
        new EngineEvent.ForIterationAdvanced(COMMAND, "/do/0/loop", data, 1, 1, AT),
        new EngineEvent.WaitScheduled(COMMAND, "/do/0/wait", data, data, 0, AT.plusSeconds(30), AT),
        new EngineEvent.DeadlineScheduled(
            COMMAND, DeadlineScope.WORKFLOW, null, AT.plusSeconds(30), AT),
        new EngineEvent.TryEntered(COMMAND, "/do/0/try", data, data, 1, AT),
        new EngineEvent.ForkEntered(
            COMMAND,
            "/do/0/fork",
            data,
            data,
            List.of("left", "right"),
            List.of(1, 2),
            List.of(2, 3),
            false,
            0,
            AT),
        new EngineEvent.ForkBranchAdvanced(COMMAND, "/do/0/fork", 0, output, 2, 1, null, AT),
        new EngineEvent.ForkBranchTaskEntered(
            COMMAND, "/do/0/fork", 0, "/do/0/fork/left", data, data, 2, 1, AT),
        new EngineEvent.ForkBranchExtensionEntered(
            COMMAND,
            "/do/0/fork",
            0,
            "/do/0/fork/extended",
            data,
            data,
            List.of(true, false),
            2,
            1,
            AT),
        new EngineEvent.ForkBranchFunctionEntered(
            COMMAND,
            "/do/0/fork",
            0,
            "/do/0/fork/function",
            data,
            data,
            functionOperation,
            2,
            1,
            AT),
        new EngineEvent.ForkBranchTaskCompleted(
            COMMAND, "/do/0/fork", 0, "/do/0/fork/left", output, data, 2, 1, null, AT),
        new EngineEvent.ForkBranchForEntered(
            COMMAND,
            "/do/0/fork",
            0,
            "/do/0/fork/loop",
            data,
            data,
            JsonNodeFactory.instance.arrayNode().add(7),
            0,
            "item",
            "index",
            2,
            1,
            AT),
        new EngineEvent.ForkBranchForAdvanced(
            COMMAND, "/do/0/fork", 0, "/do/0/fork/loop", data, 1, 2, 1, AT),
        new EngineEvent.ForkNestedEntered(
            COMMAND,
            "/do/0/fork",
            List.of(0),
            "/do/0/fork/nested",
            data,
            data,
            List.of("a", "b"),
            List.of(2, 3),
            List.of(3, 4),
            false,
            AT),
        new EngineEvent.ForkNestedBranchAdvanced(
            COMMAND, "/do/0/fork", List.of(0, 1), output, 4, AT),
        new EngineEvent.ForkNestedCompleted(
            COMMAND, "/do/0/fork", List.of(0), "/do/0/fork/nested", output, 5, AT),
        new EngineEvent.ForkNestedTaskEntered(
            COMMAND, "/do/0/fork", List.of(0, 1), "/do/0/fork/nested/do", data, data, 4, AT),
        new EngineEvent.ForkNestedExtensionEntered(
            COMMAND,
            "/do/0/fork",
            List.of(0, 1),
            "/do/0/fork/nested/extended",
            data,
            data,
            List.of(true, false),
            4,
            AT),
        new EngineEvent.ForkNestedFunctionEntered(
            COMMAND,
            "/do/0/fork",
            List.of(0, 1),
            "/do/0/fork/nested/function",
            data,
            data,
            functionOperation,
            4,
            AT),
        new EngineEvent.ForkNestedTaskCompleted(
            COMMAND, "/do/0/fork", List.of(0, 1), "/do/0/fork/nested/do", output, 4, AT),
        new EngineEvent.ForkNestedForEntered(
            COMMAND,
            "/do/0/fork",
            List.of(0, 1),
            "/do/0/fork/nested/loop",
            data,
            data,
            JsonNodeFactory.instance.arrayNode().add(7),
            0,
            "item",
            "index",
            4,
            AT),
        new EngineEvent.ForkNestedForAdvanced(
            COMMAND, "/do/0/fork", List.of(0, 1), "/do/0/fork/nested/loop", data, 1, 4, AT),
        new EngineEvent.ForkBranchWaitScheduled(
            COMMAND,
            "/do/0/fork",
            List.of(0, 1),
            "/do/0/fork/nested/wait",
            data,
            data,
            4,
            AT.plusSeconds(30),
            true,
            AT),
        new EngineEvent.ForkBranchWaitCompleted(
            COMMAND, "/do/0/fork", List.of(0, 1), "/do/0/fork/nested/wait", output, 5, false, AT),
        new EngineEvent.ForkBranchesWaiting(COMMAND, "/do/0/fork", AT.plusSeconds(30), AT),
        new EngineEvent.ForkBranchContextUpdated(COMMAND, "/do/0/fork", List.of(0, 1), data, AT),
        new EngineEvent.ForkBranchTryEntered(
            COMMAND, "/do/0/fork", List.of(0, 1), "/do/0/fork/try", data, data, 4, AT),
        new EngineEvent.ForkBranchTryCompleted(
            COMMAND, "/do/0/fork", List.of(0, 1), "/do/0/fork/try", output, data, 5, AT),
        new EngineEvent.ForkBranchErrorCaught(
            COMMAND, "/do/0/fork", List.of(0, 1), "/do/0/fork/try", error, 6, AT),
        new EngineEvent.ForkBranchRetryScheduled(
            COMMAND,
            "/do/0/fork",
            List.of(0, 1),
            "/do/0/fork/try",
            error,
            2,
            4,
            AT.plusSeconds(30),
            AT,
            true,
            AT),
        new EngineEvent.ForkBranchRetryStarted(
            COMMAND, "/do/0/fork", List.of(0, 1), "/do/0/fork/try", 2, 4, false, AT),
        new EngineEvent.ForkBranchEmitRequested(
            COMMAND,
            "/do/0/fork",
            List.of(0, 1),
            "/do/0/fork/emit",
            data,
            data,
            "fork-emit-fixture",
            scheduleCloudEvent,
            true,
            AT),
        new EngineEvent.ForkBranchEmitAcknowledged(
            COMMAND,
            "/do/0/fork",
            List.of(0, 1),
            "/do/0/fork/emit",
            "fork-emit-fixture",
            output,
            data,
            5,
            true,
            AT),
        new EngineEvent.ForkBranchHttpCallRequested(
            COMMAND,
            "/do/0/fork",
            List.of(0, 1),
            "/do/0/fork/http",
            data,
            data,
            httpOperation,
            true,
            AT),
        new EngineEvent.ForkBranchHttpCallCompleted(
            COMMAND,
            "/do/0/fork",
            List.of(0, 1),
            "/do/0/fork/http",
            "http-fixture",
            output,
            data,
            5,
            true,
            AT),
        new EngineEvent.ForkBranchProtocolCallRequested(
            COMMAND,
            "/do/0/fork",
            List.of(0, 1),
            "/do/0/fork/protocol",
            data,
            data,
            protocolOperation,
            true,
            AT),
        new EngineEvent.ForkBranchProtocolCallItemAccepted(
            COMMAND,
            "/do/0/fork",
            List.of(0, 1),
            "/do/0/fork/protocol",
            "protocol-fixture",
            output,
            true,
            AT),
        new EngineEvent.ForkBranchProtocolCallCompleted(
            COMMAND,
            "/do/0/fork",
            List.of(0, 1),
            "/do/0/fork/protocol",
            "protocol-fixture",
            output,
            data,
            5,
            true,
            AT),
        new EngineEvent.ForkBranchProtocolCallIterationStarted(
            COMMAND,
            "/do/0/fork",
            List.of(0, 1),
            "/do/0/fork/protocol",
            "protocol-fixture",
            data,
            data,
            JsonNodeFactory.instance.arrayNode().add(data).add(output),
            "message",
            "messageIndex",
            5,
            false,
            AT),
        new EngineEvent.ForkBranchProtocolCallIterationAdvanced(
            COMMAND,
            "/do/0/fork",
            List.of(0, 1),
            "/do/0/fork/protocol",
            JsonNodeFactory.instance.arrayNode().add(output).add(data),
            1,
            data,
            output,
            5,
            false,
            true,
            AT),
        new EngineEvent.ForkBranchListenStarted(
            COMMAND,
            "/do/0/fork",
            List.of(0, 1),
            "/do/0/fork/listen",
            data,
            data,
            "fork-listen-fixture",
            Set.of("example.schedule.v1"),
            true,
            AT),
        new EngineEvent.ForkBranchListenAccepted(
            COMMAND,
            "/do/0/fork",
            scheduleCloudEvent,
            List.of(
                new EngineEvent.ForkListenUpdate(
                    List.of(0, 1),
                    "/do/0/fork/listen",
                    "fork-listen-fixture",
                    List.of(scheduleCloudEvent),
                    java.util.Map.of(),
                    Set.of(0),
                    null,
                    EngineEvent.ForkListenDisposition.COMPLETE,
                    output,
                    data,
                    5,
                    null,
                    null,
                    null)),
            false,
            false,
            AT),
        new EngineEvent.ForkBranchListenIterationAdvanced(
            COMMAND,
            "/do/0/fork",
            List.of(0, 1),
            "/do/0/fork/listen",
            JsonNodeFactory.instance.arrayNode().add(output).add(data),
            1,
            data,
            output,
            5,
            false,
            true,
            true,
            AT),
        new EngineEvent.ForkBranchEffectSkipped(
            COMMAND, "/do/0/fork", List.of(0, 1), "/do/0/fork/emit", output, data, 5, false, AT),
        new EngineEvent.ForkBranchSubworkflowRequested(
            COMMAND,
            "/do/0/fork",
            List.of(0, 1),
            "/do/0/fork/child",
            data,
            data,
            5,
            scheduledExecution.value().toString(),
            scheduledExecution,
            ACTOR,
            subflow,
            data,
            true,
            null,
            null,
            true,
            AT),
        new EngineEvent.ForkBranchSubworkflowCompleted(
            COMMAND,
            "/do/0/fork",
            List.of(0, 1),
            "/do/0/fork/child",
            scheduledExecution.value().toString(),
            scheduledExecution,
            ExecutionStatus.COMPLETED,
            output,
            data,
            5,
            false,
            AT),
        new EngineEvent.SubworkflowRequested(
            COMMAND,
            "/do/0/child",
            data,
            data,
            5,
            scheduledExecution.value().toString(),
            scheduledExecution,
            ACTOR,
            subflow,
            data,
            true,
            null,
            null,
            AT),
        new EngineEvent.SubworkflowCompleted(
            COMMAND,
            "/do/0/child",
            scheduledExecution.value().toString(),
            scheduledExecution,
            ExecutionStatus.COMPLETED,
            output,
            data,
            5,
            AT),
        new EngineEvent.EmitRequested(
            COMMAND,
            "/do/0/emit",
            data,
            data,
            4,
            "emit-fixture",
            new com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent(
                "1.0",
                "event-fixture",
                java.net.URI.create("https://events.example.test"),
                "example.fixture.v1",
                null,
                AT,
                "application/json",
                data,
                java.util.Map.of()),
            AT),
        new EngineEvent.EmitAcknowledged(
            COMMAND, "/do/0/emit", "emit-fixture", output, data, 5, AT),
        new EngineEvent.HttpCallRequested(COMMAND, "/do/0/http", data, data, 5, httpOperation, AT),
        new EngineEvent.HttpCallCompleted(
            COMMAND, "/do/0/http", "http-fixture", output, data, 5, AT),
        new EngineEvent.ProtocolCallRequested(
            COMMAND, "/do/0/protocol", data, data, 5, protocolOperation, AT),
        new EngineEvent.ProtocolCallItemAccepted(
            COMMAND, "/do/0/protocol", "protocol-fixture", output, AT),
        new EngineEvent.ProtocolCallCompleted(
            COMMAND, "/do/0/protocol", "protocol-fixture", output, data, 5, AT),
        new EngineEvent.ProtocolCallIterationStarted(
            COMMAND,
            "/do/0/protocol",
            "protocol-fixture",
            data,
            data,
            JsonNodeFactory.instance.arrayNode().add(data).add(output),
            "message",
            "messageIndex",
            5,
            AT),
        new EngineEvent.ProtocolCallIterationAdvanced(
            COMMAND,
            "/do/0/protocol",
            JsonNodeFactory.instance.arrayNode().add(output).add(data),
            1,
            data,
            output,
            5,
            false,
            AT),
        new EngineEvent.CorrelatedWorkerRequested(
            COMMAND,
            "/do/0/worker",
            data,
            data,
            5,
            "correlated-worker-fixture",
            protocolOperation,
            correlatedWorkerEventsOperation,
            correlatedWorkerCancellationOperation,
            AT),
        new EngineEvent.CorrelatedWorkerCommandPublished(
            COMMAND, "/do/0/worker", "correlated-worker-fixture", AT),
        new EngineEvent.CorrelatedWorkerProgressObserved(
            COMMAND, "/do/0/worker", "correlated-worker-fixture", "PROGRESS", output, AT),
        new EngineEvent.CorrelatedWorkerCompleted(
            COMMAND, "/do/0/worker", "correlated-worker-fixture", output, data, 5, AT),
        new EngineEvent.CorrelatedWorkerCancellationDispatched(
            COMMAND,
            "/do/0/worker",
            "correlated-worker-fixture",
            correlatedWorkerCancellationOperation,
            AT),
        new EngineEvent.ListenStarted(COMMAND, "/do/0/listen", data, data, 4, "listen-fixture", AT),
        new EngineEvent.ListenEventAccepted(
            COMMAND,
            "/do/0/listen",
            "listen-fixture",
            new com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent(
                "1.0",
                "received-fixture",
                java.net.URI.create("https://events.example.test"),
                "example.received.v1",
                null,
                AT,
                "application/json",
                data,
                java.util.Map.of()),
            List.of(
                new com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent(
                    "1.0",
                    "received-fixture",
                    java.net.URI.create("https://events.example.test"),
                    "example.received.v1",
                    null,
                    AT,
                    "application/json",
                    data,
                    java.util.Map.of())),
            java.util.Map.of("caseId", data),
            Set.of(0),
            true,
            output,
            data,
            5,
            AT),
        new EngineEvent.ListenIterationStarted(
            COMMAND,
            "/do/0/listen",
            data,
            data,
            JsonNodeFactory.instance.arrayNode().add(data).add(output),
            "event",
            "eventIndex",
            5,
            AT),
        new EngineEvent.ListenIterationAdvanced(
            COMMAND,
            "/do/0/listen",
            JsonNodeFactory.instance.arrayNode().add(output).add(data),
            1,
            data,
            output,
            5,
            AT),
        new EngineEvent.ListenUntilAdvanced(
            COMMAND,
            "/do/0/listen",
            "listen-fixture",
            scheduleCloudEvent,
            new EventConsumptionWindow(List.of(scheduleCloudEvent), java.util.Map.of(), Set.of(0)),
            AT),
        new EngineEvent.ErrorRaised(COMMAND, "/do/0/try/try/0/raise", error, AT),
        new EngineEvent.ErrorCaught(COMMAND, "/do/0/try", error, 2, AT),
        new EngineEvent.RetryScheduled(
            COMMAND, "/do/0/try", error, 2, 1, AT.plusSeconds(30), AT, AT),
        new EngineEvent.RetryStarted(COMMAND, "/do/0/try", 2, 1, AT),
        new EngineEvent.TaskCompleted(COMMAND, "/do/0/initialize", 1, output, data, AT),
        new EngineEvent.PauseRequested(COMMAND, ACTOR, AT),
        new EngineEvent.Paused(COMMAND, AT),
        new EngineEvent.Resumed(COMMAND, ACTOR, AT),
        new EngineEvent.CancellationRequested(COMMAND, ACTOR, AT),
        new EngineEvent.Cancelled(COMMAND, AT),
        new EngineEvent.Completed(COMMAND, output, AT),
        new EngineEvent.Failed(COMMAND, "fixture failure", AT),
        new WorkflowReply.Accepted(COMMAND, EXECUTION, 7, ExecutionStatus.PAUSED),
        new WorkflowReply.Rejected(
            COMMAND, EXECUTION, 7, ExecutionStatus.PAUSED, "fixture", "fixture rejection"),
        new WorkflowReply.StateSnapshot(EXECUTION, 7, ExecutionStatus.PAUSED, data),
        new WorkflowReply.RuntimeState(WorkflowRuntimeState.from(new WorkflowState.New(EXECUTION))),
        new WorkflowState.New(EXECUTION),
        new WorkflowState.Running(
            EXECUTION, plan, data, 0, 1, receipts, data, data, List.of(), AT.plusSeconds(30)),
        new WorkflowState.Waiting(
            EXECUTION,
            plan,
            data,
            0,
            2,
            receipts,
            "timer",
            AT.plusSeconds(30),
            data,
            data,
            List.of(),
            AT.plusSeconds(30)),
        new WorkflowState.Pausing(
            EXECUTION, plan, data, 0, 2, receipts, data, data, List.of(), AT.plusSeconds(30)),
        new WorkflowState.Paused(
            EXECUTION, plan, data, 0, 3, receipts, data, data, List.of(), AT.plusSeconds(30)),
        new WorkflowState.Cancelling(
            EXECUTION, plan, data, 0, 4, receipts, data, data, List.of(), AT.plusSeconds(30)),
        new WorkflowState.Cancelled(EXECUTION, data, 5, receipts),
        new WorkflowState.Completed(EXECUTION, output, 5, receipts),
        new WorkflowState.Failed(EXECUTION, data, 5, receipts, "fixture failure"),
        new ScheduleCommand.Register(
            COMMAND, scheduleId, ACTOR, schedulePlan, data, AT, scheduleReplyTo),
        new ScheduleCommand.Due(scheduleId, ScheduleTriggerKind.EVERY, AT.plusSeconds(30)),
        new ScheduleCommand.ExecutionCompleted(scheduleId, scheduledExecution, AT),
        new ScheduleCommand.EventReceived(scheduleId, scheduleCloudEvent, AT),
        new ScheduleCommand.DispatchAcknowledged(scheduleId, scheduledExecution, AT),
        new ScheduleCommand.Recheck(scheduleId),
        new ScheduleCommand.GetState(scheduleId, scheduleReplyTo),
        new ScheduleEvent.Registered(
            COMMAND, scheduleId, ACTOR, schedulePlan, data, AT.plusSeconds(30), null, AT),
        new ScheduleEvent.AfterScheduled(scheduledExecution, AT.plusSeconds(60), AT),
        new ScheduleEvent.EventAccepted(
            scheduleCloudEvent.source() + "|" + scheduleCloudEvent.id(),
            new EventConsumptionWindow(List.of(scheduleCloudEvent), java.util.Map.of(), Set.of(0)),
            null,
            AT),
        new ScheduleEvent.LaunchRequested(request, AT.plusSeconds(3600), null, null, AT),
        new ScheduleEvent.DispatchAcknowledged(scheduledExecution, AT),
        new ScheduleReply.Accepted(COMMAND, scheduleId, 2),
        new ScheduleReply.Rejected(COMMAND, scheduleId, 2, "fixture", "fixture rejection"),
        new ScheduleReply.Snapshot(
            scheduleId,
            2,
            true,
            AT.plusSeconds(30),
            null,
            Set.of(AT.plusSeconds(60)),
            Set.of(scheduledExecution)),
        new ScheduleState.Unregistered(scheduleId),
        new ScheduleState.Active(
            scheduleId,
            ACTOR,
            schedulePlan,
            data,
            2,
            AT.plusSeconds(30),
            null,
            Set.of(AT.plusSeconds(60)),
            Set.of(scheduledExecution),
            List.of(request)),
        request,
        new SubworkflowCoordinatorCommand.Launch(
            COMMAND,
            EXECUTION,
            scheduledExecution,
            scheduledExecution.value().toString(),
            ACTOR,
            plan,
            data,
            true,
            AT,
            coordinatorReplyTo),
        new SubworkflowCoordinatorCommand.Poll(),
        new SubworkflowCoordinatorCommand.ParentObserved(coordinatorObservation, null),
        new SubworkflowCoordinatorCommand.ChildObserved(coordinatorObservation, null, false, false),
        new SubworkflowCoordinatorCommand.ParentDeliveryObserved(true, null),
        new SubworkflowCoordinatorEvent.Launched(
            COMMAND,
            EXECUTION,
            scheduledExecution,
            scheduledExecution.value().toString(),
            ACTOR,
            plan,
            data,
            true,
            AT),
        new SubworkflowCoordinatorEvent.ChildTerminalObserved(
            ExecutionStatus.COMPLETED, output, null, AT),
        new SubworkflowCoordinatorEvent.ParentNotified(AT),
        new SubworkflowCoordinatorReply(scheduledExecution, 1, true),
        new SubworkflowCoordinatorState.Empty(scheduledExecution),
        coordinatorActive,
        coordinatorTerminal,
        new SubworkflowCoordinatorState.Delivered(coordinatorTerminal, 3),
        new ProtocolOperationCoordinatorCommand.Start(
            EXECUTION,
            "protocol-fixture",
            testKit.system().<ProtocolOperationCoordinatorReply>ignoreRef()),
        new ProtocolOperationCoordinatorCommand.Poll(),
        new ProtocolOperationCoordinatorCommand.StateObserved(
            WorkflowRuntimeState.from(new WorkflowState.New(EXECUTION)), null),
        new ProtocolOperationCoordinatorCommand.TransportEnded(1, null),
        new ProtocolOperationCoordinatorCommand.DeadlineObserved(null),
        new ProtocolOperationCoordinatorReply(EXECUTION, "protocol-fixture", true));
  }
}
