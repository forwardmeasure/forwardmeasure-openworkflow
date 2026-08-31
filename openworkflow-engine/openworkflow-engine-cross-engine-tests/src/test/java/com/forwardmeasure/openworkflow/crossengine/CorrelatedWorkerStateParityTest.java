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
package com.forwardmeasure.openworkflow.crossengine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.actor.EventExecutionFrame;
import com.forwardmeasure.openworkflow.actor.TaskExecutionFrame;
import com.forwardmeasure.openworkflow.actor.WorkflowState;
import com.forwardmeasure.openworkflow.definition.AsyncApiSubscriptionPlan;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceKind;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceReference;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionStatus;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionEventType;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.OksKafkaRuntime;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Both execution engines must agree on the shared policy question "does a pending correlated-worker
 * interaction mean WAITING, not RUNNING" - see {@code BlockingConstructs} (openworkflow-engine-api)
 * and docs/engine-construct-gap-audit.md gap #4. Before {@code BlockingConstructs} existed, this
 * rule was two independently-authored copies (one per engine) that happened to agree; nothing would
 * have caught it if they silently diverged. This test drives each engine's real production entry
 * point - {@link WorkflowState.Running#status()} on Pekko, {@link
 * OksKafkaRuntime#mapping(ExecutionEventType)} on Kafka-Streams - with the equivalent
 * "correlated-worker call in flight" input and asserts they resolve to the same semantic outcome.
 *
 * <p>This is deliberately narrower than exercising each engine's full runtime (actor system / Kafka
 * topology end-to-end) - that would need a much larger integration harness sharing both engines'
 * test infrastructure in one process, which doesn't exist today (see "Not in this plan" in the
 * engine-parity plan for the follow-up this is scoped from). What this test does verify directly is
 * that both engines' real, production status-decision code paths are wired to the same shared
 * source of truth, not two hand-authored copies of the same literal.
 */
final class CorrelatedWorkerStateParityTest {

  @Test
  void bothEnginesReportWaitingWhilePendingOnACorrelatedWorkerCall() {
    assertEquals(ExecutionStatus.WAITING, pekkoStatusWithPendingCorrelatedWorkerFrame(), "Pekko");

    assertEquals(
        "WAITING",
        OksKafkaRuntime.mapping(ExecutionEventType.CORRELATED_WORKER_STARTED).state().name(),
        "Kafka-Streams: CORRELATED_WORKER_STARTED");
    assertEquals(
        "WAITING",
        OksKafkaRuntime.mapping(ExecutionEventType.CORRELATED_WORKER_COMMAND_PUBLISHED)
            .state()
            .name(),
        "Kafka-Streams: CORRELATED_WORKER_COMMAND_PUBLISHED");
    assertEquals(
        "WAITING",
        OksKafkaRuntime.mapping(ExecutionEventType.CORRELATED_WORKER_PROGRESS).state().name(),
        "Kafka-Streams: CORRELATED_WORKER_PROGRESS");
    assertEquals(
        "WAITING",
        OksKafkaRuntime.mapping(ExecutionEventType.CORRELATED_WORKER_ACCEPTED).state().name(),
        "Kafka-Streams: CORRELATED_WORKER_ACCEPTED");
  }

  @Test
  void neitherEngineReportsWaitingWithNoPendingCorrelatedWorkerInteraction() {
    assertEquals(ExecutionStatus.RUNNING, pekkoStatusWithNoPendingFrame());
    assertEquals(
        "RUNNING",
        OksKafkaRuntime.mapping(ExecutionEventType.TASK_STARTED).state().name(),
        "Kafka-Streams: TASK_STARTED");
  }

  private static ExecutionStatus pekkoStatusWithPendingCorrelatedWorkerFrame() {
    var execution = new ExecutionId(new TenantId(UUID.randomUUID()), UUID.randomUUID());
    var command = correlatedWorkerOperation("worker-1", ProtocolOperationDescriptor.Mode.PUBLISH);
    var events =
        correlatedWorkerOperation("worker-1:events", ProtocolOperationDescriptor.Mode.SUBSCRIBE);
    var frame =
        TaskExecutionFrame.eventing(
            "/do/0/execute",
            JsonNodeFactory.instance.objectNode(),
            JsonNodeFactory.instance.objectNode(),
            EventExecutionFrame.correlatedWorker("worker-1", command, events, null));
    var running =
        new WorkflowState.Running(
            execution,
            plan(),
            JsonNodeFactory.instance.objectNode(),
            0,
            1,
            Set.of(),
            JsonNodeFactory.instance.objectNode(),
            JsonNodeFactory.instance.objectNode(),
            List.of(frame));
    return running.status();
  }

  private static ExecutionStatus pekkoStatusWithNoPendingFrame() {
    var execution = new ExecutionId(new TenantId(UUID.randomUUID()), UUID.randomUUID());
    var running =
        new WorkflowState.Running(
            execution, plan(), JsonNodeFactory.instance.objectNode(), 0, 1, Set.of());
    return running.status();
  }

  private static ProtocolOperationDescriptor correlatedWorkerOperation(
      String operationId, ProtocolOperationDescriptor.Mode mode) {
    boolean subscribe = mode == ProtocolOperationDescriptor.Mode.SUBSCRIBE;
    return new ProtocolOperationDescriptor(
        operationId,
        ProtocolOperationDescriptor.Kind.ASYNC_API,
        mode,
        new WorkflowResourceReference(
            WorkflowResourceKind.ASYNC_API_DOCUMENT,
            URI.create("https://contracts.example.test/workers.yaml"),
            "b".repeat(64)),
        "kafka",
        URI.create("kafka://broker.example.test:9092"),
        subscribe ? "receiveWorkerEvents" : "publishWorkerCommand",
        JsonNodeFactory.instance.objectNode(),
        subscribe
            ? new AsyncApiSubscriptionPlan(
                null,
                new AsyncApiSubscriptionPlan.Consumption(
                    AsyncApiSubscriptionPlan.Consumption.Mode.UNTIL,
                    null,
                    "${ .payload.status == \"SUCCEEDED\" }",
                    null),
                null,
                null,
                null)
            : null,
        null,
        null,
        null,
        null);
  }

  private static com.forwardmeasure.openworkflow.definition.WorkflowPlan plan() {
    return new OpenWorkflowCompiler()
        .compile(
            """
            document:
              dsl: '1.0.3'
              namespace: forwardmeasure
              name: cross-engine-parity-test
              version: '1.0.0'
            do:
              - finish: { set: { done: true } }
            """
                .getBytes(StandardCharsets.UTF_8));
  }
}
