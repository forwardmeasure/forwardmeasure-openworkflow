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
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A pending correlated-worker call is a real wait, not active computation - without this, an
 * execution blocked on an external worker for hours reports RUNNING the whole time,
 * indistinguishable from genuine computation through the public contract. See
 * docs/engine-construct-gap-audit.md gap #4.
 */
final class WorkflowStateTest {

  @Test
  void reportsWaitingWhilePendingOnACorrelatedWorkerCall() {
    var execution =
        new ExecutionId(
            TestTenantIds.tenant("did:web:forwardmeasure.com:tenant:workflow-state-waiting"),
            UUID.randomUUID());
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

    assertEquals(ExecutionStatus.WAITING, running.status());
  }

  @Test
  void reportsRunningWithNoPendingCorrelatedWorkerCall() {
    var execution =
        new ExecutionId(
            TestTenantIds.tenant("did:web:forwardmeasure.com:tenant:workflow-state-running"),
            UUID.randomUUID());
    var running =
        new WorkflowState.Running(
            execution, plan(), JsonNodeFactory.instance.objectNode(), 0, 1, Set.of());

    assertEquals(ExecutionStatus.RUNNING, running.status());
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
              name: workflow-state-test
              version: '1.0.0'
            do:
              - finish: { set: { done: true } }
            """
                .getBytes(StandardCharsets.UTF_8));
  }
}
