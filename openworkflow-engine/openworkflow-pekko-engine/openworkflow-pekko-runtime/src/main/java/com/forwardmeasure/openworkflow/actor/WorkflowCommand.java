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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.DeadlineScope;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import org.apache.pekko.actor.typed.ActorRef;

/** Commands accepted by one tenant-qualified persistent workflow FSM. */
public sealed interface WorkflowCommand
    permits WorkflowCommand.Start,
        WorkflowCommand.RunNext,
        WorkflowCommand.Pause,
        WorkflowCommand.Resume,
        WorkflowCommand.Cancel,
        WorkflowCommand.TimerElapsed,
        WorkflowCommand.RetryElapsed,
        WorkflowCommand.DeadlineElapsed,
        WorkflowCommand.RecheckTimers,
        WorkflowCommand.EffectAcknowledged,
        WorkflowCommand.HttpCallCompleted,
        WorkflowCommand.ProtocolCallObserved,
        WorkflowCommand.SubworkflowCompleted,
        WorkflowCommand.CloudEventReceived,
        WorkflowCommand.GetState,
        WorkflowCommand.GetRuntimeState {

  ExecutionId executionId();

  ActorRef<WorkflowReply> replyTo();

  record Start(
      UUID commandId,
      ExecutionId executionId,
      ActorIdentity actor,
      WorkflowPlan plan,
      JsonNode input,
      Instant requestedAt,
      ActorRef<WorkflowReply> replyTo)
      implements WorkflowCommand {

    public Start {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(actor, "actor");
      Objects.requireNonNull(plan, "plan");
      input = Objects.requireNonNull(input, "input").deepCopy();
      Objects.requireNonNull(requestedAt, "requestedAt");
      Objects.requireNonNull(replyTo, "replyTo");
      if (!executionId.tenantId().equals(actor.tenantId())) {
        throw new IllegalArgumentException("Execution and authenticated actor must share a tenant");
      }
    }
  }

  /** Internal execution-plane command that advances at most one durable task. */
  record RunNext(
      UUID commandId,
      ExecutionId executionId,
      ActorIdentity actor,
      Instant requestedAt,
      ActorRef<WorkflowReply> replyTo)
      implements WorkflowCommand {
    public RunNext {
      validateControl(commandId, executionId, actor, requestedAt, replyTo);
    }
  }

  record Pause(
      UUID commandId,
      ExecutionId executionId,
      ActorIdentity actor,
      Instant requestedAt,
      ActorRef<WorkflowReply> replyTo)
      implements WorkflowCommand {
    public Pause {
      validateControl(commandId, executionId, actor, requestedAt, replyTo);
    }
  }

  record Resume(
      UUID commandId,
      ExecutionId executionId,
      ActorIdentity actor,
      Instant requestedAt,
      ActorRef<WorkflowReply> replyTo)
      implements WorkflowCommand {
    public Resume {
      validateControl(commandId, executionId, actor, requestedAt, replyTo);
    }
  }

  record Cancel(
      UUID commandId,
      ExecutionId executionId,
      ActorIdentity actor,
      Instant requestedAt,
      ActorRef<WorkflowReply> replyTo)
      implements WorkflowCommand {
    public Cancel {
      validateControl(commandId, executionId, actor, requestedAt, replyTo);
    }
  }

  record GetState(ExecutionId executionId, ActorRef<WorkflowReply> replyTo)
      implements WorkflowCommand {

    public GetState {
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(replyTo, "replyTo");
    }
  }

  /** Internal full-state query used only by durable effect coordinators. */
  record GetRuntimeState(ExecutionId executionId, ActorRef<WorkflowReply> replyTo)
      implements WorkflowCommand {
    public GetRuntimeState {
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(replyTo, "replyTo");
    }
  }

  /** Internal timer delivery; the persisted deadline makes duplicates harmless. */
  record TimerElapsed(ExecutionId executionId, String taskPath, Instant deadline)
      implements WorkflowCommand {
    public TimerElapsed {
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(taskPath, "taskPath");
      Objects.requireNonNull(deadline, "deadline");
    }

    @Override
    public ActorRef<WorkflowReply> replyTo() {
      return null;
    }
  }

  record RetryElapsed(ExecutionId executionId, String tryTaskPath, Instant deadline)
      implements WorkflowCommand {
    public RetryElapsed {
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(tryTaskPath, "tryTaskPath");
      Objects.requireNonNull(deadline, "deadline");
    }

    @Override
    public ActorRef<WorkflowReply> replyTo() {
      return null;
    }
  }

  /** Adapter acknowledgement for an idempotent durable effect intent. */
  record EffectAcknowledged(
      ExecutionId executionId,
      String operationId,
      Instant acknowledgedAt,
      ActorRef<WorkflowReply> replyTo)
      implements WorkflowCommand {
    public EffectAcknowledged(ExecutionId executionId, String operationId, Instant acknowledgedAt) {
      this(executionId, operationId, acknowledgedAt, null);
    }

    public EffectAcknowledged {
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(operationId, "operationId");
      Objects.requireNonNull(acknowledgedAt, "acknowledgedAt");
    }
  }

  /** Persist-confirmed adapter observation for a durable HTTP/OpenAPI intent. */
  record HttpCallCompleted(
      ExecutionId executionId,
      String operationId,
      JsonNode result,
      boolean failed,
      Instant observedAt,
      ActorRef<WorkflowReply> replyTo)
      implements WorkflowCommand {
    public HttpCallCompleted(
        ExecutionId executionId,
        String operationId,
        JsonNode output,
        JsonNode error,
        Instant observedAt) {
      this(executionId, operationId, output, error, observedAt, null);
    }

    public HttpCallCompleted(
        ExecutionId executionId,
        String operationId,
        JsonNode output,
        JsonNode error,
        Instant observedAt,
        ActorRef<WorkflowReply> replyTo) {
      this(
          executionId,
          operationId,
          output == null ? error : output,
          error != null,
          observedAt,
          replyTo);
      if ((output == null) == (error == null)) {
        throw new IllegalArgumentException("Exactly one HTTP call output or error is required");
      }
    }

    public HttpCallCompleted {
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(operationId, "operationId");
      Objects.requireNonNull(observedAt, "observedAt");
      result = Objects.requireNonNull(result, "result").deepCopy();
    }

    @JsonIgnore
    public JsonNode output() {
      return failed ? null : result.deepCopy();
    }

    @JsonIgnore
    public JsonNode error() {
      return failed ? result.deepCopy() : null;
    }
  }

  /** Persist-confirmed item, terminal marker, or failure from a protocol adapter. */
  record ProtocolCallObserved(
      ExecutionId executionId,
      String operationId,
      String observationId,
      JsonNode observation,
      boolean failed,
      boolean terminal,
      Instant observedAt,
      ActorRef<WorkflowReply> replyTo)
      implements WorkflowCommand {
    public ProtocolCallObserved(
        ExecutionId executionId,
        String operationId,
        JsonNode item,
        boolean terminal,
        Instant observedAt) {
      this(
          executionId,
          operationId,
          observedAt.toString(),
          item == null ? com.fasterxml.jackson.databind.node.NullNode.getInstance() : item,
          false,
          terminal,
          observedAt,
          null);
    }

    public ProtocolCallObserved(
        ExecutionId executionId,
        String operationId,
        JsonNode observation,
        boolean failed,
        boolean terminal,
        Instant observedAt,
        ActorRef<WorkflowReply> replyTo) {
      this(
          executionId,
          operationId,
          observedAt.toString(),
          observation,
          failed,
          terminal,
          observedAt,
          replyTo);
    }

    public static ProtocolCallObserved failure(
        ExecutionId executionId,
        String operationId,
        JsonNode error,
        Instant observedAt,
        ActorRef<WorkflowReply> replyTo) {
      return new ProtocolCallObserved(
          executionId, operationId, observedAt.toString(), error, true, true, observedAt, replyTo);
    }

    public ProtocolCallObserved {
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(operationId, "operationId");
      Objects.requireNonNull(observedAt, "observedAt");
      observationId = observationId == null ? observedAt.toString() : observationId;
      if (observationId.isBlank())
        throw new IllegalArgumentException("observationId must not be blank");
      observation = Objects.requireNonNull(observation, "observation").deepCopy();
      if (failed && !terminal)
        throw new IllegalArgumentException("A failed protocol observation must be terminal");
    }

    @JsonIgnore
    public JsonNode item() {
      return failed || observation.isNull() ? null : observation.deepCopy();
    }

    @JsonIgnore
    public JsonNode error() {
      return failed ? observation.deepCopy() : null;
    }
  }

  /** Persist-confirmed terminal observation from a durable child coordinator. */
  record SubworkflowCompleted(
      UUID commandId,
      ExecutionId executionId,
      String operationId,
      ExecutionId childExecutionId,
      com.forwardmeasure.openworkflow.engine.api.ExecutionStatus childStatus,
      JsonNode output,
      String failure,
      Instant observedAt,
      ActorRef<WorkflowReply> replyTo)
      implements WorkflowCommand {
    public SubworkflowCompleted(
        ExecutionId executionId,
        String operationId,
        ExecutionId childExecutionId,
        com.forwardmeasure.openworkflow.engine.api.ExecutionStatus childStatus,
        JsonNode output,
        String failure,
        Instant observedAt) {
      this(
          subworkflowCommandId(executionId, operationId, childStatus),
          executionId,
          operationId,
          childExecutionId,
          childStatus,
          output,
          failure,
          observedAt,
          null);
    }

    public SubworkflowCompleted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(operationId, "operationId");
      Objects.requireNonNull(childExecutionId, "childExecutionId");
      Objects.requireNonNull(childStatus, "childStatus");
      output =
          output == null
              ? com.fasterxml.jackson.databind.node.NullNode.getInstance()
              : output.deepCopy();
      Objects.requireNonNull(observedAt, "observedAt");
      if (!executionId.tenantId().equals(childExecutionId.tenantId())) {
        throw new IllegalArgumentException("Parent and child executions must share a tenant");
      }
      if (childStatus != com.forwardmeasure.openworkflow.engine.api.ExecutionStatus.COMPLETED
          && childStatus != com.forwardmeasure.openworkflow.engine.api.ExecutionStatus.CANCELLED
          && childStatus != com.forwardmeasure.openworkflow.engine.api.ExecutionStatus.FAILED) {
        throw new IllegalArgumentException("Child status must be terminal");
      }
    }

    private static UUID subworkflowCommandId(
        ExecutionId executionId,
        String operationId,
        com.forwardmeasure.openworkflow.engine.api.ExecutionStatus status) {
      return UUID.nameUUIDFromBytes(
          (executionId.entityId() + "|subworkflow-completed|" + operationId + "|" + status)
              .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
  }

  /** Tenant-routed inbound CloudEvent offered to a durable listen task. */
  record CloudEventReceived(
      UUID commandId,
      ExecutionId executionId,
      com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent event,
      Instant receivedAt,
      ActorRef<WorkflowReply> replyTo)
      implements WorkflowCommand {
    public CloudEventReceived(
        ExecutionId executionId,
        com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent event,
        Instant receivedAt) {
      this(eventCommandId(executionId, event), executionId, event, receivedAt, null);
    }

    public CloudEventReceived(
        ExecutionId executionId,
        com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent event,
        Instant receivedAt,
        ActorRef<WorkflowReply> replyTo) {
      this(eventCommandId(executionId, event), executionId, event, receivedAt, replyTo);
    }

    public CloudEventReceived {
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(event, "event");
      Objects.requireNonNull(receivedAt, "receivedAt");
      commandId = commandId == null ? eventCommandId(executionId, event) : commandId;
    }

    private static UUID eventCommandId(
        ExecutionId executionId,
        com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent event) {
      return UUID.nameUUIDFromBytes(
          (executionId.entityId() + "|cloud-event|" + event.source() + "|" + event.id())
              .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
  }

  /** Internal delivery for a previously persisted workflow or task deadline. */
  record DeadlineElapsed(
      ExecutionId executionId, DeadlineScope scope, String taskPath, Instant deadline)
      implements WorkflowCommand {
    public DeadlineElapsed {
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(scope, "scope");
      if (scope == DeadlineScope.TASK && (taskPath == null || taskPath.isBlank())) {
        throw new IllegalArgumentException("A task deadline requires taskPath");
      }
      if (scope == DeadlineScope.WORKFLOW && taskPath != null) {
        throw new IllegalArgumentException("A workflow deadline cannot carry taskPath");
      }
      Objects.requireNonNull(deadline, "deadline");
    }

    @Override
    public ActorRef<WorkflowReply> replyTo() {
      return null;
    }
  }

  /** Internal bounded-horizon wake-up for deadlines beyond Pekko's timer range. */
  record RecheckTimers(ExecutionId executionId) implements WorkflowCommand {
    public RecheckTimers {
      Objects.requireNonNull(executionId, "executionId");
    }

    @Override
    public ActorRef<WorkflowReply> replyTo() {
      return null;
    }
  }

  private static void validateControl(
      UUID commandId,
      ExecutionId executionId,
      ActorIdentity actor,
      Instant requestedAt,
      ActorRef<WorkflowReply> replyTo) {
    Objects.requireNonNull(commandId, "commandId");
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(actor, "actor");
    Objects.requireNonNull(requestedAt, "requestedAt");
    Objects.requireNonNull(replyTo, "replyTo");
    if (!executionId.tenantId().equals(actor.tenantId())) {
      throw new IllegalArgumentException("Execution and authenticated actor must share a tenant");
    }
  }
}
