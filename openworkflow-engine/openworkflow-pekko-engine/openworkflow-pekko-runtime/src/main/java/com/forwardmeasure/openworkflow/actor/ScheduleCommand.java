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

import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import org.apache.pekko.actor.typed.ActorRef;

/** Commands accepted by one tenant-qualified durable schedule entity. */
public sealed interface ScheduleCommand
    permits ScheduleCommand.Register,
        ScheduleCommand.Due,
        ScheduleCommand.ExecutionCompleted,
        ScheduleCommand.EventReceived,
        ScheduleCommand.DispatchAcknowledged,
        ScheduleCommand.Recheck,
        ScheduleCommand.GetState {

  ScheduleId scheduleId();

  ActorRef<ScheduleReply> replyTo();

  record Register(
      UUID commandId,
      ScheduleId scheduleId,
      ActorIdentity actor,
      WorkflowPlan plan,
      JsonNode input,
      Instant registeredAt,
      ActorRef<ScheduleReply> replyTo)
      implements ScheduleCommand {
    public Register {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(scheduleId, "scheduleId");
      Objects.requireNonNull(actor, "actor");
      Objects.requireNonNull(plan, "plan");
      input = Objects.requireNonNull(input, "input").deepCopy();
      Objects.requireNonNull(registeredAt, "registeredAt");
      Objects.requireNonNull(replyTo, "replyTo");
      if (!scheduleId.tenantId().equals(actor.tenantId())
          || !scheduleId.definition().equals(plan.coordinates())) {
        throw new IllegalArgumentException(
            "Schedule registration identity does not match its plan");
      }
      if (plan.schedule() == null) {
        throw new IllegalArgumentException("Workflow has no schedule");
      }
    }
  }

  record Due(ScheduleId scheduleId, ScheduleTriggerKind trigger, Instant deadline)
      implements ScheduleCommand {
    public Due {
      Objects.requireNonNull(scheduleId, "scheduleId");
      Objects.requireNonNull(trigger, "trigger");
      Objects.requireNonNull(deadline, "deadline");
      if (trigger == ScheduleTriggerKind.EVENT) {
        throw new IllegalArgumentException("Event triggers are not timer deliveries");
      }
    }

    @Override
    public ActorRef<ScheduleReply> replyTo() {
      return null;
    }
  }

  record ExecutionCompleted(ScheduleId scheduleId, ExecutionId executionId, Instant completedAt)
      implements ScheduleCommand {
    public ExecutionCompleted {
      Objects.requireNonNull(scheduleId, "scheduleId");
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(completedAt, "completedAt");
      if (!scheduleId.tenantId().equals(executionId.tenantId())) {
        throw new IllegalArgumentException("Completed execution has another tenant");
      }
    }

    @Override
    public ActorRef<ScheduleReply> replyTo() {
      return null;
    }
  }

  record EventReceived(
      UUID commandId,
      ScheduleId scheduleId,
      com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent event,
      Instant receivedAt,
      ActorRef<ScheduleReply> replyTo)
      implements ScheduleCommand {
    public EventReceived(
        ScheduleId scheduleId,
        com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent event,
        Instant receivedAt) {
      this(eventCommandId(scheduleId, event), scheduleId, event, receivedAt, null);
    }

    public EventReceived(
        ScheduleId scheduleId,
        com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent event,
        Instant receivedAt,
        ActorRef<ScheduleReply> replyTo) {
      this(eventCommandId(scheduleId, event), scheduleId, event, receivedAt, replyTo);
    }

    public EventReceived {
      Objects.requireNonNull(scheduleId, "scheduleId");
      Objects.requireNonNull(event, "event");
      Objects.requireNonNull(receivedAt, "receivedAt");
      commandId = commandId == null ? eventCommandId(scheduleId, event) : commandId;
    }

    private static UUID eventCommandId(
        ScheduleId scheduleId,
        com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent event) {
      return UUID.nameUUIDFromBytes(
          (scheduleId.entityId() + "|cloud-event|" + event.source() + "|" + event.id())
              .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
  }

  record DispatchAcknowledged(
      ScheduleId scheduleId, ExecutionId executionId, Instant acknowledgedAt)
      implements ScheduleCommand {
    public DispatchAcknowledged {
      Objects.requireNonNull(scheduleId, "scheduleId");
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(acknowledgedAt, "acknowledgedAt");
    }

    @Override
    public ActorRef<ScheduleReply> replyTo() {
      return null;
    }
  }

  record Recheck(ScheduleId scheduleId) implements ScheduleCommand {
    public Recheck {
      Objects.requireNonNull(scheduleId, "scheduleId");
    }

    @Override
    public ActorRef<ScheduleReply> replyTo() {
      return null;
    }
  }

  record GetState(ScheduleId scheduleId, ActorRef<ScheduleReply> replyTo)
      implements ScheduleCommand {
    public GetState {
      Objects.requireNonNull(scheduleId, "scheduleId");
      Objects.requireNonNull(replyTo, "replyTo");
    }
  }
}
