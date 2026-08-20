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
import com.forwardmeasure.openworkflow.engine.api.EventConsumptionWindow;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Events persisted by the durable schedule coordinator. */
public sealed interface ScheduleEvent
    permits ScheduleEvent.Registered,
        ScheduleEvent.AfterScheduled,
        ScheduleEvent.EventAccepted,
        ScheduleEvent.LaunchRequested,
        ScheduleEvent.DispatchAcknowledged {

  Instant occurredAt();

  record Registered(
      UUID commandId,
      ScheduleId scheduleId,
      ActorIdentity actor,
      WorkflowPlan plan,
      JsonNode input,
      Instant nextEvery,
      Instant nextCron,
      Instant occurredAt)
      implements ScheduleEvent {
    public Registered {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(scheduleId, "scheduleId");
      Objects.requireNonNull(actor, "actor");
      Objects.requireNonNull(plan, "plan");
      input = Objects.requireNonNull(input, "input").deepCopy();
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record AfterScheduled(ExecutionId completedExecutionId, Instant deadline, Instant occurredAt)
      implements ScheduleEvent {
    public AfterScheduled {
      Objects.requireNonNull(completedExecutionId, "completedExecutionId");
      Objects.requireNonNull(deadline, "deadline");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record EventAccepted(
      String eventKey,
      EventConsumptionWindow window,
      ScheduledExecutionRequest request,
      Instant occurredAt)
      implements ScheduleEvent {
    public EventAccepted {
      Objects.requireNonNull(eventKey, "eventKey");
      Objects.requireNonNull(window, "window");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record LaunchRequested(
      ScheduledExecutionRequest request,
      Instant nextEvery,
      Instant nextCron,
      Instant consumedAfter,
      Instant occurredAt)
      implements ScheduleEvent {
    public LaunchRequested {
      Objects.requireNonNull(request, "request");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record DispatchAcknowledged(ExecutionId executionId, Instant occurredAt)
      implements ScheduleEvent {
    public DispatchAcknowledged {
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }
}
