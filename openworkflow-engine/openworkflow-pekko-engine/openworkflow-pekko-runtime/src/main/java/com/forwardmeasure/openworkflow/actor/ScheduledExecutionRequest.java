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

/** Durable at-least-once request emitted by a workflow schedule. */
public record ScheduledExecutionRequest(
    ScheduleId scheduleId,
    ExecutionId executionId,
    ActorIdentity actor,
    WorkflowPlan plan,
    JsonNode input,
    ScheduleTriggerKind trigger,
    Instant scheduledAt) {
  public ScheduledExecutionRequest {
    Objects.requireNonNull(scheduleId, "scheduleId");
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(actor, "actor");
    Objects.requireNonNull(plan, "plan");
    input = Objects.requireNonNull(input, "input").deepCopy();
    Objects.requireNonNull(trigger, "trigger");
    Objects.requireNonNull(scheduledAt, "scheduledAt");
    if (!scheduleId.tenantId().equals(executionId.tenantId())
        || !scheduleId.tenantId().equals(actor.tenantId())) {
      throw new IllegalArgumentException("Schedule, execution, and actor must share a tenant");
    }
  }
}
