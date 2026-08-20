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
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Durable schedule FSM state. */
public sealed interface ScheduleState permits ScheduleState.Unregistered, ScheduleState.Active {
  ScheduleId scheduleId();

  long revision();

  record Unregistered(ScheduleId scheduleId) implements ScheduleState {
    public Unregistered {
      Objects.requireNonNull(scheduleId, "scheduleId");
    }

    @Override
    public long revision() {
      return 0;
    }
  }

  record Active(
      ScheduleId scheduleId,
      ActorIdentity actor,
      WorkflowPlan plan,
      JsonNode input,
      long revision,
      Instant nextEvery,
      Instant nextCron,
      Set<Instant> afterDeadlines,
      Set<ExecutionId> completedExecutions,
      List<ScheduledExecutionRequest> pending,
      EventConsumptionWindow eventWindow,
      Set<String> seenEventKeys)
      implements ScheduleState {
    public Active(
        ScheduleId scheduleId,
        ActorIdentity actor,
        WorkflowPlan plan,
        JsonNode input,
        long revision,
        Instant nextEvery,
        Instant nextCron,
        Set<Instant> afterDeadlines,
        Set<ExecutionId> completedExecutions,
        List<ScheduledExecutionRequest> pending) {
      this(
          scheduleId,
          actor,
          plan,
          input,
          revision,
          nextEvery,
          nextCron,
          afterDeadlines,
          completedExecutions,
          pending,
          EventConsumptionWindow.empty(),
          Set.of());
    }

    public Active {
      Objects.requireNonNull(scheduleId, "scheduleId");
      Objects.requireNonNull(actor, "actor");
      Objects.requireNonNull(plan, "plan");
      input = Objects.requireNonNull(input, "input").deepCopy();
      if (revision < 1) throw new IllegalArgumentException("revision must be positive");
      afterDeadlines = Set.copyOf(afterDeadlines);
      completedExecutions = Set.copyOf(completedExecutions);
      pending = List.copyOf(pending);
      eventWindow = eventWindow == null ? EventConsumptionWindow.empty() : eventWindow;
      seenEventKeys = seenEventKeys == null ? Set.of() : Set.copyOf(seenEventKeys);
    }
  }
}
