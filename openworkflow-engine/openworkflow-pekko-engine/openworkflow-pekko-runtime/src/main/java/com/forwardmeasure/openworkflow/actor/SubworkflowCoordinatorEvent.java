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
import com.forwardmeasure.openworkflow.engine.api.ExecutionStatus;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Durable facts for at-least-once child launch and parent observation. */
public sealed interface SubworkflowCoordinatorEvent
    permits SubworkflowCoordinatorEvent.Launched,
        SubworkflowCoordinatorEvent.ChildTerminalObserved,
        SubworkflowCoordinatorEvent.ParentNotified {

  Instant occurredAt();

  record Launched(
      UUID commandId,
      ExecutionId parentExecutionId,
      ExecutionId childExecutionId,
      String operationId,
      ActorIdentity actor,
      WorkflowPlan childPlan,
      JsonNode childInput,
      boolean awaitParent,
      Instant occurredAt)
      implements SubworkflowCoordinatorEvent {
    public Launched {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(parentExecutionId, "parentExecutionId");
      Objects.requireNonNull(childExecutionId, "childExecutionId");
      Objects.requireNonNull(operationId, "operationId");
      Objects.requireNonNull(actor, "actor");
      Objects.requireNonNull(childPlan, "childPlan");
      childInput = Objects.requireNonNull(childInput, "childInput").deepCopy();
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record ChildTerminalObserved(
      ExecutionStatus status, JsonNode output, String failure, Instant occurredAt)
      implements SubworkflowCoordinatorEvent {
    public ChildTerminalObserved {
      Objects.requireNonNull(status, "status");
      output =
          output == null
              ? com.fasterxml.jackson.databind.node.NullNode.getInstance()
              : output.deepCopy();
      Objects.requireNonNull(occurredAt, "occurredAt");
      if (status != ExecutionStatus.COMPLETED
          && status != ExecutionStatus.CANCELLED
          && status != ExecutionStatus.FAILED) {
        throw new IllegalArgumentException("Observed child status must be terminal");
      }
    }
  }

  record ParentNotified(Instant occurredAt) implements SubworkflowCoordinatorEvent {
    public ParentNotified {
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }
}
