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

/** Commands for one durable parent/child execution coordinator. */
public sealed interface SubworkflowCoordinatorCommand
    permits SubworkflowCoordinatorCommand.Launch,
        SubworkflowCoordinatorCommand.Poll,
        SubworkflowCoordinatorCommand.ParentObserved,
        SubworkflowCoordinatorCommand.ChildObserved,
        SubworkflowCoordinatorCommand.ParentDeliveryObserved {

  record Launch(
      UUID commandId,
      ExecutionId parentExecutionId,
      ExecutionId childExecutionId,
      String operationId,
      ActorIdentity actor,
      WorkflowPlan childPlan,
      JsonNode childInput,
      boolean awaitParent,
      Instant requestedAt,
      ActorRef<SubworkflowCoordinatorReply> replyTo)
      implements SubworkflowCoordinatorCommand {
    public Launch {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(parentExecutionId, "parentExecutionId");
      Objects.requireNonNull(childExecutionId, "childExecutionId");
      Objects.requireNonNull(operationId, "operationId");
      Objects.requireNonNull(actor, "actor");
      Objects.requireNonNull(childPlan, "childPlan");
      childInput = Objects.requireNonNull(childInput, "childInput").deepCopy();
      Objects.requireNonNull(requestedAt, "requestedAt");
      Objects.requireNonNull(replyTo, "replyTo");
      if (!parentExecutionId.tenantId().equals(childExecutionId.tenantId())
          || !parentExecutionId.tenantId().equals(actor.tenantId())) {
        throw new IllegalArgumentException("Parent, child, and actor must share one tenant");
      }
      if (!childExecutionId.value().toString().equals(operationId)) {
        throw new IllegalArgumentException("Operation ID must equal the child execution UUID");
      }
    }
  }

  record Poll() implements SubworkflowCoordinatorCommand {}

  record ParentObserved(WorkflowObservation observation, String failure)
      implements SubworkflowCoordinatorCommand {}

  record ChildObserved(
      WorkflowObservation observation, String failure, boolean cancellation, boolean pause)
      implements SubworkflowCoordinatorCommand {
    public ChildObserved(WorkflowObservation observation, String failure) {
      this(observation, failure, false, false);
    }

    public ChildObserved(WorkflowObservation observation, String failure, boolean cancellation) {
      this(observation, failure, cancellation, false);
    }

    public ChildObserved {
      if (cancellation && pause) {
        throw new IllegalArgumentException(
            "A child observation cannot be both pause and cancellation");
      }
    }
  }

  record ParentDeliveryObserved(boolean accepted, String failure)
      implements SubworkflowCoordinatorCommand {}

  record WorkflowObservation(
      ExecutionId executionId,
      long revision,
      com.forwardmeasure.openworkflow.engine.api.ExecutionStatus status,
      JsonNode data,
      boolean accepted,
      boolean snapshot,
      String rejectionCode) {
    public WorkflowObservation {
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(status, "status");
      data =
          data == null
              ? com.fasterxml.jackson.databind.node.NullNode.getInstance()
              : data.deepCopy();
      if (revision < 0) throw new IllegalArgumentException("revision must not be negative");
    }
  }
}
