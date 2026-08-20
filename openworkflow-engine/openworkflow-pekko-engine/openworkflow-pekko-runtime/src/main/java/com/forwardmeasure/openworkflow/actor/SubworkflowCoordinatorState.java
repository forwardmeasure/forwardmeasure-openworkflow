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
import java.util.Objects;
import java.util.UUID;

/** Durable state of one child workflow launch/observation handshake. */
public sealed interface SubworkflowCoordinatorState
    permits SubworkflowCoordinatorState.Empty,
        SubworkflowCoordinatorState.Active,
        SubworkflowCoordinatorState.Terminal,
        SubworkflowCoordinatorState.Delivered {
  ExecutionId childExecutionId();

  long revision();

  record Empty(ExecutionId childExecutionId) implements SubworkflowCoordinatorState {
    public Empty {
      Objects.requireNonNull(childExecutionId, "childExecutionId");
    }

    @Override
    public long revision() {
      return 0;
    }
  }

  record Active(
      UUID commandId,
      ExecutionId parentExecutionId,
      ExecutionId childExecutionId,
      String operationId,
      ActorIdentity actor,
      WorkflowPlan childPlan,
      JsonNode childInput,
      boolean awaitParent,
      long revision)
      implements SubworkflowCoordinatorState {
    public Active {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(parentExecutionId, "parentExecutionId");
      Objects.requireNonNull(childExecutionId, "childExecutionId");
      Objects.requireNonNull(operationId, "operationId");
      Objects.requireNonNull(actor, "actor");
      Objects.requireNonNull(childPlan, "childPlan");
      childInput = Objects.requireNonNull(childInput, "childInput").deepCopy();
      if (revision < 1) throw new IllegalArgumentException("revision must be positive");
    }
  }

  record Terminal(
      Active launch, ExecutionStatus status, JsonNode output, String failure, long revision)
      implements SubworkflowCoordinatorState {
    public Terminal {
      Objects.requireNonNull(launch, "launch");
      Objects.requireNonNull(status, "status");
      output = Objects.requireNonNull(output, "output").deepCopy();
      if (revision <= launch.revision()) {
        throw new IllegalArgumentException("terminal revision must advance");
      }
    }

    @Override
    public ExecutionId childExecutionId() {
      return launch.childExecutionId();
    }
  }

  record Delivered(Terminal terminal, long revision) implements SubworkflowCoordinatorState {
    public Delivered {
      Objects.requireNonNull(terminal, "terminal");
      if (revision <= terminal.revision()) {
        throw new IllegalArgumentException("delivered revision must advance");
      }
    }

    @Override
    public ExecutionId childExecutionId() {
      return terminal.childExecutionId();
    }
  }
}
