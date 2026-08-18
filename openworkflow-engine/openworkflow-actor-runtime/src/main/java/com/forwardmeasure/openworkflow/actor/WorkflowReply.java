package com.forwardmeasure.openworkflow.actor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionStatus;
import java.util.Objects;
import java.util.UUID;

/** Persist-confirmed replies from a workflow FSM. */
public sealed interface WorkflowReply
    permits WorkflowReply.Accepted,
        WorkflowReply.Rejected,
        WorkflowReply.StateSnapshot,
        WorkflowReply.RuntimeState {

  record Accepted(UUID commandId, ExecutionId executionId, long revision, ExecutionStatus status)
      implements WorkflowReply {

    public Accepted {
      Objects.requireNonNull(commandId, "commandId");
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(status, "status");
    }
  }

  record Rejected(
      UUID commandId,
      ExecutionId executionId,
      long revision,
      ExecutionStatus status,
      String code,
      String message)
      implements WorkflowReply {

    public Rejected {
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(status, "status");
      Objects.requireNonNull(code, "code");
      Objects.requireNonNull(message, "message");
    }
  }

  record StateSnapshot(
      ExecutionId executionId, long revision, ExecutionStatus status, JsonNode data)
      implements WorkflowReply {

    public StateSnapshot {
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(status, "status");
      data = data == null ? NullNode.getInstance() : data.deepCopy();
    }
  }

  /** Internal response; never exposed by the HTTP management API. */
  record RuntimeState(WorkflowRuntimeState state) implements WorkflowReply {
    public RuntimeState {
      Objects.requireNonNull(state, "state");
    }
  }
}
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
