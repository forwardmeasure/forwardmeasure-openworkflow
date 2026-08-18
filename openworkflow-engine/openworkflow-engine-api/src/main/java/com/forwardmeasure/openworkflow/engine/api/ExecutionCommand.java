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
package com.forwardmeasure.openworkflow.engine.api;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import java.util.Objects;

/** Portable commands accepted by every execution engine provider. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = ExecutionCommand.Start.class, name = "start"),
  @JsonSubTypes.Type(value = ExecutionCommand.Pause.class, name = "pause"),
  @JsonSubTypes.Type(value = ExecutionCommand.Resume.class, name = "resume"),
  @JsonSubTypes.Type(value = ExecutionCommand.Cancel.class, name = "cancel")
})
public sealed interface ExecutionCommand {
  ExecutionId executionId();

  record Start(
      ExecutionId executionId,
      DefinitionRevision definition,
      WorkflowPlan plan,
      String sourceDocument,
      JsonNode input)
      implements ExecutionCommand {
    public Start {
      Objects.requireNonNull(executionId, "executionId");
      Objects.requireNonNull(definition, "definition");
      Objects.requireNonNull(plan, "plan");
      input = ContractSupport.copy(input, "input");
      if (sourceDocument != null && sourceDocument.isBlank()) {
        throw new IllegalArgumentException("sourceDocument must not be blank when present");
      }
      if (!definition.coordinates().equals(plan.coordinates())
          || !definition.definitionSha256().equals(plan.definitionSha256())
          || !definition.compilerSha256().equals(plan.compilerSha256())) {
        throw new IllegalArgumentException(
            "definition revision does not identify the supplied plan");
      }
    }

    public Start(
        ExecutionId executionId, DefinitionRevision definition, WorkflowPlan plan, JsonNode input) {
      this(executionId, definition, plan, null, input);
    }

    @Override
    public JsonNode input() {
      return input.deepCopy();
    }
  }

  record Pause(ExecutionId executionId) implements ExecutionCommand {
    public Pause {
      Objects.requireNonNull(executionId, "executionId");
    }
  }

  record Resume(ExecutionId executionId) implements ExecutionCommand {
    public Resume {
      Objects.requireNonNull(executionId, "executionId");
    }
  }

  record Cancel(ExecutionId executionId, String reason) implements ExecutionCommand {
    public Cancel {
      Objects.requireNonNull(executionId, "executionId");
      ContractSupport.requireText(reason, "reason");
    }
  }
}
