package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Authenticated request to validate and immutably admit workflow source. */
public record AdmitWorkflowDefinitionCommand(
    String commandId,
    WorkflowDefinitionKey key,
    String source,
    List<ResolvedWorkflowResource> resources,
    ActorContext actor,
    Instant requestedAt) {

  public AdmitWorkflowDefinitionCommand {
    Objects.requireNonNull(commandId, "commandId");
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(source, "source");
    resources = resources == null ? List.of() : List.copyOf(resources);
    Objects.requireNonNull(actor, "actor");
    Objects.requireNonNull(requestedAt, "requestedAt");
    if (commandId.isBlank()) {
      throw new IllegalArgumentException("commandId must not be blank");
    }
    if (source.isBlank()) {
      throw new IllegalArgumentException("source must not be blank");
    }
    if (!key.tenantId().equals(actor.tenantId())) {
      throw new IllegalArgumentException("Definition and actor tenants must match");
    }
  }

  public AdmitWorkflowDefinitionCommand(
      String commandId,
      WorkflowDefinitionKey key,
      String source,
      ActorContext actor,
      Instant requestedAt) {
    this(commandId, key, source, List.of(), actor, requestedAt);
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
