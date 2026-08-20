package com.forwardmeasure.openworkflow.workflow.runtime.api;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Durable audit decision for one definition-admission command. */
public record WorkflowDefinitionAdmissionEvent(
    String eventId,
    String commandId,
    WorkflowDefinitionKey key,
    WorkflowDefinitionAdmissionStatus status,
    String sourceSha256,
    String definitionSha256,
    String compilerSha256,
    List<String> issues,
    ActorContext actor,
    Instant occurredAt) {

  public WorkflowDefinitionAdmissionEvent {
    requireText(eventId, "eventId");
    requireText(commandId, "commandId");
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(status, "status");
    issues = List.copyOf(Objects.requireNonNull(issues, "issues"));
    Objects.requireNonNull(actor, "actor");
    Objects.requireNonNull(occurredAt, "occurredAt");
    if (!key.tenantId().equals(actor.tenantId())) {
      throw new IllegalArgumentException(
          "Admission actor and definition must belong to the " + "same tenant");
    }
    if (status == WorkflowDefinitionAdmissionStatus.REJECTED) {
      if (issues.isEmpty()) {
        throw new IllegalArgumentException("A rejected admission requires at least one issue");
      }
    } else {
      requireSha256(sourceSha256, "sourceSha256");
      requireSha256(definitionSha256, "definitionSha256");
      requireSha256(compilerSha256, "compilerSha256");
      if (!issues.isEmpty()) {
        throw new IllegalArgumentException("A successful admission cannot contain issues");
      }
    }
  }

  private static void requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
  }

  private static void requireSha256(String value, String name) {
    Objects.requireNonNull(value, name);
    if (!value.matches("[0-9a-f]{64}")) {
      throw new IllegalArgumentException(name + " must be lowercase SHA-256");
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
