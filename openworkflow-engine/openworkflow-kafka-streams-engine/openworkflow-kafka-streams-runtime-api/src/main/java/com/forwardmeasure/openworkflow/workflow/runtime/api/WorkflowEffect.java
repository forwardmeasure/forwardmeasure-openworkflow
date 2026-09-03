package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.forwardmeasure.openworkflow.data.DataReference;
import java.time.Instant;
import java.util.Objects;

/** Transactional outbox record produced by the pure Open Workflow state machine. */
public record WorkflowEffect(
    String effectId,
    ExecutionKey key,
    WorkflowEffectType type,
    String taskPath,
    DataReference payload,
    ActorContext actor,
    Instant requestedAt) {

  public WorkflowEffect {
    requireText(effectId, "effectId");
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(type, "type");
    requireText(taskPath, "taskPath");
    Objects.requireNonNull(payload, "payload");
    Objects.requireNonNull(actor, "actor");
    Objects.requireNonNull(requestedAt, "requestedAt");
    if (!actor.tenantId().equals(key.tenantId())) {
      throw new IllegalArgumentException(
          "Effect actor and execution must belong to the same tenant");
    }
  }

  private static void requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
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
