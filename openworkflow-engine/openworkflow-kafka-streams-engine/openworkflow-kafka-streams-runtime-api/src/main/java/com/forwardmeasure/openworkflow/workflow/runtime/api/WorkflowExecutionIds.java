package com.forwardmeasure.openworkflow.workflow.runtime.api;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/** Stable execution identities for idempotent workflow submission. */
public final class WorkflowExecutionIds {
  private WorkflowExecutionIds() {}

  public static WorkflowExecutionId idempotent(
      OksTenantId tenantId, String workflowName, String workflowVersion, String idempotencyKey) {
    Objects.requireNonNull(tenantId, "tenantId");
    requireText(workflowName, "workflowName");
    requireText(workflowVersion, "workflowVersion");
    requireText(idempotencyKey, "idempotencyKey");
    String identity =
        tenantId + "\u0000" + workflowName + "\u0000" + workflowVersion + "\u0000" + idempotencyKey;
    return new WorkflowExecutionId(
        UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8)).toString());
  }

  private static void requireText(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
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
