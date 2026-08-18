package com.forwardmeasure.openworkflow.workflow.runtime.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class WorkflowExecutionIdsTest {
  private static final OksTenantId TENANT = OksTenantId.parse("did:web:ssb.example");

  @Test
  void derivesStableIdentityFromTheCompleteSubmissionIdentity() {
    WorkflowExecutionId first =
        WorkflowExecutionIds.idempotent(TENANT, "information-extraction", "1.0.0", "request-42");
    WorkflowExecutionId replay =
        WorkflowExecutionIds.idempotent(TENANT, "information-extraction", "1.0.0", "request-42");

    assertEquals(first, replay);
    assertNotEquals(
        first,
        WorkflowExecutionIds.idempotent(TENANT, "information-extraction", "2.0.0", "request-42"));
    assertNotEquals(
        first,
        WorkflowExecutionIds.idempotent(
            OksTenantId.parse("did:web:other.example"),
            "information-extraction",
            "1.0.0",
            "request-42"));
  }

  @Test
  void rejectsIncompleteSubmissionIdentity() {
    assertThrows(
        IllegalArgumentException.class,
        () -> WorkflowExecutionIds.idempotent(TENANT, " ", "1.0.0", "request-42"));
    assertThrows(
        IllegalArgumentException.class,
        () -> WorkflowExecutionIds.idempotent(TENANT, "information-extraction", "1.0.0", " "));
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
