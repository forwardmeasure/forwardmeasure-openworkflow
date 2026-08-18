package com.forwardmeasure.openworkflow.workflow.runtime.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ExecutionKeyTest {

  @Test
  void roundTripsExistingPlatformTenantUuid() {
    var key =
        new ExecutionKey(
            OksTenantId.parse("did:web:tenant.example.com"), new WorkflowExecutionId("run-001"));

    assertEquals(key, ExecutionKey.parse(key.canonical()));
  }

  @Test
  void sameExecutionIdInDifferentTenantsCannotCollide() {
    var first =
        new ExecutionKey(
            OksTenantId.parse("did:web:tenant-a.example"), new WorkflowExecutionId("run-001"));
    var second =
        new ExecutionKey(
            OksTenantId.parse("did:web:tenant-b.example"), new WorkflowExecutionId("run-001"));

    assertNotEquals(first.canonical(), second.canonical());
  }

  @Test
  void rejectsMissingOrUnsafeTenantIdentity() {
    assertThrows(IllegalArgumentException.class, () -> OksTenantId.parse(""));
    assertThrows(IllegalArgumentException.class, () -> OksTenantId.parse("tenant/other"));
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
