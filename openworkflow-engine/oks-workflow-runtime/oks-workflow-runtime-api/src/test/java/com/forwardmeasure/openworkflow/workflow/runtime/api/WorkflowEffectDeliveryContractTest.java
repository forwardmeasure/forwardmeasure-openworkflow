package com.forwardmeasure.openworkflow.workflow.runtime.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.EnumSet;
import org.junit.jupiter.api.Test;

class WorkflowEffectDeliveryContractTest {

  @Test
  void everyEffectTypeDeclaresItsDeliveryGuarantee() {
    for (WorkflowEffectType type : WorkflowEffectType.values()) {
      assertNotNull(type.deliveryGuarantee(), () -> type + " has no delivery guarantee");
    }
  }

  @Test
  void onlyRemoteOperationDispatchRequiresExternalDeduplication() {
    EnumSet<WorkflowEffectType> externallyDeduplicated = EnumSet.noneOf(WorkflowEffectType.class);
    for (WorkflowEffectType type : WorkflowEffectType.values()) {
      if (type.deliveryGuarantee()
          == EffectDeliveryGuarantee.AT_LEAST_ONCE_EXTERNAL_DEDUPLICATION) {
        externallyDeduplicated.add(type);
      }
    }
    assertEquals(EnumSet.of(WorkflowEffectType.DISPATCH_OPERATION), externallyDeduplicated);
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
