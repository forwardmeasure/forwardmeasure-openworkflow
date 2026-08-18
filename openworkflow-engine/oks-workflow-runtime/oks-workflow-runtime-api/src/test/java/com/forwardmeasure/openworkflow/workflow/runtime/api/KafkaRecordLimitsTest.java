package com.forwardmeasure.openworkflow.workflow.runtime.api;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class KafkaRecordLimitsTest {
  private static final OksTenantId TENANT = OksTenantId.parse("did:web:tenant.example.test");
  private static final ActorId RUNTIME =
      ActorId.parse("did:web:tenant.example.test:actors:oks-workflow-runtime");

  @Test
  void enforcesRuntimeAndDefinitionBoundariesExactly() {
    assertDoesNotThrow(
        () ->
            KafkaRecordLimits.requireRuntimeTransition(KafkaRecordLimits.RUNTIME_TRANSITION_BYTES));
    KafkaRecordLimitException runtime =
        assertThrows(
            KafkaRecordLimitException.class,
            () ->
                KafkaRecordLimits.requireRuntimeTransition(
                    KafkaRecordLimits.RUNTIME_TRANSITION_BYTES + 1));
    assertEquals(KafkaRecordLimits.RUNTIME_TRANSITION_BYTES, runtime.maximumBytes());

    assertDoesNotThrow(
        () ->
            KafkaRecordLimits.requireDefinitionPayload(KafkaRecordLimits.DEFINITION_PAYLOAD_BYTES));
    KafkaRecordLimitException definition =
        assertThrows(
            KafkaRecordLimitException.class,
            () ->
                KafkaRecordLimits.requireDefinitionPayload(
                    KafkaRecordLimits.DEFINITION_PAYLOAD_BYTES + 1));
    assertEquals(KafkaRecordLimits.DEFINITION_PAYLOAD_BYTES, definition.maximumBytes());
  }

  @Test
  void untrustedComputationObservationCannotBypassTransitionLimit() {
    var transition = new ObjectMapper().createObjectNode();
    transition.put("oversized", "x".repeat(KafkaRecordLimits.RUNTIME_TRANSITION_BYTES));

    assertThrows(
        KafkaRecordLimitException.class,
        () ->
            new ObserveWorkflowComputationCommand(
                "computation-1:result",
                new ExecutionKey(TENANT, new WorkflowExecutionId("run-1")),
                "computation-1",
                1,
                "a".repeat(64),
                transition,
                Actors.system(
                    TENANT, RUNTIME, "oks-computation", Instant.parse("2026-07-31T00:00:00Z")),
                Instant.parse("2026-07-31T00:00:00Z")));
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
