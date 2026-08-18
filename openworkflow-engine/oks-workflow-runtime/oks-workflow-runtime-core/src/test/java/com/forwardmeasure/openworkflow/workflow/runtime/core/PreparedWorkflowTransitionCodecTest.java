package com.forwardmeasure.openworkflow.workflow.runtime.core;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.definition.PlanStepKind;
import com.forwardmeasure.openworkflow.definition.WorkflowCoordinates;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReferences;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionEventType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionHistoryEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.KafkaRecordLimitException;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OksTenantId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowExecutionId;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PreparedWorkflowTransitionCodecTest {
  private static final Instant NOW = Instant.parse("2026-07-31T00:00:00Z");
  private static final OksTenantId TENANT = OksTenantId.parse("did:web:tenant.example.test");

  @Test
  void rejectsAggregateTransitionBeforeItCanExceedKafkaRecordLimit() {
    ObjectNode value = new ObjectMapper().createObjectNode();
    value.put("payload", "x".repeat(31_500));
    DataReference data = DataReferences.inline(value);
    ActorContext actor =
        new ActorContext(
            TENANT,
            ActorId.parse(TENANT + ":actors:user-1"),
            ActorType.HUMAN,
            "User One",
            "runtime-test",
            Set.of(),
            null,
            NOW);
    ExecutionKey key = new ExecutionKey(TENANT, new WorkflowExecutionId("record-limit-run"));
    WorkflowDefinitionReference definition =
        new WorkflowDefinitionReference(
            new WorkflowDefinitionKey(
                TENANT, new WorkflowCoordinates("limits", "aggregate", "1.0.0", "1.0.3")),
            "a".repeat(64),
            "b".repeat(64));
    ExecutionSnapshot snapshot =
        new ExecutionSnapshot(
            key,
            definition,
            null,
            actor,
            NOW,
            ExecutionPhase.RUNNING,
            ExecutionCursor.start(data),
            data,
            data,
            data,
            20);
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    for (int index = 0; index < 12; index++) {
      events.add(
          new ExecutionHistoryEvent(
              "event-" + index,
              key,
              index,
              ExecutionEventType.TASK_COMPLETED,
              definition.definitionSha256(),
              "/do/" + index,
              "step-" + index,
              PlanStepKind.SET,
              data,
              data,
              actor,
              NOW));
    }

    assertThrows(
        KafkaRecordLimitException.class,
        () ->
            new PreparedWorkflowTransitionCodec(new ObjectMapper())
                .encode(new PreparedWorkflowTransition(snapshot, events, List.of(), List.of())));
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
