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
package com.forwardmeasure.openworkflow.engine.contract;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.forwardmeasure.openworkflow.definition.WorkflowCoordinates;
import com.forwardmeasure.openworkflow.engine.api.ActorId;
import com.forwardmeasure.openworkflow.engine.api.CommandAcknowledgement;
import com.forwardmeasure.openworkflow.engine.api.DefinitionRevision;
import com.forwardmeasure.openworkflow.engine.api.EngineHealth;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommandEnvelope;
import com.forwardmeasure.openworkflow.engine.api.ExecutionError;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionHistoryEntry;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import com.forwardmeasure.openworkflow.engine.api.ExecutionQuery;
import com.forwardmeasure.openworkflow.engine.api.TenantActorContext;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PortableContractSerializationTest {
  private static final ObjectMapper JSON = new ObjectMapper().registerModule(new JavaTimeModule());
  private static final TenantId TENANT =
      new TenantId(UUID.fromString("5126cac4-834d-4c16-a4c7-a2415d787e6c"));
  private static final ExecutionId EXECUTION =
      new ExecutionId(TENANT, UUID.fromString("e051ab62-fad8-4ad9-bb6d-6434d066542b"));
  private static final Instant NOW = Instant.parse("2026-08-17T12:00:00Z");

  @Test
  void commandEnvelopeRoundTripsWithItsConcreteControlCommand() throws Exception {
    var value =
        new ExecutionCommandEnvelope(
            UUID.fromString("877450cc-ae6e-4570-983d-4bba03a10072"),
            "correlation-1",
            new TenantActorContext(
                TENANT, "org-1", new ActorId("actor-1"), Set.of("workflow-execution-controller")),
            EngineId.PEKKO,
            3,
            NOW,
            new ExecutionCommand.Cancel(EXECUTION, "requested"));

    assertEquals(value, roundTrip(value, ExecutionCommandEnvelope.class));
  }

  @Test
  void tenantActorContextReadsLegacyRecordsWithoutOrganizationRoles() throws Exception {
    TenantActorContext legacy =
        JSON.readValue(
            """
            {
              "tenantId": "5126cac4-834d-4c16-a4c7-a2415d787e6c",
              "organizationId": "org-1",
              "actorId": {"value": "actor-1"}
            }
            """,
            TenantActorContext.class);

    assertEquals(Set.of(), legacy.organizationRoles());
  }

  @Test
  void acknowledgementHealthEventAndCanonicalQueryRecordsRoundTrip() throws Exception {
    var acknowledgement =
        new CommandAcknowledgement(
            UUID.randomUUID(), EXECUTION, EngineId.PEKKO, ExecutionLifecycleState.RUNNING, 3, NOW);
    var health =
        new EngineHealth(
            EngineId.PEKKO, EngineHealth.HealthState.UP, true, true, NOW, Map.of("journal", "up"));
    var event =
        new ExecutionEvent(
            UUID.randomUUID(),
            acknowledgement.commandId(),
            EXECUTION,
            EngineId.PEKKO,
            3,
            ExecutionEvent.EventType.TASK_COMPLETED,
            ExecutionLifecycleState.RUNNING,
            NOW,
            JsonNodeFactory.instance.objectNode().put("task", "initialize"));
    var query =
        new ExecutionQuery(
            TENANT,
            Set.of(ExecutionLifecycleState.RUNNING),
            EngineId.PEKKO,
            NOW.minusSeconds(60),
            NOW);

    assertEquals(acknowledgement, roundTrip(acknowledgement, CommandAcknowledgement.class));
    assertEquals(health, roundTrip(health, EngineHealth.class));
    assertEquals(event, roundTrip(event, ExecutionEvent.class));
    assertEquals(query, roundTrip(query, ExecutionQuery.class));
  }

  @Test
  void definitionErrorAndHistoryRecordsRoundTrip() throws Exception {
    String digest = "a".repeat(64);
    var definition =
        new DefinitionRevision(
            UUID.randomUUID(),
            new WorkflowCoordinates("example", "flow", "1.0.0", "1.0.3"),
            digest,
            digest);
    var error =
        new ExecutionError(
            "TASK_FAILED",
            "task failed",
            "initialize",
            true,
            JsonNodeFactory.instance.objectNode().put("attempt", 1));
    var history =
        new ExecutionHistoryEntry(
            UUID.randomUUID(),
            4,
            ExecutionLifecycleState.FAILED,
            "failed",
            "initialize",
            NOW,
            error.details());

    assertEquals(definition, roundTrip(definition, DefinitionRevision.class));
    assertEquals(error, roundTrip(error, ExecutionError.class));
    assertEquals(history, roundTrip(history, ExecutionHistoryEntry.class));
  }

  @Test
  void canonicalJsonPayloadsAreDefensivelyCopied() {
    var source = JsonNodeFactory.instance.objectNode().put("task", "initialize");
    var event =
        new ExecutionEvent(
            UUID.randomUUID(),
            UUID.randomUUID(),
            EXECUTION,
            EngineId.PEKKO,
            1,
            ExecutionEvent.EventType.TASK_ENTERED,
            ExecutionLifecycleState.RUNNING,
            NOW,
            source);

    source.put("constructorLeak", true);
    ((ObjectNode) event.data()).put("accessorLeak", true);

    assertFalse(event.data().has("constructorLeak"));
    assertFalse(event.data().has("accessorLeak"));
  }

  private static <T> T roundTrip(T value, Class<T> type) throws Exception {
    return JSON.readValue(JSON.writeValueAsBytes(value), type);
  }
}
