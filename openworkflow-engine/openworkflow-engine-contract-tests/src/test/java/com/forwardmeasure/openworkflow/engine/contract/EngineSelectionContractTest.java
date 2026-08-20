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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.forwardmeasure.openworkflow.definition.WorkflowCoordinates;
import com.forwardmeasure.openworkflow.engine.api.ActorId;
import com.forwardmeasure.openworkflow.engine.api.CommandAcknowledgement;
import com.forwardmeasure.openworkflow.engine.api.DefinitionRevision;
import com.forwardmeasure.openworkflow.engine.api.EngineCommandException;
import com.forwardmeasure.openworkflow.engine.api.EngineHealth;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommandEnvelope;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProviders;
import com.forwardmeasure.openworkflow.engine.api.TenantActorContext;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class EngineSelectionContractTest {
  private static final String DIGEST = "a".repeat(64);
  private static final TenantActorContext CONTEXT =
      new TenantActorContext(
          new TenantId(UUID.fromString("ca54c5f9-d688-42d6-9d90-7b08f2ddc101")),
          "org-selection",
          new ActorId("actor-selection"));
  private static final DefinitionRevision DEFINITION =
      new DefinitionRevision(
          UUID.fromString("66d84900-a7fc-4cc2-a832-2fc848d0af1d"),
          null,
          new WorkflowCoordinates("selection", "workflow", "1.0.0", "1.0.3"),
          DIGEST,
          DIGEST);

  @Test
  void trustedPolicyCanSelectOnlyAReadyRegisteredProvider() {
    var providers =
        new ExecutionEngineProviders(
            List.of(
                new StubProvider(EngineId.PEKKO, true),
                new StubProvider(EngineId.KAFKA_STREAMS, false)));

    var selected =
        providers.select(CONTEXT, DEFINITION, request -> request.availableEngines().getFirst());

    assertEquals(EngineId.PEKKO, selected.engineId());
  }

  @Test
  void unavailablePolicyChoiceIsRejected() {
    var providers = new ExecutionEngineProviders(List.of(new StubProvider(EngineId.PEKKO, true)));

    EngineCommandException failure =
        assertThrows(
            EngineCommandException.class,
            () -> providers.select(CONTEXT, DEFINITION, request -> EngineId.KAFKA_STREAMS));

    assertEquals(EngineCommandException.FailureKind.UNAVAILABLE, failure.kind());
  }

  @Test
  void duplicateProviderIdentityIsRejected() {
    var first = new StubProvider(EngineId.PEKKO, true);
    assertThrows(
        IllegalArgumentException.class, () -> new ExecutionEngineProviders(List.of(first, first)));
  }

  private record StubProvider(EngineId engineId, boolean ready) implements ExecutionEngineProvider {
    @Override
    public CompletionStage<CommandAcknowledgement> submit(ExecutionCommandEnvelope command) {
      throw new UnsupportedOperationException("selection test does not dispatch commands");
    }

    @Override
    public EngineHealth health() {
      return new EngineHealth(
          engineId,
          ready ? EngineHealth.HealthState.UP : EngineHealth.HealthState.DOWN,
          ready,
          ready,
          Instant.parse("2026-08-17T12:00:00Z"),
          Map.of());
    }
  }
}
