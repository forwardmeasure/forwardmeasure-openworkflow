/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.engine.api.ActorId;
import com.forwardmeasure.openworkflow.engine.api.DefinitionRevision;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommandEnvelope;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import com.forwardmeasure.openworkflow.engine.api.ExecutionProjection;
import com.forwardmeasure.openworkflow.engine.api.TenantActorContext;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.engine.contract.ExecutionEngineContractFixture;
import com.forwardmeasure.openworkflow.engine.contract.ExecutionEngineProviderContract;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class KafkaStreamsExecutionEngineProviderContractTest extends ExecutionEngineProviderContract {
  private static final TenantId TENANT =
      new TenantId(UUID.fromString("134b09a7-1c36-4b89-86e7-a28c88bc5cef"));

  @Test
  void acknowledgementUsesCanonicalCommandVersionAfterGatewayRestart() {
    var start = (ExecutionCommand.Start) newStartCommand(EngineId.KAFKA_STREAMS).command();
    var envelope =
        new ExecutionCommandEnvelope(
            UUID.randomUUID(),
            "post-restart-resume",
            new TenantActorContext(TENANT, "organization-1", new ActorId("contract-actor")),
            EngineId.KAFKA_STREAMS,
            7,
            Instant.parse("2026-08-17T12:00:00Z"),
            new ExecutionCommand.Resume(start.executionId()));
    var provider =
        new KafkaStreamsExecutionEngineProvider(
            ignored ->
                CompletableFuture.completedFuture(
                    new KafkaCommandGateway.PublishedCommand(
                        0, Instant.parse("2026-08-17T12:00:01Z"))),
            ignored -> CompletableFuture.completedFuture(null),
            Clock.fixed(Instant.parse("2026-08-17T12:00:01Z"), ZoneOffset.UTC));

    var acknowledgement = provider.submit(envelope).toCompletableFuture().join();

    assertEquals(7, acknowledgement.version());
  }

  @Override
  protected ExecutionEngineContractFixture createFixture() {
    return new Fixture();
  }

  @Override
  protected ExecutionCommandEnvelope newStartCommand(EngineId engineId) {
    var plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: contract
                  name: kafka-provider
                  version: '1.0.0'
                do:
                  - initialize:
                      set:
                        ready: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var id = new ExecutionId(TENANT, UUID.randomUUID());
    return new ExecutionCommandEnvelope(
        UUID.randomUUID(),
        "contract-correlation",
        new TenantActorContext(TENANT, "organization-1", new ActorId("contract-actor")),
        engineId,
        0,
        Instant.parse("2026-08-17T12:00:00Z"),
        new ExecutionCommand.Start(
            id,
            DefinitionRevision.from(UUID.randomUUID(), plan),
            plan,
            JsonNodeFactory.instance.objectNode()));
  }

  private static final class Fixture implements ExecutionEngineContractFixture {
    private final Map<UUID, KafkaCommandGateway.PublishedCommand> receipts = new HashMap<>();
    private final Map<ExecutionId, ExecutionProjection> projections = new HashMap<>();
    private ExecutionCommand.Start start;
    private final KafkaStreamsExecutionEngineProvider provider =
        new KafkaStreamsExecutionEngineProvider(
            envelope ->
                CompletableFuture.completedFuture(
                    receipts.computeIfAbsent(
                        envelope.commandId(),
                        ignored ->
                            new KafkaCommandGateway.PublishedCommand(
                                projections.containsKey(envelope.command().executionId())
                                    ? projections.get(envelope.command().executionId()).version()
                                        + 1
                                    : 0,
                                Instant.parse("2026-08-17T12:00:01Z")))),
            event -> {
              var prior = projections.get(event.executionId());
              projections.put(
                  event.executionId(),
                  new ExecutionProjection(
                      event.executionId(),
                      EngineId.KAFKA_STREAMS,
                      prior == null ? start.definition() : prior.definition(),
                      event.state(),
                      event.sequence(),
                      "contract-correlation",
                      event.sequence(),
                      prior == null ? event.occurredAt() : prior.createdAt(),
                      event.occurredAt(),
                      event.state().terminal() ? event.occurredAt() : null,
                      prior == null ? start.input() : prior.input(),
                      null,
                      null,
                      List.of(),
                      List.of()));
              return CompletableFuture.completedFuture(null);
            },
            Clock.fixed(Instant.parse("2026-08-17T12:00:01Z"), ZoneOffset.UTC));

    @Override
    public com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider provider() {
      return new com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider() {
        @Override
        public EngineId engineId() {
          return provider.engineId();
        }

        @Override
        public java.util.concurrent.CompletionStage<
                com.forwardmeasure.openworkflow.engine.api.CommandAcknowledgement>
            submit(ExecutionCommandEnvelope envelope) {
          if (envelope.command() instanceof ExecutionCommand.Start value) start = value;
          return provider.submit(envelope);
        }

        @Override
        public com.forwardmeasure.openworkflow.engine.api.EngineHealth health() {
          return provider.health();
        }
      };
    }

    @Override
    public ExecutionProjection awaitProjection(ExecutionId id, ExecutionLifecycleState state) {
      var value = projections.get(id);
      if (value == null || value.state() != state)
        throw new AssertionError("Kafka projection did not reach " + state);
      return value;
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
