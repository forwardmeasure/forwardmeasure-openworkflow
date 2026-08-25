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
package com.forwardmeasure.openworkflow.execution.management;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.jpa.tenancy.ThreadBoundTenantScope;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.engine.api.ActorId;
import com.forwardmeasure.openworkflow.engine.api.CommandAcknowledgement;
import com.forwardmeasure.openworkflow.engine.api.DefinitionRevision;
import com.forwardmeasure.openworkflow.engine.api.EngineHealth;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommandEnvelope;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProviders;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import com.forwardmeasure.openworkflow.engine.api.TenantActorContext;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class ExecutionManagementServiceTest {
  private static final Instant NOW = Instant.parse("2026-08-17T12:00:00Z");
  private static final TenantId TENANT = new TenantId(uuid("10000000-0000-0000-0000-000000000001"));
  private static final TenantActorContext ACTOR =
      new TenantActorContext(TENANT, "organization-1", new ActorId("actor-1"));
  private static final UUID REVISION_ID = uuid("20000000-0000-0000-0000-000000000001");
  private static final UUID EXECUTION_UUID = uuid("30000000-0000-0000-0000-000000000001");

  @Test
  void pinsSelectedEngineAndDoesNotDispatchDuplicateStart() {
    var pekko = new Provider(EngineId.PEKKO);
    var kafka = new Provider(EngineId.KAFKA_STREAMS);
    var fixture = fixture(java.util.List.of(pekko, kafka));
    var request = request("start-once", uuid("40000000-0000-0000-0000-000000000001"));

    CanonicalExecution first = fixture.service.start(request).toCompletableFuture().join();
    CanonicalExecution duplicate = fixture.service.start(request).toCompletableFuture().join();

    assertEquals(EngineId.PEKKO, first.engineId());
    assertEquals(first.executionId(), duplicate.executionId());
    assertEquals(1, pekko.submissions);
    assertEquals(0, kafka.submissions);
  }

  @Test
  void rejectsStaleCommandBeforeDispatchAndKeepsEnginePinned() {
    var pekko = new Provider(EngineId.PEKKO);
    var fixture = fixture(java.util.List.of(pekko));
    CanonicalExecution execution =
        fixture
            .service
            .start(request("stale-test", uuid("40000000-0000-0000-0000-000000000002")))
            .toCompletableFuture()
            .join();

    var failure =
        assertThrows(
            ExecutionManagementException.class,
            () ->
                fixture.service.pause(
                    ACTOR,
                    execution.executionId(),
                    uuid("40000000-0000-0000-0000-000000000003"),
                    0,
                    "pause-correlation"));

    assertEquals(ExecutionManagementException.Kind.STALE_VERSION, failure.kind());
    assertEquals(1, pekko.submissions);
  }

  @Test
  void hidesAnExecutionFromAnotherTenant() {
    var fixture = fixture(java.util.List.of(new Provider(EngineId.PEKKO)));
    CanonicalExecution execution =
        fixture
            .service
            .start(request("tenant-test", uuid("40000000-0000-0000-0000-000000000004")))
            .toCompletableFuture()
            .join();
    var otherTenant = new TenantId(uuid("10000000-0000-0000-0000-000000000002"));
    var otherActor = new TenantActorContext(otherTenant, "organization-2", new ActorId("actor-2"));

    var failure =
        assertThrows(
            ExecutionManagementException.class,
            () ->
                fixture.service.pause(
                    otherActor,
                    execution.executionId(),
                    UUID.randomUUID(),
                    execution.version(),
                    "cross-tenant"));

    assertEquals(ExecutionManagementException.Kind.NOT_FOUND, failure.kind());
  }

  private static Fixture fixture(Collection<Provider> providers) {
    var plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: wp4
                  name: admission
                  version: '1.0.0'
                do:
                  - initialize:
                      set:
                        ready: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var publication = new PublishedWorkflow(DefinitionRevision.from(REVISION_ID, plan), plan);
    var repository = new MemoryRepository();
    var service =
        new ExecutionManagementService(
            (context, revisionId, correlationId) -> {
              assertSame(ACTOR, context);
              assertEquals(REVISION_ID, revisionId);
              return publication;
            },
            (context, action, resourceId, correlationId) -> {},
            repository,
            new ExecutionEngineProviders(providers),
            request -> EngineId.PEKKO,
            Clock.fixed(NOW, ZoneOffset.UTC),
            () -> EXECUTION_UUID,
            new ThreadBoundTenantScope(),
            new ExecutionTransactionExecutor() {
              @Override
              public <T> T execute(Supplier<T> work) {
                return work.get();
              }
            });
    return new Fixture(service);
  }

  private static ExecutionAdmissionRequest request(String idempotencyKey, UUID commandId) {
    return new ExecutionAdmissionRequest(
        ACTOR,
        REVISION_ID,
        commandId,
        idempotencyKey,
        idempotencyKey + "-correlation",
        JsonNodeFactory.instance.objectNode());
  }

  private static UUID uuid(String value) {
    return UUID.fromString(value);
  }

  private record Fixture(ExecutionManagementService service) {}

  private static final class Provider implements ExecutionEngineProvider {
    private final EngineId id;
    private int submissions;

    private Provider(EngineId id) {
      this.id = id;
    }

    @Override
    public EngineId engineId() {
      return id;
    }

    @Override
    public CompletionStage<CommandAcknowledgement> submit(ExecutionCommandEnvelope command) {
      submissions++;
      return CompletableFuture.completedFuture(
          new CommandAcknowledgement(
              command.commandId(),
              command.command().executionId(),
              id,
              ExecutionLifecycleState.RUNNING,
              1,
              NOW));
    }

    @Override
    public EngineHealth health() {
      return new EngineHealth(id, EngineHealth.HealthState.UP, true, true, NOW, Map.of());
    }
  }

  private static final class MemoryRepository implements ExecutionRepository {
    private final Map<String, CanonicalExecution> byIdempotency = new HashMap<>();
    private final Map<ExecutionId, CanonicalExecution> byId = new HashMap<>();
    private final Map<UUID, CommandReceipt> receipts = new HashMap<>();

    @Override
    public synchronized Optional<CanonicalExecution> findByIdempotencyKey(
        TenantId tenantId, String idempotencyKey) {
      return Optional.ofNullable(byIdempotency.get(tenantId + ":" + idempotencyKey));
    }

    @Override
    public synchronized Admission admit(CanonicalExecution candidate) {
      String key = candidate.executionId().tenantId() + ":" + candidate.idempotencyKey();
      CanonicalExecution existing = byIdempotency.get(key);
      if (existing != null) {
        return new Admission(existing, false);
      }
      byIdempotency.put(key, candidate);
      byId.put(candidate.executionId(), candidate);
      return new Admission(candidate, true);
    }

    @Override
    public synchronized Optional<CanonicalExecution> find(
        TenantId tenantId, ExecutionId executionId) {
      if (!tenantId.equals(executionId.tenantId())) {
        return Optional.empty();
      }
      return Optional.ofNullable(byId.get(executionId));
    }

    @Override
    public synchronized Optional<CommandReceipt> findReceipt(TenantId tenantId, UUID commandId) {
      CommandReceipt receipt = receipts.get(commandId);
      return receipt != null && receipt.executionId().tenantId().equals(tenantId)
          ? Optional.of(receipt)
          : Optional.empty();
    }

    @Override
    public synchronized CommandReceipt recordCommand(CommandReceipt candidate) {
      return receipts.computeIfAbsent(candidate.commandId(), ignored -> candidate);
    }

    @Override
    public synchronized CanonicalExecution acknowledge(
        TenantId tenantId, ExecutionId executionId, CommandAcknowledgement acknowledgement) {
      CanonicalExecution prior = find(tenantId, executionId).orElseThrow();
      var updated =
          new CanonicalExecution(
              prior.executionId(),
              prior.definition(),
              prior.engineId(),
              acknowledgement.state(),
              acknowledgement.version(),
              prior.idempotencyKey(),
              prior.correlationId(),
              prior.startedBy(),
              prior.input(),
              prior.createdAt(),
              acknowledgement.acknowledgedAt());
      byId.put(executionId, updated);
      byIdempotency.put(tenantId + ":" + prior.idempotencyKey(), updated);
      return updated;
    }
  }
}
