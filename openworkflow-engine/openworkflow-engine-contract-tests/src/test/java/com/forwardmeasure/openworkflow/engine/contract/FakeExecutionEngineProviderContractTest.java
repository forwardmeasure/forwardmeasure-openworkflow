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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.engine.api.ActorId;
import com.forwardmeasure.openworkflow.engine.api.CommandAcknowledgement;
import com.forwardmeasure.openworkflow.engine.api.DefinitionRevision;
import com.forwardmeasure.openworkflow.engine.api.EngineCommandException;
import com.forwardmeasure.openworkflow.engine.api.EngineHealth;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommandEnvelope;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import com.forwardmeasure.openworkflow.engine.api.ExecutionProjection;
import com.forwardmeasure.openworkflow.engine.api.TenantActorContext;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Proves that the reusable contract kit itself is executable without becoming a production engine.
 */
class FakeExecutionEngineProviderContractTest extends ExecutionEngineProviderContract {
  private static final TenantId TENANT =
      new TenantId(UUID.fromString("134b09a7-1c36-4b89-86e7-a28c88bc5cef"));

  @Override
  protected ExecutionEngineContractFixture createFixture() {
    return new FakeFixture();
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
                  name: provider
                  version: '1.0.0'
                do:
                  - initialize:
                      set:
                        ready: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var executionId = new ExecutionId(TENANT, UUID.randomUUID());
    return new ExecutionCommandEnvelope(
        UUID.randomUUID(),
        "contract-correlation",
        new TenantActorContext(TENANT, "organization-1", new ActorId("contract-actor")),
        engineId,
        0,
        Instant.now(),
        new ExecutionCommand.Start(
            executionId,
            DefinitionRevision.from(UUID.randomUUID(), plan),
            plan,
            JsonNodeFactory.instance.objectNode()));
  }

  private static final class FakeFixture implements ExecutionEngineContractFixture {
    private final FakeProvider provider = new FakeProvider();

    @Override
    public ExecutionEngineProvider provider() {
      return provider;
    }

    @Override
    public ExecutionProjection awaitProjection(
        ExecutionId executionId, ExecutionLifecycleState state) {
      var projection = provider.projections.get(executionId);
      if (projection == null || projection.state() != state) {
        throw new AssertionError("fake projection did not reach " + state);
      }
      return projection;
    }
  }

  private static final class FakeProvider implements ExecutionEngineProvider {
    private final Map<UUID, CommandAcknowledgement> acknowledgements = new HashMap<>();
    private final Map<ExecutionId, ExecutionProjection> projections = new HashMap<>();

    @Override
    public EngineId engineId() {
      return EngineId.PEKKO;
    }

    @Override
    public CompletionStage<CommandAcknowledgement> submit(ExecutionCommandEnvelope envelope) {
      if (!engineId().equals(envelope.selectedEngine())) {
        return CompletableFuture.failedFuture(
            new EngineCommandException(
                EngineCommandException.FailureKind.ENGINE_MISMATCH,
                "execution is pinned to another engine"));
      }
      var duplicate = acknowledgements.get(envelope.commandId());
      if (duplicate != null) {
        return CompletableFuture.completedFuture(duplicate);
      }
      var state = stateFor(envelope.command());
      var prior = projections.get(envelope.command().executionId());
      if (!(envelope.command() instanceof ExecutionCommand.Start) && prior == null) {
        return CompletableFuture.failedFuture(
            new EngineCommandException(
                EngineCommandException.FailureKind.NOT_FOUND, "execution does not exist"));
      }
      long version = prior == null ? 0 : prior.version() + 1;
      Instant now = Instant.now();
      var definition =
          envelope.command() instanceof ExecutionCommand.Start start
              ? start.definition()
              : prior.definition();
      var input =
          envelope.command() instanceof ExecutionCommand.Start start
              ? start.input()
              : prior.input();
      var projection =
          new ExecutionProjection(
              envelope.command().executionId(),
              engineId(),
              definition,
              state,
              version,
              envelope.correlationId(),
              version,
              prior == null ? now : prior.createdAt(),
              now,
              state.terminal() ? now : null,
              input,
              null,
              null,
              List.of(),
              List.of());
      projections.put(projection.executionId(), projection);
      var acknowledgement =
          new CommandAcknowledgement(
              envelope.commandId(), projection.executionId(), engineId(), state, version, now);
      acknowledgements.put(envelope.commandId(), acknowledgement);
      return CompletableFuture.completedFuture(acknowledgement);
    }

    private static ExecutionLifecycleState stateFor(ExecutionCommand command) {
      return switch (command) {
        case ExecutionCommand.Start ignored -> ExecutionLifecycleState.RUNNING;
        case ExecutionCommand.Pause ignored -> ExecutionLifecycleState.PAUSED;
        case ExecutionCommand.Resume ignored -> ExecutionLifecycleState.RUNNING;
        case ExecutionCommand.Cancel ignored -> ExecutionLifecycleState.CANCELLED;
      };
    }

    @Override
    public EngineHealth health() {
      return new EngineHealth(
          engineId(), EngineHealth.HealthState.UP, true, true, Instant.now(), Map.of());
    }
  }
}
