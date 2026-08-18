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
package com.forwardmeasure.openworkflow.actor;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.engine.api.ActorId;
import com.forwardmeasure.openworkflow.engine.api.DefinitionRevision;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommandEnvelope;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import com.forwardmeasure.openworkflow.engine.api.ExecutionProjection;
import com.forwardmeasure.openworkflow.engine.api.ExecutionStatus;
import com.forwardmeasure.openworkflow.engine.api.TenantActorContext;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.engine.contract.ExecutionEngineContractFixture;
import com.forwardmeasure.openworkflow.engine.contract.ExecutionEngineProviderContract;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;

class PekkoExecutionEngineProviderContractTest extends ExecutionEngineProviderContract {
  private static final TenantId TENANT =
      new TenantId(UUID.fromString("134b09a7-1c36-4b89-86e7-a28c88bc5cef"));

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
                  name: pekko-provider
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
        Instant.parse("2026-08-17T12:00:00Z"),
        new ExecutionCommand.Start(
            executionId,
            DefinitionRevision.from(UUID.randomUUID(), plan),
            plan,
            JsonNodeFactory.instance.objectNode()));
  }

  private static final class Fixture implements ExecutionEngineContractFixture {
    private final ActorTestKit testKit = ActorTestKit.create();
    private final Map<ExecutionId, ExecutionProjection> projections = new HashMap<>();
    private final Map<UUID, WorkflowReply.Accepted> replies = new HashMap<>();
    private ExecutionCommand lastStart;
    private final ExecutionEngineProvider provider =
        new PekkoExecutionEngineProvider(
            (executionId, factory, timeout) -> {
              WorkflowCommand command =
                  factory.apply(testKit.<WorkflowReply>createTestProbe().ref());
              UUID commandId = commandId(command);
              WorkflowReply.Accepted accepted =
                  replies.computeIfAbsent(
                      commandId,
                      ignored ->
                          new WorkflowReply.Accepted(
                              commandId, executionId, revision(executionId), status(command)));
              return CompletableFuture.completedFuture(accepted);
            },
            Duration.ofSeconds(1),
            Clock.fixed(Instant.parse("2026-08-17T12:00:01Z"), ZoneOffset.UTC),
            event -> {
              var prior = projections.get(event.executionId());
              var start = prior == null ? (ExecutionCommand.Start) lastStart : null;
              var projection =
                  new ExecutionProjection(
                      event.executionId(),
                      EngineId.PEKKO,
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
                      List.of());
              projections.put(event.executionId(), projection);
              return CompletableFuture.completedFuture(null);
            });

    private long revision(ExecutionId executionId) {
      return projections.containsKey(executionId) ? projections.get(executionId).version() + 2 : 1;
    }

    @Override
    public ExecutionEngineProvider provider() {
      return new ExecutionEngineProvider() {
        @Override
        public EngineId engineId() {
          return provider.engineId();
        }

        @Override
        public java.util.concurrent.CompletionStage<
                com.forwardmeasure.openworkflow.engine.api.CommandAcknowledgement>
            submit(ExecutionCommandEnvelope command) {
          if (command.command() instanceof ExecutionCommand.Start start) {
            lastStart = start;
          }
          return provider.submit(command);
        }

        @Override
        public com.forwardmeasure.openworkflow.engine.api.EngineHealth health() {
          return provider.health();
        }
      };
    }

    @Override
    public ExecutionProjection awaitProjection(
        ExecutionId executionId, ExecutionLifecycleState state) {
      ExecutionProjection projection = projections.get(executionId);
      if (projection == null || projection.state() != state) {
        throw new AssertionError("Pekko projection did not reach " + state);
      }
      return projection;
    }

    @Override
    public void close() {
      testKit.shutdownTestKit();
    }

    private static ExecutionStatus status(WorkflowCommand command) {
      return switch (command) {
        case WorkflowCommand.Start ignored -> ExecutionStatus.RUNNING;
        case WorkflowCommand.Pause ignored -> ExecutionStatus.PAUSED;
        case WorkflowCommand.Resume ignored -> ExecutionStatus.RUNNING;
        case WorkflowCommand.Cancel ignored -> ExecutionStatus.CANCELLED;
        default -> throw new AssertionError("unexpected provider command: " + command);
      };
    }

    private static UUID commandId(WorkflowCommand command) {
      return switch (command) {
        case WorkflowCommand.Start value -> value.commandId();
        case WorkflowCommand.Pause value -> value.commandId();
        case WorkflowCommand.Resume value -> value.commandId();
        case WorkflowCommand.Cancel value -> value.commandId();
        default -> throw new AssertionError("unexpected provider command: " + command);
      };
    }
  }
}
