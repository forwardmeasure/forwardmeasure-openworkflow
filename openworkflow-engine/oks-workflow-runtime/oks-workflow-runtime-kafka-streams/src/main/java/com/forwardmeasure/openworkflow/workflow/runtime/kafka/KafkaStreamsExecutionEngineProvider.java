/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.engine.api.CommandAcknowledgement;
import com.forwardmeasure.openworkflow.engine.api.EngineCommandException;
import com.forwardmeasure.openworkflow.engine.api.EngineHealth;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommandEnvelope;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEventSink;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/** Common engine-SPI adapter for the transactional OKS Kafka Streams runtime. */
public final class KafkaStreamsExecutionEngineProvider implements ExecutionEngineProvider {
  private final KafkaCommandGateway commands;
  private final ExecutionEventSink events;
  private final Clock clock;
  private final boolean deferProjectionUntilAdmission;

  public KafkaStreamsExecutionEngineProvider(
      KafkaCommandGateway commands, ExecutionEventSink events, Clock clock) {
    this(commands, events, clock, false);
  }

  public KafkaStreamsExecutionEngineProvider(
      KafkaCommandGateway commands,
      ExecutionEventSink events,
      Clock clock,
      boolean deferProjectionUntilAdmission) {
    this.commands = Objects.requireNonNull(commands, "commands");
    this.events = Objects.requireNonNull(events, "events");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.deferProjectionUntilAdmission = deferProjectionUntilAdmission;
  }

  @Override
  public EngineId engineId() {
    return EngineId.KAFKA_STREAMS;
  }

  @Override
  public CompletionStage<CommandAcknowledgement> submit(ExecutionCommandEnvelope envelope) {
    Objects.requireNonNull(envelope, "envelope");
    if (!engineId().equals(envelope.selectedEngine())) {
      return CompletableFuture.failedFuture(
          new EngineCommandException(
              EngineCommandException.FailureKind.ENGINE_MISMATCH,
              "execution is not pinned to Kafka Streams"));
    }
    return commands
        .publish(envelope)
        .thenCompose(
            published -> {
              ExecutionLifecycleState state = state(envelope.command());
              var acknowledgement =
                  new CommandAcknowledgement(
                      envelope.commandId(),
                      envelope.command().executionId(),
                      engineId(),
                      state,
                      envelope.expectedVersion(),
                      published.acknowledgedAt());
              if (deferProjectionUntilAdmission) {
                projectAfterAdmission(envelope, acknowledgement, 0);
                return CompletableFuture.completedFuture(acknowledgement);
              }
              return project(envelope, event(envelope, acknowledgement))
                  .thenApply(ignored -> acknowledgement);
            });
  }

  private void projectAfterAdmission(
      ExecutionCommandEnvelope envelope, CommandAcknowledgement acknowledgement, int attempts) {
    CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS)
        .execute(
            () ->
                project(envelope, event(envelope, acknowledgement))
                    .whenComplete(
                        (ignored, failure) -> {
                          if (failure != null && attempts < 49) {
                            projectAfterAdmission(envelope, acknowledgement, attempts + 1);
                          }
                        }));
  }

  private CompletionStage<Void> project(ExecutionCommandEnvelope envelope, ExecutionEvent event) {
    return envelope.command() instanceof ExecutionCommand.Start
        ? events.project(event)
        : events.projectNext(event);
  }

  private ExecutionEvent event(
      ExecutionCommandEnvelope envelope, CommandAcknowledgement acknowledgement) {
    ExecutionEvent.EventType type =
        switch (envelope.command()) {
          case ExecutionCommand.Start ignored -> ExecutionEvent.EventType.STARTED;
          case ExecutionCommand.Pause ignored -> ExecutionEvent.EventType.PAUSED;
          case ExecutionCommand.Resume ignored -> ExecutionEvent.EventType.RESUMED;
          case ExecutionCommand.Cancel ignored -> ExecutionEvent.EventType.CANCELLED;
        };
    var data = JsonNodeFactory.instance.objectNode();
    if (envelope.command() instanceof ExecutionCommand.Start start)
      data.set("input", start.input());
    if (envelope.command() instanceof ExecutionCommand.Cancel cancel)
      data.put("reason", cancel.reason());
    long sequence = envelope.expectedVersion();
    String identity =
        engineId().value()
            + ':'
            + acknowledgement.executionId().entityId()
            + ':'
            + sequence
            + ':'
            + type;
    return new ExecutionEvent(
        UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8)),
        envelope.commandId(),
        acknowledgement.executionId(),
        engineId(),
        sequence,
        type,
        acknowledgement.state(),
        acknowledgement.acknowledgedAt(),
        data);
  }

  private static ExecutionLifecycleState state(ExecutionCommand command) {
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
        engineId(), EngineHealth.HealthState.UP, true, true, Instant.now(clock), Map.of());
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
