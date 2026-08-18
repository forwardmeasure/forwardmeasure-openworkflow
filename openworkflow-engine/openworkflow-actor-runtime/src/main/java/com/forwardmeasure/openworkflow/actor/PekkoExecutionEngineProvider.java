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
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
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
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/** Consolidated engine-SPI adapter for the tenant-qualified Pekko FSM. */
public final class PekkoExecutionEngineProvider implements ExecutionEngineProvider {
  private final PekkoCommandGateway commands;
  private final Duration timeout;
  private final Clock clock;
  private final ExecutionEventSink events;
  private final boolean autoAdvance;

  public PekkoExecutionEngineProvider(WorkflowSharding workflows, Duration timeout, Clock clock) {
    this(
        workflows,
        timeout,
        clock,
        ignored -> java.util.concurrent.CompletableFuture.completedFuture(null));
  }

  public PekkoExecutionEngineProvider(
      WorkflowSharding workflows, Duration timeout, Clock clock, ExecutionEventSink events) {
    this(
        (executionId, commandFactory, askTimeout) ->
            Objects.requireNonNull(workflows, "workflows")
                .entityRef(executionId)
                .ask(commandFactory::apply, askTimeout),
        timeout,
        clock,
        events,
        true);
  }

  public PekkoExecutionEngineProvider(
      PekkoCommandGateway commands, Duration timeout, Clock clock, ExecutionEventSink events) {
    this(commands, timeout, clock, events, false);
  }

  private PekkoExecutionEngineProvider(
      PekkoCommandGateway commands,
      Duration timeout,
      Clock clock,
      ExecutionEventSink events,
      boolean autoAdvance) {
    this.commands = Objects.requireNonNull(commands, "commands");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.events = Objects.requireNonNull(events, "events");
    this.autoAdvance = autoAdvance;
  }

  @Override
  public EngineId engineId() {
    return EngineId.PEKKO;
  }

  @Override
  public CompletionStage<CommandAcknowledgement> submit(ExecutionCommandEnvelope envelope) {
    Objects.requireNonNull(envelope, "envelope");
    if (!engineId().equals(envelope.selectedEngine())) {
      return java.util.concurrent.CompletableFuture.failedFuture(
          new EngineCommandException(
              EngineCommandException.FailureKind.ENGINE_MISMATCH,
              "execution is not pinned to Pekko"));
    }
    ActorIdentity actor =
        new ActorIdentity(
            envelope.context().tenantId(),
            actorDid(envelope.context().actorId().value()),
            envelope.context().organizationId(),
            envelope.context().organizationRoles(),
            envelope.correlationId());
    ExecutionCommand command = envelope.command();
    try {
      var reply =
          commands
              .ask(
                  command.executionId(),
                  replyTo -> toActorCommand(envelope, actor, replyTo),
                  timeout)
              .toCompletableFuture()
              .join();
      CommandAcknowledgement acknowledgement = acknowledgement(envelope, reply);
      if (!autoAdvance) {
        return project(envelope, toProjectionEvent(envelope, acknowledgement))
            .thenApply(ignored -> acknowledgement);
      }
      projectAfterAdmission(envelope, actor, acknowledgement, 0);
      return CompletableFuture.completedFuture(acknowledgement);
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private void projectAfterAdmission(
      ExecutionCommandEnvelope envelope,
      ActorIdentity actor,
      CommandAcknowledgement acknowledgement,
      int attempts) {
    CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS)
        .execute(
            () ->
                project(envelope, toProjectionEvent(envelope, acknowledgement))
                    .whenComplete(
                        (ignored, failure) -> {
                          if (failure != null && attempts < 49) {
                            projectAfterAdmission(envelope, actor, acknowledgement, attempts + 1);
                            return;
                          }
                          if (failure == null
                              && autoAdvance
                              && (envelope.command() instanceof ExecutionCommand.Start
                                  || envelope.command() instanceof ExecutionCommand.Resume)) {
                            advance(envelope, actor, 0);
                          }
                        }));
  }

  private void advance(ExecutionCommandEnvelope envelope, ActorIdentity actor, int advances) {
    if (advances >= 10_000) {
      return;
    }
    UUID commandId = UUID.randomUUID();
    commands
        .ask(
            envelope.command().executionId(),
            replyTo ->
                new WorkflowCommand.RunNext(
                    commandId,
                    envelope.command().executionId(),
                    actor,
                    Instant.now(clock),
                    replyTo),
            timeout)
        .thenAccept(
            reply -> {
              if (!(reply instanceof WorkflowReply.Accepted accepted)) {
                return;
              }
              switch (accepted.status()) {
                case RUNNING -> advance(envelope, actor, advances + 1);
                case COMPLETED, FAILED -> projectTerminal(envelope, accepted);
                case WAITING -> observeUntilTerminal(envelope, actor, advances);
                default -> {}
              }
            });
  }

  private void observeUntilTerminal(
      ExecutionCommandEnvelope envelope, ActorIdentity actor, int observations) {
    if (observations >= 10_000) {
      return;
    }
    CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS)
        .execute(
            () ->
                commands
                    .ask(
                        envelope.command().executionId(),
                        replyTo ->
                            new WorkflowCommand.GetState(envelope.command().executionId(), replyTo),
                        timeout)
                    .whenComplete(
                        (reply, failure) -> {
                          if (failure != null) {
                            observeUntilTerminal(envelope, actor, observations + 1);
                            return;
                          }
                          if (!(reply instanceof WorkflowReply.StateSnapshot snapshot)) {
                            return;
                          }
                          switch (snapshot.status()) {
                            case COMPLETED, FAILED ->
                                projectTerminal(
                                    envelope,
                                    new WorkflowReply.Accepted(
                                        UUID.nameUUIDFromBytes(
                                            (snapshot.executionId().entityId()
                                                    + "|terminal-projection|"
                                                    + snapshot.revision())
                                                .getBytes(StandardCharsets.UTF_8)),
                                        snapshot.executionId(),
                                        snapshot.revision(),
                                        snapshot.status()));
                            case RUNNING -> advance(envelope, actor, observations + 1);
                            case WAITING -> observeUntilTerminal(envelope, actor, observations + 1);
                            default -> {}
                          }
                        }));
  }

  private void projectTerminal(
      ExecutionCommandEnvelope envelope, WorkflowReply.Accepted terminalReply) {
    commands
        .ask(
            terminalReply.executionId(),
            replyTo -> new WorkflowCommand.GetState(terminalReply.executionId(), replyTo),
            timeout)
        .thenCompose(
            reply -> {
              if (!(reply instanceof WorkflowReply.StateSnapshot snapshot)) {
                return java.util.concurrent.CompletableFuture.completedFuture(null);
              }
              ExecutionLifecycleState state =
                  ExecutionLifecycleState.valueOf(snapshot.status().name());
              ExecutionEvent.EventType type =
                  state == ExecutionLifecycleState.COMPLETED
                      ? ExecutionEvent.EventType.COMPLETED
                      : ExecutionEvent.EventType.FAILED;
              long sequence = envelope.expectedVersion() + 1;
              String identity =
                  engineId().value()
                      + ':'
                      + snapshot.executionId().entityId()
                      + ':'
                      + sequence
                      + ':'
                      + type;
              return events.projectNext(
                  new ExecutionEvent(
                      UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8)),
                      terminalReply.commandId(),
                      snapshot.executionId(),
                      engineId(),
                      sequence,
                      type,
                      state,
                      Instant.now(clock),
                      snapshot.data()));
            });
  }

  private CompletionStage<Void> project(ExecutionCommandEnvelope envelope, ExecutionEvent event) {
    return envelope.command() instanceof ExecutionCommand.Start
        ? events.project(event)
        : events.projectNext(event);
  }

  private WorkflowCommand toActorCommand(
      ExecutionCommandEnvelope envelope,
      ActorIdentity actor,
      org.apache.pekko.actor.typed.ActorRef<WorkflowReply> replyTo) {
    return switch (envelope.command()) {
      case ExecutionCommand.Start start ->
          new WorkflowCommand.Start(
              envelope.commandId(),
              start.executionId(),
              actor,
              start.plan(),
              start.input(),
              envelope.issuedAt(),
              replyTo);
      case ExecutionCommand.Pause pause ->
          new WorkflowCommand.Pause(
              envelope.commandId(), pause.executionId(), actor, envelope.issuedAt(), replyTo);
      case ExecutionCommand.Resume resume ->
          new WorkflowCommand.Resume(
              envelope.commandId(), resume.executionId(), actor, envelope.issuedAt(), replyTo);
      case ExecutionCommand.Cancel cancel ->
          new WorkflowCommand.Cancel(
              envelope.commandId(), cancel.executionId(), actor, envelope.issuedAt(), replyTo);
    };
  }

  private CommandAcknowledgement acknowledgement(
      ExecutionCommandEnvelope envelope, WorkflowReply reply) {
    if (reply instanceof WorkflowReply.Rejected rejected) {
      throw new EngineCommandException(
          EngineCommandException.FailureKind.REJECTED, rejected.code() + ": " + rejected.message());
    }
    WorkflowReply.Accepted accepted = (WorkflowReply.Accepted) reply;
    return new CommandAcknowledgement(
        accepted.commandId(),
        accepted.executionId(),
        engineId(),
        ExecutionLifecycleState.valueOf(accepted.status().name()),
        accepted.revision(),
        Instant.now(clock));
  }

  private ExecutionEvent toProjectionEvent(
      ExecutionCommandEnvelope envelope, CommandAcknowledgement acknowledgement) {
    ExecutionEvent.EventType type =
        switch (envelope.command()) {
          case ExecutionCommand.Start ignored -> ExecutionEvent.EventType.STARTED;
          case ExecutionCommand.Pause ignored -> ExecutionEvent.EventType.PAUSED;
          case ExecutionCommand.Resume ignored -> ExecutionEvent.EventType.RESUMED;
          case ExecutionCommand.Cancel ignored -> ExecutionEvent.EventType.CANCELLED;
        };
    var data = JsonNodeFactory.instance.objectNode();
    if (envelope.command() instanceof ExecutionCommand.Start start) {
      data.set("input", start.input());
    } else if (envelope.command() instanceof ExecutionCommand.Cancel cancel) {
      data.put("reason", cancel.reason());
    }
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
        acknowledgement.commandId(),
        acknowledgement.executionId(),
        engineId(),
        sequence,
        type,
        acknowledgement.state(),
        acknowledgement.acknowledgedAt(),
        data);
  }

  @Override
  public EngineHealth health() {
    return new EngineHealth(
        engineId(), EngineHealth.HealthState.UP, true, true, Instant.now(clock), Map.of());
  }

  private static String actorDid(String actorId) {
    return actorId.startsWith("did:") ? actorId : "did:forwardmeasure:actor:" + actorId;
  }
}
