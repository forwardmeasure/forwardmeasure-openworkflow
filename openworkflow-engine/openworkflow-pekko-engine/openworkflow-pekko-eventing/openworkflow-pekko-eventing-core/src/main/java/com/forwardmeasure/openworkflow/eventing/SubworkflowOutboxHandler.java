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
package com.forwardmeasure.openworkflow.eventing;

import com.forwardmeasure.openworkflow.actor.SubworkflowCoordinatorCommand;
import com.forwardmeasure.openworkflow.actor.SubworkflowCoordinatorReply;
import com.forwardmeasure.openworkflow.actor.SubworkflowCoordinatorSharding;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.Done;
import org.apache.pekko.projection.eventsourced.EventEnvelope;
import org.apache.pekko.projection.javadsl.Handler;

/** Resolves a pinned child and advances its offset only after durable coordination. */
public final class SubworkflowOutboxHandler extends Handler<EventEnvelope<EngineEvent>> {
  private static final String PERSISTENCE_PREFIX = "workflow-execution|";
  private final SubworkflowPlanResolver definitions;
  private final SubworkflowLauncher launcher;

  public SubworkflowOutboxHandler(
      SubworkflowPlanResolver definitions,
      SubworkflowCoordinatorSharding coordinators,
      Duration askTimeout) {
    this(
        definitions,
        (request, plan) ->
            coordinators
                .entityRef(request.childExecutionId())
                .<SubworkflowCoordinatorReply>ask(
                    replyTo ->
                        new SubworkflowCoordinatorCommand.Launch(
                            request.commandId(), request.parentExecutionId(),
                            request.childExecutionId(), request.operationId(),
                            request.actor(), plan,
                            request.childInput(), request.awaitParent(),
                            request.requestedAt(), replyTo),
                    askTimeout));
    Objects.requireNonNull(coordinators, "coordinators");
    Objects.requireNonNull(askTimeout, "askTimeout");
    if (askTimeout.isZero() || askTimeout.isNegative()) {
      throw new IllegalArgumentException("askTimeout must be positive");
    }
  }

  SubworkflowOutboxHandler(SubworkflowPlanResolver definitions, SubworkflowLauncher launcher) {
    this.definitions = Objects.requireNonNull(definitions, "definitions");
    this.launcher = Objects.requireNonNull(launcher, "launcher");
  }

  @Override
  public CompletionStage<Done> process(EventEnvelope<EngineEvent> envelope) {
    Intent requested = intent(envelope.event());
    if (requested == null) {
      return CompletableFuture.completedFuture(Done.getInstance());
    }
    ExecutionId parent = executionId(envelope.persistenceId());
    if (!parent.tenantId().equals(requested.childExecutionId().tenantId())) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("Subworkflow journal and child tenant differ"));
    }
    final com.forwardmeasure.openworkflow.definition.WorkflowPlan childPlan;
    try {
      childPlan = definitions.resolve(parent.tenantId(), requested.actor(), requested.subflow());
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
    var launch =
        new LaunchRequest(
            requested.commandId(),
            parent,
            requested.childExecutionId(),
            requested.operationId(),
            requested.actor(),
            requested.childInput(),
            requested.awaitParent(),
            requested.requestedAt());
    return launcher
        .launch(launch, childPlan)
        .thenCompose(
            reply ->
                reply.accepted()
                    ? CompletableFuture.completedFuture(Done.getInstance())
                    : CompletableFuture.failedFuture(
                        new IllegalStateException(
                            "Subworkflow coordinator rejected " + requested.operationId())));
  }

  private static Intent intent(EngineEvent event) {
    if (event instanceof EngineEvent.SubworkflowRequested requested) {
      return new Intent(
          requested.commandId(),
          requested.childExecutionId(),
          requested.operationId(),
          requested.actor(),
          requested.subflow(),
          requested.childInput(),
          requested.await(),
          requested.occurredAt());
    }
    if (event instanceof EngineEvent.ForkBranchSubworkflowRequested requested) {
      return new Intent(
          requested.commandId(),
          requested.childExecutionId(),
          requested.operationId(),
          requested.actor(),
          requested.subflow(),
          requested.childInput(),
          requested.await(),
          requested.occurredAt());
    }
    return null;
  }

  private static ExecutionId executionId(String persistenceId) {
    if (!persistenceId.startsWith(PERSISTENCE_PREFIX)) {
      throw new IllegalArgumentException("Unexpected workflow persistence ID " + persistenceId);
    }
    return ExecutionId.fromEntityId(persistenceId.substring(PERSISTENCE_PREFIX.length()));
  }

  record LaunchRequest(
      java.util.UUID commandId,
      ExecutionId parentExecutionId,
      ExecutionId childExecutionId,
      String operationId,
      com.forwardmeasure.openworkflow.engine.api.ActorIdentity actor,
      com.fasterxml.jackson.databind.JsonNode childInput,
      boolean awaitParent,
      java.time.Instant requestedAt) {}

  private record Intent(
      java.util.UUID commandId,
      ExecutionId childExecutionId,
      String operationId,
      com.forwardmeasure.openworkflow.engine.api.ActorIdentity actor,
      com.forwardmeasure.openworkflow.definition.ResolvedSubflow subflow,
      com.fasterxml.jackson.databind.JsonNode childInput,
      boolean awaitParent,
      java.time.Instant requestedAt) {}

  @FunctionalInterface
  interface SubworkflowLauncher {
    CompletionStage<SubworkflowCoordinatorReply> launch(
        LaunchRequest request, com.forwardmeasure.openworkflow.definition.WorkflowPlan childPlan);
  }
}
