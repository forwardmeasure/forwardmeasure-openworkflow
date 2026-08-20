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

import com.forwardmeasure.openworkflow.actor.WorkflowCommand;
import com.forwardmeasure.openworkflow.actor.WorkflowReply;
import com.forwardmeasure.openworkflow.actor.WorkflowSharding;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.execution.query.ExecutionQueryRepository;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.Done;
import org.apache.pekko.projection.eventsourced.EventEnvelope;
import org.apache.pekko.projection.javadsl.Handler;

/** At-least-once outbox delivery that advances its offset only after actor acknowledgement. */
public final class CloudEventOutboxHandler extends Handler<EventEnvelope<EngineEvent>> {
  public static final String PERSISTENCE_ID_PREFIX = "workflow-execution|";
  private static final int LIFECYCLE_COORDINATE_CACHE_MAXIMUM = 4096;

  private final WorkflowSharding workflows;
  private final CloudEventPublisher publisher;
  private final Duration askTimeout;
  private final Clock clock;
  private final EffectAcknowledger acknowledger;
  private final ExecutionQueryRepository executions;
  private final Map<ExecutionId, com.forwardmeasure.openworkflow.definition.WorkflowCoordinates>
      lifecycleCoordinates =
          Collections.synchronizedMap(
              new LinkedHashMap<>(128, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(
                    Map.Entry<
                            ExecutionId,
                            com.forwardmeasure.openworkflow.definition.WorkflowCoordinates>
                        eldest) {
                  return size() > LIFECYCLE_COORDINATE_CACHE_MAXIMUM;
                }
              });
  private final LifecycleCloudEventMapper lifecycle = new LifecycleCloudEventMapper();

  public CloudEventOutboxHandler(
      WorkflowSharding workflows,
      CloudEventPublisher publisher,
      Duration askTimeout,
      Clock clock,
      ExecutionQueryRepository executions) {
    this.workflows = Objects.requireNonNull(workflows, "workflows");
    this.publisher = Objects.requireNonNull(publisher, "publisher");
    this.askTimeout = Objects.requireNonNull(askTimeout, "askTimeout");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.executions = Objects.requireNonNull(executions, "executions");
    if (askTimeout.isZero() || askTimeout.isNegative()) {
      throw new IllegalArgumentException("askTimeout must be positive");
    }
    this.acknowledger =
        (executionId, operationId) ->
            this.workflows
                .entityRef(executionId)
                .<WorkflowReply>ask(
                    replyTo ->
                        new WorkflowCommand.EffectAcknowledged(
                            executionId, operationId, this.clock.instant(), replyTo),
                    this.askTimeout);
  }

  CloudEventOutboxHandler(CloudEventPublisher publisher, EffectAcknowledger acknowledger) {
    this(publisher, acknowledger, null);
  }

  CloudEventOutboxHandler(
      CloudEventPublisher publisher,
      EffectAcknowledger acknowledger,
      ExecutionQueryRepository executions) {
    this.workflows = null;
    this.publisher = Objects.requireNonNull(publisher, "publisher");
    this.askTimeout = Duration.ofSeconds(1);
    this.clock = Clock.systemUTC();
    this.acknowledger = Objects.requireNonNull(acknowledger, "acknowledger");
    this.executions = executions;
  }

  @Override
  public CompletionStage<Done> process(EventEnvelope<EngineEvent> envelope) {
    Objects.requireNonNull(envelope, "envelope");
    String operationId;
    com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent event;
    if (envelope.event() instanceof EngineEvent.EmitRequested requested) {
      operationId = requested.operationId();
      event = requested.event();
    } else if (envelope.event() instanceof EngineEvent.ForkBranchEmitRequested requested) {
      operationId = requested.operationId();
      event = requested.event();
    } else return publishLifecycle(envelope);
    ExecutionId executionId = executionId(envelope.persistenceId());
    return publishLifecycle(envelope)
        .thenCompose(ignored -> publisher.publish(operationId, event))
        .thenCompose(ignored -> acknowledger.acknowledge(executionId, operationId))
        .thenCompose(
            reply ->
                acknowledged(reply)
                    ? CompletableFuture.completedFuture(Done.getInstance())
                    : CompletableFuture.failedFuture(
                        new IllegalStateException(
                            "Workflow did not acknowledge emitted operation " + operationId)));
  }

  private CompletionStage<Done> publishLifecycle(EventEnvelope<EngineEvent> envelope) {
    if (executions == null) {
      return CompletableFuture.completedFuture(Done.getInstance());
    }
    ExecutionId executionId = executionId(envelope.persistenceId());
    com.forwardmeasure.openworkflow.definition.WorkflowCoordinates coordinates;
    if (envelope.event() instanceof EngineEvent.Started started) {
      coordinates = started.plan().coordinates();
      lifecycleCoordinates.put(executionId, coordinates);
    } else {
      coordinates =
          lifecycleCoordinates.computeIfAbsent(
              executionId,
              ignored -> {
                var view =
                    executions
                        .find(executionId.tenantId(), executionId)
                        .orElseThrow(
                            () ->
                                new IllegalStateException(
                                    "Execution projection has not materialized lifecycle context"
                                        + " for "
                                        + executionId.value()));
                return view.definition().coordinates();
              });
    }
    CompletionStage<Done> published = CompletableFuture.completedFuture(Done.getInstance());
    for (var event : lifecycle.map(executionId, coordinates, envelope.event())) {
      published =
          published.thenCompose(
              ignored ->
                  publisher.publish(event.id(), event).thenApply(done -> Done.getInstance()));
    }
    if (envelope.event() instanceof EngineEvent.Cancelled
        || envelope.event() instanceof EngineEvent.Completed
        || envelope.event() instanceof EngineEvent.Failed) {
      var terminalCoordinates = coordinates;
      return published.thenApply(
          done -> {
            lifecycleCoordinates.remove(executionId, terminalCoordinates);
            return done;
          });
    }
    return published;
  }

  private static boolean acknowledged(WorkflowReply reply) {
    return reply instanceof WorkflowReply.Accepted;
  }

  static ExecutionId executionId(String persistenceId) {
    Objects.requireNonNull(persistenceId, "persistenceId");
    if (!persistenceId.startsWith(PERSISTENCE_ID_PREFIX)) {
      throw new IllegalArgumentException("Unexpected workflow persistence ID");
    }
    return ExecutionId.fromEntityId(persistenceId.substring(PERSISTENCE_ID_PREFIX.length()));
  }

  @FunctionalInterface
  interface EffectAcknowledger {
    CompletionStage<WorkflowReply> acknowledge(ExecutionId executionId, String operationId);
  }
}
