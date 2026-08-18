package com.forwardmeasure.openworkflow.operation;

import com.forwardmeasure.openworkflow.actor.WorkflowCommand;
import com.forwardmeasure.openworkflow.actor.WorkflowReply;
import com.forwardmeasure.openworkflow.actor.WorkflowSharding;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.HttpOperationDescriptor;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.Done;
import org.apache.pekko.projection.eventsourced.EventEnvelope;
import org.apache.pekko.projection.javadsl.Handler;

/** At-least-once HTTP outbox; its offset advances only after actor persistence. */
public final class HttpOperationOutboxHandler extends Handler<EventEnvelope<EngineEvent>> {
  public static final String PERSISTENCE_ID_PREFIX = "workflow-execution|";
  private final HttpOperationExecutor executor;
  private final ResultObserver observer;

  public HttpOperationOutboxHandler(
      WorkflowSharding workflows,
      HttpOperationExecutor executor,
      Duration askTimeout,
      Clock clock) {
    Objects.requireNonNull(workflows, "workflows");
    this.executor = Objects.requireNonNull(executor, "executor");
    Objects.requireNonNull(askTimeout, "askTimeout");
    Objects.requireNonNull(clock, "clock");
    this.observer =
        (executionId, operationId, result) ->
            workflows
                .entityRef(executionId)
                .<WorkflowReply>ask(
                    replyTo ->
                        new WorkflowCommand.HttpCallCompleted(
                            executionId,
                            operationId,
                            result.output(),
                            result.error(),
                            clock.instant(),
                            replyTo),
                    askTimeout);
  }

  HttpOperationOutboxHandler(HttpOperationExecutor executor, ResultObserver observer) {
    this.executor = Objects.requireNonNull(executor, "executor");
    this.observer = Objects.requireNonNull(observer, "observer");
  }

  @Override
  public CompletionStage<Done> process(EventEnvelope<EngineEvent> envelope) {
    HttpOperationDescriptor operation;
    if (envelope.event() instanceof EngineEvent.HttpCallRequested requested) {
      operation = requested.operation();
    } else if (envelope.event() instanceof EngineEvent.ForkBranchHttpCallRequested requested) {
      operation = requested.operation();
    } else {
      return CompletableFuture.completedFuture(Done.getInstance());
    }
    ExecutionId executionId = executionId(envelope.persistenceId());
    return executor
        .execute(executionId, operation)
        .thenCompose(result -> observer.observe(executionId, operation.operationId(), result))
        .thenCompose(
            reply ->
                reply instanceof WorkflowReply.Accepted
                    ? CompletableFuture.completedFuture(Done.getInstance())
                    : CompletableFuture.failedFuture(
                        new IllegalStateException(
                            "Workflow did not persist HTTP result " + operation.operationId())));
  }

  static ExecutionId executionId(String persistenceId) {
    if (!persistenceId.startsWith(PERSISTENCE_ID_PREFIX)) {
      throw new IllegalArgumentException("Unexpected workflow persistence ID");
    }
    return ExecutionId.fromEntityId(persistenceId.substring(PERSISTENCE_ID_PREFIX.length()));
  }

  @FunctionalInterface
  interface ResultObserver {
    CompletionStage<WorkflowReply> observe(
        ExecutionId executionId, String operationId, HttpOperationResult result);
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
