package com.forwardmeasure.openworkflow.operation;

import com.forwardmeasure.openworkflow.actor.ProtocolOperationCoordinatorCommand;
import com.forwardmeasure.openworkflow.actor.ProtocolOperationCoordinatorReply;
import com.forwardmeasure.openworkflow.actor.ProtocolOperationCoordinatorSharding;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.Done;
import org.apache.pekko.projection.eventsourced.EventEnvelope;
import org.apache.pekko.projection.javadsl.Handler;

/** At-least-once handoff from the journal to a remembered Pekko transport owner. */
public final class ProtocolOperationOutboxHandler extends Handler<EventEnvelope<EngineEvent>> {
  private final OperationStarter starter;

  public ProtocolOperationOutboxHandler(
      ProtocolOperationCoordinatorSharding coordinators, Duration askTimeout) {
    Objects.requireNonNull(coordinators, "coordinators");
    Objects.requireNonNull(askTimeout, "askTimeout");
    this.starter =
        (executionId, operationId) ->
            coordinators
                .entityRef(executionId, operationId)
                .<ProtocolOperationCoordinatorReply>ask(
                    replyTo ->
                        new ProtocolOperationCoordinatorCommand.Start(
                            executionId, operationId, replyTo),
                    askTimeout);
  }

  ProtocolOperationOutboxHandler(OperationStarter starter) {
    this.starter = Objects.requireNonNull(starter, "starter");
  }

  @Override
  public CompletionStage<Done> process(EventEnvelope<EngineEvent> envelope) {
    ProtocolOperationDescriptor operation;
    if (envelope.event() instanceof EngineEvent.ProtocolCallRequested requested) {
      operation = requested.operation();
    } else if (envelope.event() instanceof EngineEvent.ForkBranchProtocolCallRequested requested) {
      operation = requested.operation();
    } else {
      return CompletableFuture.completedFuture(Done.getInstance());
    }
    ExecutionId executionId = HttpOperationOutboxHandler.executionId(envelope.persistenceId());
    return starter
        .start(executionId, operation.operationId())
        .thenCompose(
            reply ->
                reply.accepted()
                    ? CompletableFuture.completedFuture(Done.getInstance())
                    : CompletableFuture.failedFuture(
                        new IllegalStateException(
                            "Protocol coordinator rejected " + operation.operationId())));
  }

  @FunctionalInterface
  interface OperationStarter {
    CompletionStage<ProtocolOperationCoordinatorReply> start(
        ExecutionId executionId, String operationId);
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
