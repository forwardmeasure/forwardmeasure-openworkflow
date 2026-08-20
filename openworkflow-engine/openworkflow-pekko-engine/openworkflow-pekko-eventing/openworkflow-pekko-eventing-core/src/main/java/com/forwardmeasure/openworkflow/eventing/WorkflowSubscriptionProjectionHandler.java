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

import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.Done;
import org.apache.pekko.projection.eventsourced.EventEnvelope;
import org.apache.pekko.projection.javadsl.Handler;

/** Journal-derived active listen index for workflow executions. */
public final class WorkflowSubscriptionProjectionHandler
    extends Handler<EventEnvelope<EngineEvent>> {
  private static final String PERSISTENCE_PREFIX = "workflow-execution|";
  private final CloudEventSubscriptionRepository repository;

  public WorkflowSubscriptionProjectionHandler(CloudEventSubscriptionRepository repository) {
    this.repository = Objects.requireNonNull(repository, "repository");
  }

  @Override
  public CompletionStage<Done> process(EventEnvelope<EngineEvent> envelope) {
    EngineEvent event = envelope.event();
    CloudEventSubscription subscription =
        switch (event) {
          case EngineEvent.ListenStarted started ->
              CloudEventSubscription.execution(
                  executionId(envelope.persistenceId()),
                  started.taskPath(),
                  started.eventTypes(),
                  envelope.sequenceNr(),
                  true);
          case EngineEvent.ForkBranchListenStarted started ->
              CloudEventSubscription.execution(
                  executionId(envelope.persistenceId()),
                  started.taskPath(),
                  Set.of(),
                  envelope.sequenceNr(),
                  true);
          case EngineEvent.ForkBranchListenAccepted accepted when !accepted.hasActiveListeners() ->
              inactive(envelope);
          case EngineEvent.ForkBranchListenIterationAdvanced advanced
              when !advanced.hasActiveListeners() ->
              inactive(envelope);
          case EngineEvent.ListenEventAccepted accepted when accepted.completed() ->
              inactive(envelope);
          case EngineEvent.ListenIterationStarted ignored -> inactive(envelope);
          case EngineEvent.Completed ignored -> inactive(envelope);
          case EngineEvent.Cancelled ignored -> inactive(envelope);
          case EngineEvent.Failed ignored -> inactive(envelope);
          default -> null;
        };
    if (subscription == null)
      return java.util.concurrent.CompletableFuture.completedFuture(Done.getInstance());
    return repository.store(subscription).thenApply(ignored -> Done.getInstance());
  }

  private static CloudEventSubscription inactive(EventEnvelope<EngineEvent> envelope) {
    return CloudEventSubscription.execution(
        executionId(envelope.persistenceId()), "", Set.of(), envelope.sequenceNr(), false);
  }

  private static ExecutionId executionId(String persistenceId) {
    if (!persistenceId.startsWith(PERSISTENCE_PREFIX)) {
      throw new IllegalArgumentException("Unexpected workflow persistence ID " + persistenceId);
    }
    return ExecutionId.fromEntityId(persistenceId.substring(PERSISTENCE_PREFIX.length()));
  }
}
