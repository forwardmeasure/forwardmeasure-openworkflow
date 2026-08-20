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

import com.forwardmeasure.openworkflow.actor.ScheduleEvent;
import com.forwardmeasure.openworkflow.actor.ScheduleId;
import com.forwardmeasure.openworkflow.definition.EventTypeSelector;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.Done;
import org.apache.pekko.projection.eventsourced.EventEnvelope;
import org.apache.pekko.projection.javadsl.Handler;

/** Journal-derived event-trigger index for tenant-qualified schedules. */
public final class ScheduleSubscriptionProjectionHandler
    extends Handler<EventEnvelope<ScheduleEvent>> {
  private static final String PERSISTENCE_PREFIX = "workflow-schedule|";
  private final CloudEventSubscriptionRepository repository;

  public ScheduleSubscriptionProjectionHandler(CloudEventSubscriptionRepository repository) {
    this.repository = Objects.requireNonNull(repository, "repository");
  }

  @Override
  public CompletionStage<Done> process(EventEnvelope<ScheduleEvent> envelope) {
    if (!(envelope.event() instanceof ScheduleEvent.Registered registered)
        || registered.plan().schedule().on() == null) {
      return java.util.concurrent.CompletableFuture.completedFuture(Done.getInstance());
    }
    var subscription =
        CloudEventSubscription.schedule(
            scheduleId(envelope.persistenceId()),
            EventTypeSelector.literalTypes(registered.plan().schedule().on()),
            envelope.sequenceNr(),
            true);
    return repository.store(subscription).thenApply(ignored -> Done.getInstance());
  }

  private static ScheduleId scheduleId(String persistenceId) {
    if (!persistenceId.startsWith(PERSISTENCE_PREFIX)) {
      throw new IllegalArgumentException("Unexpected schedule persistence ID " + persistenceId);
    }
    return ScheduleId.fromEntityId(persistenceId.substring(PERSISTENCE_PREFIX.length()));
  }
}
