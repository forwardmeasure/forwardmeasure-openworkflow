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
package com.forwardmeasure.openworkflow.eventing.cassandra;

import com.forwardmeasure.openworkflow.actor.WorkflowEntity;
import com.forwardmeasure.openworkflow.actor.WorkflowSharding;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.eventing.CloudEventOutboxHandler;
import com.forwardmeasure.openworkflow.eventing.CloudEventPublisher;
import com.forwardmeasure.openworkflow.execution.query.ExecutionQueryRepository;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.cluster.sharding.typed.javadsl.ShardedDaemonProcess;
import org.apache.pekko.persistence.cassandra.query.javadsl.CassandraReadJournal;
import org.apache.pekko.persistence.query.Offset;
import org.apache.pekko.projection.HandlerRecoveryStrategy;
import org.apache.pekko.projection.Projection;
import org.apache.pekko.projection.ProjectionBehavior;
import org.apache.pekko.projection.ProjectionId;
import org.apache.pekko.projection.cassandra.javadsl.CassandraProjection;
import org.apache.pekko.projection.eventsourced.EventEnvelope;
import org.apache.pekko.projection.eventsourced.javadsl.EventSourcedProvider;
import org.apache.pekko.projection.javadsl.SourceProvider;

/** Cassandra-backed durable offsets for the CloudEvent publication outbox. */
public final class CassandraCloudEventOutbox {
  private CassandraCloudEventOutbox() {}

  public static void start(
      ActorSystem<?> system,
      WorkflowSharding workflows,
      CloudEventPublisher publisher,
      Duration askTimeout,
      ExecutionQueryRepository executions) {
    Objects.requireNonNull(system, "system");
    ShardedDaemonProcess.get(system)
        .init(
            ProjectionBehavior.Command.class,
            "openworkflow-eventing-cassandra",
            WorkflowEntity.PROJECTION_TAG_COUNT,
            index ->
                ProjectionBehavior.create(
                    projection(
                        system,
                        workflows,
                        publisher,
                        askTimeout,
                        executions,
                        WorkflowEntity.projectionTags().get(index))),
            ProjectionBehavior.stopMessage());
  }

  private static Projection<EventEnvelope<EngineEvent>> projection(
      ActorSystem<?> system,
      WorkflowSharding workflows,
      CloudEventPublisher publisher,
      Duration askTimeout,
      ExecutionQueryRepository executions,
      String tag) {
    SourceProvider<Offset, EventEnvelope<EngineEvent>> source =
        EventSourcedProvider.eventsByTag(system, CassandraReadJournal.Identifier(), tag);
    return CassandraProjection.atLeastOnce(
            ProjectionId.of("openworkflow-eventing", tag),
            source,
            () ->
                new CloudEventOutboxHandler(
                    workflows, publisher, askTimeout, Clock.systemUTC(), executions))
        .withSaveOffset(1, Duration.ZERO)
        .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndFail(20, Duration.ofMillis(100)))
        .withRestartBackoff(Duration.ofMillis(200), Duration.ofSeconds(5), 0.2);
  }
}
