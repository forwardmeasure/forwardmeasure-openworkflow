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
package com.forwardmeasure.openworkflow.eventing.postgresql;

import com.forwardmeasure.openworkflow.actor.ScheduleEvent;
import com.forwardmeasure.openworkflow.actor.WorkflowEntity;
import com.forwardmeasure.openworkflow.actor.WorkflowScheduleEntity;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.eventing.CloudEventSubscriptionRepository;
import com.forwardmeasure.openworkflow.eventing.ScheduleSubscriptionProjectionHandler;
import com.forwardmeasure.openworkflow.eventing.WorkflowSubscriptionProjectionHandler;
import java.time.Duration;
import javax.sql.DataSource;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.cluster.sharding.typed.javadsl.ShardedDaemonProcess;
import org.apache.pekko.persistence.jdbc.query.javadsl.JdbcReadJournal;
import org.apache.pekko.persistence.query.Offset;
import org.apache.pekko.projection.Projection;
import org.apache.pekko.projection.ProjectionBehavior;
import org.apache.pekko.projection.ProjectionId;
import org.apache.pekko.projection.eventsourced.EventEnvelope;
import org.apache.pekko.projection.eventsourced.javadsl.EventSourcedProvider;
import org.apache.pekko.projection.javadsl.SourceProvider;
import org.apache.pekko.projection.jdbc.javadsl.JdbcProjection;

/** PostgreSQL-offset projections that maintain workflow and schedule event targets. */
public final class PostgresqlCloudEventSubscriptionProjection {
  private PostgresqlCloudEventSubscriptionProjection() {}

  public static void start(
      ActorSystem<?> system, DataSource dataSource, CloudEventSubscriptionRepository repository) {
    ShardedDaemonProcess.get(system)
        .init(
            ProjectionBehavior.Command.class,
            "openworkflow-event-subscriptions-workflow-postgresql",
            WorkflowEntity.PROJECTION_TAG_COUNT,
            index ->
                ProjectionBehavior.create(
                    workflow(
                        system,
                        dataSource,
                        repository,
                        WorkflowEntity.projectionTags().get(index))),
            ProjectionBehavior.stopMessage());
    ShardedDaemonProcess.get(system)
        .init(
            ProjectionBehavior.Command.class,
            "openworkflow-event-subscriptions-schedule-postgresql",
            WorkflowScheduleEntity.PROJECTION_TAG_COUNT,
            index ->
                ProjectionBehavior.create(
                    schedule(
                        system,
                        dataSource,
                        repository,
                        WorkflowScheduleEntity.projectionTags().get(index))),
            ProjectionBehavior.stopMessage());
  }

  private static Projection<EventEnvelope<EngineEvent>> workflow(
      ActorSystem<?> system,
      DataSource dataSource,
      CloudEventSubscriptionRepository repository,
      String tag) {
    SourceProvider<Offset, EventEnvelope<EngineEvent>> source =
        EventSourcedProvider.eventsByTag(system, JdbcReadJournal.Identifier(), tag);
    return JdbcProjection.atLeastOnceAsync(
            ProjectionId.of("openworkflow-event-subscriptions-workflow", tag),
            source,
            () -> new DataSourceJdbcSession(dataSource),
            () -> new WorkflowSubscriptionProjectionHandler(repository),
            system)
        .withSaveOffset(1, Duration.ZERO);
  }

  private static Projection<EventEnvelope<ScheduleEvent>> schedule(
      ActorSystem<?> system,
      DataSource dataSource,
      CloudEventSubscriptionRepository repository,
      String tag) {
    SourceProvider<Offset, EventEnvelope<ScheduleEvent>> source =
        EventSourcedProvider.eventsByTag(system, JdbcReadJournal.Identifier(), tag);
    return JdbcProjection.atLeastOnceAsync(
            ProjectionId.of("openworkflow-event-subscriptions-schedule", tag),
            source,
            () -> new DataSourceJdbcSession(dataSource),
            () -> new ScheduleSubscriptionProjectionHandler(repository),
            system)
        .withSaveOffset(1, Duration.ZERO);
  }
}
