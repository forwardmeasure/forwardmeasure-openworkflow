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

import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.openworkflow.actor.PostgresConnectionSettings;
import com.forwardmeasure.openworkflow.actor.SubworkflowCoordinatorSharding;
import com.forwardmeasure.openworkflow.actor.TenantPersistencePlugins;
import com.forwardmeasure.openworkflow.actor.WorkflowEntity;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.eventing.SubworkflowOutboxHandler;
import com.forwardmeasure.openworkflow.eventing.SubworkflowPlanResolver;
import java.time.Duration;
import java.util.Objects;
import javax.sql.DataSource;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.cluster.sharding.typed.javadsl.ShardedDaemonProcess;
import org.apache.pekko.persistence.query.Offset;
import org.apache.pekko.projection.Projection;
import org.apache.pekko.projection.ProjectionBehavior;
import org.apache.pekko.projection.ProjectionId;
import org.apache.pekko.projection.eventsourced.EventEnvelope;
import org.apache.pekko.projection.eventsourced.javadsl.EventSourcedProvider;
import org.apache.pekko.projection.javadsl.SourceProvider;
import org.apache.pekko.projection.jdbc.javadsl.JdbcProjection;

/**
 * PostgreSQL-offset subworkflow launch projection - one instance per tenant, see
 * PostgresqlCloudEventOutbox.
 */
public final class PostgresqlSubworkflowOutbox {
  private PostgresqlSubworkflowOutbox() {}

  public static void start(
      ActorSystem<?> system,
      DataSource dataSource,
      TenantSchema schema,
      PostgresConnectionSettings connection,
      SubworkflowPlanResolver definitions,
      SubworkflowCoordinatorSharding coordinators,
      Duration askTimeout) {
    Objects.requireNonNull(schema, "schema");
    ShardedDaemonProcess.get(system)
        .init(
            ProjectionBehavior.Command.class,
            "openworkflow-subflows-postgresql-" + schema.value(),
            WorkflowEntity.PROJECTION_TAG_COUNT,
            index ->
                ProjectionBehavior.create(
                    projection(
                        system,
                        dataSource,
                        schema,
                        connection,
                        definitions,
                        coordinators,
                        askTimeout,
                        WorkflowEntity.projectionTags().get(index))),
            ProjectionBehavior.stopMessage());
  }

  private static Projection<EventEnvelope<EngineEvent>> projection(
      ActorSystem<?> system,
      DataSource dataSource,
      TenantSchema schema,
      PostgresConnectionSettings connection,
      SubworkflowPlanResolver definitions,
      SubworkflowCoordinatorSharding coordinators,
      Duration askTimeout,
      String tag) {
    SourceProvider<Offset, EventEnvelope<EngineEvent>> source =
        EventSourcedProvider.eventsByTag(
            system,
            TenantPersistencePlugins.readJournalPluginId(schema),
            TenantPersistencePlugins.readJournalPluginConfig(system, schema, connection),
            tag);
    return JdbcProjection.atLeastOnceAsync(
            ProjectionId.of("openworkflow-subflows-" + schema.value(), tag),
            source,
            () -> new DataSourceJdbcSession(dataSource, schema),
            () -> new SubworkflowOutboxHandler(definitions, coordinators, askTimeout),
            system)
        .withSaveOffset(1, Duration.ZERO);
  }
}
