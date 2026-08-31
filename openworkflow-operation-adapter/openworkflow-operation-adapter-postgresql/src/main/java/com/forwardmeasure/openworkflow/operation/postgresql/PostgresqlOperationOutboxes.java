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
package com.forwardmeasure.openworkflow.operation.postgresql;

import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.openworkflow.actor.PostgresConnectionSettings;
import com.forwardmeasure.openworkflow.actor.ProtocolOperationCoordinatorSharding;
import com.forwardmeasure.openworkflow.actor.TenantPersistencePlugins;
import com.forwardmeasure.openworkflow.actor.WorkflowEntity;
import com.forwardmeasure.openworkflow.actor.WorkflowSharding;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.operation.HttpOperationExecutor;
import com.forwardmeasure.openworkflow.operation.HttpOperationOutboxHandler;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationOutboxHandler;
import java.time.Clock;
import java.time.Duration;
import javax.sql.DataSource;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.cluster.sharding.typed.ShardedDaemonProcessSettings;
import org.apache.pekko.cluster.sharding.typed.javadsl.ShardedDaemonProcess;
import org.apache.pekko.persistence.query.Offset;
import org.apache.pekko.projection.ProjectionBehavior;
import org.apache.pekko.projection.ProjectionId;
import org.apache.pekko.projection.eventsourced.EventEnvelope;
import org.apache.pekko.projection.eventsourced.javadsl.EventSourcedProvider;
import org.apache.pekko.projection.javadsl.Handler;
import org.apache.pekko.projection.javadsl.SourceProvider;
import org.apache.pekko.projection.jdbc.javadsl.JdbcProjection;

/**
 * Starts durable-offset PostgreSQL HTTP and protocol operation projections - one instance per
 * tenant, since the journal it reads and the offset store it writes both live inside that tenant's
 * own Postgres schema (same shape as {@code PostgresqlCloudEventOutbox}).
 */
public final class PostgresqlOperationOutboxes {
  private PostgresqlOperationOutboxes() {}

  public static void startHttp(
      ActorSystem<?> system,
      DataSource dataSource,
      TenantSchema schema,
      PostgresConnectionSettings connection,
      WorkflowSharding workflows,
      HttpOperationExecutor executor,
      Duration askTimeout) {
    start(
        system,
        dataSource,
        schema,
        connection,
        "openworkflow-http-operations-postgresql-" + schema.value(),
        "openworkflow-http-operations-" + schema.value(),
        tag -> new HttpOperationOutboxHandler(workflows, executor, askTimeout, Clock.systemUTC()));
  }

  public static void startProtocol(
      ActorSystem<?> system,
      DataSource dataSource,
      TenantSchema schema,
      PostgresConnectionSettings connection,
      ProtocolOperationCoordinatorSharding coordinators,
      Duration askTimeout) {
    start(
        system,
        dataSource,
        schema,
        connection,
        "openworkflow-protocol-operations-postgresql-" + schema.value(),
        "openworkflow-protocol-operations-" + schema.value(),
        tag -> new ProtocolOperationOutboxHandler(coordinators, askTimeout));
  }

  private static void start(
      ActorSystem<?> system,
      DataSource dataSource,
      TenantSchema schema,
      PostgresConnectionSettings connection,
      String processName,
      String projectionName,
      java.util.function.Function<String, Handler<EventEnvelope<EngineEvent>>> handlers) {
    ShardedDaemonProcess.get(system)
        .init(
            ProjectionBehavior.Command.class,
            processName,
            WorkflowEntity.PROJECTION_TAG_COUNT,
            index -> {
              String tag = WorkflowEntity.projectionTags().get(index);
              SourceProvider<Offset, EventEnvelope<EngineEvent>> source =
                  EventSourcedProvider.eventsByTag(
                      system,
                      TenantPersistencePlugins.readJournalPluginId(schema),
                      TenantPersistencePlugins.readJournalPluginConfig(system, schema, connection),
                      tag);
              return ProjectionBehavior.create(
                  JdbcProjection.atLeastOnceAsync(
                          ProjectionId.of(projectionName, tag),
                          source,
                          () -> new DataSourceJdbcSession(dataSource, schema),
                          () -> handlers.apply(tag),
                          system)
                      .withSaveOffset(1, Duration.ZERO));
            },
            ShardedDaemonProcessSettings.create(system).withRole("operation-adapter"),
            java.util.Optional.of(ProjectionBehavior.stopMessage()));
  }
}
