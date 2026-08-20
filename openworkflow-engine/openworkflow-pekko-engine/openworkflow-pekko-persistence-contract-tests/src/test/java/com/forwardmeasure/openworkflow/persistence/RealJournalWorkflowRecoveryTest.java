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
package com.forwardmeasure.openworkflow.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.openworkflow.actor.WorkflowCommand;
import com.forwardmeasure.openworkflow.actor.WorkflowEntity;
import com.forwardmeasure.openworkflow.actor.WorkflowReply;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionStatus;
import com.forwardmeasure.openworkflow.migration.OpenWorkflowCassandraMigrator;
import com.forwardmeasure.openworkflow.migration.OpenWorkflowTenantMigrator;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.japi.function.Function;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Process-boundary recovery contract shared by both production journal profiles. */
class RealJournalWorkflowRecoveryTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(45);
  private static final Instant AT = Instant.parse("2026-08-17T12:00:00Z");
  private static final com.forwardmeasure.openworkflow.engine.api.TenantId ENGINE_TENANT =
      new com.forwardmeasure.openworkflow.engine.api.TenantId(
          UUID.fromString("134b09a7-1c36-4b89-86e7-a28c88bc5cef"));

  @Test
  void recoversCompletedWorkflowFromPostgresqlJournal() throws Exception {
    try (var postgres = new PostgreSQLContainer("postgres:18-alpine")) {
      postgres.start();
      var dataSource =
          new DriverDataSource(
              postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
      new OpenWorkflowTenantMigrator(dataSource)
          .provisionAndMigrate(new TenantId(ENGINE_TENANT.value()));
      Config config =
          PersistenceConfigLoader.withConnection(
              PersistenceConfigLoader.select(ConfigFactory.load(), PersistenceProfile.POSTGRESQL),
              PersistenceProfile.POSTGRESQL,
              withCurrentSchema(postgres.getJdbcUrl(), "t_134b09a71c364b8986e7a28c88bc5cef"),
              postgres.getUsername(),
              postgres.getPassword(),
              null);
      proveRecovery(config, "Postgresql");
    }
  }

  @Test
  void recoversCompletedWorkflowFromCassandraJournal() throws Exception {
    try (var cassandra = new CassandraContainer("cassandra:5.0.5")) {
      cassandra.start();
      InetSocketAddress address =
          new InetSocketAddress(cassandra.getHost(), cassandra.getMappedPort(9042));
      try (CqlSession session =
          CqlSession.builder()
              .addContactPoint(address)
              .withLocalDatacenter(cassandra.getLocalDatacenter())
              .build()) {
        OpenWorkflowCassandraMigrator.migrate(session);
      }
      String endpoint = address.getHostString() + ':' + address.getPort();
      Config config =
          PersistenceConfigLoader.withConnection(
              PersistenceConfigLoader.select(ConfigFactory.load(), PersistenceProfile.CASSANDRA),
              PersistenceProfile.CASSANDRA,
              endpoint,
              "",
              "",
              cassandra.getLocalDatacenter());
      proveRecovery(config, "Cassandra");
    }
  }

  private static void proveRecovery(Config config, String suffix) throws Exception {
    var plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: recovery
                  name: journal
                  version: '1.0.0'
                do:
                  - first:
                      set:
                        recovered: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    ExecutionId executionId = new ExecutionId(ENGINE_TENANT, UUID.randomUUID());
    ActorIdentity actor = new ActorIdentity(ENGINE_TENANT, "did:forwardmeasure:actor:recovery");
    ActorSystem<WorkflowCommand> writer = system(executionId, config, suffix + "Writer");
    try {
      await(
          ask(
              writer,
              replyTo ->
                  new WorkflowCommand.Start(
                      UUID.randomUUID(),
                      executionId,
                      actor,
                      plan,
                      JsonNodeFactory.instance.objectNode(),
                      AT,
                      replyTo)));
      WorkflowReply completed =
          await(
              ask(
                  writer,
                  replyTo ->
                      new WorkflowCommand.RunNext(
                          UUID.randomUUID(), executionId, actor, AT.plusSeconds(1), replyTo)));
      assertEquals(ExecutionStatus.COMPLETED, status(completed));
    } finally {
      stop(writer);
    }

    ActorSystem<WorkflowCommand> reader = system(executionId, config, suffix + "Reader");
    try {
      var recovered =
          (WorkflowReply.StateSnapshot)
              await(ask(reader, replyTo -> new WorkflowCommand.GetState(executionId, replyTo)));
      assertEquals(ExecutionStatus.COMPLETED, recovered.status());
      assertEquals(true, recovered.data().required("recovered").booleanValue());
    } finally {
      stop(reader);
    }
  }

  private static String withCurrentSchema(String jdbcUrl, String schema) {
    return jdbcUrl + (jdbcUrl.contains("?") ? '&' : '?') + "currentSchema=" + schema;
  }

  private static ActorSystem<WorkflowCommand> system(
      ExecutionId executionId, Config config, String suffix) {
    return ActorSystem.create(
        WorkflowEntity.create(executionId), "openworkflowRecovery" + suffix, config);
  }

  private static CompletionStage<WorkflowReply> ask(
      ActorSystem<WorkflowCommand> system,
      Function<ActorRef<WorkflowReply>, WorkflowCommand> command) {
    return AskPattern.ask(system, command, TIMEOUT, system.scheduler());
  }

  private static WorkflowReply await(CompletionStage<WorkflowReply> stage) throws Exception {
    return stage.toCompletableFuture().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
  }

  private static ExecutionStatus status(WorkflowReply reply) {
    return switch (reply) {
      case WorkflowReply.Accepted accepted -> accepted.status();
      case WorkflowReply.Rejected rejected -> rejected.status();
      case WorkflowReply.StateSnapshot snapshot -> snapshot.status();
      case WorkflowReply.RuntimeState runtime -> runtime.state().status();
    };
  }

  private static void stop(ActorSystem<?> system) throws Exception {
    system.terminate();
    system.getWhenTerminated().toCompletableFuture().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
  }

  private record DriverDataSource(String url, String username, String password)
      implements DataSource {
    @Override
    public Connection getConnection() throws SQLException {
      return DriverManager.getConnection(url, username, password);
    }

    @Override
    public Connection getConnection(String user, String secret) throws SQLException {
      return DriverManager.getConnection(url, user, secret);
    }

    @Override
    public PrintWriter getLogWriter() {
      return DriverManager.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter writer) {
      DriverManager.setLogWriter(writer);
    }

    @Override
    public void setLoginTimeout(int seconds) {
      DriverManager.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() {
      return DriverManager.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
      throw new SQLException("not a wrapper");
    }

    @Override
    public boolean isWrapperFor(Class<?> type) {
      return false;
    }
  }
}
