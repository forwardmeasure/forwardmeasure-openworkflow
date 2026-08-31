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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.actor.PostgresConnectionSettings;
import com.forwardmeasure.openworkflow.actor.SubworkflowCoordinatorSharding;
import com.forwardmeasure.openworkflow.actor.WorkflowCommand;
import com.forwardmeasure.openworkflow.actor.WorkflowReply;
import com.forwardmeasure.openworkflow.actor.WorkflowSharding;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.ResolvedSubflow;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceBundleCodec;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionStatus;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.eventing.cassandra.CassandraSubworkflowOutbox;
import com.forwardmeasure.openworkflow.eventing.persistence.HibernateSessionSubworkflowPlanResolver;
import com.forwardmeasure.openworkflow.eventing.postgresql.PostgresqlSubworkflowOutbox;
import com.forwardmeasure.openworkflow.migration.CassandraMigrationTarget;
import com.forwardmeasure.openworkflow.migration.OpenWorkflowCassandraMigrator;
import com.forwardmeasure.openworkflow.migration.OpenWorkflowTenantMigrator;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import jakarta.persistence.EntityManagerFactory;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.cluster.MemberStatus;
import org.apache.pekko.cluster.typed.Cluster;
import org.apache.pekko.cluster.typed.Join;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Real both-backend recovery and control-propagation contract for child workflows. Adapted from
 * openworkflow-actor-engine's version, scoped down deliberately: oae's original also drove two
 * unrelated scenarios (a durable function invocation, an HTTP call with pause/resume) through the
 * same actor-system restart - genuinely useful coverage there, but for a construct neither specific
 * to nor exercised by subworkflow recovery, and already covered by fowf's own existing suites. What
 * this test keeps is the property that actually needs proving: a real, previously-nonexistent piece
 * of production code ({@code JpaSubworkflowPlanResolver}/{@code
 * HibernateSessionSubworkflowPlanResolver}, built alongside this test) correctly resolves the
 * pinned child plan, launches it, propagates pause/resume/cancel to it, and both parent and child
 * recover correctly after an actor-system restart.
 */
class RealSubworkflowRecoveryTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(45);
  private static final TenantId TENANT =
      new TenantId(UUID.fromString("134b09a7-1c36-4b89-86e7-a28c88bc5cef"));
  private static final ActorIdentity ACTOR =
      new ActorIdentity(TENANT, "did:forwardmeasure:actor:subworkflow-recovery-test");
  private static final Instant AT = Instant.parse("2026-08-17T12:00:00Z");
  private static final String CHILD_SOURCE =
      """
      document:
        dsl: '1.0.3'
        namespace: forwardmeasure
        name: real-subworkflow-child
        version: '1.0.0'
      do:
        - delay:
            wait: PT3S
        - finish:
            set:
              child: completed
      """;
  private static final String PARENT_SOURCE =
      """
      document:
        dsl: '1.0.3'
        namespace: forwardmeasure
        name: real-subworkflow-parent
        version: '1.0.0'
      do:
        - child:
            run:
              await: true
              workflow:
                namespace: forwardmeasure
                name: real-subworkflow-child
                version: '1.0.0'
                input:
                  seed: '${ .seed }'
      """;

  @Test
  void postgresqlRecoversAndPropagatesControlToAWaitingChild() throws Exception {
    try (var postgres = new PostgreSQLContainer("postgres:18-alpine")) {
      postgres.start();
      var jpaTenant = new com.forwardmeasure.jpa.tenancy.TenantId(TENANT.value());
      var adminDataSource = adminDataSource(postgres);
      var migrator = new OpenWorkflowTenantMigrator(adminDataSource, "openworkflow_runtime");
      migrator.ensureRuntimeRole(postgres.getPassword());
      migrator.provisionAndMigrate(jpaTenant);
      String schema = TenantSchema.forTenant(jpaTenant).value();
      var tenantDataSource = adminDataSource(postgres);
      tenantDataSource.setCurrentSchema(schema);

      var childPlan =
          new OpenWorkflowCompiler().compile(CHILD_SOURCE.getBytes(StandardCharsets.UTF_8));
      seedPublishedChild(tenantDataSource, schema, childPlan);
      var parentPlan = compileParent(childPlan);

      Config config =
          clusterConfig(
              PersistenceConfigLoader.withConnection(
                  PersistenceConfigLoader.select(
                      ConfigFactory.load(), PersistenceProfile.POSTGRESQL),
                  PersistenceProfile.POSTGRESQL,
                  withCurrentSchema(postgres.getJdbcUrl(), schema),
                  postgres.getUsername(),
                  postgres.getPassword(),
                  null));

      TenantSchema tenantSchema = TenantSchema.forTenant(jpaTenant);
      PostgresConnectionSettings connection =
          new PostgresConnectionSettings(
              postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
      try (SessionFactory sessions = sessionFactory(postgres, schema)) {
        var resolver =
            new HibernateSessionSubworkflowPlanResolver(noopTenantScope(), asFactory(sessions));
        proveRecoveryAndControls(
            config,
            tenantDataSource,
            tenantSchema,
            connection,
            false,
            resolver,
            parentPlan,
            childPlan,
            "Postgresql");
      }
    }
  }

  @Test
  void cassandraRecoversAndPropagatesControlToAWaitingChild() throws Exception {
    try (var postgres = new PostgreSQLContainer("postgres:18-alpine");
        var cassandra = new CassandraContainer("cassandra:5.0.5")) {
      postgres.start();
      cassandra.start();
      var jpaTenant = new com.forwardmeasure.jpa.tenancy.TenantId(TENANT.value());
      var adminDataSource = adminDataSource(postgres);
      var migrator = new OpenWorkflowTenantMigrator(adminDataSource, "openworkflow_runtime");
      migrator.ensureRuntimeRole(postgres.getPassword());
      migrator.provisionAndMigrate(jpaTenant);
      String schema = TenantSchema.forTenant(jpaTenant).value();
      var tenantDataSource = adminDataSource(postgres);
      tenantDataSource.setCurrentSchema(schema);

      var childPlan =
          new OpenWorkflowCompiler().compile(CHILD_SOURCE.getBytes(StandardCharsets.UTF_8));
      seedPublishedChild(tenantDataSource, schema, childPlan);
      var parentPlan = compileParent(childPlan);

      InetSocketAddress address =
          new InetSocketAddress(cassandra.getHost(), cassandra.getMappedPort(9042));
      OpenWorkflowCassandraMigrator.migrate(
          CassandraMigrationTarget.unauthenticated(address, cassandra.getLocalDatacenter()));
      Config config =
          clusterConfig(
              PersistenceConfigLoader.withConnection(
                  PersistenceConfigLoader.select(
                      ConfigFactory.load(), PersistenceProfile.CASSANDRA),
                  PersistenceProfile.CASSANDRA,
                  address.getHostString() + ':' + address.getPort(),
                  "",
                  "",
                  cassandra.getLocalDatacenter()));

      try (SessionFactory sessions = sessionFactory(postgres, schema)) {
        var resolver =
            new HibernateSessionSubworkflowPlanResolver(noopTenantScope(), asFactory(sessions));
        proveRecoveryAndControls(
            config, null, null, null, true, resolver, parentPlan, childPlan, "Cassandra");
      }
    }
  }

  private static void proveRecoveryAndControls(
      Config config,
      DataSource dataSource,
      TenantSchema schema,
      PostgresConnectionSettings connection,
      boolean cassandra,
      HibernateSessionSubworkflowPlanResolver resolver,
      WorkflowPlan parentPlan,
      WorkflowPlan childPlan,
      String suffix)
      throws Exception {
    ExecutionId parentExecution = new ExecutionId(TENANT, UUID.randomUUID());
    ExecutionId childExecution = childExecutionId(parentExecution, childPlan);

    ActorSystem<Void> first = system(config, "openworkflowSubflow" + suffix + "First");
    try {
      WorkflowSharding workflows = WorkflowSharding.initialize(first);
      startSubworkflowOutbox(first, dataSource, schema, connection, cassandra, workflows, resolver);
      startParent(workflows, parentExecution, parentPlan);
      awaitStatus(workflows, childExecution, ExecutionStatus.WAITING);

      WorkflowReply paused =
          ask(
              workflows,
              parentExecution,
              replyTo ->
                  new WorkflowCommand.Pause(
                      UUID.randomUUID(), parentExecution, ACTOR, AT.plusSeconds(2), replyTo));
      assertEquals(ExecutionStatus.PAUSED, status(paused));
      awaitStatus(workflows, childExecution, ExecutionStatus.PAUSED);
    } finally {
      stop(first);
    }

    ActorSystem<Void> second = system(config, "openworkflowSubflow" + suffix + "Second");
    try {
      WorkflowSharding workflows = WorkflowSharding.initialize(second);
      startSubworkflowOutbox(
          second, dataSource, schema, connection, cassandra, workflows, resolver);
      awaitStatus(workflows, parentExecution, ExecutionStatus.PAUSED);
      awaitStatus(workflows, childExecution, ExecutionStatus.PAUSED);

      WorkflowReply resumed =
          ask(
              workflows,
              parentExecution,
              replyTo ->
                  new WorkflowCommand.Resume(
                      UUID.randomUUID(), parentExecution, ACTOR, AT.plusSeconds(3), replyTo));
      assertEquals(ExecutionStatus.WAITING, status(resumed));
      awaitStatus(workflows, childExecution, ExecutionStatus.COMPLETED);
      awaitStatus(workflows, parentExecution, ExecutionStatus.COMPLETED);

      ExecutionId cancelledParent = new ExecutionId(TENANT, UUID.randomUUID());
      ExecutionId cancelledChild = childExecutionId(cancelledParent, childPlan);
      startParent(workflows, cancelledParent, parentPlan);
      awaitStatus(workflows, cancelledChild, ExecutionStatus.WAITING);
      WorkflowReply cancelled =
          ask(
              workflows,
              cancelledParent,
              replyTo ->
                  new WorkflowCommand.Cancel(
                      UUID.randomUUID(), cancelledParent, ACTOR, AT.plusSeconds(11), replyTo));
      assertEquals(ExecutionStatus.CANCELLED, status(cancelled));
      awaitStatus(workflows, cancelledChild, ExecutionStatus.CANCELLED);
    } finally {
      stop(second);
    }
  }

  private static WorkflowPlan compileParent(WorkflowPlan childPlan) {
    var subflow =
        new ResolvedSubflow(
            childPlan.coordinates(), childPlan.sourceSha256(), childPlan.definitionSha256());
    return new OpenWorkflowCompiler()
        .compile(
            PARENT_SOURCE.getBytes(StandardCharsets.UTF_8),
            List.of(),
            (namespace, name, version) ->
                namespace.equals(childPlan.coordinates().namespace())
                        && name.equals(childPlan.coordinates().name())
                        && version.equals(childPlan.coordinates().version())
                    ? Optional.of(subflow)
                    : Optional.empty());
  }

  /**
   * Mirrors {@code WorkflowEntity}'s own derivation exactly (see its {@code onRunWorkflow}-style
   * handling of {@code RunPlan.Kind.WORKFLOW}): {@code parentEntityId|subworkflow|stepPath|
   * revision|subflow.canonical()}. {@code revision} is {@code 1} here because the parent's very
   * first RunNext after Start is what emits the SubworkflowRequested event.
   */
  private static ExecutionId childExecutionId(ExecutionId parentExecution, WorkflowPlan childPlan) {
    var subflow =
        new ResolvedSubflow(
            childPlan.coordinates(), childPlan.sourceSha256(), childPlan.definitionSha256());
    UUID childUuid =
        UUID.nameUUIDFromBytes(
            (parentExecution.entityId() + "|subworkflow|/do/0/child|1|" + subflow.canonical())
                .getBytes(StandardCharsets.UTF_8));
    return new ExecutionId(parentExecution.tenantId(), childUuid);
  }

  private static void startParent(
      WorkflowSharding workflows, ExecutionId executionId, WorkflowPlan plan) throws Exception {
    WorkflowReply started =
        ask(
            workflows,
            executionId,
            replyTo ->
                new WorkflowCommand.Start(
                    UUID.randomUUID(),
                    executionId,
                    ACTOR,
                    plan,
                    JsonNodeFactory.instance.objectNode().put("seed", "real-1"),
                    AT,
                    replyTo));
    assertEquals(ExecutionStatus.RUNNING, status(started));
    WorkflowReply requested =
        ask(
            workflows,
            executionId,
            replyTo ->
                new WorkflowCommand.RunNext(
                    UUID.randomUUID(), executionId, ACTOR, AT.plusSeconds(1), replyTo));
    assertEquals(ExecutionStatus.WAITING, status(requested));
  }

  private static void startSubworkflowOutbox(
      ActorSystem<?> system,
      DataSource dataSource,
      TenantSchema schema,
      PostgresConnectionSettings connection,
      boolean cassandra,
      WorkflowSharding workflows,
      HibernateSessionSubworkflowPlanResolver resolver) {
    SubworkflowCoordinatorSharding coordinators =
        SubworkflowCoordinatorSharding.initialize(system, workflows);
    if (cassandra) {
      CassandraSubworkflowOutbox.start(system, resolver, coordinators, Duration.ofSeconds(5));
    } else {
      PostgresqlSubworkflowOutbox.start(
          system, dataSource, schema, connection, resolver, coordinators, Duration.ofSeconds(5));
    }
  }

  private static ExecutionStatus awaitStatus(
      WorkflowSharding workflows, ExecutionId executionId, ExecutionStatus expected)
      throws Exception {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    ExecutionStatus current = null;
    while (System.nanoTime() < deadline) {
      current =
          status(
              ask(
                  workflows,
                  executionId,
                  replyTo -> new WorkflowCommand.GetState(executionId, replyTo)));
      if (current == expected) return current;
      Thread.sleep(100);
    }
    throw new AssertionError(
        "Expected " + executionId + " to become " + expected + " but was " + current);
  }

  private static WorkflowReply ask(
      WorkflowSharding workflows,
      ExecutionId executionId,
      java.util.function.Function<
              org.apache.pekko.actor.typed.ActorRef<WorkflowReply>, WorkflowCommand>
          command)
      throws Exception {
    return workflows
        .entityRef(executionId)
        .<WorkflowReply>ask(command::apply, TIMEOUT)
        .toCompletableFuture()
        .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
  }

  private static ExecutionStatus status(WorkflowReply reply) {
    return switch (reply) {
      case WorkflowReply.Accepted accepted -> accepted.status();
      case WorkflowReply.Rejected rejected -> rejected.status();
      case WorkflowReply.StateSnapshot snapshot -> snapshot.status();
      case WorkflowReply.RuntimeState runtime -> runtime.state().status();
    };
  }

  private static void seedPublishedChild(
      DataSource tenantDataSource, String schema, WorkflowPlan plan) throws SQLException {
    try (var connection = tenantDataSource.getConnection();
        var statement = connection.createStatement()) {
      statement.executeUpdate(
          "insert into "
              + schema
              + ".actor (id,version,uuid,subject_identifier,identity_type) values"
              + " (1,0,'10000000-0000-0000-0000-000000000001','actor','HUMAN')");
      statement.executeUpdate(
          "insert into "
              + schema
              + ".workflow (id,version,uuid,name,title,owner_id) values"
              + " (1,0,'20000000-0000-0000-0000-000000000001','real-subworkflow-child','Real"
              + " Subworkflow Child',1)");
      statement.executeUpdate(
          "insert into "
              + schema
              + ".workflow_definition"
              + " (id,uuid,version,workflow_id,revision_number,lifecycle_state,source_document,"
              + "resolved_document,resolved_resources,namespace,document_version,"
              + "specification_version,compiler_profile,source_digest,resolved_digest,"
              + "author_actor_id) values (1,'30000000-0000-0000-0000-000000000001',0,1,1,"
              + "'PUBLISHED',$workflow$"
              + CHILD_SOURCE
              + "$workflow$,$workflow$"
              + plan.definition()
              + "$workflow$,$resources$"
              + WorkflowResourceBundleCodec.encode(plan.resources())
              + "$resources$,'"
              + plan.coordinates().namespace()
              + "','"
              + plan.coordinates().version()
              + "','"
              + plan.coordinates().dsl()
              + "','default','"
              + plan.sourceSha256()
              + "','"
              + plan.definitionSha256()
              + "',1)");
      statement.executeUpdate(
          "insert into "
              + schema
              + ".workflow_publication (id,version,definition_id,actor_id,definition_digest)"
              + " values (1,0,1,1,'"
              + plan.definitionSha256()
              + "')");
    }
  }

  private static TenantScope noopTenantScope() {
    return new TenantScope() {
      @Override
      public Optional<com.forwardmeasure.jpa.tenancy.TenantSchema> current() {
        return Optional.empty();
      }

      @Override
      public Scope open(com.forwardmeasure.jpa.tenancy.TenantSchema schema) {
        return () -> {};
      }
    };
  }

  private static EntityManagerFactory asFactory(SessionFactory sessions) {
    return sessions.unwrap(EntityManagerFactory.class);
  }

  private static String withCurrentSchema(String jdbcUrl, String schema) {
    return jdbcUrl + (jdbcUrl.contains("?") ? '&' : '?') + "currentSchema=" + schema;
  }

  private static PGSimpleDataSource adminDataSource(PostgreSQLContainer postgres) {
    var dataSource = new PGSimpleDataSource();
    dataSource.setURL(postgres.getJdbcUrl());
    dataSource.setUser(postgres.getUsername());
    dataSource.setPassword(postgres.getPassword());
    return dataSource;
  }

  private static SessionFactory sessionFactory(PostgreSQLContainer postgres, String schema) {
    return new Configuration()
        .setProperty("hibernate.connection.url", postgres.getJdbcUrl())
        .setProperty("hibernate.connection.username", postgres.getUsername())
        .setProperty("hibernate.connection.password", postgres.getPassword())
        .setProperty("hibernate.connection.driver_class", "org.postgresql.Driver")
        .setProperty("hibernate.default_schema", schema)
        .setProperty("hibernate.hbm2ddl.auto", "none")
        .buildSessionFactory();
  }

  private static ActorSystem<Void> system(Config config, String name) throws Exception {
    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), name, config);
    Cluster cluster = Cluster.get(system);
    cluster.manager().tell(Join.create(cluster.selfMember().address()));
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      var memberStatus = cluster.selfMember().status();
      if (memberStatus.equals(MemberStatus.up()) || memberStatus.equals(MemberStatus.weaklyUp())) {
        return system;
      }
      Thread.sleep(100);
    }
    stop(system);
    throw new AssertionError("cluster did not become Up");
  }

  private static Config clusterConfig(Config persistence) {
    return persistence
        .withValue("pekko.actor.provider", ConfigValueFactory.fromAnyRef("cluster"))
        .withValue(
            "pekko.remote.artery.canonical.hostname", ConfigValueFactory.fromAnyRef("127.0.0.1"))
        .withValue("pekko.remote.artery.bind.hostname", ConfigValueFactory.fromAnyRef("127.0.0.1"))
        .withValue("pekko.remote.artery.canonical.port", ConfigValueFactory.fromAnyRef(0))
        .withValue("pekko.remote.artery.bind.port", ConfigValueFactory.fromAnyRef(0))
        .withValue("pekko.cluster.seed-nodes", ConfigValueFactory.fromIterable(List.of()))
        .resolve();
  }

  private static void stop(ActorSystem<?> system) throws Exception {
    system.terminate();
    system.getWhenTerminated().toCompletableFuture().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
  }
}
