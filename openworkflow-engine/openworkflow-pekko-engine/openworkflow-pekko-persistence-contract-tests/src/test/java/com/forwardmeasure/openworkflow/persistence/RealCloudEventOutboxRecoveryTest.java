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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.openworkflow.actor.PostgresConnectionSettings;
import com.forwardmeasure.openworkflow.actor.ScheduledExecutionRequest;
import com.forwardmeasure.openworkflow.actor.WorkflowCommand;
import com.forwardmeasure.openworkflow.actor.WorkflowReply;
import com.forwardmeasure.openworkflow.actor.WorkflowScheduleSharding;
import com.forwardmeasure.openworkflow.actor.WorkflowSharding;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import com.forwardmeasure.openworkflow.engine.api.ExecutionStatus;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.eventing.CloudEventIngressGateway;
import com.forwardmeasure.openworkflow.eventing.CloudEventPublisher;
import com.forwardmeasure.openworkflow.eventing.CloudEventSubscriptionRepository;
import com.forwardmeasure.openworkflow.eventing.cassandra.CassandraCloudEventOutbox;
import com.forwardmeasure.openworkflow.eventing.cassandra.CassandraCloudEventSubscriptionProjection;
import com.forwardmeasure.openworkflow.eventing.cassandra.CassandraCloudEventSubscriptionRepository;
import com.forwardmeasure.openworkflow.eventing.postgresql.PostgresqlCloudEventOutbox;
import com.forwardmeasure.openworkflow.eventing.postgresql.PostgresqlCloudEventSubscriptionProjection;
import com.forwardmeasure.openworkflow.eventing.postgresql.PostgresqlCloudEventSubscriptionRepository;
import com.forwardmeasure.openworkflow.execution.query.ExecutionPage;
import com.forwardmeasure.openworkflow.execution.query.ExecutionProjectionStore;
import com.forwardmeasure.openworkflow.execution.query.ExecutionQueryRepository;
import com.forwardmeasure.openworkflow.execution.query.ExecutionSearch;
import com.forwardmeasure.openworkflow.execution.query.persistence.JpaTenantRoutingExecutionStore;
import com.forwardmeasure.openworkflow.migration.CassandraMigrationTarget;
import com.forwardmeasure.openworkflow.migration.OpenWorkflowCassandraMigrator;
import com.forwardmeasure.openworkflow.migration.OpenWorkflowTenantMigrator;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
 * Real offset-store recovery contract for the durable CloudEvent outbox. Adapted from
 * openworkflow-actor-engine's version of this test, not a verbatim port: oae's {@code
 * Postgresql|CassandraExecutionQueryRepository} pair doesn't exist here - fowf routes every engine
 * through one unified JPA {@code ExecutionQueryRepository} regardless of which backend runs the
 * actor journal, so the Cassandra scenario below still provisions a Postgres container purely to
 * host that query store, alongside the Cassandra journal under test. Because that JPA store applies
 * events against an already-admitted {@code workflow_execution} row (the same precondition {@code
 * JpaExecutionProjectionStoreTest} seeds by hand), this test seeds that same row directly rather
 * than through the execution-management REST facade, which is out of scope for a persistence
 * contract test.
 */
class RealCloudEventOutboxRecoveryTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(45);
  private static final TenantId TENANT =
      new TenantId(UUID.fromString("134b09a7-1c36-4b89-86e7-a28c88bc5cef"));
  private static final ActorIdentity ACTOR =
      new ActorIdentity(TENANT, "did:forwardmeasure:actor:outbox-test");
  private static final Instant AT = Instant.parse("2026-08-17T12:00:00Z");
  private static final String OUTBOX_RECOVERY_SOURCE =
      """
      document:
        dsl: '1.0.3'
        namespace: forwardmeasure
        name: real-outbox-recovery
        version: '1.0.0'
      do:
        - publish:
            emit:
              event:
                with:
                  source: urn:forwardmeasure:outbox-test
                  type: com.forwardmeasure.outbox.test.v1
                  data:
                    recovered: true
      """;

  @Test
  void retriesUncommittedPostgresqlPublicationAfterActorSystemRestart() throws Exception {
    try (var postgres = new PostgreSQLContainer("postgres:18-alpine")) {
      postgres.start();
      var jpaTenant = new com.forwardmeasure.jpa.tenancy.TenantId(TENANT.value());
      var adminDataSource = adminDataSource(postgres);
      // OpenWorkflowTenantMigrator always connects as the administrator credential and
      // provisions a SEPARATE runtime role - never the same identity it connects as (see its own
      // class Javadoc). Testcontainers' admin user is "test"; reusing that name here would have
      // the migrator GRANT test TO test, which Postgres rejects as circular self-membership.
      var migrator = new OpenWorkflowTenantMigrator(adminDataSource, "openworkflow_runtime");
      migrator.ensureRuntimeRole(postgres.getPassword());
      migrator.provisionAndMigrate(jpaTenant);
      String schema = TenantSchema.forTenant(jpaTenant).value();

      var tenantDataSource = adminDataSource(postgres);
      tenantDataSource.setCurrentSchema(schema);
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
        var executions =
            new SessionScopedExecutionStore(sessions, tenantDataSource, schema, new ObjectMapper());
        proveRecovery(
            config, tenantDataSource, tenantSchema, connection, false, executions, "Postgresql");
      }
      try (var subscriptions = new PostgresqlCloudEventSubscriptionRepository(tenantDataSource)) {
        proveDiscovery(
            config, tenantDataSource, tenantSchema, connection, null, subscriptions, "Postgresql");
      }
    }
  }

  @Test
  void retriesUncommittedCassandraPublicationAfterActorSystemRestart() throws Exception {
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

      try (SessionFactory sessions = sessionFactory(postgres, schema);
          CqlSession session =
              CqlSession.builder()
                  .addContactPoint(address)
                  .withLocalDatacenter(cassandra.getLocalDatacenter())
                  .withKeyspace(OpenWorkflowCassandraMigrator.DEFAULT_APPLICATION_KEYSPACE)
                  .build()) {
        var executions =
            new SessionScopedExecutionStore(sessions, tenantDataSource, schema, new ObjectMapper());
        proveRecovery(config, null, null, null, true, executions, "Cassandra");
        proveDiscovery(
            config,
            null,
            null,
            null,
            true,
            new CassandraCloudEventSubscriptionRepository(session),
            "Cassandra");
      }
    }
  }

  private static void proveDiscovery(
      Config config,
      javax.sql.DataSource postgres,
      TenantSchema schema,
      PostgresConnectionSettings connection,
      Boolean cassandra,
      CloudEventSubscriptionRepository subscriptions,
      String suffix)
      throws Exception {
    ExecutionId executionId = new ExecutionId(TENANT, UUID.randomUUID());
    var plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: real-subscription-discovery
                  version: '1.0.0'
                do:
                  - receive:
                      listen:
                        to:
                          one:
                            with: { type: com.forwardmeasure.discovery.test.v1 }
                        read: data
                """
                    .getBytes(StandardCharsets.UTF_8));
    ActorSystem<Void> system = system(config, "openworkflowDiscovery" + suffix);
    try {
      WorkflowSharding workflows = WorkflowSharding.initialize(system);
      WorkflowScheduleSharding schedules =
          WorkflowScheduleSharding.initialize(system, ignoreScheduleRef(system));
      if (Boolean.TRUE.equals(cassandra)) {
        CassandraCloudEventSubscriptionProjection.start(system, subscriptions);
      } else {
        PostgresqlCloudEventSubscriptionProjection.start(
            system, postgres, schema, connection, subscriptions);
      }
      var ref = workflows.entityRef(executionId);
      await(
          ref.ask(
              replyTo ->
                  new WorkflowCommand.Start(
                      UUID.randomUUID(),
                      executionId,
                      ACTOR,
                      plan,
                      JsonNodeFactory.instance.objectNode(),
                      AT,
                      replyTo),
              TIMEOUT));
      assertEquals(
          ExecutionStatus.WAITING,
          status(
              await(
                  ref.ask(
                      replyTo ->
                          new WorkflowCommand.RunNext(
                              UUID.randomUUID(), executionId, ACTOR, AT.plusSeconds(1), replyTo),
                      TIMEOUT))));

      long deadline = System.nanoTime() + TIMEOUT.toNanos();
      while (System.nanoTime() < deadline
          && subscriptions
              .candidates(TENANT, "com.forwardmeasure.discovery.test.v1", 10)
              .toCompletableFuture()
              .get(5, TimeUnit.SECONDS)
              .isEmpty()) {
        Thread.sleep(100);
      }
      var ingress =
          new CloudEventIngressGateway(
              workflows, schedules, system, Duration.ofSeconds(5), subscriptions, 100);
      var event =
          new com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent(
              "1.0",
              "discovery-1",
              java.net.URI.create("urn:test:discovery"),
              "com.forwardmeasure.discovery.test.v1",
              null,
              AT,
              "application/json",
              JsonNodeFactory.instance.objectNode().put("routed", true),
              java.util.Map.of());
      var routed =
          ingress
              .route(TENANT, event, AT.plusSeconds(2))
              .toCompletableFuture()
              .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      assertTrue(routed.accepted());
      assertEquals(1, routed.discoveredTargets());
      assertEquals(
          ExecutionStatus.COMPLETED,
          status(
              await(
                  ref.ask(
                      replyTo -> new WorkflowCommand.GetState(executionId, replyTo), TIMEOUT))));
      var other =
          ingress
              .route(
                  new TenantId(UUID.fromString("245c1ab8-2d47-4c9a-97f8-3b9dd0cd1234")),
                  event,
                  AT.plusSeconds(3))
              .toCompletableFuture()
              .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      assertTrue(other.accepted());
      assertEquals(0, other.discoveredTargets());
    } finally {
      stop(system);
    }
  }

  private static void proveRecovery(
      Config config,
      javax.sql.DataSource postgres,
      TenantSchema schema,
      PostgresConnectionSettings connection,
      boolean cassandra,
      SessionScopedExecutionStore executions,
      String suffix)
      throws Exception {
    ExecutionId executionId = new ExecutionId(TENANT, UUID.randomUUID());
    var plan =
        new OpenWorkflowCompiler().compile(OUTBOX_RECOVERY_SOURCE.getBytes(StandardCharsets.UTF_8));
    executions.seedAdmittedExecution(executionId, plan);
    CountDownLatch failedAttempt = new CountDownLatch(1);
    CloudEventPublisher unavailable =
        (operationId, event) -> {
          failedAttempt.countDown();
          return CompletableFuture.failedFuture(
              new IllegalStateException("intentional sink outage"));
        };

    ActorSystem<Void> first = system(config, "openworkflowOutbox" + suffix);
    try {
      WorkflowSharding workflows = WorkflowSharding.initialize(first);
      startOutbox(
          first, postgres, schema, connection, cassandra, workflows, unavailable, executions);
      var ref = workflows.entityRef(executionId);
      await(
          ref.ask(
              replyTo ->
                  new WorkflowCommand.Start(
                      UUID.randomUUID(),
                      executionId,
                      ACTOR,
                      plan,
                      JsonNodeFactory.instance.objectNode(),
                      AT,
                      replyTo),
              TIMEOUT));
      executions.apply(startedEvent(executionId));
      WorkflowReply waiting =
          await(
              ref.ask(
                  replyTo ->
                      new WorkflowCommand.RunNext(
                          UUID.randomUUID(), executionId, ACTOR, AT.plusSeconds(1), replyTo),
                  TIMEOUT));
      assertEquals(ExecutionStatus.WAITING, status(waiting));
      assertTrue(failedAttempt.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
    } finally {
      stop(first);
    }

    CountDownLatch delivered = new CountDownLatch(1);
    AtomicInteger publications = new AtomicInteger();
    AtomicInteger lifecyclePublications = new AtomicInteger();
    CloudEventPublisher available =
        (operationId, event) -> {
          if ("com.forwardmeasure.outbox.test.v1".equals(event.type())) {
            publications.incrementAndGet();
            delivered.countDown();
          } else {
            lifecyclePublications.incrementAndGet();
          }
          return CompletableFuture.completedFuture(null);
        };
    ActorSystem<Void> second = system(config, "openworkflowOutbox" + suffix);
    try {
      WorkflowSharding workflows = WorkflowSharding.initialize(second);
      startOutbox(
          second, postgres, schema, connection, cassandra, workflows, available, executions);
      assertTrue(delivered.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
      var ref = workflows.entityRef(executionId);
      long deadline = System.nanoTime() + TIMEOUT.toNanos();
      ExecutionStatus current = null;
      while (System.nanoTime() < deadline) {
        current =
            status(
                await(
                    ref.ask(
                        replyTo -> new WorkflowCommand.GetState(executionId, replyTo), TIMEOUT)));
        if (current == ExecutionStatus.COMPLETED) break;
        Thread.sleep(100);
      }
      assertEquals(ExecutionStatus.COMPLETED, current);
      assertEquals(1, publications.get());
      assertTrue(lifecyclePublications.get() > 0);
    } finally {
      stop(second);
    }
  }

  private static ExecutionEvent startedEvent(ExecutionId executionId) {
    String identity = "pekko:" + executionId.entityId() + ":0:STARTED";
    return new ExecutionEvent(
        UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8)),
        UUID.randomUUID(),
        executionId,
        EngineId.PEKKO,
        0,
        ExecutionEvent.EventType.STARTED,
        ExecutionLifecycleState.RUNNING,
        AT,
        JsonNodeFactory.instance.objectNode());
  }

  private static void startOutbox(
      ActorSystem<?> system,
      javax.sql.DataSource postgres,
      TenantSchema schema,
      PostgresConnectionSettings connection,
      boolean cassandra,
      WorkflowSharding workflows,
      CloudEventPublisher publisher,
      ExecutionQueryRepository executions) {
    if (cassandra) {
      CassandraCloudEventOutbox.start(
          system, workflows, publisher, Duration.ofSeconds(5), executions);
    } else {
      PostgresqlCloudEventOutbox.start(
          system,
          postgres,
          schema,
          connection,
          workflows,
          publisher,
          Duration.ofSeconds(5),
          executions);
    }
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

  private static org.apache.pekko.actor.typed.ActorRef<ScheduledExecutionRequest> ignoreScheduleRef(
      ActorSystem<Void> system) {
    return system.ignoreRef().narrow();
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

  /**
   * Opens one Hibernate session per call, mirroring {@code QuarkusExecutionEventSink}'s real
   * production transaction-boundary pattern - {@link JpaTenantRoutingExecutionStore} binds to a
   * single {@code EntityManager} and isn't safe to share across the concurrent Pekko dispatcher
   * threads the outbox projection calls this from.
   */
  private static final class SessionScopedExecutionStore
      implements ExecutionQueryRepository, ExecutionProjectionStore {
    private final SessionFactory sessions;
    private final javax.sql.DataSource dataSource;
    private final String schema;
    private final ObjectMapper objectMapper;

    SessionScopedExecutionStore(
        SessionFactory sessions,
        javax.sql.DataSource dataSource,
        String schema,
        ObjectMapper objectMapper) {
      this.sessions = sessions;
      this.dataSource = dataSource;
      this.schema = schema;
      this.objectMapper = objectMapper;
    }

    /**
     * The JPA query store projects incremental events against an already-admitted {@code
     * workflow_execution} row - the same precondition {@code JpaExecutionProjectionStoreTest} seeds
     * by hand. In production this row is created by execution-management's governed publish/start
     * flow before the pinned engine ever runs; a persistence contract test drives the actor
     * directly and re-creates only that one precondition row, not the whole admission service.
     */
    void seedAdmittedExecution(
        ExecutionId executionId, com.forwardmeasure.openworkflow.definition.WorkflowPlan plan)
        throws SQLException {
      try (var connection = dataSource.getConnection();
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
                + " (1,0,'20000000-0000-0000-0000-000000000001','outbox-recovery','Outbox"
                + " Recovery',1)");
        statement.executeUpdate(
            "insert into "
                + schema
                + ".workflow_definition (id,version,uuid,workflow_id,revision_number,"
                + "lifecycle_state,source_document,resolved_document,resolved_resources,"
                + "namespace,document_version,specification_version,compiler_profile,"
                + "source_digest,resolved_digest,author_actor_id) values"
                + " (1,0,'30000000-0000-0000-0000-000000000001',1,1,'PUBLISHED',$outbox$"
                + OUTBOX_RECOVERY_SOURCE
                + "$outbox$,$outbox$"
                + OUTBOX_RECOVERY_SOURCE
                + "$outbox$,'[]','forwardmeasure','1.0.0','1.0.3','default','"
                + plan.sourceSha256()
                + "','"
                + plan.definitionSha256()
                + "',1)");
        statement.executeUpdate(
            "insert into "
                + schema
                + ".workflow_execution (id,uuid,revision_id,revision_digest,engine_id,"
                + "lifecycle_state,correlation_id,idempotency_key,started_by_actor_id,input)"
                + " values (1,'"
                + executionId.value()
                + "',1,'"
                + plan.definitionSha256()
                + "','pekko','NEW','outbox-recovery','outbox-recovery',1,'{}')");
      }
    }

    @Override
    public Optional<com.forwardmeasure.openworkflow.engine.api.ExecutionProjection> find(
        TenantId tenantId, ExecutionId executionId) {
      return call(store -> store.find(tenantId, executionId));
    }

    @Override
    public ExecutionPage search(ExecutionSearch search) {
      return call(store -> store.search(search));
    }

    @Override
    public List<com.forwardmeasure.openworkflow.engine.api.ExecutionHistoryEntry> history(
        TenantId tenantId, ExecutionId executionId, long afterSequence, int limit) {
      return call(store -> store.history(tenantId, executionId, afterSequence, limit));
    }

    @Override
    public ProjectionApplyResult apply(ExecutionEvent event) {
      return callInTransaction(store -> store.apply(event));
    }

    @Override
    public ProjectionApplyResult applyNext(ExecutionEvent event) {
      return callInTransaction(store -> store.applyNext(event));
    }

    private <T> T call(java.util.function.Function<JpaTenantRoutingExecutionStore, T> body) {
      try (var entityManager = sessions.createEntityManager()) {
        return body.apply(new JpaTenantRoutingExecutionStore(entityManager, objectMapper));
      }
    }

    private <T> T callInTransaction(
        java.util.function.Function<JpaTenantRoutingExecutionStore, T> body) {
      try (var entityManager = sessions.createEntityManager()) {
        var transaction = entityManager.getTransaction();
        transaction.begin();
        try {
          T result = body.apply(new JpaTenantRoutingExecutionStore(entityManager, objectMapper));
          transaction.commit();
          return result;
        } catch (RuntimeException | Error failure) {
          transaction.rollback();
          throw failure;
        }
      }
    }
  }
}
