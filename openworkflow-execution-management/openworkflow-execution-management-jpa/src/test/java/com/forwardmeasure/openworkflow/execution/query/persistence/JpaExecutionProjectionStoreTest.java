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
package com.forwardmeasure.openworkflow.execution.query.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import com.forwardmeasure.openworkflow.execution.query.ExecutionProjectionStore;
import com.forwardmeasure.openworkflow.execution.query.ExecutionSearch;
import com.forwardmeasure.openworkflow.migration.OpenWorkflowTenantMigrator;
import com.forwardmeasure.testcontainers.junit.postgresql.WithPostgreSqlContainer;
import com.forwardmeasure.testcontainers.postgresql.PostgreSqlTestContainer;
import jakarta.persistence.EntityManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.Test;

@WithPostgreSqlContainer(databaseName = "openworkflow_projection_store")
class JpaExecutionProjectionStoreTest {
  private static final TenantId TENANT = TenantId.parse("11111111-1111-1111-1111-111111111111");
  private static final UUID EXECUTION = UUID.fromString("40000000-0000-0000-0000-000000000001");
  private static final String SOURCE =
      """
      document:
        dsl: '1.0.3'
        namespace: wp4
        name: projection
        version: '1.0.0'
      do:
        - initialize:
            set:
              ready: true
      """;

  @Test
  void projectsInOrderIdempotentlyAndRejectsGapsAndEngineMismatch(PostgreSqlTestContainer database)
      throws Exception {
    new OpenWorkflowTenantMigrator(database.dataSource()).provisionAndMigrate(TENANT);
    seedExecution(database);
    try (SessionFactory factory = sessionFactory(database);
        EntityManager entityManager = factory.createEntityManager()) {
      entityManager.getTransaction().begin();
      var store = new JpaExecutionProjectionStore(TENANT, entityManager, new ObjectMapper());
      ExecutionEvent first = event("50000000-0000-0000-0000-000000000001", 0, EngineId.PEKKO);

      assertEquals(ExecutionProjectionStore.ProjectionApplyResult.APPLIED, store.apply(first));
      assertEquals(ExecutionProjectionStore.ProjectionApplyResult.DUPLICATE, store.apply(first));
      assertEquals(
          ExecutionProjectionStore.ProjectionApplyResult.OUT_OF_ORDER,
          store.apply(event("50000000-0000-0000-0000-000000000002", 2, EngineId.PEKKO)));
      assertEquals(
          ExecutionProjectionStore.ProjectionApplyResult.ENGINE_MISMATCH,
          store.apply(event("50000000-0000-0000-0000-000000000003", 1, EngineId.KAFKA_STREAMS)));
      assertEquals(
          ExecutionProjectionStore.ProjectionApplyResult.APPLIED,
          store.apply(event("50000000-0000-0000-0000-000000000004", 1, EngineId.PEKKO)));
      assertEquals(2, count(entityManager, "workflow_execution_history"));
      assertEquals(1, count(entityManager, "workflow_execution_projection"));
      var queries = new JpaExecutionQueryRepository(TENANT, entityManager, new ObjectMapper());
      var executionId =
          new ExecutionId(
              new com.forwardmeasure.openworkflow.engine.api.TenantId(TENANT.value()), EXECUTION);
      assertEquals(2, queries.find(executionId.tenantId(), executionId).orElseThrow().version());
      assertEquals(2, queries.history(executionId.tenantId(), executionId, -1, 10).size());
      assertEquals(
          1,
          queries
              .search(
                  new ExecutionSearch(
                      executionId.tenantId(),
                      Set.of(ExecutionLifecycleState.RUNNING),
                      EngineId.PEKKO,
                      "correlation",
                      null,
                      null,
                      null,
                      10))
              .items()
              .size());
      entityManager.getTransaction().commit();
    }
  }

  private static ExecutionEvent event(String id, long sequence, EngineId engine) {
    var tenant = new com.forwardmeasure.openworkflow.engine.api.TenantId(TENANT.value());
    return new ExecutionEvent(
        UUID.fromString(id),
        UUID.fromString("60000000-0000-0000-0000-000000000001"),
        new ExecutionId(tenant, EXECUTION),
        engine,
        sequence,
        sequence == 0 ? ExecutionEvent.EventType.STARTED : ExecutionEvent.EventType.TASK_COMPLETED,
        ExecutionLifecycleState.RUNNING,
        Instant.parse("2026-08-17T12:00:00Z").plusSeconds(sequence),
        JsonNodeFactory.instance.objectNode().put("taskPath", "do/0"));
  }

  private static long count(EntityManager entityManager, String table) {
    return ((Number)
            entityManager
                .createNativeQuery(
                    "select count(*) from " + TenantSchema.forTenant(TENANT).value() + "." + table)
                .getSingleResult())
        .longValue();
  }

  private static void seedExecution(PostgreSqlTestContainer database) throws SQLException {
    String schema = TenantSchema.forTenant(TENANT).value();
    var plan =
        new com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler()
            .compile(SOURCE.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    try (var connection = database.dataSource().getConnection();
        var statement = connection.createStatement()) {
      statement.executeUpdate(
          "insert into "
              + schema
              + ".actor (id,version,uuid,subject_identifier,identity_type) values"
              + " (1,0,'10000000-0000-0000-0000-000000000001','actor','HUMAN')");
      statement.executeUpdate(
          "insert into "
              + schema
              + ".workflow_definition (id,uuid,definition_key,display_name) values"
              + " (1,'20000000-0000-0000-0000-000000000001','projection','Projection')");
      statement.executeUpdate(
          "insert into "
              + schema
              + ".workflow_revision (id,uuid,definition_id,revision_number,lifecycle_state,"
              + "source_document,resolved_document,resolved_resources,specification_version,compiler_profile,source_digest,resolved_digest,author_actor_id)"
              + " values (1,'30000000-0000-0000-0000-000000000001',1,1,'PUBLISHED',$workflow$"
              + SOURCE
              + "$workflow$,$workflow$"
              + SOURCE
              + "$workflow$,'[]','1.0.3','default','"
              + plan.sourceSha256()
              + "','"
              + plan.definitionSha256()
              + "',1)");
      statement.executeUpdate(
          "insert into "
              + schema
              + ".workflow_execution (id,uuid,revision_id,revision_digest,engine_id,"
              + "lifecycle_state,correlation_id,idempotency_key,started_by_actor_id,input) values"
              + " (1,'"
              + EXECUTION
              + "',1,'"
              + plan.definitionSha256()
              + "','pekko','NEW','correlation','idempotency',1,'{}')");
    }
  }

  private static SessionFactory sessionFactory(PostgreSqlTestContainer database) {
    return new Configuration()
        .setProperty("hibernate.connection.url", database.hostJdbcUrl())
        .setProperty("hibernate.connection.username", database.username())
        .setProperty("hibernate.connection.password", database.password())
        .setProperty("hibernate.connection.driver_class", "org.postgresql.Driver")
        .setProperty("hibernate.default_schema", TenantSchema.forTenant(TENANT).value())
        .setProperty("hibernate.hbm2ddl.auto", "none")
        .buildSessionFactory();
  }
}
