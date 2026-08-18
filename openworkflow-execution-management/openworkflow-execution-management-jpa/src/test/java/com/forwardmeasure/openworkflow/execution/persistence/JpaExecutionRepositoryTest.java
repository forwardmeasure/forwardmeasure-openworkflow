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
package com.forwardmeasure.openworkflow.execution.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.engine.api.ActorId;
import com.forwardmeasure.openworkflow.engine.api.CommandAcknowledgement;
import com.forwardmeasure.openworkflow.engine.api.DefinitionRevision;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import com.forwardmeasure.openworkflow.execution.management.CanonicalExecution;
import com.forwardmeasure.openworkflow.execution.management.CommandReceipt;
import com.forwardmeasure.openworkflow.execution.management.ExecutionManagementException;
import com.forwardmeasure.openworkflow.migration.OpenWorkflowTenantMigrator;
import com.forwardmeasure.testcontainers.junit.postgresql.WithPostgreSqlContainer;
import com.forwardmeasure.testcontainers.postgresql.PostgreSqlTestContainer;
import jakarta.persistence.EntityManager;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.Test;

@WithPostgreSqlContainer(databaseName = "openworkflow_execution_repository")
class JpaExecutionRepositoryTest {
  private static final TenantId TENANT_A = TenantId.parse("11111111-1111-1111-1111-111111111111");
  private static final TenantId TENANT_B = TenantId.parse("22222222-2222-2222-2222-222222222222");
  private static final UUID REVISION = UUID.fromString("30000000-0000-0000-0000-000000000001");
  private static final UUID EXECUTION = UUID.fromString("40000000-0000-0000-0000-000000000001");
  private static final Instant NOW = Instant.parse("2026-08-17T12:00:00Z");
  private static final String SOURCE =
      """
      document:
        dsl: '1.0.3'
        namespace: wp4
        name: persisted
        version: '1.0.0'
      do:
        - initialize:
            set:
              ready: true
      """;

  @Test
  void admitsOncePinsTheEngineAndRejectsCrossTenantAccess(PostgreSqlTestContainer database)
      throws Exception {
    var migrator = new OpenWorkflowTenantMigrator(database.dataSource());
    migrator.provisionAndMigrate(TENANT_A);
    migrator.provisionAndMigrate(TENANT_B);
    var plan = new OpenWorkflowCompiler().compile(SOURCE.getBytes(StandardCharsets.UTF_8));
    seedPublishedRevision(database, TENANT_A, plan.definitionSha256());

    try (SessionFactory factory = sessionFactory(database, TENANT_A);
        EntityManager entityManager = factory.createEntityManager()) {
      entityManager.getTransaction().begin();
      var repository = new JpaExecutionRepository(TENANT_A, entityManager, new ObjectMapper());
      CanonicalExecution candidate = candidate(plan);

      assertTrue(repository.admit(candidate).created());
      assertFalse(repository.admit(candidate).created());
      CanonicalExecution admitted =
          repository.find(engineTenant(TENANT_A), candidate.executionId()).orElseThrow();
      assertEquals(EngineId.PEKKO, admitted.engineId());
      assertEquals(plan.definitionSha256(), admitted.definition().definitionSha256());
      assertThrows(
          ExecutionManagementException.class,
          () -> repository.find(engineTenant(TENANT_B), candidate.executionId()));

      var receipt =
          new CommandReceipt(
              UUID.fromString("50000000-0000-0000-0000-000000000001"),
              admitted.executionId(),
              "PAUSE",
              0,
              true,
              null,
              new ActorId("execution-controller"),
              "pause-correlation",
              NOW);
      assertEquals(receipt.commandId(), repository.recordCommand(receipt).commandId());
      CanonicalExecution acknowledged =
          repository.acknowledge(
              engineTenant(TENANT_A),
              admitted.executionId(),
              new CommandAcknowledgement(
                  receipt.commandId(),
                  admitted.executionId(),
                  EngineId.PEKKO,
                  ExecutionLifecycleState.PAUSED,
                  1,
                  NOW.plusSeconds(1)));
      assertEquals(ExecutionLifecycleState.PAUSED, acknowledged.state());
      assertEquals(1, acknowledged.version());
      entityManager.getTransaction().commit();
    }
  }

  private static CanonicalExecution candidate(
      com.forwardmeasure.openworkflow.definition.WorkflowPlan plan) {
    var tenant = engineTenant(TENANT_A);
    return new CanonicalExecution(
        new ExecutionId(tenant, EXECUTION),
        DefinitionRevision.from(REVISION, plan),
        EngineId.PEKKO,
        ExecutionLifecycleState.NEW,
        0,
        "start-once",
        "start-correlation",
        new ActorId("execution-controller"),
        JsonNodeFactory.instance.objectNode(),
        NOW,
        NOW);
  }

  private static com.forwardmeasure.openworkflow.engine.api.TenantId engineTenant(TenantId tenant) {
    return new com.forwardmeasure.openworkflow.engine.api.TenantId(tenant.value());
  }

  private static void seedPublishedRevision(
      PostgreSqlTestContainer database, TenantId tenantId, String digest) throws SQLException {
    String schema = TenantSchema.forTenant(tenantId).value();
    try (var connection = database.dataSource().getConnection();
        var statement = connection.createStatement()) {
      statement.executeUpdate(
          "insert into "
              + schema
              + ".actor (id,version,uuid,subject_identifier,identity_type,identity_provider) values"
              + " (1,0,'10000000-0000-0000-0000-000000000001','workflow-author','HUMAN','keycloak')");
      statement.execute("select setval('" + schema + ".actor_id_seq', 50, false)");
      statement.executeUpdate(
          "insert into "
              + schema
              + ".workflow_definition (id,uuid,definition_key,display_name) values"
              + " (1,'20000000-0000-0000-0000-000000000001','persisted','Persisted')");
      statement.executeUpdate(
          "insert into "
              + schema
              + ".workflow_revision"
              + " (id,uuid,definition_id,revision_number,lifecycle_state,source_document,"
              + "resolved_document,resolved_resources,specification_version,compiler_profile,source_digest,resolved_digest,author_actor_id)"
              + " values (1,'"
              + REVISION
              + "',1,1,'PUBLISHED',$workflow$"
              + SOURCE
              + "$workflow$,$workflow$"
              + SOURCE
              + "$workflow$,'[]','1.0.3','default','"
              + digest
              + "','"
              + digest
              + "',1)");
      statement.executeUpdate(
          "insert into "
              + schema
              + ".workflow_publication (id,revision_id,actor_id,revision_digest) values"
              + " (1,1,1,'"
              + digest
              + "')");
    }
  }

  private static SessionFactory sessionFactory(
      PostgreSqlTestContainer database, TenantId tenantId) {
    return new Configuration()
        .setProperty("hibernate.connection.url", database.hostJdbcUrl())
        .setProperty("hibernate.connection.username", database.username())
        .setProperty("hibernate.connection.password", database.password())
        .setProperty("hibernate.connection.driver_class", "org.postgresql.Driver")
        .setProperty("hibernate.default_schema", TenantSchema.forTenant(tenantId).value())
        .setProperty("hibernate.hbm2ddl.auto", "none")
        .setProperty("hibernate.show_sql", "false")
        .buildSessionFactory();
  }
}
