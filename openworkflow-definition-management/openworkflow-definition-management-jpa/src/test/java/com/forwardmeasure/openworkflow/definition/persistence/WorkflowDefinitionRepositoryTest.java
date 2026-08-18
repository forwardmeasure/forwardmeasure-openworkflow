/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.forwardmeasure.openworkflow.definition.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.openworkflow.definition.management.DefinitionManagementException;
import com.forwardmeasure.openworkflow.definition.management.ManagedWorkflowRevision;
import com.forwardmeasure.openworkflow.definition.management.WorkflowLifecycleState;
import com.forwardmeasure.openworkflow.migration.OpenWorkflowTenantMigrator;
import com.forwardmeasure.testcontainers.junit.postgresql.WithPostgreSqlContainer;
import com.forwardmeasure.testcontainers.postgresql.PostgreSqlTestContainer;
import jakarta.persistence.EntityManager;
import java.sql.SQLException;
import java.util.UUID;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.Test;

@WithPostgreSqlContainer(databaseName = "openworkflow_definition_repository")
class WorkflowDefinitionRepositoryTest {
  private static final TenantId TENANT_A = TenantId.parse("11111111-1111-1111-1111-111111111111");
  private static final TenantId TENANT_B = TenantId.parse("22222222-2222-2222-2222-222222222222");

  @Test
  void persistsImmutableRevisionsWithoutCrossTenantVisibility(PostgreSqlTestContainer database)
      throws Exception {
    OpenWorkflowTenantMigrator migrator = new OpenWorkflowTenantMigrator(database.dataSource());
    migrator.provisionAndMigrate(TENANT_A);
    migrator.provisionAndMigrate(TENANT_B);
    seedActor(database, TENANT_A);
    seedActor(database, TENANT_B);

    try (SessionFactory tenantA = sessionFactory(database, TENANT_A);
        SessionFactory tenantB = sessionFactory(database, TENANT_B)) {
      long revisionId = persistRevision(tenantA);

      assertEquals("order-fulfilment", findRevision(tenantA, 1).getDefinition().getDefinitionKey());
      assertEquals("a".repeat(64), findRevision(tenantA, 1).getSourceDigest());
      assertJpaAdapter(tenantA);
      assertFalse(findDefinition(tenantB, "order-fulfilment"));
      assertFalse(findRevisionOptional(tenantB, 1));

      transitionThroughAdapter(tenantA);
      assertEquals(WorkflowLifecycleState.IN_REVIEW, findRevision(tenantA, 1).getLifecycleState());
      assertGovernanceEvidence(database, TENANT_A);
      assertThrows(SQLException.class, () -> mutateImmutableSource(database, TENANT_A, revisionId));
    }
  }

  private static void assertJpaAdapter(SessionFactory factory) {
    try (EntityManager entityManager = factory.createEntityManager()) {
      var adapter = new JpaDefinitionRepository(TENANT_A, entityManager);
      assertEquals(
          "wp3-author",
          adapter.find(TENANT_A, "order-fulfilment", 1).orElseThrow().authorActorId());
      assertEquals(1, adapter.list(TENANT_A).size());
      assertEquals(2, adapter.nextRevisionNumber(TENANT_A, "order-fulfilment"));
      assertThrows(
          DefinitionManagementException.class, () -> adapter.exists(TENANT_B, "order-fulfilment"));
    }
  }

  private static long persistRevision(SessionFactory factory) {
    try (EntityManager entityManager = factory.createEntityManager()) {
      entityManager.getTransaction().begin();
      var revisions = new WorkflowRevisionRepository();
      revisions.bindPersistenceContext(entityManager);
      var revision =
          new ManagedWorkflowRevision(
              UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
              "order-fulfilment",
              "Order fulfilment",
              1,
              WorkflowLifecycleState.DRAFT,
              "document: source",
              "document: resolved",
              "1.0.3",
              "default",
              "a".repeat(64),
              "b".repeat(64),
              "wp3-author");
      new JpaDefinitionRepository(TENANT_A, entityManager)
          .save(TENANT_A, revision, "wp3-author", "create-correlation");
      entityManager.getTransaction().commit();
      return revisions.findRevision("order-fulfilment", 1).orElseThrow().getId();
    }
  }

  private static WorkflowRevisionEntity findRevision(SessionFactory factory, int number) {
    try (EntityManager entityManager = factory.createEntityManager()) {
      var repository = new WorkflowRevisionRepository();
      repository.bindPersistenceContext(entityManager);
      return repository.findRevision("order-fulfilment", number).orElseThrow();
    }
  }

  private static boolean findRevisionOptional(SessionFactory factory, int number) {
    try (EntityManager entityManager = factory.createEntityManager()) {
      var repository = new WorkflowRevisionRepository();
      repository.bindPersistenceContext(entityManager);
      return repository.findRevision("order-fulfilment", number).isPresent();
    }
  }

  private static boolean findDefinition(SessionFactory factory, String key) {
    try (EntityManager entityManager = factory.createEntityManager()) {
      var repository = new WorkflowDefinitionRepository();
      repository.bindPersistenceContext(entityManager);
      return repository.findByKey(key).isPresent();
    }
  }

  private static void transitionThroughAdapter(SessionFactory factory) {
    try (EntityManager entityManager = factory.createEntityManager()) {
      entityManager.getTransaction().begin();
      var adapter = new JpaDefinitionRepository(TENANT_A, entityManager);
      ManagedWorkflowRevision revision =
          adapter.find(TENANT_A, "order-fulfilment", 1).orElseThrow();
      adapter.save(
          TENANT_A,
          revision.transitionTo(WorkflowLifecycleState.IN_REVIEW),
          "wp3-author",
          "submit-correlation");
      entityManager.getTransaction().commit();
    }
  }

  private static void assertGovernanceEvidence(PostgreSqlTestContainer database, TenantId tenantId)
      throws SQLException {
    String schema = TenantSchema.forTenant(tenantId).value();
    try (var connection = database.dataSource().getConnection();
        var statement = connection.createStatement()) {
      try (var rows =
          statement.executeQuery("select count(*) from " + schema + ".workflow_validation")) {
        assertTrue(rows.next());
        assertEquals(1, rows.getInt(1));
      }
      try (var rows =
          statement.executeQuery(
              "select count(*) from " + schema + ".workflow_lifecycle_history")) {
        assertTrue(rows.next());
        assertEquals(2, rows.getInt(1));
      }
    }
  }

  private static SessionFactory sessionFactory(
      PostgreSqlTestContainer database, TenantId tenantId) {
    return new Configuration()
        .addAnnotatedClass(WorkflowDefinitionEntity.class)
        .addAnnotatedClass(WorkflowRevisionEntity.class)
        .setProperty("hibernate.connection.url", database.hostJdbcUrl())
        .setProperty("hibernate.connection.username", database.username())
        .setProperty("hibernate.connection.password", database.password())
        .setProperty("hibernate.connection.driver_class", "org.postgresql.Driver")
        .setProperty("hibernate.default_schema", TenantSchema.forTenant(tenantId).value())
        .setProperty("hibernate.hbm2ddl.auto", "validate")
        .setProperty("hibernate.show_sql", "false")
        .buildSessionFactory();
  }

  private static void seedActor(PostgreSqlTestContainer database, TenantId tenantId)
      throws SQLException {
    String schema = TenantSchema.forTenant(tenantId).value();
    try (var connection = database.dataSource().getConnection();
        var statement =
            connection.prepareStatement(
                "insert into "
                    + schema
                    + ".actor"
                    + " (id,version,uuid,created_at,updated_at,subject_identifier,identity_type)"
                    + " values (1,0,?,current_timestamp,current_timestamp,'wp3-author','USER')")) {
      statement.setObject(1, UUID.randomUUID());
      assertEquals(1, statement.executeUpdate());
    }
  }

  private static void mutateImmutableSource(
      PostgreSqlTestContainer database, TenantId tenantId, long revisionId) throws SQLException {
    String schema = TenantSchema.forTenant(tenantId).value();
    try (var connection = database.dataSource().getConnection();
        var statement =
            connection.prepareStatement(
                "update "
                    + schema
                    + ".workflow_revision set source_document = 'tampered' where id = ?")) {
      statement.setLong(1, revisionId);
      assertTrue(statement.executeUpdate() > 0);
    }
  }
}
