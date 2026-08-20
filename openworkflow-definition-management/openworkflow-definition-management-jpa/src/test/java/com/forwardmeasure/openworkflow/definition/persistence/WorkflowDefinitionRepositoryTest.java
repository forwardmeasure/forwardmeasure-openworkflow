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
package com.forwardmeasure.openworkflow.definition.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.jpa.identity.entity.Actor;
import com.forwardmeasure.jpa.identity.entity.IdentityType;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.openworkflow.definition.domain.entity.Workflow;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowDefinition;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowLifecycleHistory;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowLifecycleState;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowPublication;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowReview;
import com.forwardmeasure.openworkflow.definition.management.domain.repository.jpa.JpaWorkflowDefinitionRepository;
import com.forwardmeasure.openworkflow.definition.management.domain.repository.jpa.JpaWorkflowLifecycleHistoryRepository;
import com.forwardmeasure.openworkflow.definition.management.domain.repository.jpa.JpaWorkflowPublicationRepository;
import com.forwardmeasure.openworkflow.definition.management.domain.repository.jpa.JpaWorkflowRepository;
import com.forwardmeasure.openworkflow.definition.management.domain.repository.jpa.JpaWorkflowReviewRepository;
import com.forwardmeasure.openworkflow.migration.OpenWorkflowTenantMigrator;
import com.forwardmeasure.testcontainers.junit.postgresql.WithPostgreSqlContainer;
import com.forwardmeasure.testcontainers.postgresql.PostgreSqlTestContainer;
import jakarta.persistence.EntityManager;
import java.util.UUID;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.Test;

@WithPostgreSqlContainer(databaseName = "openworkflow_definition_repository")
class WorkflowDefinitionRepositoryTest {
  private static final TenantId TENANT_A = TenantId.parse("11111111-1111-1111-1111-111111111111");
  private static final TenantId TENANT_B = TenantId.parse("22222222-2222-2222-2222-222222222222");

  @Test
  void persistsWorkflowAndDefinitionsWithoutCrossTenantVisibility(
      PostgreSqlTestContainer database) {
    OpenWorkflowTenantMigrator migrator = new OpenWorkflowTenantMigrator(database.dataSource());
    migrator.provisionAndMigrate(TENANT_A);
    migrator.provisionAndMigrate(TENANT_B);

    try (SessionFactory tenantA = sessionFactory(database, TENANT_A);
        SessionFactory tenantB = sessionFactory(database, TENANT_B)) {
      UUID workflowId = persistWorkflowAndDefinition(tenantA);

      try (EntityManager entityManager = tenantA.createEntityManager()) {
        JpaWorkflowRepository workflows = new JpaWorkflowRepository(entityManager);
        Workflow workflow = workflows.findById(workflowId).orElseThrow();
        JpaWorkflowDefinitionRepository definitions =
            new JpaWorkflowDefinitionRepository(entityManager);
        assertTrue(workflows.existsByName("order-fulfilment"));
        assertEquals(1, definitions.listByWorkflow(workflow, null, 0, 20).totalItems());
        assertEquals(2, definitions.nextRevisionNumber(workflow));
      }

      try (EntityManager entityManager = tenantB.createEntityManager()) {
        JpaWorkflowRepository workflows = new JpaWorkflowRepository(entityManager);
        assertFalse(workflows.existsByName("order-fulfilment"));
        assertTrue(workflows.findById(workflowId).isEmpty());
      }
    }
  }

  @Test
  void persistsTheCompleteGovernanceHistoryAndOneImmutablePublication(
      PostgreSqlTestContainer database) {
    new OpenWorkflowTenantMigrator(database.dataSource()).provisionAndMigrate(TENANT_A);

    try (SessionFactory factory = sessionFactory(database, TENANT_A);
        EntityManager entityManager = factory.createEntityManager()) {
      entityManager.getTransaction().begin();
      var workflows = new JpaWorkflowRepository(entityManager);
      var definitions = new JpaWorkflowDefinitionRepository(entityManager);
      var reviews = new JpaWorkflowReviewRepository(entityManager);
      var publications = new JpaWorkflowPublicationRepository(entityManager);
      var history = new JpaWorkflowLifecycleHistoryRepository(entityManager);
      Workflow workflow =
          workflows.create("author", "governed-flow", "Governed flow", "Governance test");
      WorkflowDefinition definition =
          definitions.create(
              workflow,
              definitions.nextRevisionNumber(workflow),
              "author",
              "document: source",
              "document: resolved",
              "[]",
              "tests",
              "1.0.0",
              "1.0.3",
              "default",
              "a".repeat(64),
              "b".repeat(64));
      history.record(
          definition, null, WorkflowLifecycleState.DRAFT, "author", "create-correlation");
      definition.transitionTo(WorkflowLifecycleState.IN_REVIEW);
      history.record(
          definition,
          WorkflowLifecycleState.DRAFT,
          WorkflowLifecycleState.IN_REVIEW,
          "author",
          "submit-correlation");
      reviews.record(definition, "APPROVED", "reviewer", definition.getResolvedDigest(), "safe");
      definition.transitionTo(WorkflowLifecycleState.APPROVED);
      history.record(
          definition,
          WorkflowLifecycleState.IN_REVIEW,
          WorkflowLifecycleState.APPROVED,
          "reviewer",
          "approve-correlation");
      definition.transitionTo(WorkflowLifecycleState.PUBLISHED);
      publications.publish(definition, "publisher", definition.getResolvedDigest());
      history.record(
          definition,
          WorkflowLifecycleState.APPROVED,
          WorkflowLifecycleState.PUBLISHED,
          "publisher",
          "publish-correlation");
      UUID workflowId = workflow.getUuid();
      UUID definitionId = definition.getUuid();
      entityManager.getTransaction().commit();

      entityManager.clear();
      Workflow reloadedWorkflow = workflows.findById(workflowId).orElseThrow();
      WorkflowDefinition reloaded =
          definitions.findByWorkflowAndUuid(reloadedWorkflow, definitionId).orElseThrow();
      assertEquals(WorkflowLifecycleState.PUBLISHED, reloaded.getLifecycleState());
      assertEquals("author", reloaded.getAuthor().getSubjectIdentifier());
      assertEquals("publisher", reloaded.getPublication().getActor().getSubjectIdentifier());
      assertEquals(1, definitions.search(WorkflowLifecycleState.PUBLISHED, 0, 20).totalItems());
      assertEquals(1L, count(entityManager, WorkflowReview.class));
      assertEquals(1L, count(entityManager, WorkflowPublication.class));
      assertEquals(4L, count(entityManager, WorkflowLifecycleHistory.class));

      entityManager.getTransaction().begin();
      reloaded.getPublication().deprecate();
      entityManager.getTransaction().commit();
      assertTrue(reloaded.getPublication().getDeprecatedAt() != null);
    }
  }

  private static long count(EntityManager entityManager, Class<?> type) {
    return entityManager
        .createQuery("select count(entity) from " + type.getSimpleName() + " entity", Long.class)
        .getSingleResult();
  }

  private static UUID persistWorkflowAndDefinition(SessionFactory factory) {
    try (EntityManager entityManager = factory.createEntityManager()) {
      entityManager.getTransaction().begin();
      Actor actor =
          Actor.builder()
              .subjectIdentifier("author")
              .type(IdentityType.HUMAN)
              .identityProvider("test")
              .build();
      entityManager.persist(actor);
      entityManager.flush();

      Workflow workflow =
          new JpaWorkflowRepository(entityManager)
              .create("author", "order-fulfilment", "Order fulfilment", "Test workflow");
      new JpaWorkflowDefinitionRepository(entityManager)
          .create(
              workflow,
              1,
              "author",
              "document: source",
              "document: resolved",
              "[]",
              "tests",
              "1.0.0",
              "1.0.3",
              "default",
              "a".repeat(64),
              "b".repeat(64));
      entityManager.getTransaction().commit();
      return workflow.getUuid();
    }
  }

  private static SessionFactory sessionFactory(
      PostgreSqlTestContainer database, TenantId tenantId) {
    return new Configuration()
        .addAnnotatedClass(Actor.class)
        .addAnnotatedClass(Workflow.class)
        .addAnnotatedClass(WorkflowDefinition.class)
        .addAnnotatedClass(WorkflowReview.class)
        .addAnnotatedClass(WorkflowPublication.class)
        .addAnnotatedClass(WorkflowLifecycleHistory.class)
        .setProperty("hibernate.connection.url", database.hostJdbcUrl())
        .setProperty("hibernate.connection.username", database.username())
        .setProperty("hibernate.connection.password", database.password())
        .setProperty("hibernate.connection.driver_class", "org.postgresql.Driver")
        .setProperty("hibernate.default_schema", TenantSchema.forTenant(tenantId).value())
        .setProperty("hibernate.hbm2ddl.auto", "validate")
        .setProperty("hibernate.show_sql", "false")
        .buildSessionFactory();
  }
}
