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
package com.forwardmeasure.openworkflow.eventing.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.ResolvedSubflow;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceBundleCodec;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.migration.OpenWorkflowTenantMigrator;
import com.forwardmeasure.testcontainers.junit.postgresql.WithPostgreSqlContainer;
import com.forwardmeasure.testcontainers.postgresql.PostgreSqlTestContainer;
import jakarta.persistence.EntityManager;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.UUID;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.Test;

@WithPostgreSqlContainer(databaseName = "openworkflow_subworkflow_resolver")
class JpaSubworkflowPlanResolverTest {
  private static final TenantId TENANT =
      new TenantId(UUID.fromString("134b09a7-1c36-4b89-86e7-a28c88bc5cef"));
  private static final ActorIdentity ACTOR =
      new ActorIdentity(TENANT, "did:forwardmeasure:actor:subworkflow-resolver-test");
  private static final String SOURCE =
      """
      document:
        dsl: '1.0.3'
        namespace: forwardmeasure
        name: subworkflow-child
        version: '1.0.0'
      do:
        - initialize:
            set:
              ready: true
      """;

  @Test
  void resolvesThePublishedRevisionMatchingTheParentsPinnedDigest(PostgreSqlTestContainer database)
      throws Exception {
    var jpaTenant = new com.forwardmeasure.jpa.tenancy.TenantId(TENANT.value());
    var migrator = new OpenWorkflowTenantMigrator(database.dataSource(), "openworkflow_runtime");
    migrator.ensureRuntimeRole(database.password());
    migrator.provisionAndMigrate(jpaTenant);
    var plan = new OpenWorkflowCompiler().compile(SOURCE.getBytes(StandardCharsets.UTF_8));
    seedPublishedRevision(database, jpaTenant, plan);

    try (SessionFactory sessions = sessionFactory(database, jpaTenant);
        EntityManager entityManager = sessions.createEntityManager()) {
      var resolver = new JpaSubworkflowPlanResolver(entityManager);
      var subflow =
          new ResolvedSubflow(plan.coordinates(), plan.sourceSha256(), plan.definitionSha256());
      WorkflowPlan resolved = resolver.resolve(TENANT, ACTOR, subflow);
      assertEquals(plan.definitionSha256(), resolved.definitionSha256());
      assertEquals(plan.sourceSha256(), resolved.sourceSha256());
      assertEquals(plan.coordinates(), resolved.coordinates());
    }
  }

  @Test
  void rejectsAPinnedDigestThatNoLongerMatchesTheCurrentPublication(
      PostgreSqlTestContainer database) throws Exception {
    var jpaTenant = new com.forwardmeasure.jpa.tenancy.TenantId(TENANT.value());
    var migrator = new OpenWorkflowTenantMigrator(database.dataSource(), "openworkflow_runtime");
    migrator.ensureRuntimeRole(database.password());
    migrator.provisionAndMigrate(jpaTenant);
    var plan = new OpenWorkflowCompiler().compile(SOURCE.getBytes(StandardCharsets.UTF_8));
    seedPublishedRevision(database, jpaTenant, plan);

    try (SessionFactory sessions = sessionFactory(database, jpaTenant);
        EntityManager entityManager = sessions.createEntityManager()) {
      var resolver = new JpaSubworkflowPlanResolver(entityManager);
      var stalePin =
          new ResolvedSubflow(
              plan.coordinates(),
              plan.sourceSha256(),
              "0".repeat(64) // a digest that can never match a real compiled plan
              );
      assertThrows(
          JpaSubworkflowPlanResolver.SubworkflowNotPublishedException.class,
          () -> resolver.resolve(TENANT, ACTOR, stalePin));
    }
  }

  @Test
  void rejectsAnUnpublishedCoordinate(PostgreSqlTestContainer database) throws Exception {
    var jpaTenant = new com.forwardmeasure.jpa.tenancy.TenantId(TENANT.value());
    var migrator = new OpenWorkflowTenantMigrator(database.dataSource(), "openworkflow_runtime");
    migrator.ensureRuntimeRole(database.password());
    migrator.provisionAndMigrate(jpaTenant);
    var plan = new OpenWorkflowCompiler().compile(SOURCE.getBytes(StandardCharsets.UTF_8));
    truncateDefinitionTables(database, jpaTenant);

    try (SessionFactory sessions = sessionFactory(database, jpaTenant);
        EntityManager entityManager = sessions.createEntityManager()) {
      var resolver = new JpaSubworkflowPlanResolver(entityManager);
      var subflow =
          new ResolvedSubflow(plan.coordinates(), plan.sourceSha256(), plan.definitionSha256());
      assertThrows(
          JpaSubworkflowPlanResolver.SubworkflowNotPublishedException.class,
          () -> resolver.resolve(TENANT, ACTOR, subflow));
    }
  }

  private static void truncateDefinitionTables(
      PostgreSqlTestContainer database, com.forwardmeasure.jpa.tenancy.TenantId tenantId)
      throws SQLException {
    String schema = TenantSchema.forTenant(tenantId).value();
    try (var connection = database.dataSource().getConnection();
        var statement = connection.createStatement()) {
      statement.execute(
          "truncate table "
              + schema
              + ".workflow_publication,"
              + schema
              + ".workflow_definition,"
              + schema
              + ".workflow,"
              + schema
              + ".actor cascade");
    }
  }

  private static void seedPublishedRevision(
      PostgreSqlTestContainer database,
      com.forwardmeasure.jpa.tenancy.TenantId tenantId,
      WorkflowPlan plan)
      throws SQLException {
    truncateDefinitionTables(database, tenantId);
    String schema = TenantSchema.forTenant(tenantId).value();
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
              + ".workflow (id,version,uuid,name,title,owner_id) values"
              + " (1,0,'20000000-0000-0000-0000-000000000001','subworkflow-child','Subworkflow"
              + " Child',1)");
      statement.executeUpdate(
          "insert into "
              + schema
              + ".workflow_definition"
              + " (id,uuid,version,workflow_id,revision_number,lifecycle_state,source_document,"
              + "resolved_document,resolved_resources,namespace,document_version,"
              + "specification_version,compiler_profile,source_digest,resolved_digest,"
              + "author_actor_id) values (1,'30000000-0000-0000-0000-000000000001',0,1,1,"
              + "'PUBLISHED',$workflow$"
              + SOURCE
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

  private static SessionFactory sessionFactory(
      PostgreSqlTestContainer database, com.forwardmeasure.jpa.tenancy.TenantId tenantId) {
    return new Configuration()
        .setProperty("hibernate.connection.url", database.hostJdbcUrl())
        .setProperty("hibernate.connection.username", database.username())
        .setProperty("hibernate.connection.password", database.password())
        .setProperty("hibernate.connection.driver_class", "org.postgresql.Driver")
        .setProperty("hibernate.default_schema", TenantSchema.forTenant(tenantId).value())
        .setProperty("hibernate.hbm2ddl.auto", "none")
        .buildSessionFactory();
  }
}
