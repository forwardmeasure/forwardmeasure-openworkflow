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
package com.forwardmeasure.openworkflow.migration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.testcontainers.junit.postgresql.WithPostgreSqlContainer;
import com.forwardmeasure.testcontainers.postgresql.PostgreSqlTestContainer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Direct SQL-level proof of the {@code prevent_workflow_definition_content_update} trigger
 * (changeset {@code openworkflow-121-immutable-workflow-definition}): content is mutable only while
 * DRAFT, and identity/authorship never change regardless of state.
 */
@WithPostgreSqlContainer(databaseName = "openworkflow_migrations")
class WorkflowDefinitionDraftMutabilityTest {

  @Test
  void contentIsMutableInDraftAndLockedOnceSubmittedIdentityAndAuthorNeverChange(
      PostgreSqlTestContainer database) throws Exception {
    TenantId tenantId = new TenantId(UUID.fromString("33333333-3333-3333-3333-333333333333"));
    OpenWorkflowTenantMigrator migrator =
        new OpenWorkflowTenantMigrator(database.dataSource(), database.username());
    migrator.ensureRuntimeRole(database.password());
    migrator.provisionAndMigrate(tenantId);
    String schema = TenantSchema.forTenant(tenantId).value();

    try (Connection connection = database.dataSource().getConnection()) {
      connection.setAutoCommit(true);
      long actorId = insertActor(connection, schema, "actor-1");
      long otherActorId = insertActor(connection, schema, "actor-2");
      long workflowId = insertWorkflow(connection, schema, actorId);
      long definitionId = insertDraftDefinition(connection, schema, workflowId, actorId);

      assertDoesNotThrow(
          () -> updateSourceDocument(connection, schema, definitionId, "source v2"),
          "a DRAFT definition's content must be editable in place");

      execute(
          connection,
          "update "
              + schema
              + ".workflow_definition set lifecycle_state = 'IN_REVIEW' where id = "
              + definitionId);

      SQLException blocked =
          assertThrows(
              SQLException.class,
              () -> updateSourceDocument(connection, schema, definitionId, "source v3"),
              "content must be immutable once a definition has left DRAFT");
      assertTrue(blocked.getMessage().contains("immutable once submitted for review"));

      execute(
          connection,
          "update "
              + schema
              + ".workflow_definition set lifecycle_state = 'DRAFT' where id = "
              + definitionId);
      SQLException identityBlocked =
          assertThrows(
              SQLException.class,
              () -> updateAuthor(connection, schema, definitionId, otherActorId),
              "authorship must never change, even while DRAFT");
      assertTrue(identityBlocked.getMessage().contains("identity and authorship are immutable"));
    }
  }

  private static void updateSourceDocument(
      Connection connection, String schema, long definitionId, String source) throws SQLException {
    try (var statement =
        connection.prepareStatement(
            "update " + schema + ".workflow_definition set source_document = ? where id = ?")) {
      statement.setString(1, source);
      statement.setLong(2, definitionId);
      statement.executeUpdate();
    }
  }

  private static void updateAuthor(
      Connection connection, String schema, long definitionId, long actorId) throws SQLException {
    try (var statement =
        connection.prepareStatement(
            "update " + schema + ".workflow_definition set author_actor_id = ? where id = ?")) {
      statement.setLong(1, actorId);
      statement.setLong(2, definitionId);
      statement.executeUpdate();
    }
  }

  private static long insertActor(Connection connection, String schema, String subject)
      throws SQLException {
    try (var statement =
        connection.prepareStatement(
            "insert into "
                + schema
                + ".actor (id, version, uuid, subject_identifier, identity_type,"
                + " identity_provider) values (nextval('"
                + schema
                + ".actor_id_seq'), 0, ?, ?, 'HUMAN', 'test') returning id")) {
      statement.setObject(1, UUID.randomUUID());
      statement.setString(2, subject);
      try (var result = statement.executeQuery()) {
        result.next();
        return result.getLong(1);
      }
    }
  }

  private static long insertWorkflow(Connection connection, String schema, long ownerActorId)
      throws SQLException {
    try (var statement =
        connection.prepareStatement(
            "insert into "
                + schema
                + ".workflow (id, uuid, version, name, title, owner_id) values"
                + " (nextval('"
                + schema
                + ".workflow_id_seq'), ?, 0, ?, 'Draft mutability test', ?) returning id")) {
      statement.setObject(1, UUID.randomUUID());
      statement.setString(2, "draft-mutability-" + UUID.randomUUID());
      statement.setLong(3, ownerActorId);
      try (var result = statement.executeQuery()) {
        result.next();
        return result.getLong(1);
      }
    }
  }

  private static long insertDraftDefinition(
      Connection connection, String schema, long workflowId, long authorActorId)
      throws SQLException {
    try (var statement =
        connection.prepareStatement(
            "insert into "
                + schema
                + ".workflow_definition (id, uuid, version, workflow_id, revision_number,"
                + " lifecycle_state, source_document, resolved_document, resolved_resources,"
                + " namespace, document_version, specification_version, compiler_profile,"
                + " source_digest, resolved_digest, author_actor_id) values (nextval('"
                + schema
                + ".workflow_definition_id_seq'), ?, 0, ?, 1, 'DRAFT', 'source v1',"
                + " 'resolved v1', '[]', 'draft-mutability', '1.0.0', '1.0.3', 'test-profile',"
                + " '0000000000000000000000000000000000000000000000000000000000000001',"
                + " '0000000000000000000000000000000000000000000000000000000000000002', ?)"
                + " returning id")) {
      statement.setObject(1, UUID.randomUUID());
      statement.setLong(2, workflowId);
      statement.setLong(3, authorActorId);
      try (var result = statement.executeQuery()) {
        result.next();
        return result.getLong(1);
      }
    }
  }

  private static void execute(Connection connection, String sql) throws SQLException {
    try (var statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }
}
