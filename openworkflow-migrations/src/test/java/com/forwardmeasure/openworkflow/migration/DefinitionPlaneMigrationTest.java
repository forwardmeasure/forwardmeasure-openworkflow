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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.testcontainers.junit.postgresql.WithPostgreSqlContainer;
import com.forwardmeasure.testcontainers.postgresql.PostgreSqlTestContainer;
import java.sql.Connection;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

@WithPostgreSqlContainer(databaseName = "openworkflow_migrations")
class DefinitionPlaneMigrationTest {
  private static final TenantId TENANT_A =
      new TenantId(UUID.fromString("11111111-1111-1111-1111-111111111111"));
  private static final TenantId TENANT_B =
      new TenantId(UUID.fromString("22222222-2222-2222-2222-222222222222"));

  @Test
  void installsDefinitionPlaneIndependentlyIntoEveryTenantSchema(PostgreSqlTestContainer database)
      throws Exception {
    OpenWorkflowTenantMigrator migrator =
        new OpenWorkflowTenantMigrator(database.dataSource(), database.username());
    migrator.ensureRuntimeRole(database.password());

    migrator.provisionAndMigrate(TENANT_A);
    migrator.provisionAndMigrate(TENANT_B);
    migrator.provisionAndMigrate(TENANT_A);

    List<String> expected =
        List.of(
            "openworkflow_event_subscription",
            "pekko_projection_management",
            "pekko_projection_offset_store",
            "workflow",
            "workflow_authorization_audit",
            "workflow_command_receipt",
            "workflow_definition",
            "workflow_execution",
            "workflow_execution_history",
            "workflow_execution_projection",
            "workflow_lifecycle_history",
            "workflow_publication",
            "workflow_review");
    assertEquals(expected, applicationTables(database, TenantSchema.forTenant(TENANT_A).value()));
    assertEquals(expected, applicationTables(database, TenantSchema.forTenant(TENANT_B).value()));
    assertEquals(22, changeSetCount(database, TenantSchema.forTenant(TENANT_A).value()));
    assertEquals(22, changeSetCount(database, TenantSchema.forTenant(TENANT_B).value()));
    // openworkflow-170 unconditionally aligns every definition-plane sequence to the next
    // 50-boundary, even on a fresh schema - matches incrementBy=50 on these sequences, which
    // matches allocationSize=50 on each entity's @SequenceGenerator.
    assertEquals(50, nextSequenceValue(database, TenantSchema.forTenant(TENANT_A).value()));
    assertEquals(50, nextSequenceValue(database, TenantSchema.forTenant(TENANT_B).value()));
  }

  private static List<String> applicationTables(PostgreSqlTestContainer database, String schema)
      throws Exception {
    try (Connection connection = database.dataSource().getConnection();
        var statement =
            connection.prepareStatement(
                "select table_name from information_schema.tables where table_schema = ? and"
                    + " (table_name = 'openworkflow_event_subscription' or table_name = 'workflow'"
                    + " or table_name like 'workflow\\_%' escape '\\' or"
                    + " table_name like 'pekko_projection_%') order by table_name")) {
      statement.setString(1, schema);
      try (var result = statement.executeQuery()) {
        var tables = new java.util.ArrayList<String>();
        while (result.next()) {
          tables.add(result.getString(1));
        }
        return List.copyOf(tables);
      }
    }
  }

  private static int changeSetCount(PostgreSqlTestContainer database, String schema)
      throws Exception {
    try (Connection connection = database.dataSource().getConnection();
        var statement =
            connection.prepareStatement("select count(*) from " + schema + ".databasechangelog");
        var result = statement.executeQuery()) {
      result.next();
      return result.getInt(1);
    }
  }

  private static long nextSequenceValue(PostgreSqlTestContainer database, String schema)
      throws Exception {
    try (Connection connection = database.dataSource().getConnection();
        var statement =
            connection.prepareStatement(
                "select nextval('" + schema + ".workflow_definition_id_seq')");
        var result = statement.executeQuery()) {
      result.next();
      return result.getLong(1);
    }
  }
}
