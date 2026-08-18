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

import com.forwardmeasure.database.migration.api.MigrationResult;
import com.forwardmeasure.jpa.liquibase.TenantSchemaMigrator;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import javax.sql.DataSource;

/** Deployment-owned schema creation and composed common migration runner. */
public final class OpenWorkflowTenantMigrator {
  public static final String CHANGELOG = "db/changelog/openworkflow-master.xml";

  private final DataSource dataSource;
  private final TenantSchemaMigrator migrator;

  public OpenWorkflowTenantMigrator(DataSource dataSource) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    this.migrator =
        new TenantSchemaMigrator(
            dataSource, CHANGELOG, Thread.currentThread().getContextClassLoader());
  }

  public MigrationResult provisionAndMigrate(TenantId tenantId) {
    TenantSchema schema = TenantSchema.forTenant(Objects.requireNonNull(tenantId, "tenantId"));
    createSchema(schema);
    return migrator.migrate(schema);
  }

  private void createSchema(TenantSchema schema) {
    // TenantSchema is the sole constructor of this identifier and accepts only t_{32 lowercase
    // hex}.
    String sql = "CREATE SCHEMA IF NOT EXISTS \"" + schema.value() + "\"";
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException exception) {
      throw new TenantSchemaProvisioningException(schema, exception);
    }
  }
}
