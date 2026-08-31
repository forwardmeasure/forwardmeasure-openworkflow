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

import com.forwardmeasure.jpa.tenancy.TenantSchema;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * Discovers which tenant schemas actually exist in Postgres - the one DB-native signal that
 * reflects "a tenant was really provisioned" ({@link OpenWorkflowTenantMigrator#createSchema}
 * creates exactly the {@code t_%} pattern queried here). No local tenant registry (JPA entity,
 * table, or otherwise) exists anywhere in this codebase; this is the only reliable enumeration.
 */
public final class ProvisionedTenantSchemas {
  private static final Logger LOG = Logger.getLogger(ProvisionedTenantSchemas.class.getName());
  private static final String QUERY =
      "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE ? ESCAPE '\\'";
  private static final String TENANT_SCHEMA_PATTERN = "t\\_%";

  private ProvisionedTenantSchemas() {}

  public static List<TenantSchema> scan(DataSource dataSource) {
    Objects.requireNonNull(dataSource, "dataSource");
    List<TenantSchema> schemas = new ArrayList<>();
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(QUERY)) {
      statement.setString(1, TENANT_SCHEMA_PATTERN);
      try (ResultSet rows = statement.executeQuery()) {
        while (rows.next()) {
          String name = rows.getString(1);
          try {
            schemas.add(new TenantSchema(name));
          } catch (IllegalArgumentException notATenantSchema) {
            LOG.log(
                Level.WARNING, "Skipping schema that matched t_% but failed validation: {0}", name);
          }
        }
      }
    } catch (SQLException failure) {
      throw new IllegalStateException("Could not enumerate provisioned tenant schemas", failure);
    }
    return schemas;
  }
}
