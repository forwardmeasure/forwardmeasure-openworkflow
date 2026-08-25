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
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * Deployment-owned schema creation, runtime-role provisioning, and composed common migration
 * runner. This class always connects as the administrator credential (see {@link
 * com.forwardmeasure.openworkflow.migration.OpenWorkflowMigrationsMain} for the two-credential
 * wiring) - {@code runtimeUsername} names a role this class creates/rotates and grants scoped
 * privileges to, never a role it connects as. See openworkflow-k8s-setup/docs/operations.md's
 * "Database role provisioning" section for the procedure this implements.
 */
public final class OpenWorkflowTenantMigrator {
  public static final String CHANGELOG = "db/changelog/openworkflow-master.xml";

  // Postgres role/identifier names; not user input - sourced from a Kubernetes Secret backed by
  // Terraform-managed GCP Secret Manager - but validated anyway before use in DDL that can't be
  // parameterized via JDBC bind variables.
  private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]{0,62}");
  private static final SecureRandom RANDOM = new SecureRandom();

  private final DataSource dataSource;
  private final TenantSchemaMigrator migrator;
  private final String runtimeUsername;

  public OpenWorkflowTenantMigrator(DataSource dataSource, String runtimeUsername) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    this.runtimeUsername = requireSafeIdentifier(runtimeUsername, "runtimeUsername");
    this.migrator =
        new TenantSchemaMigrator(
            dataSource, CHANGELOG, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Creates the runtime role if it does not already exist, and unconditionally syncs its password -
   * safe to call on every deploy, not just the first. Must run once, before any per-tenant work:
   * the runtime role is one role for the whole database, not per-tenant.
   */
  public void ensureRuntimeRole(String runtimePassword) {
    ensureRuntimeRole(runtimeUsername, runtimePassword);
  }

  /**
   * Same as {@link #ensureRuntimeRole(String)}, but for an arbitrary role rather than this
   * instance's own {@code runtimeUsername} - this admin connection's workload identity is
   * authorized (per operations.md's "Database role provisioning" section and the matching
   * administrator_database_keys/runtime_database_keys grant in Terraform) to provision every
   * declared runtime database's role, not just this application's own. CREATE ROLE / ALTER ROLE are
   * cluster-wide operations, so this works regardless of which database this connection is to.
   * Added because keycloak's and superset's runtime roles were never actually created (Terraform
   * only generates their passwords in Secret Manager, per its own documented
   * database_role_provisioning_required contract) - confirmed the hard way: Keycloak's own pod
   * crash-looped on "password authentication failed for user \"keycloak\"" even though its Secret
   * Manager password was being synced correctly, because the role didn't exist with that password.
   */
  public void ensureRuntimeRole(String runtimeUsername, String runtimePassword) {
    String safeUsername = requireSafeIdentifier(runtimeUsername, "runtimeUsername");
    Objects.requireNonNull(runtimePassword, "runtimePassword");
    String quotedIdentifier = quoteIdentifier(safeUsername);
    String passwordLiteral = dollarQuote(runtimePassword);
    String createIfMissing =
        "DO $$ BEGIN "
            + "IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '"
            + safeUsername
            + "') THEN "
            + "CREATE ROLE "
            + quotedIdentifier
            + " WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD "
            + passwordLiteral
            + "; END IF; END $$;";
    String syncPassword =
        "ALTER ROLE " + quotedIdentifier + " WITH PASSWORD " + passwordLiteral + ";";
    // Role creation alone doesn't grant the creator membership in the new role - Postgres
    // requires SET ROLE privilege (i.e. membership) to transfer ownership of anything to it, and
    // this admin connection is deliberately NOSUPERUSER, so without this grant
    // ensureDatabaseOwnership below fails with "must be able to SET ROLE". Confirmed the hard
    // way: ALTER DATABASE keycloak OWNER TO keycloak failing right after this same connection had
    // just created the keycloak role.
    String grantMembership = "GRANT " + quotedIdentifier + " TO CURRENT_USER;";
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(createIfMissing);
      statement.execute(syncPassword);
      statement.execute(grantMembership);
    } catch (SQLException exception) {
      throw new RuntimeRoleProvisioningException(safeUsername, exception);
    }
  }

  /**
   * Makes {@code runtimeUsername} the owner of {@code databaseName} - for a single-tenant
   * application database that role fully manages itself (Keycloak, Superset: each owns and migrates
   * its own whole database), unlike openworkflow's own multi-tenant database, where the runtime
   * role instead gets scoped per-tenant-schema grants via {@link #provisionAndMigrate}. ALTER
   * DATABASE ... OWNER TO is a catalog-level operation and works regardless of which database this
   * connection is to, same as {@link #ensureRuntimeRole(String, String)}.
   */
  public void ensureDatabaseOwnership(String databaseName, String runtimeUsername) {
    String safeDatabase = requireSafeIdentifier(databaseName, "databaseName");
    String safeUsername = requireSafeIdentifier(runtimeUsername, "runtimeUsername");
    String sql =
        "ALTER DATABASE "
            + quoteIdentifier(safeDatabase)
            + " OWNER TO "
            + quoteIdentifier(safeUsername)
            + ";";
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException exception) {
      throw new RuntimeRoleProvisioningException(safeUsername, exception);
    }
  }

  public MigrationResult provisionAndMigrate(TenantId tenantId) {
    TenantSchema schema = TenantSchema.forTenant(Objects.requireNonNull(tenantId, "tenantId"));
    createSchema(schema);
    MigrationResult result = migrator.migrate(schema);
    grantRuntimeAccess(schema);
    return result;
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

  /**
   * Grants the runtime role only what it needs within this tenant's own schema - never ownership,
   * never anything outside this schema, satisfying operations.md's isolation requirement
   * structurally rather than by separate verification.
   */
  private void grantRuntimeAccess(TenantSchema schema) {
    String quotedSchema = "\"" + schema.value() + "\"";
    String quotedIdentifier = quoteIdentifier(runtimeUsername);
    String sql =
        "GRANT USAGE ON SCHEMA "
            + quotedSchema
            + " TO "
            + quotedIdentifier
            + "; GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "
            + quotedSchema
            + " TO "
            + quotedIdentifier
            + "; GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA "
            + quotedSchema
            + " TO "
            + quotedIdentifier
            + "; ALTER DEFAULT PRIVILEGES IN SCHEMA "
            + quotedSchema
            + " GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "
            + quotedIdentifier
            + "; ALTER DEFAULT PRIVILEGES IN SCHEMA "
            + quotedSchema
            + " GRANT USAGE, SELECT ON SEQUENCES TO "
            + quotedIdentifier
            + ";";
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException exception) {
      throw new TenantSchemaProvisioningException(schema, exception);
    }
  }

  private static String requireSafeIdentifier(String value, String name) {
    Objects.requireNonNull(value, name);
    if (!SAFE_IDENTIFIER.matcher(value).matches()) {
      throw new IllegalArgumentException(name + " must be a safe SQL identifier: " + value);
    }
    return value;
  }

  private static String quoteIdentifier(String identifier) {
    return "\"" + identifier + "\"";
  }

  // Postgres DDL (CREATE ROLE / ALTER ROLE ... PASSWORD) does not support JDBC bind parameters -
  // dollar-quoting with a randomly generated tag safely embeds an arbitrary password literal
  // (including one containing quotes or backslashes) without string-concatenation escaping.
  private static String dollarQuote(String value) {
    String tag;
    do {
      tag = "pw" + Long.toHexString(RANDOM.nextLong());
    } while (value.contains("$" + tag + "$"));
    return "$" + tag + "$" + value + "$" + tag + "$";
  }
}
