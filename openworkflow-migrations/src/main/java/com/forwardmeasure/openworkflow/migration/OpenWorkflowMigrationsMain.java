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
package com.forwardmeasure.openworkflow.migration;

import com.forwardmeasure.jpa.tenancy.TenantId;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import javax.sql.DataSource;

/** Bounded Kubernetes migration-job entry point. */
public final class OpenWorkflowMigrationsMain {
  private OpenWorkflowMigrationsMain() {}

  public static void main(String[] arguments) {
    // Administrator credential - this process's own connection. Creates/rotates the runtime
    // role below and applies schema migrations; never the role application services connect as.
    String url = required("OPENWORKFLOW_DATABASE_URL");
    String username = required("OPENWORKFLOW_DATABASE_USERNAME");
    String password = required("OPENWORKFLOW_DATABASE_PASSWORD");
    // Runtime credential - never connected as here, only used to create/rotate that role and
    // grant it scoped, per-tenant-schema privileges. See
    // openworkflow-k8s-setup/docs/operations.md's "Database role provisioning" section.
    String runtimeUsername = required("OPENWORKFLOW_RUNTIME_DATABASE_USERNAME");
    String runtimePassword = required("OPENWORKFLOW_RUNTIME_DATABASE_PASSWORD");
    OpenWorkflowTenantMigrator migrator =
        new OpenWorkflowTenantMigrator(
            new DriverManagerDataSource(url, username, password), runtimeUsername);
    migrator.ensureRuntimeRole(runtimePassword);
    Arrays.stream(required("OPENWORKFLOW_TENANT_IDS").split(","))
        .map(String::trim)
        .filter(value -> !value.isEmpty())
        .map(TenantId::parse)
        .forEach(migrator::provisionAndMigrate);
    provisionAdditionalRuntimeDatabasesIfConfigured(migrator);
    migrateCassandraIfConfigured();
  }

  // This admin credential's workload identity is authorized (administrator_database_keys in
  // Terraform) to provision every declared runtime database's role, not just openworkflow's own
  // - see openworkflow-k8s-setup/docs/operations.md's "Database role provisioning" section. Other
  // applications on this same Cloud SQL instance (Keycloak, Superset) fully own and migrate their
  // own database themselves once their role exists - unlike openworkflow's per-tenant-schema
  // grants, they just need CREATE ROLE + ALTER DATABASE ... OWNER TO, once, here. Username always
  // equals the database name, matching Terraform's own runtime_username = database convention for
  // every entry in var.databases (confirmed directly against terraform.tfvars).
  private static void provisionAdditionalRuntimeDatabasesIfConfigured(
      OpenWorkflowTenantMigrator migrator) {
    String databases = optional("OPENWORKFLOW_ADDITIONAL_RUNTIME_DATABASES");
    if (databases == null) {
      return;
    }
    Arrays.stream(databases.split(","))
        .map(String::trim)
        .filter(value -> !value.isEmpty())
        .forEach(
            database -> {
              String passwordEnvVar =
                  "OPENWORKFLOW_RUNTIME_DATABASE_"
                      + database.toUpperCase(java.util.Locale.ROOT).replace('-', '_')
                      + "_PASSWORD";
              String databasePassword = required(passwordEnvVar);
              migrator.ensureRuntimeRole(database, databasePassword);
              migrator.ensureDatabaseOwnership(database, database);
            });
  }

  private static void migrateCassandraIfConfigured() {
    String contactPoints = optional("OPENWORKFLOW_CASSANDRA_CONTACT_POINTS");
    if (contactPoints == null) {
      return;
    }
    String localDatacenter = required("OPENWORKFLOW_CASSANDRA_LOCAL_DATACENTER");
    List<InetSocketAddress> endpoints =
        Arrays.stream(contactPoints.split(","))
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .map(OpenWorkflowMigrationsMain::contactPoint)
            .toList();
    String username = optional("OPENWORKFLOW_CASSANDRA_USERNAME");
    String password = optional("OPENWORKFLOW_CASSANDRA_PASSWORD");
    if ((username == null) != (password == null)) {
      throw new IllegalStateException(
          "OPENWORKFLOW_CASSANDRA_USERNAME and OPENWORKFLOW_CASSANDRA_PASSWORD must be supplied"
              + " together");
    }
    String applicationKeyspace = optional("OPENWORKFLOW_CASSANDRA_APPLICATION_KEYSPACE");
    String migrationKeyspace = optional("OPENWORKFLOW_CASSANDRA_MIGRATION_KEYSPACE");
    String configuredReplicationFactor = optional("OPENWORKFLOW_CASSANDRA_REPLICATION_FACTOR");
    int replicationFactor =
        configuredReplicationFactor == null ? 1 : Integer.parseInt(configuredReplicationFactor);
    OpenWorkflowCassandraMigrator.migrate(
        new CassandraMigrationTarget(
            endpoints,
            localDatacenter,
            Optional.ofNullable(username),
            Optional.ofNullable(password),
            migrationKeyspace == null
                ? OpenWorkflowCassandraMigrator.DEFAULT_MIGRATION_KEYSPACE
                : migrationKeyspace,
            applicationKeyspace == null
                ? OpenWorkflowCassandraMigrator.DEFAULT_APPLICATION_KEYSPACE
                : applicationKeyspace,
            replicationFactor));
  }

  static InetSocketAddress contactPoint(String value) {
    URI endpoint = URI.create("cql://" + value);
    if (endpoint.getHost() == null || endpoint.getPort() < 1 || endpoint.getPort() > 65535) {
      throw new IllegalArgumentException(
          "Cassandra contact point must use host:port syntax: " + value);
    }
    String host = endpoint.getHost();
    if (host.startsWith("[") && host.endsWith("]")) {
      host = host.substring(1, host.length() - 1);
    }
    return InetSocketAddress.createUnresolved(host, endpoint.getPort());
  }

  private static String required(String name) {
    String value = optional(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException(name + " is required");
    }
    return value;
  }

  private static String optional(String name) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? null : value.trim();
  }

  private record DriverManagerDataSource(String url, String username, String password)
      implements DataSource {
    @Override
    public Connection getConnection() throws SQLException {
      return DriverManager.getConnection(url, username, password);
    }

    @Override
    public Connection getConnection(String suppliedUsername, String suppliedPassword)
        throws SQLException {
      return DriverManager.getConnection(url, suppliedUsername, suppliedPassword);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
      return DriverManager.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter output) throws SQLException {
      DriverManager.setLogWriter(output);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
      DriverManager.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
      return DriverManager.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
      throw new SQLException("Not a wrapper");
    }

    @Override
    public boolean isWrapperFor(Class<?> type) {
      return false;
    }
  }
}
