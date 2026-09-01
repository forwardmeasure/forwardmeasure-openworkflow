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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.testcontainers.junit.postgresql.WithPostgreSqlContainer;
import com.forwardmeasure.testcontainers.postgresql.PostgreSqlTestContainer;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

@WithPostgreSqlContainer(databaseName = "openworkflow_migrations_scan")
class ProvisionedTenantSchemasTest {

  @Test
  void scanEnumeratesOnlyRealTenantSchemasAndExcludesNonTenantOnes(
      PostgreSqlTestContainer database) {
    Set<TenantSchema> baseline = Set.copyOf(ProvisionedTenantSchemas.scan(database.dataSource()));

    TenantSchema tenantA = TenantSchema.forTenant(new TenantId(UUID.randomUUID()));
    TenantSchema tenantB = TenantSchema.forTenant(new TenantId(UUID.randomUUID()));
    String nonTenantSchema = "reporting_" + UUID.randomUUID().toString().replace("-", "");
    database.createSchema(tenantA.value());
    database.createSchema(tenantB.value());
    database.createSchema(nonTenantSchema);

    Set<TenantSchema> scanned = Set.copyOf(ProvisionedTenantSchemas.scan(database.dataSource()));

    // Exactly the two newly provisioned tenant schemas were added on top of whatever the
    // container already had - nothing else (not "public", not the arbitrary non-tenant schema
    // just created, which doesn't match the t_% pattern at all).
    Set<TenantSchema> expected = new HashSet<>(baseline);
    expected.add(tenantA);
    expected.add(tenantB);
    assertEquals(expected, scanned);
    assertFalse(scanned.stream().anyMatch(TenantSchema.PUBLIC::equals));
    assertFalse(scanned.stream().map(TenantSchema::value).anyMatch(nonTenantSchema::equals));
  }

  @Test
  void scanSkipsAndLogsASchemaThatMatchesTheSqlPatternButFailsTenantSchemaValidation(
      PostgreSqlTestContainer database) {
    Set<TenantSchema> baseline = Set.copyOf(ProvisionedTenantSchemas.scan(database.dataSource()));

    // Matches the SQL LIKE 't\_%' pattern used by the query (starts with "t_") but is not
    // "t_" followed by exactly 32 lowercase hex characters, so TenantSchema's own constructor
    // regex rejects it. This must be skipped-and-logged, never thrown out of scan().
    String malformedButSqlMatching = "t_not_a_valid_tenant_schema_name";
    database.createSchema(malformedButSqlMatching);

    List<TenantSchema> afterMalformedSchemaCreated =
        ProvisionedTenantSchemas.scan(database.dataSource());

    // The scan completed without throwing (proving skip-not-throw) and the malformed schema is
    // absent from the result, so the set of returned schemas is unchanged from the baseline.
    assertEquals(baseline, Set.copyOf(afterMalformedSchemaCreated));
  }

  @Test
  void scanRejectsANullDataSource() {
    assertThrows(NullPointerException.class, () -> ProvisionedTenantSchemas.scan(null));
  }

  @Test
  void scanWrapsSqlExceptionsAsIllegalStateException() {
    DataSource failingToConnect = new FailingDataSource();

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class, () -> ProvisionedTenantSchemas.scan(failingToConnect));

    assertInstanceOf(SQLException.class, failure.getCause());
  }

  /** Deliberately fails every connection attempt to exercise the SQLException-wrapping path. */
  private static final class FailingDataSource implements DataSource {
    @Override
    public Connection getConnection() throws SQLException {
      throw new SQLException("simulated connection failure");
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      throw new SQLException("simulated connection failure");
    }

    @Override
    public PrintWriter getLogWriter() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setLogWriter(PrintWriter out) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setLoginTimeout(int seconds) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getLoginTimeout() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }
  }
}
