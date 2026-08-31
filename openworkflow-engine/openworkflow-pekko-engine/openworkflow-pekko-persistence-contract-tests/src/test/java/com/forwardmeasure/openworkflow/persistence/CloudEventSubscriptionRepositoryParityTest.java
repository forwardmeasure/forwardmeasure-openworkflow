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
package com.forwardmeasure.openworkflow.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.openworkflow.actor.ScheduleId;
import com.forwardmeasure.openworkflow.definition.WorkflowCoordinates;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.eventing.CloudEventSubscription;
import com.forwardmeasure.openworkflow.eventing.CloudEventSubscriptionRepository;
import com.forwardmeasure.openworkflow.eventing.cassandra.CassandraCloudEventSubscriptionRepository;
import com.forwardmeasure.openworkflow.eventing.postgresql.PostgresqlCloudEventSubscriptionRepository;
import com.forwardmeasure.openworkflow.migration.CassandraMigrationTarget;
import com.forwardmeasure.openworkflow.migration.OpenWorkflowCassandraMigrator;
import com.forwardmeasure.openworkflow.migration.OpenWorkflowTenantMigrator;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Identical monotonic routing-index behavior on both persistence backends. Adapted from
 * openworkflow-actor-engine's version of this test, not a verbatim port: that version's shared
 * {@code contract(repository)} method also asserted cross-tenant isolation using one repository
 * instance for two tenants - that doesn't translate here, because fowf's Postgres tenancy model is
 * schema-per-tenant (a genuinely stronger isolation guarantee than oae's tenant-column model), so
 * one repository instance is only ever bound to one tenant's schema. Cross-tenant isolation is
 * already covered per-backend by {@code PostgresqlCloudEventSubscriptionRepositoryTest} and {@code
 * CassandraCloudEventSubscriptionRepositoryTest}; what this test adds on top of those is the thing
 * they can't provide individually - proof the *same* monotonic-revision/tombstone sequence produces
 * identical outcomes on both backends, via one shared assertion method instead of two
 * independently-authored copies.
 */
class CloudEventSubscriptionRepositoryParityTest {
  private static final TenantId TENANT =
      new TenantId(UUID.fromString("134b09a7-1c36-4b89-86e7-a28c88bc5cef"));

  @Test
  void postgresqlSubscriptionIndexContract() throws Exception {
    try (var postgres = new PostgreSQLContainer("postgres:18-alpine")) {
      postgres.start();
      var jpaTenant = new com.forwardmeasure.jpa.tenancy.TenantId(TENANT.value());
      var adminDataSource = new PGSimpleDataSource();
      adminDataSource.setURL(postgres.getJdbcUrl());
      adminDataSource.setUser(postgres.getUsername());
      adminDataSource.setPassword(postgres.getPassword());
      // OpenWorkflowTenantMigrator always connects as the administrator credential and
      // provisions a SEPARATE runtime role - never the same identity it connects as (see its own
      // class Javadoc). Testcontainers' admin user is "test"; reusing that name here would have
      // the migrator GRANT test TO test, which Postgres rejects as circular self-membership.
      var migrator = new OpenWorkflowTenantMigrator(adminDataSource, "openworkflow_runtime");
      migrator.ensureRuntimeRole(postgres.getPassword());
      migrator.provisionAndMigrate(jpaTenant);

      var tenantDataSource = new PGSimpleDataSource();
      tenantDataSource.setURL(postgres.getJdbcUrl());
      tenantDataSource.setUser(postgres.getUsername());
      tenantDataSource.setPassword(postgres.getPassword());
      tenantDataSource.setCurrentSchema(TenantSchema.forTenant(jpaTenant).value());
      try (var repository = new PostgresqlCloudEventSubscriptionRepository(tenantDataSource)) {
        contract(repository);
      }
    }
  }

  @Test
  void cassandraSubscriptionIndexContract() throws Exception {
    try (var cassandra = new CassandraContainer("cassandra:5.0.5")) {
      cassandra.start();
      InetSocketAddress address =
          new InetSocketAddress(cassandra.getHost(), cassandra.getMappedPort(9042));
      OpenWorkflowCassandraMigrator.migrate(
          CassandraMigrationTarget.unauthenticated(address, cassandra.getLocalDatacenter()));
      try (CqlSession session =
          CqlSession.builder()
              .addContactPoint(address)
              .withLocalDatacenter(cassandra.getLocalDatacenter())
              .withKeyspace(OpenWorkflowCassandraMigrator.DEFAULT_APPLICATION_KEYSPACE)
              .build()) {
        contract(new CassandraCloudEventSubscriptionRepository(session));
      }
    }
  }

  private static void contract(CloudEventSubscriptionRepository repository) throws Exception {
    var execution = new ExecutionId(TENANT, UUID.randomUUID());
    var schedule =
        new ScheduleId(
            TENANT,
            new WorkflowCoordinates("forwardmeasure", "subscription-parity", "1.0.0", "1.0.3"));
    await(
        repository.store(
            CloudEventSubscription.execution(
                execution, "/do/0/listen", Set.of("orders.created.v1"), 2, true)));
    await(repository.store(CloudEventSubscription.execution(execution, "", Set.of(), 1, false)));
    await(repository.store(CloudEventSubscription.schedule(schedule, Set.of(), 1, true)));

    var created = await(repository.candidates(TENANT, "orders.created.v1", 10));
    assertEquals(2, created.size());
    var cancelled = await(repository.candidates(TENANT, "orders.cancelled.v1", 10));
    assertEquals(1, cancelled.size());
    assertEquals(CloudEventSubscription.TargetKind.SCHEDULE, cancelled.getFirst().targetKind());

    await(repository.store(CloudEventSubscription.execution(execution, "", Set.of(), 3, false)));
    await(
        repository.store(
            CloudEventSubscription.execution(
                execution, "/do/0/listen", Set.of("orders.created.v1"), 2, true)));
    var afterTombstone = await(repository.candidates(TENANT, "orders.created.v1", 10));
    assertEquals(1, afterTombstone.size());
    assertTrue(
        afterTombstone.getFirst().targetKind() == CloudEventSubscription.TargetKind.SCHEDULE);
  }

  private static <T> T await(CompletionStage<T> stage) throws Exception {
    return stage.toCompletableFuture().get(30, TimeUnit.SECONDS);
  }
}
