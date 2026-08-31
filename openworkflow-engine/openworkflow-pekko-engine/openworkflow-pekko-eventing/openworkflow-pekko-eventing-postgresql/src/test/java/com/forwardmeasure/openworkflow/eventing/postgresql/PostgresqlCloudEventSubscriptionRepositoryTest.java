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
package com.forwardmeasure.openworkflow.eventing.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.eventing.CloudEventSubscription;
import com.forwardmeasure.openworkflow.migration.OpenWorkflowTenantMigrator;
import com.forwardmeasure.testcontainers.junit.postgresql.WithPostgreSqlContainer;
import com.forwardmeasure.testcontainers.postgresql.PostgreSqlTestContainer;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

@WithPostgreSqlContainer(databaseName = "openworkflow_eventing")
class PostgresqlCloudEventSubscriptionRepositoryTest {
  private static final TenantId TENANT_A =
      new TenantId(UUID.fromString("11111111-1111-1111-1111-111111111111"));
  private static final TenantId TENANT_B =
      new TenantId(UUID.fromString("22222222-2222-2222-2222-222222222222"));

  @Test
  void storesMonotonicTenantIsolatedSubscriptionsInMigratedSchemas(
      PostgreSqlTestContainer database) {
    var migrator = new OpenWorkflowTenantMigrator(database.dataSource(), "openworkflow_runtime");
    migrator.ensureRuntimeRole(database.password());
    migrator.provisionAndMigrate(jpaTenant(TENANT_A));
    migrator.provisionAndMigrate(jpaTenant(TENANT_B));

    try (var tenantA =
            new PostgresqlCloudEventSubscriptionRepository(dataSource(database, TENANT_A));
        var tenantB =
            new PostgresqlCloudEventSubscriptionRepository(dataSource(database, TENANT_B))) {
      ExecutionId firstExecution = new ExecutionId(TENANT_A, UUID.randomUUID());
      ExecutionId secondExecution = new ExecutionId(TENANT_A, UUID.randomUUID());
      ExecutionId otherTenantExecution = new ExecutionId(TENANT_B, UUID.randomUUID());

      tenantA
          .store(
              CloudEventSubscription.execution(
                  firstExecution, "wait-for-order", Set.of("order.created"), 2, true))
          .toCompletableFuture()
          .join();
      tenantA
          .store(
              CloudEventSubscription.execution(
                  secondExecution, "wait-for-anything", Set.of(), 1, true))
          .toCompletableFuture()
          .join();
      tenantB
          .store(
              CloudEventSubscription.execution(
                  otherTenantExecution, "other-tenant", Set.of("order.created"), 1, true))
          .toCompletableFuture()
          .join();

      assertEquals(
          Set.of(firstExecution, secondExecution),
          executionIds(tenantA, TENANT_A, "order.created", 10));
      assertEquals(Set.of(secondExecution), executionIds(tenantA, TENANT_A, "order.cancelled", 10));
      assertEquals(
          1, tenantA.candidates(TENANT_A, "order.created", 1).toCompletableFuture().join().size());
      assertEquals(
          Set.of(otherTenantExecution), executionIds(tenantB, TENANT_B, "order.created", 10));

      tenantA
          .store(
              CloudEventSubscription.execution(
                  firstExecution, "stale", Set.of("order.cancelled"), 1, false))
          .toCompletableFuture()
          .join();
      assertEquals(
          "wait-for-order",
          tenantA.candidates(TENANT_A, "order.created", 10).toCompletableFuture().join().stream()
              .filter(value -> value.executionId().equals(firstExecution))
              .findFirst()
              .orElseThrow()
              .taskPath());

      tenantA
          .store(
              CloudEventSubscription.execution(
                  firstExecution, "completed", Set.of("order.created"), 3, false))
          .toCompletableFuture()
          .join();
      assertEquals(Set.of(secondExecution), executionIds(tenantA, TENANT_A, "order.created", 10));
    }
  }

  private static Set<ExecutionId> executionIds(
      PostgresqlCloudEventSubscriptionRepository repository,
      TenantId tenantId,
      String eventType,
      int limit) {
    return repository.candidates(tenantId, eventType, limit).toCompletableFuture().join().stream()
        .map(CloudEventSubscription::executionId)
        .collect(java.util.stream.Collectors.toUnmodifiableSet());
  }

  private static com.forwardmeasure.jpa.tenancy.TenantId jpaTenant(TenantId tenantId) {
    return new com.forwardmeasure.jpa.tenancy.TenantId(tenantId.value());
  }

  private static PGSimpleDataSource dataSource(
      PostgreSqlTestContainer database, TenantId tenantId) {
    var dataSource = new PGSimpleDataSource();
    dataSource.setURL(database.hostJdbcUrl());
    dataSource.setUser(database.username());
    dataSource.setPassword(database.password());
    dataSource.setCurrentSchema(TenantSchema.forTenant(jpaTenant(tenantId)).value());
    return dataSource;
  }
}
