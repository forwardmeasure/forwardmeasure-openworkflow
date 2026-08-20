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
package com.forwardmeasure.openworkflow.eventing.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.driver.api.core.CqlSession;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.eventing.CloudEventSubscription;
import com.forwardmeasure.openworkflow.migration.CassandraMigrationTarget;
import com.forwardmeasure.openworkflow.migration.OpenWorkflowCassandraMigrator;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;

class CassandraCloudEventSubscriptionRepositoryTest {
  private static final TenantId TENANT_A =
      new TenantId(UUID.fromString("11111111-1111-1111-1111-111111111111"));
  private static final TenantId TENANT_B =
      new TenantId(UUID.fromString("22222222-2222-2222-2222-222222222222"));

  @Test
  void storesMonotonicTenantIsolatedSubscriptionsInTheMigratedSchema() {
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
        var repository = new CassandraCloudEventSubscriptionRepository(session);
        ExecutionId firstExecution = new ExecutionId(TENANT_A, UUID.randomUUID());
        ExecutionId secondExecution = new ExecutionId(TENANT_A, UUID.randomUUID());
        ExecutionId otherTenantExecution = new ExecutionId(TENANT_B, UUID.randomUUID());

        repository
            .store(
                CloudEventSubscription.execution(
                    firstExecution, "wait-for-order", Set.of("order.created"), 2, true))
            .toCompletableFuture()
            .join();
        repository
            .store(
                CloudEventSubscription.execution(
                    secondExecution, "wait-for-anything", Set.of(), 1, true))
            .toCompletableFuture()
            .join();
        repository
            .store(
                CloudEventSubscription.execution(
                    otherTenantExecution, "other-tenant", Set.of("order.created"), 1, true))
            .toCompletableFuture()
            .join();

        assertEquals(
            Set.of(firstExecution, secondExecution),
            repository
                .candidates(TENANT_A, "order.created", 10)
                .toCompletableFuture()
                .join()
                .stream()
                .map(CloudEventSubscription::executionId)
                .collect(java.util.stream.Collectors.toUnmodifiableSet()));
        assertEquals(
            Set.of(secondExecution),
            repository
                .candidates(TENANT_A, "order.cancelled", 10)
                .toCompletableFuture()
                .join()
                .stream()
                .map(CloudEventSubscription::executionId)
                .collect(java.util.stream.Collectors.toUnmodifiableSet()));
        assertEquals(
            1,
            repository
                .candidates(TENANT_A, "order.created", 1)
                .toCompletableFuture()
                .join()
                .size());

        repository
            .store(
                CloudEventSubscription.execution(
                    firstExecution, "stale", Set.of("order.cancelled"), 1, false))
            .toCompletableFuture()
            .join();
        assertEquals(
            "wait-for-order",
            repository
                .candidates(TENANT_A, "order.created", 10)
                .toCompletableFuture()
                .join()
                .stream()
                .filter(value -> value.executionId().equals(firstExecution))
                .findFirst()
                .orElseThrow()
                .taskPath());

        repository
            .store(
                CloudEventSubscription.execution(
                    firstExecution, "completed", Set.of("order.created"), 3, false))
            .toCompletableFuture()
            .join();
        assertEquals(
            Set.of(secondExecution),
            repository
                .candidates(TENANT_A, "order.created", 10)
                .toCompletableFuture()
                .join()
                .stream()
                .map(CloudEventSubscription::executionId)
                .collect(java.util.stream.Collectors.toUnmodifiableSet()));
      }
    }
  }
}
