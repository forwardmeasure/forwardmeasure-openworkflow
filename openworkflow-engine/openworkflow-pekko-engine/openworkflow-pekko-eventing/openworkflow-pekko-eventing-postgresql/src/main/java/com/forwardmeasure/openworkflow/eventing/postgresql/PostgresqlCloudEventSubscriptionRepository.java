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

import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.eventing.CloudEventSubscription;
import com.forwardmeasure.openworkflow.eventing.CloudEventSubscriptionRepository;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.sql.DataSource;

/** Asynchronous PostgreSQL materialized subscription index with revision tombstones. */
public final class PostgresqlCloudEventSubscriptionRepository
    implements CloudEventSubscriptionRepository, AutoCloseable {
  private final DataSource dataSource;
  private final ExecutorService executor;

  public PostgresqlCloudEventSubscriptionRepository(DataSource dataSource) {
    this(dataSource, Executors.newVirtualThreadPerTaskExecutor());
  }

  PostgresqlCloudEventSubscriptionRepository(DataSource dataSource, ExecutorService executor) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    this.executor = Objects.requireNonNull(executor, "executor");
  }

  @Override
  public CompletionStage<Void> store(CloudEventSubscription subscription) {
    Objects.requireNonNull(subscription, "subscription");
    return CompletableFuture.runAsync(
        () -> {
          String sql =
              """
              INSERT INTO openworkflow_event_subscription
                (tenant_did, target_kind, target_entity_id, task_path,
                 event_types, revision, active)
              VALUES (?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT (tenant_did, target_kind, target_entity_id)
              DO UPDATE SET task_path = EXCLUDED.task_path,
                            event_types = EXCLUDED.event_types,
                            revision = EXCLUDED.revision,
                            active = EXCLUDED.active
              WHERE openworkflow_event_subscription.revision < EXCLUDED.revision
              """;
          try (var connection = dataSource.getConnection();
              var statement = connection.prepareStatement(sql)) {
            statement.setString(1, subscription.tenantId().value().toString());
            statement.setString(2, subscription.targetKind().name());
            statement.setString(3, subscription.targetEntityId());
            statement.setString(4, subscription.taskPath());
            statement.setArray(
                5,
                connection.createArrayOf("text", subscription.eventTypes().toArray(String[]::new)));
            statement.setLong(6, subscription.revision());
            statement.setBoolean(7, subscription.active());
            statement.executeUpdate();
          } catch (SQLException failure) {
            throw new java.util.concurrent.CompletionException(failure);
          }
        },
        executor);
  }

  @Override
  public CompletionStage<List<CloudEventSubscription>> candidates(
      TenantId tenantId, String eventType, int limit) {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(eventType, "eventType");
    if (limit < 1) throw new IllegalArgumentException("limit must be positive");
    return CompletableFuture.supplyAsync(
        () -> {
          String sql =
              """
              SELECT target_kind, target_entity_id, task_path,
                     event_types, revision, active
              FROM openworkflow_event_subscription
              WHERE tenant_did = ? AND active = TRUE
                AND (cardinality(event_types) = 0
                     OR event_types @> ARRAY[?]::text[])
              ORDER BY target_kind, target_entity_id
              LIMIT ?
              """;
          try (var connection = dataSource.getConnection();
              var statement = connection.prepareStatement(sql)) {
            statement.setString(1, tenantId.value().toString());
            statement.setString(2, eventType);
            statement.setInt(3, limit);
            try (var rows = statement.executeQuery()) {
              var result = new ArrayList<CloudEventSubscription>();
              while (rows.next()) {
                String[] eventTypes = (String[]) rows.getArray("event_types").getArray();
                result.add(
                    new CloudEventSubscription(
                        tenantId,
                        CloudEventSubscription.TargetKind.valueOf(rows.getString("target_kind")),
                        rows.getString("target_entity_id"),
                        rows.getString("task_path"),
                        java.util.Set.of(eventTypes),
                        rows.getLong("revision"),
                        rows.getBoolean("active")));
              }
              return List.copyOf(result);
            }
          } catch (SQLException failure) {
            throw new java.util.concurrent.CompletionException(failure);
          }
        },
        executor);
  }

  @Override
  public void close() {
    executor.close();
  }
}
