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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.eventing.CloudEventSubscription;
import com.forwardmeasure.openworkflow.eventing.CloudEventSubscriptionRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Native asynchronous Cassandra subscription index with monotonic LWT updates. */
public final class CassandraCloudEventSubscriptionRepository
    implements CloudEventSubscriptionRepository {
  private final CqlSession session;
  private final PreparedStatement insert;
  private final PreparedStatement update;
  private final PreparedStatement candidates;

  public CassandraCloudEventSubscriptionRepository(CqlSession session) {
    this.session = Objects.requireNonNull(session, "session");
    insert =
        session.prepare(
            """
            INSERT INTO openworkflow_event_subscription
              (tenant_did, target_kind, target_entity_id, task_path,
               event_types, revision, active)
            VALUES (?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS
            """);
    update =
        session.prepare(
            """
            UPDATE openworkflow_event_subscription
            SET task_path = ?, event_types = ?, revision = ?, active = ?
            WHERE tenant_did = ? AND target_kind = ? AND target_entity_id = ?
            IF revision < ?
            """);
    candidates =
        session.prepare(
            """
            SELECT target_kind, target_entity_id, task_path,
                   event_types, revision, active
            FROM openworkflow_event_subscription
            WHERE tenant_did = ?
            """);
  }

  @Override
  public CompletionStage<Void> store(CloudEventSubscription value) {
    Objects.requireNonNull(value, "subscription");
    var inserted =
        insert.bind(
            value.tenantId().value().toString(),
            value.targetKind().name(),
            value.targetEntityId(),
            value.taskPath(),
            value.eventTypes(),
            value.revision(),
            value.active());
    return session
        .executeAsync(inserted)
        .thenCompose(
            result -> {
              var row = result.one();
              if (row != null && row.getBoolean("[applied]")) {
                return CompletableFuture.completedFuture(null);
              }
              return session
                  .executeAsync(
                      update.bind(
                          value.taskPath(),
                          value.eventTypes(),
                          value.revision(),
                          value.active(),
                          value.tenantId().value().toString(),
                          value.targetKind().name(),
                          value.targetEntityId(),
                          value.revision()))
                  .thenApply(ignored -> null);
            });
  }

  @Override
  public CompletionStage<List<CloudEventSubscription>> candidates(
      TenantId tenantId, String eventType, int limit) {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(eventType, "eventType");
    if (limit < 1) throw new IllegalArgumentException("limit must be positive");
    var result = new ArrayList<CloudEventSubscription>();
    return session
        .executeAsync(candidates.bind(tenantId.value().toString()).setPageSize(256))
        .thenCompose(rows -> collect(rows, tenantId, eventType, limit, result))
        .thenApply(
            ignored -> {
              result.sort(
                  java.util.Comparator.comparing(CloudEventSubscription::targetKind)
                      .thenComparing(CloudEventSubscription::targetEntityId));
              return List.copyOf(result);
            });
  }

  private CompletionStage<Void> collect(
      com.datastax.oss.driver.api.core.cql.AsyncResultSet rows,
      TenantId tenantId,
      String eventType,
      int limit,
      List<CloudEventSubscription> result) {
    for (var row : rows.currentPage()) {
      var eventTypes = row.getSet("event_types", String.class);
      boolean active = row.getBoolean("active");
      if (active
          && (eventTypes == null || eventTypes.isEmpty() || eventTypes.contains(eventType))) {
        result.add(
            new CloudEventSubscription(
                tenantId,
                CloudEventSubscription.TargetKind.valueOf(row.getString("target_kind")),
                row.getString("target_entity_id"),
                row.getString("task_path"),
                eventTypes,
                row.getLong("revision"),
                true));
        if (result.size() >= limit) {
          return CompletableFuture.completedFuture(null);
        }
      }
    }
    return rows.hasMorePages()
        ? rows.fetchNextPage()
            .thenCompose(next -> collect(next, tenantId, eventType, limit, result))
        : CompletableFuture.completedFuture(null);
  }
}
