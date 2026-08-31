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
package com.forwardmeasure.openworkflow.eventing.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.engine.api.ExecutionHistoryEntry;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionProjection;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.execution.query.ExecutionPage;
import com.forwardmeasure.openworkflow.execution.query.ExecutionQueryRepository;
import com.forwardmeasure.openworkflow.execution.query.ExecutionSearch;
import com.forwardmeasure.openworkflow.execution.query.persistence.JpaTenantRoutingExecutionStore;
import jakarta.persistence.EntityManagerFactory;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.hibernate.SessionFactory;

/**
 * Read-only query-store access for Pekko dispatcher/projection threads, which run outside any HTTP
 * request scope and so have no request-scoped {@code EntityManager} to inject (unlike {@code
 * ExecutionResource}). Opens one tenant-routed Hibernate session per call instead, the same
 * transaction-boundary shape {@code QuarkusExecutionEventSink} already uses for the write side -
 * only {@code CloudEventOutboxHandler}'s lifecycle-coordinate lookup after actor-system restart
 * needs this; nothing on the Pekko side needs to write through it.
 */
public final class HibernateSessionExecutionQueryRepository implements ExecutionQueryRepository {
  private final TenantScope tenants;
  private final SessionFactory sessions;
  private final ObjectMapper objectMapper;

  public HibernateSessionExecutionQueryRepository(
      TenantScope tenants, EntityManagerFactory entityManagerFactory, ObjectMapper objectMapper) {
    this.tenants = Objects.requireNonNull(tenants, "tenants");
    this.sessions =
        Objects.requireNonNull(entityManagerFactory, "entityManagerFactory")
            .unwrap(SessionFactory.class);
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
  }

  @Override
  public Optional<ExecutionProjection> find(TenantId tenantId, ExecutionId executionId) {
    return call(tenantId, store -> store.find(tenantId, executionId));
  }

  @Override
  public ExecutionPage search(ExecutionSearch search) {
    return call(search.tenantId(), store -> store.search(search));
  }

  @Override
  public List<ExecutionHistoryEntry> history(
      TenantId tenantId, ExecutionId executionId, long afterSequence, int limit) {
    return call(tenantId, store -> store.history(tenantId, executionId, afterSequence, limit));
  }

  private <T> T call(TenantId tenantId, Function<JpaTenantRoutingExecutionStore, T> body) {
    TenantSchema schema =
        TenantSchema.forTenant(new com.forwardmeasure.jpa.tenancy.TenantId(tenantId.value()));
    return tenants.call(
        schema,
        () -> {
          try (var session =
              sessions.withOptions().tenantIdentifier((Object) schema.value()).openSession()) {
            return body.apply(new JpaTenantRoutingExecutionStore(session, objectMapper));
          }
        });
  }
}
