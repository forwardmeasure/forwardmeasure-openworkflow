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
package com.forwardmeasure.openworkflow.execution.query.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionHistoryEntry;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionProjection;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.execution.query.ExecutionPage;
import com.forwardmeasure.openworkflow.execution.query.ExecutionProjectionStore;
import com.forwardmeasure.openworkflow.execution.query.ExecutionQueryRepository;
import com.forwardmeasure.openworkflow.execution.query.ExecutionSearch;
import jakarta.persistence.EntityManager;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Tenant-routing facade shared by singleton framework resources and both engine projectors. */
public final class JpaTenantRoutingExecutionStore
    implements ExecutionQueryRepository, ExecutionProjectionStore {
  private final EntityManager entityManager;
  private final ObjectMapper objectMapper;

  public JpaTenantRoutingExecutionStore(EntityManager entityManager, ObjectMapper objectMapper) {
    this.entityManager = Objects.requireNonNull(entityManager, "entityManager");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
  }

  @Override
  public Optional<ExecutionProjection> find(TenantId tenantId, ExecutionId executionId) {
    return queries(tenantId).find(tenantId, executionId);
  }

  @Override
  public ExecutionPage search(ExecutionSearch search) {
    return queries(search.tenantId()).search(search);
  }

  @Override
  public List<ExecutionHistoryEntry> history(
      TenantId tenantId, ExecutionId executionId, long afterSequence, int limit) {
    return queries(tenantId).history(tenantId, executionId, afterSequence, limit);
  }

  @Override
  public ProjectionApplyResult apply(ExecutionEvent event) {
    return new JpaExecutionProjectionStore(
            jpaTenant(event.executionId().tenantId()), entityManager, objectMapper)
        .apply(event);
  }

  @Override
  public ProjectionApplyResult applyNext(ExecutionEvent event) {
    return new JpaExecutionProjectionStore(
            jpaTenant(event.executionId().tenantId()), entityManager, objectMapper)
        .applyNext(event);
  }

  private JpaExecutionQueryRepository queries(TenantId tenantId) {
    return new JpaExecutionQueryRepository(jpaTenant(tenantId), entityManager, objectMapper);
  }

  private static com.forwardmeasure.jpa.tenancy.TenantId jpaTenant(TenantId tenantId) {
    return new com.forwardmeasure.jpa.tenancy.TenantId(
        Objects.requireNonNull(tenantId, "tenantId").value());
  }
}
