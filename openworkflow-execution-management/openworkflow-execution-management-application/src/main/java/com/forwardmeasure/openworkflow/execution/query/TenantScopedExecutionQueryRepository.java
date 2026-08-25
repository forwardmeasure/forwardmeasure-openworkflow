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
package com.forwardmeasure.openworkflow.execution.query;

import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.engine.api.ExecutionHistoryEntry;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionProjection;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.execution.management.ExecutionTransactionExecutor;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Binds {@link TenantScope} and a transaction around every call before delegating, the query-side
 * counterpart to {@link
 * com.forwardmeasure.openworkflow.execution.management.ExecutionManagementService}'s own binding -
 * callers (REST resources) must not open their own TenantScope or transaction around either port.
 */
public final class TenantScopedExecutionQueryRepository implements ExecutionQueryRepository {
  private final ExecutionQueryRepository delegate;
  private final TenantScope tenants;
  private final ExecutionTransactionExecutor transactions;

  public TenantScopedExecutionQueryRepository(
      ExecutionQueryRepository delegate,
      TenantScope tenants,
      ExecutionTransactionExecutor transactions) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.tenants = Objects.requireNonNull(tenants, "tenants");
    this.transactions = Objects.requireNonNull(transactions, "transactions");
  }

  @Override
  public Optional<ExecutionProjection> find(TenantId tenantId, ExecutionId executionId) {
    return inTenant(tenantId, () -> delegate.find(tenantId, executionId));
  }

  @Override
  public ExecutionPage search(ExecutionSearch search) {
    return inTenant(search.tenantId(), () -> delegate.search(search));
  }

  @Override
  public List<ExecutionHistoryEntry> history(
      TenantId tenantId, ExecutionId executionId, long afterSequence, int limit) {
    return inTenant(tenantId, () -> delegate.history(tenantId, executionId, afterSequence, limit));
  }

  private <T> T inTenant(TenantId tenantId, Supplier<T> operation) {
    var schema =
        TenantSchema.forTenant(new com.forwardmeasure.jpa.tenancy.TenantId(tenantId.value()));
    return tenants.call(schema, () -> transactions.execute(operation));
  }
}
