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
package com.forwardmeasure.openworkflow.binding.micronaut;

import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import jakarta.inject.Singleton;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Binds the verified active Organization's tenant schema immediately around a unit of JPA work -
 * NOT an HTTP filter, unlike {@code openworkflow-spring-binding}'s/{@code
 * openworkflow-quarkus-binding}'s own {@code TenantScopeFilter}s, and deliberately so.
 *
 * <p>A full-tree grep confirmed neither this binding nor the Quarkus one ever opened a {@link
 * TenantScope} at all before this fix - every JPA call on those two frameworks ran against whatever
 * schema Hibernate defaulted to, not the caller's actual tenant (only Spring had a real filter).
 * For Micronaut specifically, an HTTP-filter-level fix (as used for Quarkus) is not safe:
 * disassembling {@code micronaut-security}'s real {@code SecurityFilter} confirms Micronaut's HTTP
 * filter chain is built entirely on Reactor {@code Flux}/{@code Mono}, and a blocking JAX-RS
 * resource method reached through {@code micronaut-jaxrs-server} is dispatched onto a separate
 * blocking-task-executor thread - a real thread-hop between the filter chain and the resource
 * method body. {@link com.forwardmeasure.jpa.tenancy.ThreadBoundTenantScope} is plain ThreadLocal
 * and must be opened and closed on the exact thread the JPA work runs on, so an HTTP-filter-level
 * open would silently never be visible to Hibernate's tenant-identifier resolution here.
 *
 * <p>{@link ActiveOrganizationProvider#current()} itself is proven safe to call from within the
 * resource method body despite this same thread-hop - {@code MicronautActiveOrganizationProvider}
 * already does exactly that, via {@code SecurityService}, which uses Micronaut's own
 * thread-hop-aware context propagation internally. This class uses that already-safe read to open
 * the tenant scope at the latest safe point: from within each capability's own {@code
 * TransactionExecutor} implementation (e.g. {@code MicronautWorkflowTransactionExecutor}, {@code
 * MicronautExecutionTransactionExecutor}), wrapping the transactional work itself, which is
 * guaranteed to run on the same thread as the resource method that invoked it.
 */
@Singleton
public class MicronautTenantScopedExecution {
  private final ActiveOrganizationProvider organizations;
  private final TenantScope tenants;

  public MicronautTenantScopedExecution(
      ActiveOrganizationProvider organizations, TenantScope tenants) {
    this.organizations = Objects.requireNonNull(organizations, "organizations");
    this.tenants = Objects.requireNonNull(tenants, "tenants");
  }

  public <T> T call(Supplier<T> work) {
    Objects.requireNonNull(work, "work");
    TenantScope.Scope scope =
        tenants.open(TenantSchema.forTenant(organizations.current().tenantId()));
    try {
      return work.get();
    } finally {
      scope.close();
    }
  }
}
