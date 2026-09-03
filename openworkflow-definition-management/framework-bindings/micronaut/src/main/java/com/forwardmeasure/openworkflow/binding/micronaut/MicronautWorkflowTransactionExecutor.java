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

import com.forwardmeasure.openworkflow.definition.infrastructure.persistence.WorkflowTransactionExecutor;
import io.micronaut.transaction.TransactionOperations;
import jakarta.inject.Singleton;
import java.util.Objects;
import java.util.function.Supplier;
import org.hibernate.Session;

/**
 * Programmatic transaction boundary via Micronaut's own transaction API - no AOP interceptor. Opens
 * the tenant scope around the transaction (not via an HTTP filter - see {@link
 * MicronautTenantScopedExecution}'s javadoc for why that isn't safe on Micronaut) - a real,
 * previously-missing fix, confirmed by full-tree grep that no {@code TenantScope} was ever opened
 * on this framework at all before this.
 */
@Singleton
public class MicronautWorkflowTransactionExecutor implements WorkflowTransactionExecutor {
  private final TransactionOperations<Session> transactions;
  private final MicronautTenantScopedExecution tenantScoped;

  public MicronautWorkflowTransactionExecutor(
      TransactionOperations<Session> transactions, MicronautTenantScopedExecution tenantScoped) {
    this.transactions = Objects.requireNonNull(transactions, "transactions");
    this.tenantScoped = Objects.requireNonNull(tenantScoped, "tenantScoped");
  }

  @Override
  public <T> T execute(Supplier<T> work) {
    Objects.requireNonNull(work, "work");
    return tenantScoped.call(() -> transactions.executeWrite(status -> work.get()));
  }
}
