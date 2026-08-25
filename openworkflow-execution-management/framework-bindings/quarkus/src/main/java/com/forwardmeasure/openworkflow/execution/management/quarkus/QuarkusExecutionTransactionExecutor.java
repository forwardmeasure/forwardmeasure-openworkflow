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
package com.forwardmeasure.openworkflow.execution.management.quarkus;

import com.forwardmeasure.openworkflow.execution.management.ExecutionTransactionExecutor;
import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.control.ActivateRequestContext;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Programmatic transaction boundary via Quarkus's own transaction API - no AOP interceptor.
 * {@code @ApplicationScoped} (not {@code @Singleton}) matching {@code
 * QuarkusWorkflowTransactionExecutor} exactly - this class has no abstract superclass, so there's
 * no reason to deviate from the proven pattern.
 */
@ApplicationScoped
public class QuarkusExecutionTransactionExecutor implements ExecutionTransactionExecutor {

  @Override
  @ActivateRequestContext
  public <T> T execute(Supplier<T> work) {
    Objects.requireNonNull(work, "work");
    return QuarkusTransaction.requiringNew().call(work::get);
  }
}
