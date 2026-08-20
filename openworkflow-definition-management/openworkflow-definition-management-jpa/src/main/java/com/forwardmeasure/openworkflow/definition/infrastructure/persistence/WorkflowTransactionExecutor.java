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
package com.forwardmeasure.openworkflow.definition.infrastructure.persistence;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Framework-provided transaction boundary, called as a plain nested method call inside an already
 * open {@code TenantScope} — never a declarative {@code @Transactional} annotation. This sidesteps
 * any question of whether tenant-scope opening happens before or after an AOP interceptor begins a
 * transaction: there is no interceptor, so the ordering is just two sequential method calls.
 */
@FunctionalInterface
public interface WorkflowTransactionExecutor {
  <T> T execute(Supplier<T> work);

  default void execute(Runnable work) {
    Objects.requireNonNull(work, "work");
    execute(
        () -> {
          work.run();
          return null;
        });
  }
}
