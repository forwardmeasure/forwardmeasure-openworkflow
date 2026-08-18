/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.binding.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEventSink;
import com.forwardmeasure.openworkflow.execution.query.core.CanonicalExecutionEventProjector;
import com.forwardmeasure.openworkflow.execution.query.persistence.JpaTenantRoutingExecutionStore;
import jakarta.persistence.EntityManagerFactory;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.hibernate.SessionFactory;

/** Framework transaction boundary for Kafka events projected into tenant schemas. */
final class SpringExecutionEventSink implements ExecutionEventSink {
  private final TenantScope tenants;
  private final SessionFactory sessions;
  private final ObjectMapper objectMapper;

  SpringExecutionEventSink(
      TenantScope tenants, EntityManagerFactory entityManagerFactory, ObjectMapper objectMapper) {
    this.tenants = tenants;
    this.sessions = entityManagerFactory.unwrap(SessionFactory.class);
    this.objectMapper = objectMapper;
  }

  @Override
  public CompletionStage<Void> project(ExecutionEvent event) {
    return project(event, false);
  }

  @Override
  public CompletionStage<Void> projectNext(ExecutionEvent event) {
    return project(event, true);
  }

  private CompletionStage<Void> project(ExecutionEvent event, boolean next) {
    var tenant =
        new com.forwardmeasure.jpa.tenancy.TenantId(event.executionId().tenantId().value());
    TenantSchema schema = TenantSchema.forTenant(tenant);
    TenantScope.Scope scope = tenants.open(schema);
    try {
      try (var session =
          sessions.withOptions().tenantIdentifier((Object) schema.value()).openSession()) {
        var transaction = session.beginTransaction();
        try {
          var delegate =
              new CanonicalExecutionEventProjector(
                  new JpaTenantRoutingExecutionStore(session, objectMapper));
          (next ? delegate.projectNext(event) : delegate.project(event))
              .toCompletableFuture()
              .join();
          transaction.commit();
          return CompletableFuture.completedFuture(null);
        } catch (RuntimeException | Error failure) {
          transaction.rollback();
          throw failure;
        }
      }
    } catch (Exception failure) {
      return CompletableFuture.failedFuture(failure);
    } finally {
      scope.close();
    }
  }
}
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
