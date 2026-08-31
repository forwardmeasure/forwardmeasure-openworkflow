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

import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.definition.ResolvedSubflow;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.eventing.SubworkflowPlanResolver;
import jakarta.persistence.EntityManagerFactory;
import java.util.Objects;
import org.hibernate.SessionFactory;

/**
 * Opens one tenant-routed Hibernate session per resolution, delegating to {@link
 * JpaSubworkflowPlanResolver} - {@code SubworkflowOutboxHandler} calls {@link #resolve} from a
 * Pekko projection thread, outside any HTTP request scope, so (like {@code
 * HibernateSessionExecutionQueryRepository} and {@code QuarkusExecutionEventSink}) this cannot rely
 * on a container-managed request-scoped {@code EntityManager}.
 */
public final class HibernateSessionSubworkflowPlanResolver implements SubworkflowPlanResolver {
  private final TenantScope tenants;
  private final SessionFactory sessions;

  public HibernateSessionSubworkflowPlanResolver(
      TenantScope tenants, EntityManagerFactory entityManagerFactory) {
    this.tenants = Objects.requireNonNull(tenants, "tenants");
    this.sessions =
        Objects.requireNonNull(entityManagerFactory, "entityManagerFactory")
            .unwrap(SessionFactory.class);
  }

  @Override
  public WorkflowPlan resolve(TenantId tenantId, ActorIdentity actor, ResolvedSubflow subflow) {
    TenantSchema schema =
        TenantSchema.forTenant(new com.forwardmeasure.jpa.tenancy.TenantId(tenantId.value()));
    return tenants.call(
        schema,
        () -> {
          try (var session =
              sessions.withOptions().tenantIdentifier((Object) schema.value()).openSession()) {
            return new JpaSubworkflowPlanResolver(session).resolve(tenantId, actor, subflow);
          }
        });
  }
}
