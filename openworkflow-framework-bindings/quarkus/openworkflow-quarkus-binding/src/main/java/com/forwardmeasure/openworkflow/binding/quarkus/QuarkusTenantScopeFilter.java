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
package com.forwardmeasure.openworkflow.binding.quarkus;

import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.annotation.Priority;
import jakarta.inject.Inject;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;

/**
 * Binds the verified active Organization's tenant schema before any JPA work happens - the Quarkus
 * counterpart to {@code openworkflow-spring-binding}'s own real {@code SpringTenantScopeFilter},
 * which fowf had for Spring only until now. A full-tree grep confirmed neither the Quarkus nor the
 * Micronaut binding ever opened a {@link TenantScope} at all - every JPA call on those two
 * frameworks ran against whatever schema Hibernate defaulted to, not the caller's actual tenant.
 * Fixed here (Quarkus) and in {@code openworkflow-micronaut-binding} (a different mechanism there -
 * see that fix's own javadoc for why an HTTP filter isn't safe on Micronaut's reactive filter
 * chain).
 *
 * <p>Safe as a pair of JAX-RS request/response filters because Quarkus REST runs a synchronous
 * (non-reactive) resource method's filters and the method body itself on one worker thread for the
 * whole request - the {@link com.forwardmeasure.jpa.tenancy.ThreadBoundTenantScope} this opens is
 * ThreadLocal-based and requires closing on the same thread it was opened on.
 *
 * <p>Runs after Quarkus's own OIDC authentication filter (lower {@link Priorities#AUTHENTICATION}
 * priority runs first) and is a no-op for anonymous requests - some registered paths (health checks
 * etc.) may not require authentication even though every real business resource does.
 */
@Provider
@Priority(Priorities.AUTHENTICATION + 1)
public class QuarkusTenantScopeFilter implements ContainerRequestFilter, ContainerResponseFilter {
  private static final String SCOPE_PROPERTY = QuarkusTenantScopeFilter.class.getName() + ".scope";

  private final ActiveOrganizationProvider organizations;
  private final TenantScope tenants;
  private final SecurityIdentity identity;

  @Inject
  public QuarkusTenantScopeFilter(
      ActiveOrganizationProvider organizations, TenantScope tenants, SecurityIdentity identity) {
    this.organizations = organizations;
    this.tenants = tenants;
    this.identity = identity;
  }

  @Override
  public void filter(ContainerRequestContext requestContext) {
    if (identity.isAnonymous()) {
      return;
    }
    TenantScope.Scope scope =
        tenants.open(TenantSchema.forTenant(organizations.current().tenantId()));
    requestContext.setProperty(SCOPE_PROPERTY, scope);
  }

  @Override
  public void filter(
      ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
    if (requestContext.getProperty(SCOPE_PROPERTY) instanceof TenantScope.Scope scope) {
      scope.close();
    }
  }
}
