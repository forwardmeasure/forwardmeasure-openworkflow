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
package com.forwardmeasure.openworkflow.binding.spring;

import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/** Binds the verified active Organization before Spring starts a tenant JPA transaction. */
@Component
public class SpringTenantScopeFilter extends OncePerRequestFilter {
  private final ActiveOrganizationProvider organizations;
  private final TenantScope tenants;

  public SpringTenantScopeFilter(ActiveOrganizationProvider organizations, TenantScope tenants) {
    this.organizations = organizations;
    this.tenants = tenants;
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    if (SecurityContextHolder.getContext().getAuthentication() instanceof JwtAuthenticationToken) {
      TenantScope.Scope scope =
          tenants.open(TenantSchema.forTenant(organizations.current().tenantId()));
      try {
        chain.doFilter(request, response);
      } finally {
        scope.close();
      }
    } else {
      chain.doFilter(request, response);
    }
  }
}
