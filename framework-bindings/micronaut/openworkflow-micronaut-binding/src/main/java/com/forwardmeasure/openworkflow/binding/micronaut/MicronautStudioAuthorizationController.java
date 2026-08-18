/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.binding.micronaut;

import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.StudioAuthorizationResource;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.annotation.Post;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.rules.SecurityRule;
import jakarta.ws.rs.core.Response;

/** Micronaut transport metadata for the portable Studio AuthZEN batch facade. */
@Controller("/api/v1/authorizations")
@Secured(SecurityRule.IS_AUTHENTICATED)
public final class MicronautStudioAuthorizationController {
  private final StudioAuthorizationResource delegate;
  private final MicronautActiveOrganizationProvider organizations;
  private final TenantScope tenants;

  public MicronautStudioAuthorizationController(
      StudioAuthorizationResource delegate,
      MicronautActiveOrganizationProvider organizations,
      TenantScope tenants) {
    this.delegate = delegate;
    this.organizations = organizations;
    this.tenants = tenants;
  }

  @Post
  HttpResponse<Object> evaluate(
      @Header("X-Correlation-ID") String correlationId,
      @Body StudioAuthorizationResource.BatchRequest request,
      Authentication authentication) {
    return organizations.call(
        authentication,
        () ->
            tenants.call(
                TenantSchema.forTenant(organizations.current().tenantId()),
                () -> adapt(delegate.evaluate(correlationId, request))));
  }

  private static HttpResponse<Object> adapt(Response response) {
    return HttpResponse.status(response.getStatus(), null).body(response.getEntity());
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
