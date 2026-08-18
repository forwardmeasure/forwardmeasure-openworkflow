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
import com.forwardmeasure.openworkflow.definition.management.api.model.DefinitionValidationRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.DefinitionWrite;
import com.forwardmeasure.openworkflow.definition.management.api.model.LifecycleAction;
import com.forwardmeasure.openworkflow.definition.management.api.model.RevisionWrite;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.DefinitionManagementResource;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.rules.SecurityRule;
import jakarta.ws.rs.core.Response;
import java.util.function.Supplier;

/** Micronaut route metadata delegating every operation to the portable JAX-RS resource. */
@Controller("/v1/workflow-definitions")
@Secured(SecurityRule.IS_AUTHENTICATED)
public final class MicronautDefinitionManagementController {
  private static final System.Logger LOGGER =
      System.getLogger(MicronautDefinitionManagementController.class.getName());
  private final DefinitionManagementResource delegate;
  private final MicronautActiveOrganizationProvider organizations;
  private final TenantScope tenants;

  public MicronautDefinitionManagementController(
      DefinitionManagementResource delegate,
      MicronautActiveOrganizationProvider organizations,
      TenantScope tenants) {
    this.delegate = delegate;
    this.organizations = organizations;
    this.tenants = tenants;
  }

  @Post
  HttpResponse<Object> create(
      @Header("X-Correlation-ID") String correlationId,
      @Body DefinitionWrite request,
      Authentication authentication) {
    return inTenant(authentication, () -> delegate.createDefinition(correlationId, request));
  }

  @Get
  HttpResponse<Object> list(
      @Header("X-Correlation-ID") String correlationId, Authentication authentication) {
    return inTenant(authentication, () -> delegate.listDefinitions(correlationId));
  }

  @Post("/validation")
  HttpResponse<Object> validate(
      @Header("X-Correlation-ID") String correlationId,
      @Body DefinitionValidationRequest request,
      Authentication authentication) {
    return inTenant(authentication, () -> delegate.validateDefinition(correlationId, request));
  }

  @Post("/{definitionKey}/revisions")
  HttpResponse<Object> revise(
      @PathVariable String definitionKey,
      @Header("X-Correlation-ID") String correlationId,
      @Body RevisionWrite request,
      Authentication authentication) {
    return inTenant(
        authentication, () -> delegate.reviseDefinition(definitionKey, correlationId, request));
  }

  @Get("/{definitionKey}/revisions/{revisionNumber}")
  HttpResponse<Object> retrieve(
      @PathVariable String definitionKey,
      @PathVariable Integer revisionNumber,
      @Header("X-Correlation-ID") String correlationId,
      Authentication authentication) {
    return inTenant(
        authentication,
        () -> delegate.retrieveDefinition(definitionKey, revisionNumber, correlationId));
  }

  @Post("/{definitionKey}/revisions/{revisionNumber}/{action}")
  HttpResponse<Object> transition(
      @PathVariable String definitionKey,
      @PathVariable Integer revisionNumber,
      @PathVariable LifecycleAction action,
      @Header("X-Correlation-ID") String correlationId,
      Authentication authentication) {
    return inTenant(
        authentication,
        () -> delegate.transitionDefinition(definitionKey, revisionNumber, correlationId, action));
  }

  private HttpResponse<Object> inTenant(
      Authentication authentication, Supplier<Response> operation) {
    try {
      return organizations.call(
          authentication,
          () ->
              tenants.call(
                  TenantSchema.forTenant(organizations.current().tenantId()),
                  () -> adapt(operation.get())));
    } catch (RuntimeException failure) {
      LOGGER.log(System.Logger.Level.ERROR, "Definition-management request failed", failure);
      throw failure;
    }
  }

  private static HttpResponse<Object> adapt(Response source) {
    MutableHttpResponse<Object> target =
        HttpResponse.status(HttpStatus.valueOf(source.getStatus()));
    return source.hasEntity() ? target.body(source.getEntity()) : target;
  }
}
