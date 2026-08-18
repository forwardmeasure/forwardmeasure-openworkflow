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
import com.forwardmeasure.openworkflow.definition.management.api.DefinitionsApi;
import com.forwardmeasure.openworkflow.definition.management.api.model.DefinitionValidationRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.DefinitionWrite;
import com.forwardmeasure.openworkflow.definition.management.api.model.LifecycleAction;
import com.forwardmeasure.openworkflow.definition.management.api.model.RevisionWrite;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.DefinitionManagementResource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import java.util.function.Supplier;

/** Quarkus-discoverable endpoint delegating every operation to the portable JAX-RS resource. */
@ApplicationScoped
@Path("/v1/workflow-definitions")
public class QuarkusDefinitionManagementResource implements DefinitionsApi {
  private final DefinitionManagementResource delegate;
  private final ActiveOrganizationProvider organizations;
  private final TenantScope tenants;

  public QuarkusDefinitionManagementResource(
      DefinitionManagementResource delegate,
      ActiveOrganizationProvider organizations,
      TenantScope tenants) {
    this.delegate = delegate;
    this.organizations = organizations;
    this.tenants = tenants;
  }

  @Override
  public Response createDefinition(String correlationId, DefinitionWrite request) {
    return inTenant(() -> delegate.createDefinition(correlationId, request));
  }

  @Override
  public Response listDefinitions(String correlationId) {
    return inTenant(() -> delegate.listDefinitions(correlationId));
  }

  @Override
  public Response retrieveDefinition(String key, Integer revision, String correlationId) {
    return inTenant(() -> delegate.retrieveDefinition(key, revision, correlationId));
  }

  @Override
  public Response reviseDefinition(String key, String correlationId, RevisionWrite request) {
    return inTenant(() -> delegate.reviseDefinition(key, correlationId, request));
  }

  @Override
  public Response transitionDefinition(
      String key, Integer revision, String correlationId, LifecycleAction action) {
    return inTenant(() -> delegate.transitionDefinition(key, revision, correlationId, action));
  }

  @Override
  public Response validateDefinition(String correlationId, DefinitionValidationRequest request) {
    return inTenant(() -> delegate.validateDefinition(correlationId, request));
  }

  private Response inTenant(Supplier<Response> operation) {
    return tenants.call(TenantSchema.forTenant(organizations.current().tenantId()), operation);
  }
}
