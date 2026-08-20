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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.execution.api.ExecutionsApi;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionControl;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionStart;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionState;
import com.forwardmeasure.openworkflow.execution.jaxrs.ExecutionContextProvider;
import com.forwardmeasure.openworkflow.execution.jaxrs.ExecutionResource;
import com.forwardmeasure.openworkflow.execution.management.ExecutionManagementService;
import com.forwardmeasure.openworkflow.execution.query.ExecutionQueryRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

/** Quarkus transaction boundary around the shared portable execution resource. */
@ApplicationScoped
@Transactional
@Path("/api/v1/executions")
public class QuarkusExecutionResource implements ExecutionsApi {
  private final ExecutionResource delegate;
  private final ActiveOrganizationProvider organizations;
  private final TenantScope tenants;

  public QuarkusExecutionResource(
      ExecutionManagementService management,
      ExecutionQueryRepository queries,
      ExecutionContextProvider contexts,
      ObjectMapper objectMapper,
      ActiveOrganizationProvider organizations,
      TenantScope tenants) {
    this.delegate =
        new ExecutionResource(management, queries, contexts, objectMapper, UUID::randomUUID);
    this.organizations = organizations;
    this.tenants = tenants;
  }

  @Override
  public Response startExecution(
      String idempotencyKey, String correlationId, ExecutionStart executionStart) {
    return inTenant(() -> delegate.startExecution(idempotencyKey, correlationId, executionStart));
  }

  @Override
  public Response pauseExecution(
      String ifMatch, String correlationId, UUID executionId, ExecutionControl control) {
    return inTenant(() -> delegate.pauseExecution(ifMatch, correlationId, executionId, control));
  }

  @Override
  public Response resumeExecution(
      String ifMatch, String correlationId, UUID executionId, ExecutionControl control) {
    return inTenant(() -> delegate.resumeExecution(ifMatch, correlationId, executionId, control));
  }

  @Override
  public Response cancelExecution(
      String ifMatch, String correlationId, UUID executionId, ExecutionControl control) {
    return inTenant(() -> delegate.cancelExecution(ifMatch, correlationId, executionId, control));
  }

  @Override
  public Response getExecution(UUID executionId) {
    return inTenant(() -> delegate.getExecution(executionId));
  }

  @Override
  public Response getExecutionHistory(UUID executionId, Long afterSequence, Integer limit) {
    return inTenant(() -> delegate.getExecutionHistory(executionId, afterSequence, limit));
  }

  @Override
  public Response listExecutions(
      List<ExecutionState> states,
      String engineId,
      String correlationId,
      Date createdFrom,
      Date createdUntil,
      String cursor,
      Integer limit) {
    return inTenant(
        () ->
            delegate.listExecutions(
                states, engineId, correlationId, createdFrom, createdUntil, cursor, limit));
  }

  private Response inTenant(Supplier<Response> operation) {
    return tenants.call(TenantSchema.forTenant(organizations.current().tenantId()), operation);
  }
}
