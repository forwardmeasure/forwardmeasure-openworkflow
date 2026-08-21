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
package com.forwardmeasure.openworkflow.definition.management.quarkus;

import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.domain.service.WorkflowGovernanceService;
import com.forwardmeasure.openworkflow.definition.domain.service.WorkflowManagementService;
import com.forwardmeasure.openworkflow.definition.domain.service.jpa.JpaWorkflowGovernanceService;
import com.forwardmeasure.openworkflow.definition.domain.service.jpa.JpaWorkflowManagementService;
import com.forwardmeasure.openworkflow.definition.infrastructure.persistence.WorkflowTransactionExecutor;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.CorrelationIdProvider;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.StudioAuthorizationResource;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.WorkflowDefinitionGovernanceResource;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.WorkflowDefinitionResource;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.WorkflowManagementResource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.persistence.EntityManager;

/**
 * Quarkus CDI composition for the definition-management resources. {@code
 * StudioAuthorizationResource} is produced by the central {@code openworkflow-quarkus-binding}
 * module, not here - it touches no repository/EntityManager, so it needs nothing capability
 * specific.
 */
@ApplicationScoped
public class OpenWorkflowDefinitionManagementQuarkusBinding {

  @Produces
  @ApplicationScoped
  OpenWorkflowCompiler openWorkflowCompiler() {
    return new OpenWorkflowCompiler();
  }

  @Produces
  @ApplicationScoped
  StudioAuthorizationResource studioAuthorizationResource(
      AuthorizationService authorization, ActiveOrganizationProvider organizations) {
    return new StudioAuthorizationResource(authorization, organizations);
  }

  @Produces
  @ApplicationScoped
  WorkflowManagementService workflowManagementService(
      EntityManager entityManager,
      TenantScope tenantScope,
      WorkflowTransactionExecutor transactions,
      AuthorizationService authorization) {
    return new JpaWorkflowManagementService(
        entityManager, tenantScope, transactions, authorization);
  }

  @Produces
  @ApplicationScoped
  WorkflowGovernanceService workflowGovernanceService(
      EntityManager entityManager,
      TenantScope tenantScope,
      WorkflowTransactionExecutor transactions,
      AuthorizationService authorization,
      OpenWorkflowCompiler compiler) {
    return new JpaWorkflowGovernanceService(
        entityManager, tenantScope, transactions, authorization, compiler);
  }

  @Produces
  @ApplicationScoped
  WorkflowManagementResource workflowManagementResource(
      WorkflowManagementService service,
      ActiveOrganizationProvider organizations,
      CorrelationIdProvider correlationIds) {
    return new WorkflowManagementResource(service, organizations, correlationIds);
  }

  @Produces
  @ApplicationScoped
  WorkflowDefinitionResource workflowDefinitionResource(
      WorkflowGovernanceService service,
      ActiveOrganizationProvider organizations,
      CorrelationIdProvider correlationIds) {
    return new WorkflowDefinitionResource(service, organizations, correlationIds);
  }

  @Produces
  @ApplicationScoped
  WorkflowDefinitionGovernanceResource workflowDefinitionGovernanceResource(
      WorkflowGovernanceService service,
      ActiveOrganizationProvider organizations,
      CorrelationIdProvider correlationIds) {
    return new WorkflowDefinitionGovernanceResource(service, organizations, correlationIds);
  }
}
