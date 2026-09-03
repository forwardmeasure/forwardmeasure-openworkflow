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
package com.forwardmeasure.openworkflow.definition.management.micronaut;

import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.domain.service.WorkflowGovernanceService;
import com.forwardmeasure.openworkflow.definition.domain.service.WorkflowManagementService;
import com.forwardmeasure.openworkflow.definition.domain.service.jpa.JpaWorkflowGovernanceService;
import com.forwardmeasure.openworkflow.definition.domain.service.jpa.JpaWorkflowManagementService;
import com.forwardmeasure.openworkflow.definition.infrastructure.persistence.WorkflowTransactionExecutor;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import jakarta.persistence.EntityManager;

/** Micronaut composition owned by definition management, not the shared framework host. */
@Factory
public class OpenWorkflowDefinitionManagementMicronautBinding {

  @Singleton
  OpenWorkflowCompiler openWorkflowCompiler() {
    return new OpenWorkflowCompiler();
  }

  @Singleton
  WorkflowManagementService workflowManagementService(
      EntityManager entityManager,
      TenantScope tenantScope,
      WorkflowTransactionExecutor transactions,
      AuthorizationService authorization) {
    return new JpaWorkflowManagementService(
        entityManager, tenantScope, transactions, authorization);
  }

  @Singleton
  WorkflowGovernanceService workflowGovernanceService(
      EntityManager entityManager,
      TenantScope tenantScope,
      WorkflowTransactionExecutor transactions,
      AuthorizationService authorization,
      OpenWorkflowCompiler compiler) {
    return new JpaWorkflowGovernanceService(
        entityManager, tenantScope, transactions, authorization, compiler);
  }
}
