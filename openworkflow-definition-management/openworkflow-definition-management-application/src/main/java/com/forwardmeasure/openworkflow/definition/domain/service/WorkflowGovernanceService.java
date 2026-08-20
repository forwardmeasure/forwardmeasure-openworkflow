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
package com.forwardmeasure.openworkflow.definition.domain.service;

import com.forwardmeasure.jpa.core.query.Page;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowDefinition;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowLifecycleState;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowDefinitionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.ReviewDecisionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.UpdateWorkflowDefinitionRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinitionValidation;
import java.util.UUID;

/**
 * The governed, maker-checker revision lifecycle: draft, validate, submit, review, publish,
 * deprecate.
 */
public interface WorkflowGovernanceService {
  WorkflowDefinition createWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      CreateWorkflowDefinitionRequest request);

  Page<WorkflowDefinition> listWorkflowDefinitions(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      WorkflowLifecycleState status,
      int offset,
      int limit);

  /** Tenant-wide, cross-workflow. Backs {@code searchWorkflowDefinitions}. */
  Page<WorkflowDefinition> searchWorkflowDefinitions(
      ActiveOrganization actor,
      String correlationId,
      WorkflowLifecycleState status,
      int offset,
      int limit);

  WorkflowDefinition getWorkflowDefinition(
      ActiveOrganization actor, String correlationId, UUID workflowId, UUID definitionId);

  WorkflowDefinition updateWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion,
      UpdateWorkflowDefinitionRequest request);

  void deleteWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion);

  /** Pure compute — recompiles the stored source and returns the result. No persisted record. */
  WorkflowDefinitionValidation validateWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion);

  WorkflowDefinition submitWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion);

  WorkflowDefinition withdrawWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion);

  WorkflowDefinition approveWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion,
      ReviewDecisionRequest request);

  WorkflowDefinition rejectWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion,
      ReviewDecisionRequest request);

  WorkflowDefinition publishWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion);

  WorkflowDefinition deprecateWorkflowDefinition(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      UUID definitionId,
      int expectedVersion);
}
