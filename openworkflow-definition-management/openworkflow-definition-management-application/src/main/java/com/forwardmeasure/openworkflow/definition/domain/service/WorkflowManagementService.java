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
import com.forwardmeasure.openworkflow.definition.domain.entity.Workflow;
import com.forwardmeasure.openworkflow.definition.management.api.model.CreateWorkflowRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.UpdateWorkflowRequest;
import java.util.UUID;

/**
 * The stable workflow identity: create, list, get, update, delete. Nothing about content or
 * governance.
 */
public interface WorkflowManagementService {
  Workflow createWorkflow(
      ActiveOrganization actor, String correlationId, CreateWorkflowRequest request);

  Page<Workflow> listWorkflows(
      ActiveOrganization actor, String correlationId, int offset, int limit);

  Workflow getWorkflow(ActiveOrganization actor, String correlationId, UUID workflowId);

  Workflow updateWorkflow(
      ActiveOrganization actor,
      String correlationId,
      UUID workflowId,
      int expectedVersion,
      UpdateWorkflowRequest request);

  void deleteWorkflow(
      ActiveOrganization actor, String correlationId, UUID workflowId, int expectedVersion);
}
