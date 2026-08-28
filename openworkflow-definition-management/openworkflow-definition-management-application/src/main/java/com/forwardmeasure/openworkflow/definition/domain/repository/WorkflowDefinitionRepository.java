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
package com.forwardmeasure.openworkflow.definition.domain.repository;

import com.forwardmeasure.jpa.core.query.Page;
import com.forwardmeasure.openworkflow.definition.domain.entity.Workflow;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowDefinition;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowLifecycleState;
import java.util.Optional;
import java.util.UUID;

/**
 * Persistence port for {@link WorkflowDefinition} — the {@code workflow_definition} table only.
 * Governance transitions mutate the fetched entity directly (setters, {@code transitionTo}, {@code
 * setContent}); Hibernate's dirty checking persists the change, so there is no separate {@code
 * update}/{@code transition} method here, matching how {@code WorkflowRepository} never needed one
 * either.
 */
public interface WorkflowDefinitionRepository {
  int nextRevisionNumber(Workflow workflow);

  WorkflowDefinition create(
      Workflow workflow,
      int revisionNumber,
      String authorActorId,
      String sourceDocument,
      String resolvedDocument,
      String resolvedResources,
      String namespace,
      String documentVersion,
      String specificationVersion,
      String compilerProfile,
      String sourceDigest,
      String resolvedDigest);

  /**
   * A DRAFT revision whose source didn't compile - see WorkflowDefinition's matching constructor.
   */
  WorkflowDefinition create(
      Workflow workflow,
      int revisionNumber,
      String authorActorId,
      String sourceDocument,
      String sourceDigest,
      String documentVersion);

  Optional<WorkflowDefinition> findByWorkflowAndUuid(Workflow workflow, UUID definitionId);

  Page<WorkflowDefinition> listByWorkflow(
      Workflow workflow, WorkflowLifecycleState status, int offset, int limit);

  /** Tenant-wide, cross-workflow — backs {@code searchWorkflowDefinitions}. */
  Page<WorkflowDefinition> search(WorkflowLifecycleState status, int offset, int limit);

  void delete(WorkflowDefinition definition);
}
