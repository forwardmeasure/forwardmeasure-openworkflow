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
import java.util.Optional;
import java.util.UUID;

/**
 * Persistence port for the {@link Workflow} identity aggregate. The real, Hibernate-backed
 * implementation lives in {@code -jpa}; this module knows nothing about {@code EntityManager}.
 *
 * <p>No {@code TenantId} appears on any method here. An implementation is constructed fresh, per
 * request, bound to an {@code EntityManager} whose connection is already scoped to one tenant's
 * schema before the implementation exists — there is no "which tenant" decision left to make per
 * call, and no benefit to re-stating it on every method.
 */
public interface WorkflowRepository {
  Workflow create(String ownerActorId, String name, String title, String description);

  boolean existsByName(String name);

  Optional<Workflow> findById(UUID workflowId);

  Page<Workflow> list(int offset, int limit);

  /**
   * Deletes a workflow. Does not itself check for existing definitions — the {@code
   * fk_definition_workflow} foreign key (no {@code ON DELETE CASCADE}) rejects the delete at the
   * database level if any {@code WorkflowDefinition} still references this workflow, which the
   * {@code -jpa} implementation translates into {@link
   * com.forwardmeasure.openworkflow.definition.management.DefinitionManagementException#conflict}.
   * Not re-implemented here as an application-level count check across aggregates.
   */
  void delete(Workflow workflow);
}
