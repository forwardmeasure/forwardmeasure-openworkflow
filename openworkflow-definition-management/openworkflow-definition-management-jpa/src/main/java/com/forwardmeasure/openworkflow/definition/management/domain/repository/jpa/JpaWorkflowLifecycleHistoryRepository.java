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
package com.forwardmeasure.openworkflow.definition.management.domain.repository.jpa;

import com.forwardmeasure.jpa.core.repository.AbstractBaseRepository;
import com.forwardmeasure.jpa.identity.entity.Actor;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowDefinition;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowLifecycleHistory;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowLifecycleState;
import com.forwardmeasure.openworkflow.definition.domain.repository.WorkflowLifecycleHistoryRepository;
import com.forwardmeasure.openworkflow.definition.infrastructure.persistence.WorkflowActorResolver;
import jakarta.persistence.EntityManager;
import java.util.Objects;

/** Hibernate-backed implementation of {@link WorkflowLifecycleHistoryRepository}. */
public final class JpaWorkflowLifecycleHistoryRepository
    extends AbstractBaseRepository<WorkflowLifecycleHistory, Long>
    implements WorkflowLifecycleHistoryRepository {
  private final WorkflowActorResolver actors;

  public JpaWorkflowLifecycleHistoryRepository(EntityManager entityManager) {
    Objects.requireNonNull(entityManager, "entityManager");
    bindPersistenceContext(entityManager);
    this.actors = new WorkflowActorResolver(entityManager);
  }

  @Override
  public WorkflowLifecycleHistory record(
      WorkflowDefinition definition,
      WorkflowLifecycleState fromState,
      WorkflowLifecycleState toState,
      String actorId,
      String correlationId) {
    Actor actor = actors.resolve(actorId);
    WorkflowLifecycleHistory entry =
        new WorkflowLifecycleHistory(definition, fromState, toState, actor, correlationId);
    persist(entry);
    return entry;
  }
}
