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
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowPublication;
import com.forwardmeasure.openworkflow.definition.domain.repository.WorkflowPublicationRepository;
import com.forwardmeasure.openworkflow.definition.infrastructure.persistence.WorkflowActorResolver;
import com.forwardmeasure.openworkflow.definition.management.DefinitionManagementException;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceException;
import java.util.Objects;

/** Hibernate-backed implementation of {@link WorkflowPublicationRepository}. */
public final class JpaWorkflowPublicationRepository
    extends AbstractBaseRepository<WorkflowPublication, Long>
    implements WorkflowPublicationRepository {
  private final WorkflowActorResolver actors;

  public JpaWorkflowPublicationRepository(EntityManager entityManager) {
    Objects.requireNonNull(entityManager, "entityManager");
    bindPersistenceContext(entityManager);
    this.actors = new WorkflowActorResolver(entityManager);
  }

  @Override
  public WorkflowPublication publish(
      WorkflowDefinition definition, String actorId, String definitionDigest) {
    Actor actor = actors.resolve(actorId);
    WorkflowPublication publication = new WorkflowPublication(definition, actor, definitionDigest);
    try {
      persist(publication);
      flush();
    } catch (PersistenceException failure) {
      throw DefinitionManagementException.conflict(
          "Workflow definition " + definition.getUuid() + " already has a publication record");
    }
    return publication;
  }
}
