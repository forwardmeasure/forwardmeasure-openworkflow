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

import com.forwardmeasure.jpa.core.query.Page;
import com.forwardmeasure.jpa.core.query.PageRequest;
import com.forwardmeasure.jpa.core.repository.AbstractAuditedEntityRepository;
import com.forwardmeasure.openworkflow.definition.domain.entity.Workflow;
import com.forwardmeasure.openworkflow.definition.domain.entity.Workflow_;
import com.forwardmeasure.openworkflow.definition.domain.repository.WorkflowRepository;
import com.forwardmeasure.openworkflow.definition.infrastructure.persistence.WorkflowActorResolver;
import com.forwardmeasure.openworkflow.definition.management.DefinitionManagementException;
import jakarta.persistence.EntityManager;
import jakarta.persistence.OptimisticLockException;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Root;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Hibernate-backed implementation of {@link WorkflowRepository}. Constructed fresh, per request,
 * bound to an {@link EntityManager} whose connection the caller has already scoped to one tenant's
 * schema — tenant isolation is a property of that connection, not of anything checked here.
 */
public final class JpaWorkflowRepository extends AbstractAuditedEntityRepository<Workflow, Long>
    implements WorkflowRepository {
  private final WorkflowActorResolver actors;

  public JpaWorkflowRepository(EntityManager entityManager) {
    Objects.requireNonNull(entityManager, "entityManager");
    bindPersistenceContext(entityManager);
    this.actors = new WorkflowActorResolver(entityManager);
  }

  @Override
  public Workflow create(String ownerActorId, String name, String title, String description) {
    Workflow workflow = new Workflow(name, title, description);
    workflow.setOwner(actors.resolve(ownerActorId));
    try {
      persist(workflow);
      flush();
    } catch (PersistenceException failure) {
      throw DefinitionManagementException.conflict("Workflow already exists: " + name);
    }
    return workflow;
  }

  @Override
  public boolean existsByName(String name) {
    CriteriaBuilder builder = criteriaBuilder();
    CriteriaQuery<Long> query = builder.createQuery(Long.class);
    Root<Workflow> root = query.from(Workflow.class);
    query.select(builder.count(root)).where(builder.equal(root.get(Workflow_.name), name));
    return entityManager().createQuery(query).getSingleResult() > 0L;
  }

  @Override
  public Optional<Workflow> findById(UUID workflowId) {
    Optional<Workflow> workflow = findByUuid(workflowId);
    workflow.ifPresent(JpaWorkflowRepository::initializeOwner);
    return workflow;
  }

  @Override
  public Page<Workflow> list(int offset, int limit) {
    Page<Workflow> page = page(new PageRequest(offset, limit, List.of()));
    page.items().forEach(JpaWorkflowRepository::initializeOwner);
    return page;
  }

  // owner is lazy (OwnedEntity), and the API mapper reads it after this
  // repository's transaction/session has already closed - touching it here,
  // while the session is still open, forces Hibernate to resolve the proxy
  // before the entity is handed back, matching how create() already sets a
  // fully-resolved owner rather than a lazy one.
  private static void initializeOwner(Workflow workflow) {
    workflow.getOwner().getUuid();
  }

  @Override
  public void delete(Workflow workflow) {
    try {
      super.delete(workflow);
      flush();
    } catch (OptimisticLockException failure) {
      throw DefinitionManagementException.preconditionFailed(
          "Workflow was modified concurrently; retry with the current ETag");
    } catch (PersistenceException failure) {
      throw DefinitionManagementException.conflict(
          "A workflow can be deleted only after all of its definition versions have been deleted");
    }
  }
}
