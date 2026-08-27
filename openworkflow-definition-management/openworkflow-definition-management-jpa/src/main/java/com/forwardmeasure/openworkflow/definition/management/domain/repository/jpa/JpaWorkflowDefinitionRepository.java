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
import com.forwardmeasure.jpa.core.repository.AbstractAuditedEntityRepository;
import com.forwardmeasure.jpa.identity.entity.Actor;
import com.forwardmeasure.openworkflow.definition.domain.entity.Workflow;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowDefinition;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowLifecycleState;
import com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowPublication;
import com.forwardmeasure.openworkflow.definition.domain.repository.WorkflowDefinitionRepository;
import com.forwardmeasure.openworkflow.definition.infrastructure.persistence.WorkflowActorResolver;
import com.forwardmeasure.openworkflow.definition.management.DefinitionManagementException;
import jakarta.persistence.EntityManager;
import jakarta.persistence.OptimisticLockException;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.TypedQuery;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Hibernate-backed implementation of {@link WorkflowDefinitionRepository}. Constructed fresh, per
 * request, bound to an {@link EntityManager} whose connection is already scoped to the correct
 * tenant's schema.
 */
public final class JpaWorkflowDefinitionRepository
    extends AbstractAuditedEntityRepository<WorkflowDefinition, Long>
    implements WorkflowDefinitionRepository {
  private final WorkflowActorResolver actors;

  public JpaWorkflowDefinitionRepository(EntityManager entityManager) {
    Objects.requireNonNull(entityManager, "entityManager");
    bindPersistenceContext(entityManager);
    this.actors = new WorkflowActorResolver(entityManager);
  }

  @Override
  public int nextRevisionNumber(Workflow workflow) {
    Integer maximum =
        entityManager()
            .createQuery(
                "select max(definition.revisionNumber) from WorkflowDefinition definition "
                    + "where definition.workflow = :workflow",
                Integer.class)
            .setParameter("workflow", workflow)
            .getSingleResult();
    return maximum == null ? 1 : maximum + 1;
  }

  @Override
  public WorkflowDefinition create(
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
      String resolvedDigest) {
    Actor author = actors.resolve(authorActorId);
    WorkflowDefinition definition =
        new WorkflowDefinition(
            workflow,
            revisionNumber,
            sourceDocument,
            resolvedDocument,
            resolvedResources,
            namespace,
            documentVersion,
            specificationVersion,
            compilerProfile,
            sourceDigest,
            resolvedDigest,
            author);
    try {
      persist(definition);
      flush();
    } catch (PersistenceException failure) {
      throw DefinitionManagementException.conflict(
          "Workflow definition revision " + revisionNumber + " already exists");
    }
    return definition;
  }

  @Override
  public Optional<WorkflowDefinition> findByWorkflowAndUuid(Workflow workflow, UUID definitionId) {
    Optional<WorkflowDefinition> definition =
        findByUuid(definitionId)
            .filter(candidate -> candidate.getWorkflow().getId().equals(workflow.getId()));
    definition.ifPresent(JpaWorkflowDefinitionRepository::initializeForApi);
    return definition;
  }

  @Override
  public Page<WorkflowDefinition> listByWorkflow(
      Workflow workflow, WorkflowLifecycleState status, int offset, int limit) {
    String base =
        "from WorkflowDefinition definition where definition.workflow = :workflow"
            + (status == null ? "" : " and definition.lifecycleState = :status");
    TypedQuery<WorkflowDefinition> dataQuery =
        entityManager()
            .createQuery(
                "select definition " + base + " order by definition.revisionNumber",
                WorkflowDefinition.class)
            .setParameter("workflow", workflow);
    TypedQuery<Long> countQuery =
        entityManager()
            .createQuery("select count(definition) " + base, Long.class)
            .setParameter("workflow", workflow);
    if (status != null) {
      dataQuery.setParameter("status", status);
      countQuery.setParameter("status", status);
    }
    List<WorkflowDefinition> items =
        dataQuery.setFirstResult(offset).setMaxResults(limit).getResultList();
    items.forEach(JpaWorkflowDefinitionRepository::initializeForApi);
    return new Page<>(items, countQuery.getSingleResult(), offset, limit);
  }

  @Override
  public Page<WorkflowDefinition> search(WorkflowLifecycleState status, int offset, int limit) {
    String base =
        "from WorkflowDefinition definition"
            + (status == null ? "" : " where definition.lifecycleState = :status");
    TypedQuery<WorkflowDefinition> dataQuery =
        entityManager()
            .createQuery(
                "select definition " + base + " order by definition.createdAt",
                WorkflowDefinition.class);
    TypedQuery<Long> countQuery =
        entityManager().createQuery("select count(definition) " + base, Long.class);
    if (status != null) {
      dataQuery.setParameter("status", status);
      countQuery.setParameter("status", status);
    }
    List<WorkflowDefinition> items =
        dataQuery.setFirstResult(offset).setMaxResults(limit).getResultList();
    items.forEach(JpaWorkflowDefinitionRepository::initializeForApi);
    return new Page<>(items, countQuery.getSingleResult(), offset, limit);
  }

  // workflow, author, and publication (plus publication's own actor) are all
  // lazy, and the API mapper reads every one of them after this repository's
  // transaction/session has already closed - each must be resolved here,
  // while the session is still open. create() doesn't need this: it sets
  // workflow/author directly from already-resolved callers, and a
  // freshly-persisted definition's publication is a plain null, not a proxy.
  private static void initializeForApi(WorkflowDefinition definition) {
    definition.getWorkflow().getUuid();
    definition.getAuthor().getUuid();
    WorkflowPublication publication = definition.getPublication();
    if (publication != null) {
      publication.getActor().getUuid();
    }
  }

  @Override
  public void delete(WorkflowDefinition definition) {
    try {
      super.delete(definition);
      flush();
    } catch (OptimisticLockException failure) {
      throw DefinitionManagementException.preconditionFailed(
          "Workflow definition was modified concurrently; retry with the current ETag");
    } catch (PersistenceException failure) {
      throw DefinitionManagementException.conflict(
          "Workflow definition cannot be deleted: it is referenced elsewhere");
    }
  }
}
