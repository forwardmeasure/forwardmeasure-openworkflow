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
package com.forwardmeasure.openworkflow.definition.persistence;

import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceBundleCodec;
import com.forwardmeasure.openworkflow.definition.management.DefinitionManagementException;
import com.forwardmeasure.openworkflow.definition.management.DefinitionRepository;
import com.forwardmeasure.openworkflow.definition.management.ManagedWorkflowRevision;
import jakarta.persistence.EntityManager;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** JPA adapter bound to one already tenant-scoped persistence context. */
public final class JpaDefinitionRepository implements DefinitionRepository {
  private final TenantId tenantId;
  private final EntityManager entityManager;
  private final WorkflowDefinitionRepository definitions = new WorkflowDefinitionRepository();
  private final WorkflowRevisionRepository revisions = new WorkflowRevisionRepository();

  public JpaDefinitionRepository(TenantId tenantId, EntityManager entityManager) {
    this.tenantId = Objects.requireNonNull(tenantId, "tenantId");
    this.entityManager = Objects.requireNonNull(entityManager, "entityManager");
    definitions.bindPersistenceContext(entityManager);
    revisions.bindPersistenceContext(entityManager);
  }

  @Override
  public boolean exists(TenantId requestedTenant, String definitionKey) {
    requireTenant(requestedTenant);
    return definitions.findByKey(definitionKey).isPresent();
  }

  @Override
  public int nextRevisionNumber(TenantId requestedTenant, String definitionKey) {
    requireTenant(requestedTenant);
    return revisions.nextRevisionNumber(definitionKey);
  }

  @Override
  public void save(
      TenantId requestedTenant,
      ManagedWorkflowRevision revision,
      String actingActorId,
      String correlationId) {
    requireTenant(requestedTenant);
    Optional<WorkflowRevisionEntity> current =
        revisions.findRevision(revision.definitionKey(), revision.revisionNumber());
    if (current.isPresent()) {
      WorkflowRevisionEntity persisted = current.orElseThrow();
      requireSameContent(persisted, revision);
      var from = persisted.getLifecycleState();
      persisted.transitionTo(revision.lifecycleState());
      recordTransition(persisted, from, actingActorId, correlationId);
      return;
    }
    WorkflowDefinitionEntity definition =
        definitions
            .findByKey(revision.definitionKey())
            .orElseGet(
                () -> {
                  WorkflowDefinitionEntity created =
                      new WorkflowDefinitionEntity(
                          revision.definitionKey(), revision.displayName());
                  definitions.persist(created);
                  return created;
                });
    definition.rename(revision.displayName());
    WorkflowRevisionEntity created =
        new WorkflowRevisionEntity(
            definition,
            revision.revisionNumber(),
            revision.sourceDocument(),
            revision.resolvedDocument(),
            WorkflowResourceBundleCodec.encode(revision.resolvedResources()),
            revision.specificationVersion(),
            revision.compilerProfile(),
            revision.sourceDigest(),
            revision.resolvedDigest(),
            actorDatabaseId(revision.authorActorId()));
    created.setUuid(revision.revisionId());
    revisions.persist(created);
    entityManager.flush();
    recordValidation(created);
    recordHistory(created, null, revision.lifecycleState(), actingActorId, correlationId);
  }

  @Override
  public Optional<ManagedWorkflowRevision> find(
      TenantId requestedTenant, String definitionKey, int revisionNumber) {
    requireTenant(requestedTenant);
    return revisions.findRevision(definitionKey, revisionNumber).map(this::map);
  }

  @Override
  public List<ManagedWorkflowRevision> list(TenantId requestedTenant) {
    requireTenant(requestedTenant);
    return revisions.list().stream().map(this::map).toList();
  }

  private ManagedWorkflowRevision map(WorkflowRevisionEntity entity) {
    return new ManagedWorkflowRevision(
        entity.getUuid(),
        entity.getDefinition().getDefinitionKey(),
        entity.getDefinition().getDisplayName(),
        entity.getRevisionNumber(),
        entity.getLifecycleState(),
        entity.getSourceDocument(),
        entity.getResolvedDocument(),
        WorkflowResourceBundleCodec.decode(entity.getResolvedResources()),
        entity.getSpecificationVersion(),
        entity.getCompilerProfile(),
        entity.getSourceDigest(),
        entity.getResolvedDigest(),
        actorSubject(entity.getAuthorActorId()));
  }

  private long actorDatabaseId(String subject) {
    String actorTable = TenantSchema.forTenant(tenantId).value() + ".actor";
    List<?> ids = actorIds(actorTable, subject);
    if (ids.isEmpty()) {
      entityManager
          .createNativeQuery(
              "insert into "
                  + actorTable
                  + " (id,version,uuid,subject_identifier,identity_type,identity_provider)"
                  + " values (nextval('"
                  + TenantSchema.forTenant(tenantId).value()
                  + ".actor_id_seq'),0,?1,?2,'HUMAN','keycloak') on conflict"
                  + " (identity_provider,subject_identifier) do nothing")
          .setParameter(
              1, UUID.nameUUIDFromBytes(("keycloak:" + subject).getBytes(StandardCharsets.UTF_8)))
          .setParameter(2, subject)
          .executeUpdate();
      ids = actorIds(actorTable, subject);
    }
    Object id = ids.stream().findFirst().orElseThrow();
    return ((Number) id).longValue();
  }

  private List<?> actorIds(String actorTable, String subject) {
    return entityManager
        .createNativeQuery("select id from " + actorTable + " where subject_identifier = ?1")
        .setParameter(1, subject)
        .setMaxResults(1)
        .getResultList();
  }

  private String actorSubject(long id) {
    String actorTable = TenantSchema.forTenant(tenantId).value() + ".actor";
    return entityManager
        .createNativeQuery("select subject_identifier from " + actorTable + " where id = ?1")
        .setParameter(1, id)
        .getSingleResult()
        .toString();
  }

  private void requireTenant(TenantId requestedTenant) {
    if (!tenantId.equals(requestedTenant)) {
      throw new DefinitionManagementException("Persistence context is bound to another tenant");
    }
  }

  private static void requireSameContent(
      WorkflowRevisionEntity persisted, ManagedWorkflowRevision requested) {
    if (!persisted.getSourceDigest().equals(requested.sourceDigest())
        || !persisted.getResolvedDigest().equals(requested.resolvedDigest())) {
      throw new DefinitionManagementException("Immutable revision content cannot be replaced");
    }
  }

  private void recordValidation(WorkflowRevisionEntity revision) {
    String schema = TenantSchema.forTenant(tenantId).value();
    entityManager
        .createNativeQuery(
            "insert into "
                + schema
                + ".workflow_validation (revision_id,valid,findings,validator_profile) values"
                + " (?1,true,cast(?2 as jsonb),?3)")
        .setParameter(1, revision.getId())
        .setParameter(2, "[]")
        .setParameter(3, revision.getCompilerProfile())
        .executeUpdate();
  }

  private void recordTransition(
      WorkflowRevisionEntity revision,
      com.forwardmeasure.openworkflow.definition.management.WorkflowLifecycleState from,
      String actingActorId,
      String correlationId) {
    var target = revision.getLifecycleState();
    recordHistory(revision, from, target, actingActorId, correlationId);
    String schema = TenantSchema.forTenant(tenantId).value();
    long actorId = actorDatabaseId(actingActorId);
    if (target
            == com.forwardmeasure.openworkflow.definition.management.WorkflowLifecycleState.APPROVED
        || target
            == com.forwardmeasure.openworkflow.definition.management.WorkflowLifecycleState
                .REJECTED) {
      entityManager
          .createNativeQuery(
              "insert into "
                  + schema
                  + ".workflow_review"
                  + " (revision_id,review_action,actor_id,revision_digest) values (?1,?2,?3,?4)")
          .setParameter(1, revision.getId())
          .setParameter(2, target.name())
          .setParameter(3, actorId)
          .setParameter(4, revision.getResolvedDigest())
          .executeUpdate();
    } else if (target
        == com.forwardmeasure.openworkflow.definition.management.WorkflowLifecycleState.PUBLISHED) {
      entityManager
          .createNativeQuery(
              "insert into "
                  + schema
                  + ".workflow_publication"
                  + " (revision_id,actor_id,revision_digest) values (?1,?2,?3)")
          .setParameter(1, revision.getId())
          .setParameter(2, actorId)
          .setParameter(3, revision.getResolvedDigest())
          .executeUpdate();
    } else if (target
        == com.forwardmeasure.openworkflow.definition.management.WorkflowLifecycleState
            .DEPRECATED) {
      entityManager
          .createNativeQuery(
              "update "
                  + schema
                  + ".workflow_publication set deprecated_at = current_timestamp where revision_id"
                  + " = ?1")
          .setParameter(1, revision.getId())
          .executeUpdate();
    }
  }

  private void recordHistory(
      WorkflowRevisionEntity revision,
      com.forwardmeasure.openworkflow.definition.management.WorkflowLifecycleState from,
      com.forwardmeasure.openworkflow.definition.management.WorkflowLifecycleState target,
      String actingActorId,
      String correlationId) {
    String schema = TenantSchema.forTenant(tenantId).value();
    entityManager
        .createNativeQuery(
            "insert into "
                + schema
                + ".workflow_lifecycle_history"
                + " (revision_id,from_state,to_state,actor_id,correlation_id)"
                + " values (?1,?2,?3,?4,?5)")
        .setParameter(1, revision.getId())
        .setParameter(2, from == null ? null : from.name())
        .setParameter(3, target.name())
        .setParameter(4, actorDatabaseId(actingActorId))
        .setParameter(5, correlationId)
        .executeUpdate();
  }
}
