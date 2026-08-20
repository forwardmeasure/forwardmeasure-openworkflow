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
package com.forwardmeasure.openworkflow.execution.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceBundleCodec;
import com.forwardmeasure.openworkflow.engine.api.ActorId;
import com.forwardmeasure.openworkflow.engine.api.CommandAcknowledgement;
import com.forwardmeasure.openworkflow.engine.api.DefinitionRevision;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import com.forwardmeasure.openworkflow.execution.management.CanonicalExecution;
import com.forwardmeasure.openworkflow.execution.management.CommandReceipt;
import com.forwardmeasure.openworkflow.execution.management.ExecutionManagementException;
import com.forwardmeasure.openworkflow.execution.management.ExecutionRepository;
import jakarta.persistence.EntityManager;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** PostgreSQL canonical execution repository bound to one tenant persistence context. */
public final class JpaExecutionRepository implements ExecutionRepository {
  private final TenantId tenantId;
  private final com.forwardmeasure.openworkflow.engine.api.TenantId engineTenantId;
  private final String schema;
  private final EntityManager entityManager;
  private final ObjectMapper objectMapper;
  private final OpenWorkflowCompiler compiler;

  public JpaExecutionRepository(
      TenantId tenantId, EntityManager entityManager, ObjectMapper objectMapper) {
    this.tenantId = Objects.requireNonNull(tenantId, "tenantId");
    this.engineTenantId = new com.forwardmeasure.openworkflow.engine.api.TenantId(tenantId.value());
    this.schema = TenantSchema.forTenant(tenantId).value();
    this.entityManager = Objects.requireNonNull(entityManager, "entityManager");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    this.compiler = new OpenWorkflowCompiler();
  }

  @Override
  public Optional<CanonicalExecution> findByIdempotencyKey(
      com.forwardmeasure.openworkflow.engine.api.TenantId requestedTenant, String idempotencyKey) {
    requireTenant(requestedTenant);
    return findByIdempotency(idempotencyKey);
  }

  @Override
  public Admission admit(CanonicalExecution candidate) {
    requireTenant(candidate.executionId().tenantId());
    ensureActor(candidate.startedBy());
    int inserted =
        entityManager
            .createNativeQuery(
                "insert into "
                    + schema
                    + ".workflow_execution"
                    + " (uuid,version,revision_id,revision_digest,engine_id,lifecycle_state,"
                    + "correlation_id,idempotency_key,started_by_actor_id,input,created_at,updated_at)"
                    + " select ?1,?2,r.id,?3,?4,?5,?6,?7,a.id,cast(?8 as jsonb),?9,?9 from "
                    + schema
                    + ".workflow_revision r join "
                    + schema
                    + ".workflow_publication p on p.revision_id=r.id join "
                    + schema
                    + ".actor a on a.subject_identifier=?10"
                    + " where r.uuid=?11 and r.lifecycle_state='PUBLISHED'"
                    + " and r.resolved_digest=?3 and p.deprecated_at is null"
                    + " on conflict (idempotency_key) do nothing")
            .setParameter(1, candidate.executionId().value())
            .setParameter(2, candidate.version())
            .setParameter(3, candidate.definition().definitionSha256())
            .setParameter(4, candidate.engineId().value())
            .setParameter(5, candidate.state().name())
            .setParameter(6, candidate.correlationId())
            .setParameter(7, candidate.idempotencyKey())
            .setParameter(8, json(candidate.input()))
            .setParameter(9, candidate.createdAt())
            .setParameter(10, candidate.startedBy().value())
            .setParameter(11, candidate.definition().revisionId())
            .executeUpdate();
    Optional<CanonicalExecution> admitted = findByIdempotency(candidate.idempotencyKey());
    if (admitted.isEmpty()) {
      throw new ExecutionManagementException(
          ExecutionManagementException.Kind.NOT_PUBLISHED,
          "revision is not an active published immutable revision");
    }
    return new Admission(admitted.orElseThrow(), inserted == 1);
  }

  private void ensureActor(ActorId actor) {
    entityManager
        .createNativeQuery(
            "insert into "
                + schema
                + ".actor (id,version,uuid,subject_identifier,identity_type,identity_provider)"
                + " values (nextval('"
                + schema
                + ".actor_id_seq'),0,?1,?2,'HUMAN','keycloak')"
                + " on conflict (identity_provider,subject_identifier) do nothing")
        .setParameter(
            1,
            UUID.nameUUIDFromBytes(("keycloak:" + actor.value()).getBytes(StandardCharsets.UTF_8)))
        .setParameter(2, actor.value())
        .executeUpdate();
  }

  @Override
  public Optional<CanonicalExecution> find(
      com.forwardmeasure.openworkflow.engine.api.TenantId requestedTenant,
      ExecutionId executionId) {
    requireTenant(requestedTenant);
    if (!requestedTenant.equals(executionId.tenantId())) {
      return Optional.empty();
    }
    return executionRows("e.uuid=?1", executionId.value()).stream().findFirst().map(this::map);
  }

  @Override
  public Optional<CommandReceipt> findReceipt(
      com.forwardmeasure.openworkflow.engine.api.TenantId requestedTenant, UUID commandId) {
    requireTenant(requestedTenant);
    List<?> rows =
        entityManager
            .createNativeQuery(
                "select c.command_id,e.uuid,c.command_type,c.expected_version,c.accepted,c.reason,"
                    + "a.subject_identifier,c.correlation_id,c.created_at from "
                    + schema
                    + ".workflow_command_receipt c join "
                    + schema
                    + ".workflow_execution e on e.id=c.execution_id join "
                    + schema
                    + ".actor a on a.id=c.actor_id where c.command_id=?1")
            .setParameter(1, commandId)
            .setMaxResults(1)
            .getResultList();
    return rows.stream().findFirst().map(row -> mapReceipt((Object[]) row));
  }

  @Override
  public CommandReceipt recordCommand(CommandReceipt candidate) {
    requireTenant(candidate.executionId().tenantId());
    entityManager
        .createNativeQuery(
            "insert into "
                + schema
                + ".workflow_command_receipt"
                + " (command_id,execution_id,command_type,expected_version,accepted,reason,actor_id,correlation_id,created_at)"
                + " select ?1,e.id,?2,?3,e.version=?3,case when e.version=?3 then ?4 else 'stale"
                + " execution version' end,a.id,?5,?6 from "
                + schema
                + ".workflow_execution e join "
                + schema
                + ".actor a on a.subject_identifier=?7 where e.uuid=?8"
                + " on conflict (command_id) do nothing")
        .setParameter(1, candidate.commandId())
        .setParameter(2, candidate.commandType())
        .setParameter(3, candidate.expectedVersion())
        .setParameter(4, candidate.reason())
        .setParameter(5, candidate.correlationId())
        .setParameter(6, candidate.createdAt())
        .setParameter(7, candidate.actorId().value())
        .setParameter(8, candidate.executionId().value())
        .executeUpdate();
    return findReceipt(engineTenantId, candidate.commandId())
        .orElseThrow(
            () ->
                new ExecutionManagementException(
                    ExecutionManagementException.Kind.NOT_FOUND,
                    "execution or actor does not exist"));
  }

  @Override
  public CanonicalExecution acknowledge(
      com.forwardmeasure.openworkflow.engine.api.TenantId requestedTenant,
      ExecutionId executionId,
      CommandAcknowledgement acknowledgement) {
    requireTenant(requestedTenant);
    int updated =
        entityManager
            .createNativeQuery(
                "update "
                    + schema
                    + ".workflow_execution set lifecycle_state=?1,version=?2,updated_at=?3,"
                    + "completed_at=case when ?4 then ?3 else completed_at end"
                    + " where uuid=?5 and engine_id=?6 and version<=?2")
            .setParameter(1, acknowledgement.state().name())
            .setParameter(2, acknowledgement.version())
            .setParameter(3, acknowledgement.acknowledgedAt())
            .setParameter(4, acknowledgement.state().terminal())
            .setParameter(5, executionId.value())
            .setParameter(6, acknowledgement.engineId().value())
            .executeUpdate();
    if (updated != 1) {
      throw new ExecutionManagementException(
          ExecutionManagementException.Kind.STALE_VERSION,
          "acknowledgement is stale or violates engine pinning");
    }
    return find(requestedTenant, executionId).orElseThrow();
  }

  private Optional<CanonicalExecution> findByIdempotency(String key) {
    return executionRows("e.idempotency_key=?1", key).stream().findFirst().map(this::map);
  }

  private List<?> executionRows(String predicate, Object parameter) {
    return entityManager
        .createNativeQuery(
            "select"
                + " e.uuid,e.version,r.uuid,r.resolved_document,r.resolved_resources,r.resolved_digest,e.engine_id,"
                + "e.lifecycle_state,e.idempotency_key,e.correlation_id,a.subject_identifier,e.input,e.created_at,e.updated_at,w.uuid"
                + " from "
                + schema
                + ".workflow_execution e join "
                + schema
                + ".workflow_definition r on r.id=e.revision_id join "
                + schema
                + ".workflow w on w.id=r.workflow_id join "
                + schema
                + ".actor a on a.id=e.started_by_actor_id where "
                + predicate)
        .setParameter(1, parameter)
        .setMaxResults(1)
        .getResultList();
  }

  private CanonicalExecution map(Object row) {
    Object[] values = (Object[]) row;
    var plan =
        compiler.compile(
            values[3].toString().getBytes(StandardCharsets.UTF_8),
            WorkflowResourceBundleCodec.decode(values[4].toString()));
    return new CanonicalExecution(
        new ExecutionId(engineTenantId, (UUID) values[0]),
        new DefinitionRevision(
            (UUID) values[2],
            (UUID) values[14],
            plan.coordinates(),
            values[5].toString(),
            plan.compilerSha256()),
        new EngineId(values[6].toString()),
        ExecutionLifecycleState.valueOf(values[7].toString()),
        ((Number) values[1]).longValue(),
        values[8].toString(),
        values[9].toString(),
        new ActorId(values[10].toString()),
        json(values[11].toString()),
        instant(values[12]),
        instant(values[13]));
  }

  private CommandReceipt mapReceipt(Object[] values) {
    return new CommandReceipt(
        (UUID) values[0],
        new ExecutionId(engineTenantId, (UUID) values[1]),
        values[2].toString(),
        ((Number) values[3]).longValue(),
        (Boolean) values[4],
        values[5] == null ? null : values[5].toString(),
        new ActorId(values[6].toString()),
        values[7].toString(),
        instant(values[8]));
  }

  private void requireTenant(com.forwardmeasure.openworkflow.engine.api.TenantId requestedTenant) {
    if (!engineTenantId.equals(requestedTenant)) {
      throw new ExecutionManagementException(
          ExecutionManagementException.Kind.NOT_FOUND, "execution does not exist");
    }
  }

  private String json(JsonNode value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("invalid execution JSON", exception);
    }
  }

  private JsonNode json(String value) {
    try {
      return objectMapper.readTree(value);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("invalid persisted execution JSON", exception);
    }
  }

  private static Instant instant(Object value) {
    return switch (value) {
      case Instant instant -> instant;
      case OffsetDateTime offset -> offset.toInstant();
      case Timestamp timestamp -> timestamp.toInstant();
      default -> throw new IllegalStateException("unsupported timestamp " + value);
    };
  }
}
