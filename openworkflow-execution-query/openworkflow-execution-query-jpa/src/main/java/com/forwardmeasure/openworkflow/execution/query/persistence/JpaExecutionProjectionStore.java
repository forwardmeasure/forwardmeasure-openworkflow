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
package com.forwardmeasure.openworkflow.execution.query.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEvent;
import com.forwardmeasure.openworkflow.execution.query.ExecutionProjectionStore;
import jakarta.persistence.EntityManager;
import java.util.List;
import java.util.Objects;

/** Atomic PostgreSQL projector bound to exactly one tenant schema. */
public final class JpaExecutionProjectionStore implements ExecutionProjectionStore {
  private final com.forwardmeasure.openworkflow.engine.api.TenantId tenantId;
  private final String schema;
  private final EntityManager entityManager;
  private final ObjectMapper objectMapper;

  public JpaExecutionProjectionStore(
      TenantId tenantId, EntityManager entityManager, ObjectMapper objectMapper) {
    Objects.requireNonNull(tenantId, "tenantId");
    this.tenantId = new com.forwardmeasure.openworkflow.engine.api.TenantId(tenantId.value());
    this.schema = TenantSchema.forTenant(tenantId).value();
    this.entityManager = Objects.requireNonNull(entityManager, "entityManager");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
  }

  @Override
  public ProjectionApplyResult apply(ExecutionEvent event) {
    Objects.requireNonNull(event, "event");
    if (!tenantId.equals(event.executionId().tenantId())) {
      return ProjectionApplyResult.NOT_FOUND;
    }
    if (eventExists(event)) {
      return ProjectionApplyResult.DUPLICATE;
    }
    List<?> rows =
        entityManager
            .createNativeQuery(
                "select e.id,e.engine_id,coalesce(p.last_sequence,-1) from "
                    + schema
                    + ".workflow_execution e left join "
                    + schema
                    + ".workflow_execution_projection p on p.execution_id=e.id"
                    + " where e.uuid=?1 for update of e")
            .setParameter(1, event.executionId().value())
            .setMaxResults(1)
            .getResultList();
    if (rows.isEmpty()) {
      return ProjectionApplyResult.NOT_FOUND;
    }
    Object[] row = (Object[]) rows.getFirst();
    long executionDatabaseId = ((Number) row[0]).longValue();
    if (!event.engineId().value().equals(row[1].toString())) {
      return ProjectionApplyResult.ENGINE_MISMATCH;
    }
    long lastSequence = ((Number) row[2]).longValue();
    if (event.sequence() <= lastSequence) {
      return ProjectionApplyResult.STALE;
    }
    if (event.sequence() != lastSequence + 1) {
      return ProjectionApplyResult.OUT_OF_ORDER;
    }
    insertHistory(executionDatabaseId, event);
    upsertProjection(executionDatabaseId, event);
    entityManager
        .createNativeQuery(
            "update "
                + schema
                + ".workflow_execution set"
                + " lifecycle_state=?1,version=greatest(version,?2),updated_at=?3,completed_at=case"
                + " when ?4 then ?3 else completed_at end where id=?5")
        .setParameter(1, event.state().name())
        .setParameter(2, event.sequence() + 1)
        .setParameter(3, event.occurredAt())
        .setParameter(4, event.state().terminal())
        .setParameter(5, executionDatabaseId)
        .executeUpdate();
    return ProjectionApplyResult.APPLIED;
  }

  @Override
  public ProjectionApplyResult applyNext(ExecutionEvent event) {
    Objects.requireNonNull(event, "event");
    if (!tenantId.equals(event.executionId().tenantId())) {
      return ProjectionApplyResult.NOT_FOUND;
    }
    if (eventExists(event)) {
      return ProjectionApplyResult.DUPLICATE;
    }
    List<?> rows =
        entityManager
            .createNativeQuery(
                "select coalesce(p.last_sequence,-1) from "
                    + schema
                    + ".workflow_execution e left join "
                    + schema
                    + ".workflow_execution_projection p on p.execution_id=e.id"
                    + " where e.uuid=?1 for update of e")
            .setParameter(1, event.executionId().value())
            .setMaxResults(1)
            .getResultList();
    if (rows.isEmpty()) {
      return ProjectionApplyResult.NOT_FOUND;
    }
    long sequence = ((Number) rows.getFirst()).longValue() + 1;
    return apply(
        new ExecutionEvent(
            event.eventId(),
            event.commandId(),
            event.executionId(),
            event.engineId(),
            sequence,
            event.type(),
            event.state(),
            event.occurredAt(),
            event.data()));
  }

  private boolean eventExists(ExecutionEvent event) {
    return !entityManager
        .createNativeQuery(
            "select 1 from " + schema + ".workflow_execution_history where event_id=?1")
        .setParameter(1, event.eventId())
        .setMaxResults(1)
        .getResultList()
        .isEmpty();
  }

  private void insertHistory(long executionId, ExecutionEvent event) {
    String taskPath =
        event.data().hasNonNull("taskPath") ? event.data().get("taskPath").asText() : null;
    entityManager
        .createNativeQuery(
            "insert into "
                + schema
                + ".workflow_execution_history"
                + " (execution_id,event_id,sequence_number,event_type,lifecycle_state,task_path,"
                + "event_data,occurred_at) values (?1,?2,?3,?4,?5,?6,cast(?7 as jsonb),?8)")
        .setParameter(1, executionId)
        .setParameter(2, event.eventId())
        .setParameter(3, event.sequence())
        .setParameter(4, event.type().name())
        .setParameter(5, event.state().name())
        .setParameter(6, taskPath)
        .setParameter(7, json(event.data()))
        .setParameter(8, event.occurredAt())
        .executeUpdate();
  }

  private void upsertProjection(long executionId, ExecutionEvent event) {
    String output = event.type() == ExecutionEvent.EventType.COMPLETED ? json(event.data()) : null;
    String error =
        event.type() == ExecutionEvent.EventType.ERROR_RAISED
                || event.type() == ExecutionEvent.EventType.FAILED
            ? json(event.data())
            : null;
    entityManager
        .createNativeQuery(
            "insert into "
                + schema
                + ".workflow_execution_projection"
                + " (execution_id,last_event_id,last_sequence,lifecycle_state,output,error,updated_at)"
                + " values (?1,?2,?3,?4,cast(?5 as jsonb),cast(?6 as jsonb),?7) on conflict"
                + " (execution_id) do update set last_event_id=excluded.last_event_id,"
                + "last_sequence=excluded.last_sequence,lifecycle_state=excluded.lifecycle_state,"
                + "output=coalesce(excluded.output,workflow_execution_projection.output),"
                + "error=coalesce(excluded.error,workflow_execution_projection.error),"
                + "updated_at=excluded.updated_at")
        .setParameter(1, executionId)
        .setParameter(2, event.eventId())
        .setParameter(3, event.sequence())
        .setParameter(4, event.state().name())
        .setParameter(5, output)
        .setParameter(6, error)
        .setParameter(7, event.occurredAt())
        .executeUpdate();
  }

  private String json(com.fasterxml.jackson.databind.JsonNode value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("invalid projection JSON", exception);
    }
  }
}
