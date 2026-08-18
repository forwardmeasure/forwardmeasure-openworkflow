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
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceBundleCodec;
import com.forwardmeasure.openworkflow.engine.api.DefinitionRevision;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEffect;
import com.forwardmeasure.openworkflow.engine.api.ExecutionError;
import com.forwardmeasure.openworkflow.engine.api.ExecutionHistoryEntry;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import com.forwardmeasure.openworkflow.engine.api.ExecutionProjection;
import com.forwardmeasure.openworkflow.engine.api.ExecutionTimer;
import com.forwardmeasure.openworkflow.execution.query.ExecutionPage;
import com.forwardmeasure.openworkflow.execution.query.ExecutionQueryRepository;
import com.forwardmeasure.openworkflow.execution.query.ExecutionSearch;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Tenant-bound PostgreSQL implementation of the canonical execution query contract. */
public final class JpaExecutionQueryRepository implements ExecutionQueryRepository {
  private static final String PROJECTION_SELECT =
      "select"
          + " e.uuid,e.version,r.uuid,r.resolved_document,r.resolved_resources,r.resolved_digest,e.engine_id,"
          + "coalesce(p.lifecycle_state,e.lifecycle_state),e.correlation_id,"
          + "greatest(coalesce(p.last_sequence,-1),0),"
          + "e.created_at,e.updated_at,e.completed_at,e.input,p.output,p.error,coalesce(p.effects,'[]'::jsonb),coalesce(p.timers,'[]'::jsonb)"
          + " from ";

  private final com.forwardmeasure.openworkflow.engine.api.TenantId tenantId;
  private final String schema;
  private final EntityManager entityManager;
  private final ObjectMapper objectMapper;
  private final OpenWorkflowCompiler compiler = new OpenWorkflowCompiler();
  private final JavaType effectsType;
  private final JavaType timersType;

  public JpaExecutionQueryRepository(
      TenantId tenantId, EntityManager entityManager, ObjectMapper objectMapper) {
    Objects.requireNonNull(tenantId, "tenantId");
    this.tenantId = new com.forwardmeasure.openworkflow.engine.api.TenantId(tenantId.value());
    this.schema = TenantSchema.forTenant(tenantId).value();
    this.entityManager = Objects.requireNonNull(entityManager, "entityManager");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    this.effectsType =
        objectMapper.getTypeFactory().constructCollectionType(List.class, ExecutionEffect.class);
    this.timersType =
        objectMapper.getTypeFactory().constructCollectionType(List.class, ExecutionTimer.class);
  }

  @Override
  public Optional<ExecutionProjection> find(
      com.forwardmeasure.openworkflow.engine.api.TenantId requestedTenant,
      ExecutionId executionId) {
    requireTenant(requestedTenant, executionId);
    List<?> rows =
        entityManager
            .createNativeQuery(projectionSql() + " where e.uuid=?1")
            .setParameter(1, executionId.value())
            .setMaxResults(1)
            .getResultList();
    return rows.stream().findFirst().map(this::mapProjection);
  }

  @Override
  public ExecutionPage search(ExecutionSearch search) {
    Objects.requireNonNull(search, "search");
    if (!tenantId.equals(search.tenantId())) {
      return new ExecutionPage(List.of(), null);
    }
    var sql = new StringBuilder(projectionSql()).append(" where 1=1");
    var parameters = new ArrayList<Object>();
    if (!search.states().isEmpty()) {
      sql.append(" and coalesce(p.lifecycle_state,e.lifecycle_state) in (");
      appendParameters(sql, parameters, search.states().stream().map(Enum::name).toList());
      sql.append(')');
    }
    add(
        sql,
        parameters,
        search.engineId() == null ? null : search.engineId().value(),
        "e.engine_id");
    add(sql, parameters, search.correlationId(), "e.correlation_id");
    addBound(sql, parameters, search.createdFrom(), "e.created_at", ">=");
    addBound(sql, parameters, search.createdUntil(), "e.created_at", "<=");
    if (search.cursor() != null) {
      Cursor cursor = decode(search.cursor());
      sql.append(" and (e.created_at<?")
          .append(parameters.size() + 1)
          .append(" or (e.created_at=?")
          .append(parameters.size() + 1)
          .append(" and e.uuid<?")
          .append(parameters.size() + 2)
          .append("))");
      parameters.add(cursor.createdAt());
      parameters.add(cursor.executionId());
    }
    sql.append(" order by e.created_at desc,e.uuid desc");
    Query query = entityManager.createNativeQuery(sql.toString()).setMaxResults(search.limit() + 1);
    for (int index = 0; index < parameters.size(); index++) {
      query.setParameter(index + 1, parameters.get(index));
    }
    List<?> rows = query.getResultList();
    List<ExecutionProjection> items = rows.stream().map(this::mapProjection).toList();
    if (items.size() <= search.limit()) {
      return new ExecutionPage(items, null);
    }
    List<ExecutionProjection> page = List.copyOf(items.subList(0, search.limit()));
    ExecutionProjection last = page.getLast();
    return new ExecutionPage(page, encode(last.createdAt(), last.executionId().value()));
  }

  @Override
  public List<ExecutionHistoryEntry> history(
      com.forwardmeasure.openworkflow.engine.api.TenantId requestedTenant,
      ExecutionId executionId,
      long afterSequence,
      int limit) {
    requireTenant(requestedTenant, executionId);
    if (afterSequence < -1 || limit < 1 || limit > 500) {
      throw new IllegalArgumentException("invalid history page");
    }
    List<?> rows =
        entityManager
            .createNativeQuery(
                "select h.event_id,h.sequence_number,h.lifecycle_state,h.event_type,h.task_path,"
                    + "h.occurred_at,h.event_data from "
                    + schema
                    + ".workflow_execution_history h join "
                    + schema
                    + ".workflow_execution e on e.id=h.execution_id"
                    + " where e.uuid=?1 and h.sequence_number>?2 order by h.sequence_number")
            .setParameter(1, executionId.value())
            .setParameter(2, afterSequence)
            .setMaxResults(limit)
            .getResultList();
    return rows.stream().map(this::mapHistory).toList();
  }

  private String projectionSql() {
    return PROJECTION_SELECT
        + schema
        + ".workflow_execution e join "
        + schema
        + ".workflow_revision r on r.id=e.revision_id left join "
        + schema
        + ".workflow_execution_projection p on p.execution_id=e.id";
  }

  private ExecutionProjection mapProjection(Object row) {
    Object[] values = (Object[]) row;
    var plan =
        compiler.compile(
            values[3].toString().getBytes(StandardCharsets.UTF_8),
            WorkflowResourceBundleCodec.decode(values[4].toString()));
    var definition =
        new DefinitionRevision(
            (UUID) values[2], plan.coordinates(), values[5].toString(), plan.compilerSha256());
    return new ExecutionProjection(
        new ExecutionId(tenantId, (UUID) values[0]),
        new EngineId(values[6].toString()),
        definition,
        ExecutionLifecycleState.valueOf(values[7].toString()),
        ((Number) values[1]).longValue(),
        values[8].toString(),
        ((Number) values[9]).longValue(),
        instant(values[10]),
        instant(values[11]),
        values[12] == null ? null : instant(values[12]),
        json(values[13].toString()),
        values[14] == null ? null : json(values[14].toString()),
        values[15] == null ? null : read(values[15].toString(), ExecutionError.class),
        read(values[16].toString(), effectsType),
        read(values[17].toString(), timersType));
  }

  private ExecutionHistoryEntry mapHistory(Object row) {
    Object[] values = (Object[]) row;
    return new ExecutionHistoryEntry(
        (UUID) values[0],
        ((Number) values[1]).longValue(),
        ExecutionLifecycleState.valueOf(values[2].toString()),
        values[3].toString(),
        values[4] == null ? null : values[4].toString(),
        instant(values[5]),
        json(values[6].toString()));
  }

  private void requireTenant(
      com.forwardmeasure.openworkflow.engine.api.TenantId requestedTenant,
      ExecutionId executionId) {
    if (!tenantId.equals(requestedTenant) || !tenantId.equals(executionId.tenantId())) {
      throw new IllegalArgumentException("execution does not exist");
    }
  }

  private JsonNode json(String value) {
    return read(value, JsonNode.class);
  }

  private <T> T read(String value, Class<T> type) {
    try {
      return objectMapper.readValue(value, type);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("invalid canonical projection JSON", exception);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T read(String value, JavaType type) {
    try {
      return (T) objectMapper.readValue(value, type);
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException("invalid canonical projection JSON", exception);
    }
  }

  private static void add(StringBuilder sql, List<Object> parameters, Object value, String column) {
    if (value != null) {
      sql.append(" and ").append(column).append("=?").append(parameters.size() + 1);
      parameters.add(value);
    }
  }

  private static void addBound(
      StringBuilder sql, List<Object> parameters, Object value, String column, String operator) {
    if (value != null) {
      sql.append(" and ").append(column).append(operator).append('?').append(parameters.size() + 1);
      parameters.add(value);
    }
  }

  private static void appendParameters(
      StringBuilder sql, List<Object> parameters, List<String> values) {
    for (String value : values) {
      if (!values.getFirst().equals(value)) {
        sql.append(',');
      }
      sql.append('?').append(parameters.size() + 1);
      parameters.add(value);
    }
  }

  private static String encode(Instant createdAt, UUID executionId) {
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString((createdAt + "|" + executionId).getBytes(StandardCharsets.UTF_8));
  }

  private static Cursor decode(String value) {
    try {
      String decoded = new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
      int separator = decoded.indexOf('|');
      return new Cursor(
          Instant.parse(decoded.substring(0, separator)),
          UUID.fromString(decoded.substring(separator + 1)));
    } catch (RuntimeException exception) {
      throw new IllegalArgumentException("invalid execution cursor", exception);
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

  private record Cursor(Instant createdAt, UUID executionId) {}
}
