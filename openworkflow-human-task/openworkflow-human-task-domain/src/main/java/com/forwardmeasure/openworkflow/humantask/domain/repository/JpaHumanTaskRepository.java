/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.domain.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.jpa.core.repository.AbstractBaseRepository;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskCommandReceipt;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskContentRevisionRecord;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskHistoryPage;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskHistoryRecord;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskListQuery;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskNotFoundException;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskOutboxMessage;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskPage;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskRepository;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskView;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.Assignment;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskEvent;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskId;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState;
import com.forwardmeasure.openworkflow.humantask.domain.entity.HumanTaskCommandReceiptEntity;
import com.forwardmeasure.openworkflow.humantask.domain.entity.HumanTaskContentRevisionEntity;
import com.forwardmeasure.openworkflow.humantask.domain.entity.HumanTaskEntity;
import com.forwardmeasure.openworkflow.humantask.domain.entity.HumanTaskEventEntity;
import com.forwardmeasure.openworkflow.humantask.domain.entity.HumanTaskOutboxEntity;
import com.forwardmeasure.openworkflow.humantask.domain.entity.HumanTaskReviewSessionEntity;
import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tenant-bound JPA implementation of the atomic Human Task persistence port. */
public final class JpaHumanTaskRepository extends AbstractBaseRepository<HumanTaskEntity, Long>
    implements HumanTaskRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(JpaHumanTaskRepository.class);
  private final ObjectMapper json;

  public JpaHumanTaskRepository(EntityManager entityManager, ObjectMapper json) {
    bindPersistenceContext(Objects.requireNonNull(entityManager, "entityManager"));
    this.json = Objects.requireNonNull(json, "json");
  }

  public JpaHumanTaskRepository(ObjectMapper json) {
    this.json = Objects.requireNonNull(json, "json");
  }

  @Override
  public Optional<HumanTaskState> find(HumanTaskId taskId) {
    Objects.requireNonNull(taskId, "taskId");
    return findEntity(taskId).map(entity -> read(entity.snapshot(), HumanTaskState.class));
  }

  @Override
  public Optional<HumanTaskView> findView(HumanTaskId taskId) {
    return findEntity(taskId)
        .map(
            entity ->
                new HumanTaskView(
                    read(entity.snapshot(), HumanTaskState.class),
                    entity.receivedAt(),
                    entity.updatedAt()));
  }

  @Override
  public HumanTaskPage list(HumanTaskListQuery query) {
    Objects.requireNonNull(query, "query");
    SortField sortField = SortField.resolve(query.sort());
    Cursor cursor = decodeCursor(query.cursor(), query.sort(), query.direction());
    Map<String, Object> parameters = new HashMap<>();
    StringBuilder hql = new StringBuilder("select task from HumanTaskEntity task where 1=1");
    if (!query.statuses().isEmpty()) {
      hql.append(" and task.status in :statuses");
      parameters.put("statuses", query.statuses());
    }
    if (query.taskType() != null && !query.taskType().isBlank()) {
      hql.append(" and task.taskType = :taskType");
      parameters.put("taskType", query.taskType());
    }
    if (query.assignment() != null && !query.assignment().isBlank()) {
      hql.append(" and task.assignmentPrincipal = :assignment");
      parameters.put("assignment", query.assignment());
    }
    if (query.reviewer() != null && !query.reviewer().isBlank()) {
      hql.append(" and task.reviewerId = :reviewer");
      parameters.put("reviewer", query.reviewer());
    }
    if (query.overdue()) {
      hql.append(
          " and task.dueAt < :now and task.status not in "
              + "('APPROVED','REJECTED','RESOLVED','CANCELLED','EXPIRED')");
      parameters.put("now", query.now());
    }
    if (sortField.blotterKey() != null) {
      parameters.put("blotterKey", sortField.blotterKey());
    }
    if (cursor != null) {
      appendCursor(hql, parameters, sortField, query.direction(), cursor);
    }
    String direction = query.direction() == HumanTaskListQuery.Direction.ASC ? "asc" : "desc";
    hql.append(" order by ")
        .append(sortField.expression())
        .append(' ')
        .append(direction)
        .append(" nulls last, task.taskId ")
        .append(direction);

    TypedQuery<HumanTaskEntity> statement =
        entityManager().createQuery(hql.toString(), HumanTaskEntity.class);
    parameters.forEach(statement::setParameter);
    List<HumanTaskEntity> entities = statement.setMaxResults(query.limit() + 1).getResultList();
    boolean more = entities.size() > query.limit();
    if (more) entities = new ArrayList<>(entities.subList(0, query.limit()));
    List<HumanTaskView> items = entities.stream().map(this::toView).toList();
    String nextCursor =
        more
            ? encodeCursor(
                query.sort(),
                query.direction(),
                sortField.value(entities.getLast()),
                entities.getLast().taskId())
            : null;
    return new HumanTaskPage(items, nextCursor);
  }

  @Override
  public HumanTaskHistoryPage history(HumanTaskId taskId, long afterSequence, int limit) {
    requireTask(taskId);
    List<HumanTaskEventEntity> entities =
        entityManager()
            .createQuery(
                "select event from HumanTaskEventEntity event "
                    + "where event.task.taskId = :taskId and event.sequence > :afterSequence "
                    + "order by event.sequence asc",
                HumanTaskEventEntity.class)
            .setParameter("taskId", taskId.value())
            .setParameter("afterSequence", afterSequence)
            .setMaxResults(limit + 1)
            .getResultList();
    boolean more = entities.size() > limit;
    if (more) entities = new ArrayList<>(entities.subList(0, limit));
    List<HumanTaskHistoryRecord> records =
        entities.stream()
            .map(
                entity ->
                    new HumanTaskHistoryRecord(
                        entity.sequence(), read(entity.eventData(), HumanTaskEvent.class)))
            .toList();
    return new HumanTaskHistoryPage(records, more ? records.getLast().sequence() : null);
  }

  @Override
  public List<HumanTaskContentRevisionRecord> revisions(HumanTaskId taskId) {
    requireTask(taskId);
    return entityManager()
        .createQuery(
            "select revision from HumanTaskContentRevisionEntity revision "
                + "where revision.task.taskId = :taskId order by revision.contentRevision asc",
            HumanTaskContentRevisionEntity.class)
        .setParameter("taskId", taskId.value())
        .getResultList()
        .stream()
        .map(entity -> toRevision(taskId, entity))
        .toList();
  }

  @Override
  public Optional<HumanTaskCommandReceipt> findReceipt(String commandId) {
    HumanTaskCommandReceiptEntity entity =
        entityManager().find(HumanTaskCommandReceiptEntity.class, commandId);
    if (entity == null) {
      return Optional.empty();
    }
    return Optional.of(
        new HumanTaskCommandReceipt(
            entity.commandId(),
            entity.requestSha256(),
            read(entity.resultingState(), HumanTaskState.class),
            entity.createdAt()));
  }

  @Override
  public void commit(
      HumanTaskState priorState,
      HumanTaskState resultingState,
      List<HumanTaskEvent> events,
      HumanTaskCommandReceipt receipt,
      List<HumanTaskOutboxMessage> outboxMessages) {
    Objects.requireNonNull(resultingState, "resultingState");
    Objects.requireNonNull(events, "events");
    Objects.requireNonNull(receipt, "receipt");
    Objects.requireNonNull(outboxMessages, "outboxMessages");
    if (events.isEmpty()) {
      throw new IllegalArgumentException("An accepted commit requires at least one event");
    }

    HumanTaskId taskId = resultingState.snapshot().definition().taskId();
    HumanTaskEntity task =
        priorState == null
            ? new HumanTaskEntity()
            : findEntity(taskId)
                .orElseThrow(
                    () -> new IllegalStateException("Human task disappeared during commit"));
    if (priorState != null && task.domainRevision() != priorState.revision()) {
      throw new HumanTaskOptimisticConflictException(
          "Stored task revision changed before the transition was committed");
    }

    Instant updatedAt = events.getLast().metadata().occurredAt();
    Instant receivedAt =
        priorState == null ? events.getFirst().metadata().occurredAt() : task.receivedAt();
    replaceProjection(task, resultingState, receivedAt, updatedAt);
    if (priorState == null) {
      entityManager().persist(task);
      entityManager().flush();
    }

    long sequence = priorState == null ? 0 : priorState.revision();
    String activeSessionId = activeSessionId(priorState);
    for (HumanTaskEvent event : events) {
      sequence++;
      JsonNode eventJson = json.valueToTree(event);
      entityManager()
          .persist(
              new HumanTaskEventEntity(
                  task,
                  sequence,
                  event.metadata().commandId(),
                  eventJson.required("type").textValue(),
                  event.metadata().actor().actorId(),
                  event.metadata().occurredAt(),
                  eventJson));
      persistContentRevision(task, priorState, event);
      activeSessionId = applyReviewSession(task, activeSessionId, event);
    }

    String commandType = json.valueToTree(events.getFirst()).required("type").textValue();
    entityManager()
        .persist(
            new HumanTaskCommandReceiptEntity(
                task,
                receipt.commandId(),
                commandType,
                receipt.requestSha256(),
                receipt.resultingState().revision(),
                json.valueToTree(receipt.resultingState()),
                receipt.createdAt()));
    for (HumanTaskOutboxMessage message : outboxMessages) {
      entityManager()
          .persist(
              new HumanTaskOutboxEntity(
                  task,
                  message.messageId(),
                  message.workflowCorrelation(),
                  message.taskPath(),
                  message.messageType(),
                  json.valueToTree(message.payload()),
                  message.createdAt()));
    }
  }

  private Optional<HumanTaskEntity> findEntity(HumanTaskId taskId) {
    return entityManager()
        .createQuery(
            "select task from HumanTaskEntity task where task.taskId = :taskId",
            HumanTaskEntity.class)
        .setParameter("taskId", taskId.value())
        .getResultStream()
        .findFirst();
  }

  private HumanTaskEntity requireTask(HumanTaskId taskId) {
    return findEntity(taskId)
        .orElseThrow(() -> new HumanTaskNotFoundException("Human task does not exist: " + taskId));
  }

  private HumanTaskView toView(HumanTaskEntity entity) {
    LOGGER.debug("Mapping Human Task entity {} to application view", entity.taskId());
    return HumanTaskEntityMapper.INSTANCE.toView(entity, json);
  }

  private HumanTaskContentRevisionRecord toRevision(
      HumanTaskId taskId, HumanTaskContentRevisionEntity entity) {
    return new HumanTaskContentRevisionRecord(
        taskId,
        entity.contentRevision(),
        entity.basedOnRevision(),
        entity.createdBy(),
        entity.createdAt(),
        entity.reviewSessionId(),
        entity.beforeSha256(),
        entity.afterSha256(),
        entity.jsonPatchReference() == null
            ? null
            : read(
                entity.jsonPatchReference(),
                com.forwardmeasure.openworkflow.data.DataReference.class),
        read(
            entity.resultContentReference(),
            com.forwardmeasure.openworkflow.data.DataReference.class),
        entity.comment());
  }

  private void appendCursor(
      StringBuilder hql,
      Map<String, Object> parameters,
      SortField sort,
      HumanTaskListQuery.Direction direction,
      Cursor cursor) {
    String comparison = direction == HumanTaskListQuery.Direction.ASC ? ">" : "<";
    String expression = sort.expression();
    if (cursor.nullValue()) {
      hql.append(" and (")
          .append(expression)
          .append(" is null and task.taskId ")
          .append(comparison)
          .append(" :cursorTaskId)");
    } else {
      hql.append(" and ((")
          .append(expression)
          .append(" is not null and (")
          .append(expression)
          .append(' ')
          .append(comparison)
          .append(" :cursorValue or (")
          .append(expression)
          .append(" = :cursorValue and task.taskId ")
          .append(comparison)
          .append(" :cursorTaskId))) or ")
          .append(expression)
          .append(" is null)");
      parameters.put("cursorValue", sort.parse(cursor.value()));
    }
    parameters.put("cursorTaskId", cursor.taskId());
  }

  private String encodeCursor(
      String sort, HumanTaskListQuery.Direction direction, Object value, String taskId) {
    try {
      byte[] payload =
          json.writeValueAsBytes(
              new Cursor(
                  sort,
                  direction.name(),
                  value == null ? null : value.toString(),
                  value == null,
                  taskId));
      return Base64.getUrlEncoder().withoutPadding().encodeToString(payload);
    } catch (JsonProcessingException exception) {
      throw new HumanTaskPersistenceException("Could not encode Human Task cursor", exception);
    }
  }

  private Cursor decodeCursor(String encoded, String sort, HumanTaskListQuery.Direction direction) {
    if (encoded == null || encoded.isBlank()) return null;
    try {
      byte[] payload = Base64.getUrlDecoder().decode(encoded.getBytes(StandardCharsets.US_ASCII));
      Cursor cursor = json.readValue(payload, Cursor.class);
      if (!cursor.sort().equals(sort) || !cursor.direction().equals(direction.name())) {
        throw new IllegalArgumentException("Cursor does not belong to the requested sort order");
      }
      return cursor;
    } catch (IOException | IllegalArgumentException exception) {
      throw new IllegalArgumentException("Human Task cursor is invalid", exception);
    }
  }

  private record Cursor(
      String sort, String direction, String value, boolean nullValue, String taskId) {}

  private record SortField(String expression, String property, String blotterKey, ValueKind kind) {
    static SortField resolve(String requested) {
      return switch (requested) {
        case "status" -> stringField("task.status", "status");
        case "taskType" -> stringField("task.taskType", "taskType");
        case "source" -> stringField("task.sourceId", "sourceId");
        case "priority" -> new SortField("task.priority", "priority", null, ValueKind.INTEGER);
        case "assignment" -> stringField("task.assignmentPrincipal", "assignmentPrincipal");
        case "reviewer" -> stringField("task.reviewerId", "reviewerId");
        case "receivedAt" -> instantField("task.receivedAt", "receivedAt");
        case "dueAt" -> instantField("task.dueAt", "dueAt");
        default -> {
          if (!requested.startsWith("blotter.") || requested.length() == "blotter.".length()) {
            throw new IllegalArgumentException("Unsupported Human Task sort field: " + requested);
          }
          String key = requested.substring("blotter.".length());
          yield new SortField(
              "cast(function('jsonb_extract_path_text', task.blotterFields, :blotterKey) as"
                  + " String)",
              null,
              key,
              ValueKind.STRING);
        }
      };
    }

    private static SortField stringField(String expression, String property) {
      return new SortField(expression, property, null, ValueKind.STRING);
    }

    private static SortField instantField(String expression, String property) {
      return new SortField(expression, property, null, ValueKind.INSTANT);
    }

    Object value(HumanTaskEntity entity) {
      if (blotterKey != null) {
        JsonNode value = entity.blotterFields().get(blotterKey);
        return value == null || value.isNull() ? null : value.asText();
      }
      return switch (property) {
        case "status" -> entity.status();
        case "taskType" -> entity.taskType();
        case "sourceId" -> entity.sourceId();
        case "priority" -> entity.priority();
        case "assignmentPrincipal" -> entity.assignmentPrincipal();
        case "reviewerId" -> entity.reviewerId();
        case "receivedAt" -> entity.receivedAt();
        case "dueAt" -> entity.dueAt();
        default -> throw new AssertionError("Unhandled Human Task sort property: " + property);
      };
    }

    Object parse(String value) {
      return switch (kind) {
        case STRING -> value;
        case INTEGER -> Integer.valueOf(value);
        case INSTANT -> Instant.parse(value);
      };
    }
  }

  private enum ValueKind {
    STRING,
    INTEGER,
    INSTANT
  }

  private void replaceProjection(
      HumanTaskEntity entity, HumanTaskState state, Instant receivedAt, Instant updatedAt) {
    var definition = state.snapshot().definition();
    Assignment assignment = assignment(state);
    String reviewer =
        state instanceof HumanTaskState.Claimed claimed
            ? claimed.reviewSession().heldBy().actorId()
            : null;
    entity.replace(
        definition.taskId().value(),
        state.revision(),
        status(state),
        definition.taskType(),
        definition.title(),
        definition.priority(),
        definition.source().kind().name(),
        definition.source().sourceId(),
        definition.reviewPlan().stages().get(state.snapshot().stageIndex()).stageId(),
        assignment == null ? null : assignment.kind().name(),
        assignment == null ? null : assignment.principal(),
        reviewer,
        receivedAt,
        definition.dueAt(),
        definition.expiresAt(),
        updatedAt,
        json.valueToTree(state),
        json.valueToTree(definition.blotterFields()));
  }

  private void persistContentRevision(
      HumanTaskEntity task, HumanTaskState priorState, HumanTaskEvent event) {
    if (event instanceof HumanTaskEvent.TaskCreated created) {
      entityManager()
          .persist(
              new HumanTaskContentRevisionEntity(
                  task,
                  0,
                  0,
                  event.metadata().actor().actorId(),
                  event.metadata().occurredAt(),
                  null,
                  null,
                  created.contentSha256(),
                  null,
                  json.valueToTree(created.definition().originalContent()),
                  "Original task content"));
    } else if (event instanceof HumanTaskEvent.ResolutionRevised revised) {
      entityManager()
          .persist(
              new HumanTaskContentRevisionEntity(
                  task,
                  revised.contentRevision(),
                  revised.basedOnContentRevision(),
                  event.metadata().actor().actorId(),
                  event.metadata().occurredAt(),
                  activeSessionId(priorState),
                  revised.beforeSha256(),
                  revised.afterSha256(),
                  revised.jsonPatch() == null ? null : json.valueToTree(revised.jsonPatch()),
                  json.valueToTree(revised.content()),
                  revised.comment()));
    }
  }

  private String applyReviewSession(
      HumanTaskEntity task, String activeSessionId, HumanTaskEvent event) {
    if (event instanceof HumanTaskEvent.ReviewStarted started) {
      var session = started.reviewSession();
      entityManager()
          .persist(
              new HumanTaskReviewSessionEntity(
                  task,
                  session.reviewSessionId(),
                  session.stageId(),
                  session.heldBy().actorId(),
                  session.acquiredAt(),
                  session.lastRenewedAt(),
                  session.expiresAt(),
                  session.leaseTokenDigest()));
      return session.reviewSessionId();
    }
    if (event instanceof HumanTaskEvent.ReviewLeaseRenewed renewed) {
      requireSession(activeSessionId).renew(event.metadata().occurredAt(), renewed.expiresAt());
      return activeSessionId;
    }
    String explicitlyClosed =
        switch (event) {
          case HumanTaskEvent.ReviewReleased released -> released.reviewSessionId();
          case HumanTaskEvent.ReviewLeaseExpired expired -> expired.reviewSessionId();
          case HumanTaskEvent.ReviewReassigned reassigned -> reassigned.displacedReviewSessionId();
          default -> null;
        };
    if (explicitlyClosed != null) {
      requireSession(explicitlyClosed).release(event.metadata().occurredAt());
      return null;
    }
    if (activeSessionId != null && endsReviewSession(event)) {
      requireSession(activeSessionId).release(event.metadata().occurredAt());
      return null;
    }
    return activeSessionId;
  }

  private HumanTaskReviewSessionEntity requireSession(String reviewSessionId) {
    HumanTaskReviewSessionEntity session =
        entityManager()
            .createQuery(
                "select session from HumanTaskReviewSessionEntity session "
                    + "where session.reviewSessionId = :reviewSessionId",
                HumanTaskReviewSessionEntity.class)
            .setParameter("reviewSessionId", reviewSessionId)
            .getSingleResult();
    return session;
  }

  private static boolean endsReviewSession(HumanTaskEvent event) {
    return event instanceof HumanTaskEvent.ReviewStageAdvanced
        || event instanceof HumanTaskEvent.TaskReworkRequested
        || event instanceof HumanTaskEvent.TaskEscalated
        || event instanceof HumanTaskEvent.TaskApproved
        || event instanceof HumanTaskEvent.TaskRejected
        || event instanceof HumanTaskEvent.TaskResolved
        || event instanceof HumanTaskEvent.TaskCancelled
        || event instanceof HumanTaskEvent.TaskExpired;
  }

  private static String activeSessionId(HumanTaskState state) {
    return state instanceof HumanTaskState.Claimed claimed
        ? claimed.reviewSession().reviewSessionId()
        : null;
  }

  private static Assignment assignment(HumanTaskState state) {
    return switch (state) {
      case HumanTaskState.Assigned assigned -> assigned.assignment();
      case HumanTaskState.Claimed claimed -> claimed.assignment();
      case HumanTaskState.AwaitingNextStage awaiting -> awaiting.assignment();
      case HumanTaskState.ReworkRequested rework -> rework.assignment();
      case HumanTaskState.Open ignored -> null;
      case HumanTaskState.Approved ignored -> null;
      case HumanTaskState.Rejected ignored -> null;
      case HumanTaskState.Resolved ignored -> null;
      case HumanTaskState.Cancelled ignored -> null;
      case HumanTaskState.Expired ignored -> null;
    };
  }

  private static String status(HumanTaskState state) {
    return switch (state) {
      case HumanTaskState.Open ignored -> "OPEN";
      case HumanTaskState.Assigned ignored -> "ASSIGNED";
      case HumanTaskState.Claimed ignored -> "CLAIMED";
      case HumanTaskState.AwaitingNextStage ignored -> "AWAITING_NEXT_STAGE";
      case HumanTaskState.ReworkRequested ignored -> "REWORK_REQUESTED";
      case HumanTaskState.Approved ignored -> "APPROVED";
      case HumanTaskState.Rejected ignored -> "REJECTED";
      case HumanTaskState.Resolved ignored -> "RESOLVED";
      case HumanTaskState.Cancelled ignored -> "CANCELLED";
      case HumanTaskState.Expired ignored -> "EXPIRED";
    };
  }

  private <T> T read(JsonNode value, Class<T> type) {
    try {
      return json.treeToValue(value, type);
    } catch (JsonProcessingException exception) {
      throw new HumanTaskPersistenceException("Stored Human Task JSON is invalid", exception);
    }
  }

  public static final class HumanTaskOptimisticConflictException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    HumanTaskOptimisticConflictException(String message) {
      super(message);
    }
  }

  public static final class HumanTaskPersistenceException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    HumanTaskPersistenceException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
