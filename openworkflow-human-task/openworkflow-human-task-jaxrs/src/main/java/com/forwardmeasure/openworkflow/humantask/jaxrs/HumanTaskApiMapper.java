/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.jaxrs;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskContentRevisionRecord;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskHistoryPage;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskHistoryRecord;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskPage;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskView;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.Assignment;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Lossless mapping between the portable domain/application model and generated API models. */
public final class HumanTaskApiMapper {
  private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {};
  private final ObjectMapper json;

  public HumanTaskApiMapper(ObjectMapper json) {
    this.json = Objects.requireNonNull(json, "json");
  }

  public com.forwardmeasure.openworkflow.humantask.api.model.HumanTask toApi(HumanTaskView view) {
    HumanTaskState state = view.state();
    var snapshot = state.snapshot();
    var definition = snapshot.definition();
    var stage = definition.reviewPlan().stages().get(snapshot.stageIndex());
    return new com.forwardmeasure.openworkflow.humantask.api.model.HumanTask()
        .taskId(definition.taskId().value())
        .taskType(definition.taskType())
        .title(definition.title())
        .description(definition.description())
        .priority(definition.priority())
        .status(status(state))
        .source(toApi(definition.source()))
        .originalContent(toApi(definition.originalContent()))
        .currentContent(toApi(snapshot.currentContent()))
        .stageId(stage.stageId())
        .stageName(stage.name())
        .reviewRound(snapshot.reviewRound())
        .contentRevision(snapshot.contentRevision())
        .revision(snapshot.revision())
        .assignment(toApi(assignment(state)))
        .activeReview(
            state instanceof HumanTaskState.Claimed claimed ? toApi(claimed.reviewSession()) : null)
        .dueAt(date(definition.dueAt()))
        .expiresAt(date(definition.expiresAt()))
        .receivedAt(date(view.receivedAt()))
        .updatedAt(date(view.updatedAt()))
        .blotterFields(toMap(json.valueToTree(definition.blotterFields())));
  }

  public com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskPage toApi(
      HumanTaskPage page) {
    return new com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskPage()
        .items(page.items().stream().map(this::toApi).toList())
        .nextCursor(page.nextCursor());
  }

  public com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskHistoryPage toApi(
      HumanTaskHistoryPage page) {
    return new com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskHistoryPage()
        .items(page.items().stream().map(this::toApi).toList())
        .nextAfterSequence(page.nextAfterSequence());
  }

  public com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskHistoryEvent toApi(
      HumanTaskHistoryRecord record) {
    JsonNode event = json.valueToTree(record.event());
    return new com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskHistoryEvent()
        .sequence(record.sequence())
        .eventType(event.required("type").textValue())
        .actorId(record.event().metadata().actor().actorId())
        .occurredAt(date(record.event().metadata().occurredAt()))
        .taskRevision(record.sequence())
        .data(toMap(event));
  }

  public List<com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskContentRevision>
      toApiRevisions(List<HumanTaskContentRevisionRecord> revisions) {
    return revisions.stream().map(this::toApi).toList();
  }

  public com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskContentRevision toApi(
      HumanTaskContentRevisionRecord revision) {
    return new com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskContentRevision()
        .taskId(revision.taskId().value())
        .contentRevision(revision.contentRevision())
        .basedOnRevision(revision.basedOnRevision())
        .createdBy(revision.createdBy())
        .createdAt(date(revision.createdAt()))
        .reviewSessionId(revision.reviewSessionId())
        .beforeSha256(revision.beforeSha256())
        .afterSha256(revision.afterSha256())
        .jsonPatch(toApi(revision.jsonPatch()))
        .resultContent(toApi(revision.resultContent()))
        .comment(revision.comment());
  }

  public com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskContentRevision toApi(
      com.forwardmeasure.openworkflow.humantask.domain.HumanTaskEvent.ResolutionRevised revision,
      String reviewSessionId) {
    return new com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskContentRevision()
        .taskId(revision.metadata().taskId().value())
        .contentRevision(revision.contentRevision())
        .basedOnRevision(revision.basedOnContentRevision())
        .createdBy(revision.metadata().actor().actorId())
        .createdAt(date(revision.metadata().occurredAt()))
        .reviewSessionId(reviewSessionId)
        .beforeSha256(revision.beforeSha256())
        .afterSha256(revision.afterSha256())
        .jsonPatch(toApi(revision.jsonPatch()))
        .resultContent(toApi(revision.content()))
        .comment(revision.comment());
  }

  public com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskPresentation presentation(
      HumanTaskView view) {
    var presentation = view.state().snapshot().definition().presentation();
    return new com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskPresentation()
        .kind(
            com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskPresentation.KindEnum
                .fromValue(presentation.kind().name().replace("A2_UI", "A2UI")))
        .inputSchema(toMap(presentation.inputSchema()))
        .uiSchema(toMap(presentation.uiSchema()))
        .resourceUri(
            presentation.resourceUri() == null ? null : URI.create(presentation.resourceUri()))
        .resourceSha256(presentation.resourceSha256());
  }

  public com.forwardmeasure.openworkflow.humantask.api.model.ReviewSession reviewSession(
      HumanTaskState.ReviewSession session, String rawToken, long taskRevision) {
    return new com.forwardmeasure.openworkflow.humantask.api.model.ReviewSession()
        .reviewSessionId(session.reviewSessionId())
        .stageId(session.stageId())
        .heldBy(session.heldBy().actorId())
        .acquiredAt(date(session.acquiredAt()))
        .lastRenewedAt(date(session.lastRenewedAt()))
        .expiresAt(date(session.expiresAt()))
        .reviewToken(rawToken)
        .taskRevision(taskRevision);
  }

  public Assignment toDomain(
      com.forwardmeasure.openworkflow.humantask.api.model.Assignment assignment) {
    return new Assignment(
        HumanTaskDefinition.AssignmentKind.valueOf(assignment.getKind().name()),
        assignment.getPrincipal());
  }

  public com.forwardmeasure.openworkflow.data.DataReference toDomain(
      com.forwardmeasure.openworkflow.humantask.api.model.DataReference reference) {
    if (reference == null) return null;
    var storage =
        com.forwardmeasure.openworkflow.data.DataReference.Storage.valueOf(
            reference.getStorage().name());
    return new com.forwardmeasure.openworkflow.data.DataReference(
        storage,
        storage == com.forwardmeasure.openworkflow.data.DataReference.Storage.INLINE
            ? json.valueToTree(reference.getInlineValue())
            : null,
        reference.getArtifactUri(),
        reference.getMediaType(),
        reference.getSizeBytes(),
        reference.getSha256());
  }

  private com.forwardmeasure.openworkflow.humantask.api.model.DataReference toApi(
      com.forwardmeasure.openworkflow.data.DataReference reference) {
    if (reference == null) return null;
    return new com.forwardmeasure.openworkflow.humantask.api.model.DataReference()
        .storage(
            com.forwardmeasure.openworkflow.humantask.api.model.DataReference.StorageEnum.valueOf(
                reference.storage().name()))
        .inlineValue(
            reference.inlineValue() == null
                ? null
                : json.convertValue(reference.inlineValue(), Object.class))
        .artifactUri(reference.artifactUri())
        .mediaType(reference.mediaType())
        .sizeBytes(reference.sizeBytes())
        .sha256(reference.sha256());
  }

  private static com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskSource toApi(
      HumanTaskDefinition.TaskSource source) {
    return new com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskSource()
        .kind(
            com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskSource.KindEnum.valueOf(
                source.kind().name()))
        .sourceId(source.sourceId())
        .executionId(source.executionId())
        .workflowTaskPath(source.workflowTaskPath())
        .correlationId(source.correlationId());
  }

  private static com.forwardmeasure.openworkflow.humantask.api.model.Assignment toApi(
      Assignment assignment) {
    if (assignment == null) return null;
    return new com.forwardmeasure.openworkflow.humantask.api.model.Assignment()
        .kind(
            com.forwardmeasure.openworkflow.humantask.api.model.Assignment.KindEnum.valueOf(
                assignment.kind().name()))
        .principal(assignment.principal());
  }

  private static com.forwardmeasure.openworkflow.humantask.api.model.ReviewSessionSummary toApi(
      HumanTaskState.ReviewSession session) {
    return new com.forwardmeasure.openworkflow.humantask.api.model.ReviewSessionSummary()
        .reviewSessionId(session.reviewSessionId())
        .stageId(session.stageId())
        .heldBy(session.heldBy().actorId())
        .acquiredAt(date(session.acquiredAt()))
        .lastRenewedAt(date(session.lastRenewedAt()))
        .expiresAt(date(session.expiresAt()));
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

  private static com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskStatus status(
      HumanTaskState state) {
    String value =
        switch (state) {
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
    return com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskStatus.valueOf(value);
  }

  private Map<String, Object> toMap(JsonNode node) {
    return node == null || node.isNull() ? null : json.convertValue(node, MAP);
  }

  private static Date date(java.time.Instant instant) {
    return instant == null ? null : Date.from(instant);
  }
}
