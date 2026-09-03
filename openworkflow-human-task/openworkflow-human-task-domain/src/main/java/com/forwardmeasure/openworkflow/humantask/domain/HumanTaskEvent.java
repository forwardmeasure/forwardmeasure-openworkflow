/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.domain;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.forwardmeasure.openworkflow.data.DataReference;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.Actor;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.Assignment;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState.Decision;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState.Outcome;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState.ReviewSession;
import java.time.Instant;
import java.util.Objects;

/** Immutable facts emitted by the human-task state machine. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = HumanTaskEvent.TaskCreated.class, name = "task-created"),
  @JsonSubTypes.Type(value = HumanTaskEvent.TaskAssigned.class, name = "task-assigned"),
  @JsonSubTypes.Type(value = HumanTaskEvent.TaskUnassigned.class, name = "task-unassigned"),
  @JsonSubTypes.Type(value = HumanTaskEvent.ReviewStarted.class, name = "review-started"),
  @JsonSubTypes.Type(
      value = HumanTaskEvent.ReviewLeaseRenewed.class,
      name = "review-lease-renewed"),
  @JsonSubTypes.Type(value = HumanTaskEvent.ResolutionRevised.class, name = "resolution-revised"),
  @JsonSubTypes.Type(value = HumanTaskEvent.CommentAdded.class, name = "comment-added"),
  @JsonSubTypes.Type(value = HumanTaskEvent.ReviewReleased.class, name = "review-released"),
  @JsonSubTypes.Type(
      value = HumanTaskEvent.ReviewLeaseExpired.class,
      name = "review-lease-expired"),
  @JsonSubTypes.Type(value = HumanTaskEvent.ReviewReassigned.class, name = "review-reassigned"),
  @JsonSubTypes.Type(value = HumanTaskEvent.DecisionRecorded.class, name = "decision-recorded"),
  @JsonSubTypes.Type(
      value = HumanTaskEvent.ReviewStageAdvanced.class,
      name = "review-stage-advanced"),
  @JsonSubTypes.Type(
      value = HumanTaskEvent.TaskReworkRequested.class,
      name = "task-rework-requested"),
  @JsonSubTypes.Type(value = HumanTaskEvent.TaskEscalated.class, name = "task-escalated"),
  @JsonSubTypes.Type(
      value = HumanTaskEvent.NextStageActivated.class,
      name = "next-stage-activated"),
  @JsonSubTypes.Type(value = HumanTaskEvent.TaskApproved.class, name = "task-approved"),
  @JsonSubTypes.Type(value = HumanTaskEvent.TaskRejected.class, name = "task-rejected"),
  @JsonSubTypes.Type(value = HumanTaskEvent.TaskResolved.class, name = "task-resolved"),
  @JsonSubTypes.Type(value = HumanTaskEvent.TaskReopened.class, name = "task-reopened"),
  @JsonSubTypes.Type(value = HumanTaskEvent.TaskCancelled.class, name = "task-cancelled"),
  @JsonSubTypes.Type(value = HumanTaskEvent.TaskExpired.class, name = "task-expired")
})
public sealed interface HumanTaskEvent {
  EventMetadata metadata();

  record EventMetadata(HumanTaskId taskId, String commandId, Actor actor, Instant occurredAt) {
    public EventMetadata {
      Objects.requireNonNull(taskId, "taskId");
      HumanTaskDefinition.requireText(commandId, "commandId");
      Objects.requireNonNull(actor, "actor");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  record TaskCreated(EventMetadata metadata, HumanTaskDefinition definition, String contentSha256)
      implements HumanTaskEvent {}

  record TaskAssigned(EventMetadata metadata, Assignment assignment) implements HumanTaskEvent {}

  record TaskUnassigned(EventMetadata metadata) implements HumanTaskEvent {}

  record ReviewStarted(EventMetadata metadata, ReviewSession reviewSession)
      implements HumanTaskEvent {}

  record ReviewLeaseRenewed(EventMetadata metadata, Instant expiresAt) implements HumanTaskEvent {}

  record ResolutionRevised(
      EventMetadata metadata,
      long basedOnContentRevision,
      long contentRevision,
      String beforeSha256,
      String afterSha256,
      DataReference content,
      DataReference jsonPatch,
      String comment)
      implements HumanTaskEvent {
    public ResolutionRevised {
      Objects.requireNonNull(content, "content");
      if (!content.sha256().equals(afterSha256)) {
        throw new IllegalArgumentException("Revised content reference and digest differ");
      }
    }
  }

  record CommentAdded(EventMetadata metadata, String reviewSessionId, String comment)
      implements HumanTaskEvent {}

  record ReviewReleased(EventMetadata metadata, String reviewSessionId, String reason)
      implements HumanTaskEvent {}

  record ReviewLeaseExpired(EventMetadata metadata, String reviewSessionId)
      implements HumanTaskEvent {}

  /** Supervisor reassignment, including the displaced lease when the task was under review. */
  record ReviewReassigned(
      EventMetadata metadata, Assignment assignment, String displacedReviewSessionId, String reason)
      implements HumanTaskEvent {
    public ReviewReassigned {
      Objects.requireNonNull(metadata, "metadata");
      Objects.requireNonNull(assignment, "assignment");
      HumanTaskDefinition.requireText(reason, "reason");
    }
  }

  record DecisionRecorded(EventMetadata metadata, Decision decision) implements HumanTaskEvent {}

  record ReviewStageAdvanced(EventMetadata metadata, int targetStageIndex)
      implements HumanTaskEvent {}

  record TaskReworkRequested(EventMetadata metadata, int targetStageIndex, Decision decision)
      implements HumanTaskEvent {}

  record TaskEscalated(
      EventMetadata metadata, int targetStageIndex, Assignment assignment, String reason)
      implements HumanTaskEvent {}

  record NextStageActivated(EventMetadata metadata, Assignment assignment)
      implements HumanTaskEvent {}

  record TaskApproved(EventMetadata metadata, Outcome outcome) implements HumanTaskEvent {}

  record TaskRejected(EventMetadata metadata, Outcome outcome) implements HumanTaskEvent {}

  record TaskResolved(EventMetadata metadata, Outcome outcome) implements HumanTaskEvent {}

  record TaskReopened(
      EventMetadata metadata,
      int targetStageIndex,
      int reviewRound,
      Assignment assignment,
      String reason)
      implements HumanTaskEvent {}

  record TaskCancelled(EventMetadata metadata, String reason) implements HumanTaskEvent {}

  record TaskExpired(EventMetadata metadata, String reason) implements HumanTaskEvent {}
}
