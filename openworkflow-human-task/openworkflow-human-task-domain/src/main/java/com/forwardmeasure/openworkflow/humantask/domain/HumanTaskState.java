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
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.DispositionKind;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Durable state variants for one human-task aggregate. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = HumanTaskState.Open.class, name = "open"),
  @JsonSubTypes.Type(value = HumanTaskState.Assigned.class, name = "assigned"),
  @JsonSubTypes.Type(value = HumanTaskState.Claimed.class, name = "claimed"),
  @JsonSubTypes.Type(value = HumanTaskState.AwaitingNextStage.class, name = "awaiting-next-stage"),
  @JsonSubTypes.Type(value = HumanTaskState.ReworkRequested.class, name = "rework-requested"),
  @JsonSubTypes.Type(value = HumanTaskState.Approved.class, name = "approved"),
  @JsonSubTypes.Type(value = HumanTaskState.Rejected.class, name = "rejected"),
  @JsonSubTypes.Type(value = HumanTaskState.Resolved.class, name = "resolved"),
  @JsonSubTypes.Type(value = HumanTaskState.Cancelled.class, name = "cancelled"),
  @JsonSubTypes.Type(value = HumanTaskState.Expired.class, name = "expired")
})
public sealed interface HumanTaskState {
  Snapshot snapshot();

  default long revision() {
    return snapshot().revision();
  }

  record Snapshot(
      HumanTaskDefinition definition,
      DataReference currentContent,
      String currentContentSha256,
      long contentRevision,
      int reviewRound,
      int stageIndex,
      List<Decision> decisions,
      long revision) {
    public Snapshot {
      Objects.requireNonNull(definition, "definition");
      Objects.requireNonNull(currentContent, "currentContent");
      if (currentContentSha256 == null || !currentContentSha256.matches("[0-9a-f]{64}")) {
        throw new IllegalArgumentException("currentContentSha256 must be lowercase SHA-256");
      }
      if (!currentContent.sha256().equals(currentContentSha256)) {
        throw new IllegalArgumentException("Current content reference and digest differ");
      }
      if (contentRevision < 0 || reviewRound < 1 || stageIndex < 0 || revision < 1) {
        throw new IllegalArgumentException("Snapshot counters are outside their valid ranges");
      }
      decisions = List.copyOf(decisions);
    }

    Snapshot next() {
      return copy(
          currentContent,
          currentContentSha256,
          contentRevision,
          reviewRound,
          stageIndex,
          decisions);
    }

    Snapshot copy(
        DataReference content,
        String digest,
        long newContentRevision,
        int newReviewRound,
        int newStageIndex,
        List<Decision> newDecisions) {
      return new Snapshot(
          definition,
          content,
          digest,
          newContentRevision,
          newReviewRound,
          newStageIndex,
          newDecisions,
          revision + 1);
    }
  }

  record ReviewSession(
      String reviewSessionId,
      String stageId,
      Actor heldBy,
      Instant acquiredAt,
      Instant lastRenewedAt,
      Instant expiresAt,
      long taskRevisionAtAcquisition,
      String leaseTokenDigest) {}

  record Decision(
      String decisionId,
      int reviewRound,
      String stageId,
      String actionCode,
      DispositionKind kind,
      Actor actor,
      Instant decidedAt,
      long contentRevision,
      String contentSha256,
      String comment) {}

  record Outcome(
      HumanTaskId taskId,
      String decisionId,
      DispositionKind kind,
      String actionCode,
      int reviewRound,
      String stageId,
      long contentRevision,
      String contentSha256,
      DataReference content,
      Actor actor,
      Instant completedAt) {
    public Outcome {
      Objects.requireNonNull(content, "content");
      if (!content.sha256().equals(contentSha256)) {
        throw new IllegalArgumentException("Outcome content reference and digest differ");
      }
    }
  }

  record Open(Snapshot snapshot) implements HumanTaskState {}

  record Assigned(Snapshot snapshot, Assignment assignment) implements HumanTaskState {}

  record Claimed(Snapshot snapshot, Assignment assignment, ReviewSession reviewSession)
      implements HumanTaskState {}

  record AwaitingNextStage(Snapshot snapshot, Assignment assignment) implements HumanTaskState {}

  record ReworkRequested(Snapshot snapshot, Assignment assignment, Decision decision)
      implements HumanTaskState {}

  record Approved(Snapshot snapshot, Outcome outcome) implements HumanTaskState {}

  record Rejected(Snapshot snapshot, Outcome outcome) implements HumanTaskState {}

  record Resolved(Snapshot snapshot, Outcome outcome) implements HumanTaskState {}

  record Cancelled(Snapshot snapshot, String reason) implements HumanTaskState {}

  record Expired(Snapshot snapshot, String reason) implements HumanTaskState {}
}
