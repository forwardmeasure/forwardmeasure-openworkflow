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
import java.time.Instant;
import java.util.Objects;

/** Intent accepted by the pure human-task state machine. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = HumanTaskCommand.Create.class, name = "create"),
  @JsonSubTypes.Type(value = HumanTaskCommand.Assign.class, name = "assign"),
  @JsonSubTypes.Type(value = HumanTaskCommand.Unassign.class, name = "unassign"),
  @JsonSubTypes.Type(value = HumanTaskCommand.BeginReview.class, name = "begin-review"),
  @JsonSubTypes.Type(value = HumanTaskCommand.RenewReviewLease.class, name = "renew-review-lease"),
  @JsonSubTypes.Type(value = HumanTaskCommand.SaveRevision.class, name = "save-revision"),
  @JsonSubTypes.Type(value = HumanTaskCommand.AddComment.class, name = "add-comment"),
  @JsonSubTypes.Type(value = HumanTaskCommand.ReleaseReview.class, name = "release-review"),
  @JsonSubTypes.Type(
      value = HumanTaskCommand.ExpireReviewLease.class,
      name = "expire-review-lease"),
  @JsonSubTypes.Type(value = HumanTaskCommand.ReassignReview.class, name = "reassign-review"),
  @JsonSubTypes.Type(value = HumanTaskCommand.SubmitDecision.class, name = "submit-decision"),
  @JsonSubTypes.Type(value = HumanTaskCommand.Escalate.class, name = "escalate"),
  @JsonSubTypes.Type(value = HumanTaskCommand.Reopen.class, name = "reopen"),
  @JsonSubTypes.Type(value = HumanTaskCommand.Cancel.class, name = "cancel"),
  @JsonSubTypes.Type(value = HumanTaskCommand.ExpireTask.class, name = "expire-task")
})
public sealed interface HumanTaskCommand {
  CommandMetadata metadata();

  record CommandMetadata(
      HumanTaskId taskId,
      String commandId,
      Actor actor,
      Instant occurredAt,
      long expectedRevision) {
    public CommandMetadata {
      Objects.requireNonNull(taskId, "taskId");
      HumanTaskDefinition.requireText(commandId, "commandId");
      Objects.requireNonNull(actor, "actor");
      Objects.requireNonNull(occurredAt, "occurredAt");
      if (expectedRevision < 0) {
        throw new IllegalArgumentException("expectedRevision must not be negative");
      }
    }
  }

  record Create(CommandMetadata metadata, HumanTaskDefinition definition)
      implements HumanTaskCommand {
    public Create {
      Objects.requireNonNull(metadata, "metadata");
      Objects.requireNonNull(definition, "definition");
      if (!metadata.taskId().equals(definition.taskId())) {
        throw new IllegalArgumentException("Command and definition task identifiers differ");
      }
    }
  }

  record Assign(CommandMetadata metadata, Assignment assignment) implements HumanTaskCommand {
    public Assign {
      Objects.requireNonNull(metadata, "metadata");
      Objects.requireNonNull(assignment, "assignment");
    }
  }

  record Unassign(CommandMetadata metadata) implements HumanTaskCommand {}

  record BeginReview(
      CommandMetadata metadata,
      String reviewSessionId,
      String leaseTokenDigest,
      Instant leaseExpiresAt)
      implements HumanTaskCommand {
    public BeginReview {
      Objects.requireNonNull(metadata, "metadata");
      HumanTaskDefinition.requireText(reviewSessionId, "reviewSessionId");
      requireDigest(leaseTokenDigest, "leaseTokenDigest");
      requireFuture(leaseExpiresAt, metadata.occurredAt(), "leaseExpiresAt");
    }
  }

  record RenewReviewLease(
      CommandMetadata metadata,
      String reviewSessionId,
      String leaseTokenDigest,
      Instant leaseExpiresAt)
      implements HumanTaskCommand {
    public RenewReviewLease {
      Objects.requireNonNull(metadata, "metadata");
      HumanTaskDefinition.requireText(reviewSessionId, "reviewSessionId");
      requireDigest(leaseTokenDigest, "leaseTokenDigest");
      requireFuture(leaseExpiresAt, metadata.occurredAt(), "leaseExpiresAt");
    }
  }

  record SaveRevision(
      CommandMetadata metadata,
      String reviewSessionId,
      String leaseTokenDigest,
      long basedOnContentRevision,
      DataReference content,
      DataReference jsonPatch,
      String comment)
      implements HumanTaskCommand {
    public SaveRevision {
      Objects.requireNonNull(metadata, "metadata");
      HumanTaskDefinition.requireText(reviewSessionId, "reviewSessionId");
      requireDigest(leaseTokenDigest, "leaseTokenDigest");
      if (basedOnContentRevision < 0) {
        throw new IllegalArgumentException("basedOnContentRevision must not be negative");
      }
      Objects.requireNonNull(content, "content");
    }
  }

  record AddComment(
      CommandMetadata metadata, String reviewSessionId, String leaseTokenDigest, String comment)
      implements HumanTaskCommand {
    public AddComment {
      Objects.requireNonNull(metadata, "metadata");
      HumanTaskDefinition.requireText(reviewSessionId, "reviewSessionId");
      requireDigest(leaseTokenDigest, "leaseTokenDigest");
      HumanTaskDefinition.requireText(comment, "comment");
    }
  }

  record ReleaseReview(
      CommandMetadata metadata, String reviewSessionId, String leaseTokenDigest, String reason)
      implements HumanTaskCommand {}

  record ExpireReviewLease(CommandMetadata metadata, String reviewSessionId)
      implements HumanTaskCommand {}

  record ReassignReview(CommandMetadata metadata, Assignment assignment, String reason)
      implements HumanTaskCommand {}

  record SubmitDecision(
      CommandMetadata metadata,
      String reviewSessionId,
      String leaseTokenDigest,
      long contentRevision,
      String actionCode,
      String comment)
      implements HumanTaskCommand {}

  record Escalate(
      CommandMetadata metadata, String targetStageId, Assignment assignment, String reason)
      implements HumanTaskCommand {}

  record Reopen(
      CommandMetadata metadata, String targetStageId, Assignment assignment, String reason)
      implements HumanTaskCommand {}

  record Cancel(CommandMetadata metadata, String reason) implements HumanTaskCommand {}

  record ExpireTask(CommandMetadata metadata, String reason) implements HumanTaskCommand {}

  private static void requireDigest(String value, String name) {
    if (value == null || !value.matches("[0-9a-f]{64}")) {
      throw new IllegalArgumentException(name + " must be lowercase SHA-256");
    }
  }

  private static void requireFuture(Instant value, Instant now, String name) {
    Objects.requireNonNull(value, name);
    if (!value.isAfter(now)) {
      throw new IllegalArgumentException(name + " must be after command time");
    }
  }
}
