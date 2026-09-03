/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.domain;

import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskCommand.CommandMetadata;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ActionTransition;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ActorKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.Assignment;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.DispositionKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ReviewAction;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ReviewStage;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.TransitionKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskEvent.EventMetadata;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState.Decision;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState.Outcome;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState.ReviewSession;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState.Snapshot;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskTransitionException.Failure;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Pure and exhaustive transition authority for the human-task aggregate. */
public final class HumanTaskStateMachine {

  public HumanTaskTransition decide(HumanTaskState current, HumanTaskCommand command) {
    Objects.requireNonNull(command, "command");
    if (command instanceof HumanTaskCommand.Create create) {
      return create(current, create);
    }
    if (current == null) {
      throw failure(Failure.NOT_FOUND, "Human task does not exist");
    }
    validateEnvelope(current, command.metadata());

    List<HumanTaskEvent> events =
        switch (command) {
          case HumanTaskCommand.Assign assign -> assign(current, assign);
          case HumanTaskCommand.Unassign unassign -> unassign(current, unassign);
          case HumanTaskCommand.BeginReview begin -> beginReview(current, begin);
          case HumanTaskCommand.RenewReviewLease renew -> renew(current, renew);
          case HumanTaskCommand.SaveRevision save -> save(current, save);
          case HumanTaskCommand.AddComment comment -> comment(current, comment);
          case HumanTaskCommand.ReleaseReview release -> release(current, release);
          case HumanTaskCommand.ExpireReviewLease expire -> expireLease(current, expire);
          case HumanTaskCommand.ReassignReview reassign -> reassign(current, reassign);
          case HumanTaskCommand.SubmitDecision decision -> decide(current, decision);
          case HumanTaskCommand.Escalate escalate -> escalate(current, escalate);
          case HumanTaskCommand.Reopen reopen -> reopen(current, reopen);
          case HumanTaskCommand.Cancel cancel -> cancel(current, cancel);
          case HumanTaskCommand.ExpireTask expire -> expireTask(current, expire);
          case HumanTaskCommand.Create ignored -> throw new AssertionError("Create handled above");
        };
    return transition(current, events);
  }

  public HumanTaskState evolve(HumanTaskState current, HumanTaskEvent event) {
    Objects.requireNonNull(event, "event");
    return switch (event) {
      case HumanTaskEvent.TaskCreated created -> evolveCreated(current, created);
      case HumanTaskEvent.TaskAssigned assigned ->
          new HumanTaskState.Assigned(existing(current).snapshot().next(), assigned.assignment());
      case HumanTaskEvent.TaskUnassigned ignored ->
          new HumanTaskState.Open(existing(current).snapshot().next());
      case HumanTaskEvent.ReviewStarted started -> {
        HumanTaskState state = existing(current);
        yield new HumanTaskState.Claimed(
            state.snapshot().next(), assignment(state), started.reviewSession());
      }
      case HumanTaskEvent.ReviewLeaseRenewed renewed -> evolveLeaseRenewed(current, renewed);
      case HumanTaskEvent.ResolutionRevised revised -> evolveResolutionRevised(current, revised);
      case HumanTaskEvent.CommentAdded ignored -> evolveCommentAdded(current);
      case HumanTaskEvent.ReviewReleased ignored -> evolveReviewAvailable(current);
      case HumanTaskEvent.ReviewLeaseExpired ignored -> evolveReviewAvailable(current);
      case HumanTaskEvent.ReviewReassigned reassigned ->
          new HumanTaskState.Assigned(existing(current).snapshot().next(), reassigned.assignment());
      case HumanTaskEvent.DecisionRecorded recorded -> evolveDecisionRecorded(current, recorded);
      case HumanTaskEvent.ReviewStageAdvanced advanced ->
          new HumanTaskState.AwaitingNextStage(
              moveStage(existing(current).snapshot(), advanced.targetStageIndex()), null);
      case HumanTaskEvent.TaskReworkRequested rework -> {
        HumanTaskState state = existing(current);
        yield new HumanTaskState.ReworkRequested(
            moveStage(state.snapshot(), rework.targetStageIndex()),
            assignment(state),
            rework.decision());
      }
      case HumanTaskEvent.TaskEscalated escalated ->
          new HumanTaskState.AwaitingNextStage(
              moveStage(existing(current).snapshot(), escalated.targetStageIndex()),
              escalated.assignment());
      case HumanTaskEvent.NextStageActivated activated ->
          available(existing(current).snapshot().next(), activated.assignment());
      case HumanTaskEvent.TaskApproved approved ->
          new HumanTaskState.Approved(existing(current).snapshot().next(), approved.outcome());
      case HumanTaskEvent.TaskRejected rejected ->
          new HumanTaskState.Rejected(existing(current).snapshot().next(), rejected.outcome());
      case HumanTaskEvent.TaskResolved resolved ->
          new HumanTaskState.Resolved(existing(current).snapshot().next(), resolved.outcome());
      case HumanTaskEvent.TaskReopened reopened -> evolveReopened(current, reopened);
      case HumanTaskEvent.TaskCancelled cancelled ->
          new HumanTaskState.Cancelled(existing(current).snapshot().next(), cancelled.reason());
      case HumanTaskEvent.TaskExpired expired ->
          new HumanTaskState.Expired(existing(current).snapshot().next(), expired.reason());
    };
  }

  private static HumanTaskState evolveCreated(
      HumanTaskState current, HumanTaskEvent.TaskCreated created) {
    if (current != null) {
      throw new IllegalStateException("TaskCreated cannot be applied to an existing task");
    }
    Snapshot snapshot =
        new Snapshot(
            created.definition(),
            created.definition().originalContent(),
            created.contentSha256(),
            0,
            1,
            0,
            List.of(),
            1);
    return created.definition().initialAssignment() == null
        ? new HumanTaskState.Open(snapshot)
        : new HumanTaskState.Assigned(snapshot, created.definition().initialAssignment());
  }

  private static HumanTaskState evolveLeaseRenewed(
      HumanTaskState current, HumanTaskEvent.ReviewLeaseRenewed renewed) {
    HumanTaskState.Claimed claimed = claimed(existing(current));
    ReviewSession old = claimed.reviewSession();
    ReviewSession session =
        new ReviewSession(
            old.reviewSessionId(),
            old.stageId(),
            old.heldBy(),
            old.acquiredAt(),
            renewed.metadata().occurredAt(),
            renewed.expiresAt(),
            old.taskRevisionAtAcquisition(),
            old.leaseTokenDigest());
    return new HumanTaskState.Claimed(claimed.snapshot().next(), claimed.assignment(), session);
  }

  private static HumanTaskState evolveResolutionRevised(
      HumanTaskState current, HumanTaskEvent.ResolutionRevised revised) {
    HumanTaskState.Claimed claimed = claimed(existing(current));
    Snapshot snapshot = claimed.snapshot();
    Snapshot next =
        snapshot.copy(
            revised.content(),
            revised.afterSha256(),
            revised.contentRevision(),
            snapshot.reviewRound(),
            snapshot.stageIndex(),
            snapshot.decisions());
    return new HumanTaskState.Claimed(next, claimed.assignment(), claimed.reviewSession());
  }

  private static HumanTaskState evolveCommentAdded(HumanTaskState current) {
    HumanTaskState.Claimed claimed = claimed(existing(current));
    return new HumanTaskState.Claimed(
        claimed.snapshot().next(), claimed.assignment(), claimed.reviewSession());
  }

  private static HumanTaskState evolveReviewAvailable(HumanTaskState current) {
    HumanTaskState state = existing(current);
    return available(state.snapshot().next(), assignment(state));
  }

  private static HumanTaskState evolveDecisionRecorded(
      HumanTaskState current, HumanTaskEvent.DecisionRecorded recorded) {
    HumanTaskState.Claimed claimed = claimed(existing(current));
    Snapshot snapshot = claimed.snapshot();
    List<Decision> decisions = new ArrayList<>(snapshot.decisions());
    decisions.add(recorded.decision());
    Snapshot next =
        snapshot.copy(
            snapshot.currentContent(),
            snapshot.currentContentSha256(),
            snapshot.contentRevision(),
            snapshot.reviewRound(),
            snapshot.stageIndex(),
            decisions);
    return new HumanTaskState.Claimed(next, claimed.assignment(), claimed.reviewSession());
  }

  private static HumanTaskState evolveReopened(
      HumanTaskState current, HumanTaskEvent.TaskReopened reopened) {
    Snapshot snapshot = existing(current).snapshot();
    Snapshot next =
        snapshot.copy(
            snapshot.currentContent(),
            snapshot.currentContentSha256(),
            snapshot.contentRevision(),
            reopened.reviewRound(),
            reopened.targetStageIndex(),
            snapshot.decisions());
    return available(next, reopened.assignment());
  }

  private static HumanTaskState existing(HumanTaskState current) {
    if (current == null) {
      throw new IllegalStateException("An event other than TaskCreated requires existing state");
    }
    return current;
  }

  private HumanTaskTransition create(HumanTaskState current, HumanTaskCommand.Create command) {
    if (current != null) {
      throw failure(Failure.ALREADY_EXISTS, "Human task already exists");
    }
    if (command.metadata().expectedRevision() != 0) {
      throw failure(Failure.REVISION_CONFLICT, "Creation expects revision zero");
    }
    if (command.definition().expiresAt() != null
        && !command.definition().expiresAt().isAfter(command.metadata().occurredAt())) {
      throw failure(Failure.VALIDATION, "A task cannot be created already expired");
    }
    HumanTaskEvent event =
        new HumanTaskEvent.TaskCreated(
            metadata(command.metadata()),
            command.definition(),
            command.definition().originalContent().sha256());
    return transition(null, List.of(event));
  }

  private static List<HumanTaskEvent> assign(
      HumanTaskState current, HumanTaskCommand.Assign command) {
    require(current instanceof HumanTaskState.Open, "Only an open task can be assigned");
    return List.of(
        new HumanTaskEvent.TaskAssigned(metadata(command.metadata()), command.assignment()));
  }

  private static List<HumanTaskEvent> unassign(
      HumanTaskState current, HumanTaskCommand.Unassign command) {
    require(current instanceof HumanTaskState.Assigned, "Only an assigned task can be unassigned");
    return List.of(new HumanTaskEvent.TaskUnassigned(metadata(command.metadata())));
  }

  private static List<HumanTaskEvent> beginReview(
      HumanTaskState current, HumanTaskCommand.BeginReview command) {
    require(
        current instanceof HumanTaskState.Open || current instanceof HumanTaskState.Assigned,
        "Only an open or assigned task can be claimed");
    ReviewStage stage = stage(current);
    if (!stage.eligible(command.metadata().actor())) {
      throw failure(Failure.INELIGIBLE_REVIEWER, "Actor is not eligible for the active stage");
    }
    Assignment assignment = assignment(current);
    if (assignment != null && !assignment.permits(command.metadata().actor())) {
      throw failure(Failure.ASSIGNMENT_CONFLICT, "Task is assigned to another principal");
    }
    ReviewSession session =
        new ReviewSession(
            command.reviewSessionId(),
            stage.stageId(),
            command.metadata().actor(),
            command.metadata().occurredAt(),
            command.metadata().occurredAt(),
            command.leaseExpiresAt(),
            current.revision(),
            command.leaseTokenDigest());
    return List.of(new HumanTaskEvent.ReviewStarted(metadata(command.metadata()), session));
  }

  private static List<HumanTaskEvent> renew(
      HumanTaskState current, HumanTaskCommand.RenewReviewLease command) {
    HumanTaskState.Claimed claimed =
        requireLease(
            current, command.metadata(), command.reviewSessionId(), command.leaseTokenDigest());
    if (!command.leaseExpiresAt().isAfter(claimed.reviewSession().expiresAt())) {
      throw failure(Failure.VALIDATION, "A renewed lease must extend the current lease");
    }
    return List.of(
        new HumanTaskEvent.ReviewLeaseRenewed(
            metadata(command.metadata()), command.leaseExpiresAt()));
  }

  private static List<HumanTaskEvent> save(
      HumanTaskState current, HumanTaskCommand.SaveRevision command) {
    requireLease(
        current, command.metadata(), command.reviewSessionId(), command.leaseTokenDigest());
    if (command.basedOnContentRevision() != current.snapshot().contentRevision()) {
      throw failure(Failure.STALE_CONTENT, "Resolution was based on a stale content revision");
    }
    String after = command.content().sha256();
    return List.of(
        new HumanTaskEvent.ResolutionRevised(
            metadata(command.metadata()),
            command.basedOnContentRevision(),
            command.basedOnContentRevision() + 1,
            current.snapshot().currentContentSha256(),
            after,
            command.content(),
            command.jsonPatch(),
            command.comment()));
  }

  private static List<HumanTaskEvent> comment(
      HumanTaskState current, HumanTaskCommand.AddComment command) {
    requireLease(
        current, command.metadata(), command.reviewSessionId(), command.leaseTokenDigest());
    return List.of(
        new HumanTaskEvent.CommentAdded(
            metadata(command.metadata()), command.reviewSessionId(), command.comment()));
  }

  private static List<HumanTaskEvent> release(
      HumanTaskState current, HumanTaskCommand.ReleaseReview command) {
    requireLease(
        current, command.metadata(), command.reviewSessionId(), command.leaseTokenDigest());
    return List.of(
        new HumanTaskEvent.ReviewReleased(
            metadata(command.metadata()), command.reviewSessionId(), command.reason()));
  }

  private static List<HumanTaskEvent> expireLease(
      HumanTaskState current, HumanTaskCommand.ExpireReviewLease command) {
    HumanTaskState.Claimed claimed = claimed(current);
    if (!claimed.reviewSession().reviewSessionId().equals(command.reviewSessionId())) {
      throw failure(Failure.LEASE_CONFLICT, "Review session does not hold this task");
    }
    if (command.metadata().occurredAt().isBefore(claimed.reviewSession().expiresAt())) {
      throw failure(Failure.ILLEGAL_TRANSITION, "Review lease has not expired");
    }
    return List.of(
        new HumanTaskEvent.ReviewLeaseExpired(
            metadata(command.metadata()), command.reviewSessionId()));
  }

  private static List<HumanTaskEvent> reassign(
      HumanTaskState current, HumanTaskCommand.ReassignReview command) {
    require(
        current instanceof HumanTaskState.Open
            || current instanceof HumanTaskState.Assigned
            || current instanceof HumanTaskState.Claimed,
        "Only active work can be reassigned");
    String displacedReviewSessionId =
        current instanceof HumanTaskState.Claimed claimed
            ? claimed.reviewSession().reviewSessionId()
            : null;
    return List.of(
        new HumanTaskEvent.ReviewReassigned(
            metadata(command.metadata()),
            command.assignment(),
            displacedReviewSessionId,
            command.reason()));
  }

  private static List<HumanTaskEvent> decide(
      HumanTaskState current, HumanTaskCommand.SubmitDecision command) {
    requireLease(
        current, command.metadata(), command.reviewSessionId(), command.leaseTokenDigest());
    if (command.contentRevision() != current.snapshot().contentRevision()) {
      throw failure(Failure.STALE_CONTENT, "Decision targets a stale content revision");
    }
    ReviewAction action =
        stage(current).actions().stream()
            .filter(candidate -> candidate.code().equals(command.actionCode()))
            .findFirst()
            .orElseThrow(
                () ->
                    failure(
                        Failure.UNKNOWN_ACTION, "Action is not configured for the active stage"));
    if (action.commentRequired() && (command.comment() == null || command.comment().isBlank())) {
      throw failure(Failure.VALIDATION, "The selected action requires a comment");
    }
    Decision decision =
        new Decision(
            command.metadata().commandId(),
            current.snapshot().reviewRound(),
            stage(current).stageId(),
            action.code(),
            action.kind(),
            command.metadata().actor(),
            command.metadata().occurredAt(),
            current.snapshot().contentRevision(),
            current.snapshot().currentContentSha256(),
            command.comment());
    List<HumanTaskEvent> events = new ArrayList<>();
    events.add(new HumanTaskEvent.DecisionRecorded(metadata(command.metadata()), decision));
    addTransitionEvents(events, current, command.metadata(), action, decision);
    return List.copyOf(events);
  }

  private static void addTransitionEvents(
      List<HumanTaskEvent> events,
      HumanTaskState current,
      CommandMetadata metadata,
      ReviewAction action,
      Decision decision) {
    ActionTransition transition = action.transition();
    EventMetadata eventMetadata = metadata(metadata);
    if (transition.kind() == TransitionKind.RESOLVE) {
      Outcome outcome = outcome(current, decision);
      if (action.kind() == DispositionKind.APPROVE) {
        events.add(new HumanTaskEvent.TaskApproved(eventMetadata, outcome));
      } else if (action.kind() == DispositionKind.DECLINE) {
        events.add(new HumanTaskEvent.TaskRejected(eventMetadata, outcome));
      } else {
        events.add(new HumanTaskEvent.TaskResolved(eventMetadata, outcome));
      }
    } else if (transition.kind() == TransitionKind.REWORK) {
      events.add(
          new HumanTaskEvent.TaskReworkRequested(
              eventMetadata, target(current, transition), decision));
    } else if (transition.kind() == TransitionKind.ADVANCE) {
      events.add(
          new HumanTaskEvent.ReviewStageAdvanced(eventMetadata, target(current, transition)));
      events.add(new HumanTaskEvent.NextStageActivated(eventMetadata, null));
    } else if (transition.kind() == TransitionKind.ESCALATE) {
      events.add(
          new HumanTaskEvent.TaskEscalated(
              eventMetadata, target(current, transition), null, "Decision escalation"));
      events.add(new HumanTaskEvent.NextStageActivated(eventMetadata, null));
    } else {
      HumanTaskState.Claimed claimed = claimed(current);
      events.add(
          new HumanTaskEvent.ReviewReleased(
              eventMetadata, claimed.reviewSession().reviewSessionId(), "Decision kept task open"));
    }
  }

  private static List<HumanTaskEvent> escalate(
      HumanTaskState current, HumanTaskCommand.Escalate command) {
    require(active(current), "Only active work can be escalated");
    int target = current.snapshot().definition().reviewPlan().indexOf(command.targetStageId());
    return List.of(
        new HumanTaskEvent.TaskEscalated(
            metadata(command.metadata()), target, command.assignment(), command.reason()),
        new HumanTaskEvent.NextStageActivated(metadata(command.metadata()), command.assignment()));
  }

  private static List<HumanTaskEvent> reopen(
      HumanTaskState current, HumanTaskCommand.Reopen command) {
    require(
        current instanceof HumanTaskState.Approved
            || current instanceof HumanTaskState.Rejected
            || current instanceof HumanTaskState.Resolved
            || current instanceof HumanTaskState.ReworkRequested,
        "Only completed or rework-requested tasks can be reopened");
    int target = current.snapshot().definition().reviewPlan().indexOf(command.targetStageId());
    return List.of(
        new HumanTaskEvent.TaskReopened(
            metadata(command.metadata()),
            target,
            current.snapshot().reviewRound() + 1,
            command.assignment(),
            command.reason()));
  }

  private static List<HumanTaskEvent> cancel(
      HumanTaskState current, HumanTaskCommand.Cancel command) {
    require(active(current), "Only active work can be cancelled");
    return List.of(
        new HumanTaskEvent.TaskCancelled(metadata(command.metadata()), command.reason()));
  }

  private static List<HumanTaskEvent> expireTask(
      HumanTaskState current, HumanTaskCommand.ExpireTask command) {
    require(active(current), "Only active work can expire");
    if (command.metadata().actor().kind() != ActorKind.SYSTEM) {
      throw failure(Failure.ILLEGAL_TRANSITION, "Task expiry requires a system actor");
    }
    if (current.snapshot().definition().expiresAt() == null) {
      throw failure(Failure.ILLEGAL_TRANSITION, "Task has no expiry");
    }
    if (command.metadata().occurredAt().isBefore(current.snapshot().definition().expiresAt())) {
      throw failure(Failure.ILLEGAL_TRANSITION, "Task expiry has not been reached");
    }
    return List.of(new HumanTaskEvent.TaskExpired(metadata(command.metadata()), command.reason()));
  }

  private HumanTaskTransition transition(HumanTaskState current, List<HumanTaskEvent> events) {
    HumanTaskState result = current;
    for (HumanTaskEvent event : events) {
      result = evolve(result, event);
    }
    return new HumanTaskTransition(events, result);
  }

  private static void validateEnvelope(HumanTaskState current, CommandMetadata metadata) {
    if (!current.snapshot().definition().taskId().equals(metadata.taskId())) {
      throw failure(Failure.NOT_FOUND, "Command targets another human task");
    }
    if (current.revision() != metadata.expectedRevision()) {
      throw failure(Failure.REVISION_CONFLICT, "Expected revision does not match current revision");
    }
  }

  private static HumanTaskState.Claimed requireLease(
      HumanTaskState current, CommandMetadata metadata, String sessionId, String tokenDigest) {
    HumanTaskState.Claimed claimed = claimed(current);
    ReviewSession session = claimed.reviewSession();
    if (!session.reviewSessionId().equals(sessionId)
        || !session.leaseTokenDigest().equals(tokenDigest)
        || !session.heldBy().actorId().equals(metadata.actor().actorId())) {
      throw failure(Failure.LEASE_CONFLICT, "Review lease does not belong to this actor/session");
    }
    if (!metadata.occurredAt().isBefore(session.expiresAt())) {
      throw failure(Failure.LEASE_EXPIRED, "Review lease has expired");
    }
    return claimed;
  }

  private static HumanTaskState.Claimed claimed(HumanTaskState state) {
    if (state instanceof HumanTaskState.Claimed claimed) {
      return claimed;
    }
    throw failure(Failure.ILLEGAL_TRANSITION, "Task is not claimed for review");
  }

  private static ReviewStage stage(HumanTaskState state) {
    return state.snapshot().definition().reviewPlan().stages().get(state.snapshot().stageIndex());
  }

  private static int target(HumanTaskState state, ActionTransition transition) {
    return state.snapshot().definition().reviewPlan().indexOf(transition.targetStageId());
  }

  private static Snapshot moveStage(Snapshot snapshot, int targetStageIndex) {
    return snapshot.copy(
        snapshot.currentContent(),
        snapshot.currentContentSha256(),
        snapshot.contentRevision(),
        snapshot.reviewRound(),
        targetStageIndex,
        snapshot.decisions());
  }

  private static HumanTaskState available(Snapshot snapshot, Assignment assignment) {
    return assignment == null
        ? new HumanTaskState.Open(snapshot)
        : new HumanTaskState.Assigned(snapshot, assignment);
  }

  private static Assignment assignment(HumanTaskState state) {
    if (state instanceof HumanTaskState.Assigned assigned) {
      return assigned.assignment();
    }
    if (state instanceof HumanTaskState.Claimed claimed) {
      return claimed.assignment();
    }
    if (state instanceof HumanTaskState.AwaitingNextStage awaiting) {
      return awaiting.assignment();
    }
    if (state instanceof HumanTaskState.ReworkRequested rework) {
      return rework.assignment();
    }
    return null;
  }

  private static boolean active(HumanTaskState state) {
    return !(state instanceof HumanTaskState.Approved
        || state instanceof HumanTaskState.Rejected
        || state instanceof HumanTaskState.Resolved
        || state instanceof HumanTaskState.Cancelled
        || state instanceof HumanTaskState.Expired);
  }

  private static Outcome outcome(HumanTaskState state, Decision decision) {
    Snapshot snapshot = state.snapshot();
    return new Outcome(
        snapshot.definition().taskId(),
        decision.decisionId(),
        decision.kind(),
        decision.actionCode(),
        snapshot.reviewRound(),
        decision.stageId(),
        snapshot.contentRevision(),
        snapshot.currentContentSha256(),
        snapshot.currentContent(),
        decision.actor(),
        decision.decidedAt());
  }

  private static EventMetadata metadata(CommandMetadata metadata) {
    return new EventMetadata(
        metadata.taskId(), metadata.commandId(), metadata.actor(), metadata.occurredAt());
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw failure(Failure.ILLEGAL_TRANSITION, message);
    }
  }

  private static HumanTaskTransitionException failure(Failure failure, String message) {
    return new HumanTaskTransitionException(failure, message);
  }
}
