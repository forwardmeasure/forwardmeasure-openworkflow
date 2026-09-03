/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.forwardmeasure.openworkflow.data.DataReferences;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskCommand.CommandMetadata;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ActionTransition;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.Actor;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ActorKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.Assignment;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.AssignmentKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.DispositionKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.Presentation;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.PresentationKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ReviewAction;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ReviewPlan;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ReviewStage;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.SourceKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.TaskSource;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.TransitionKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskTransitionException.Failure;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class HumanTaskStateMachineTest {
  private static final HumanTaskId TASK_ID = new HumanTaskId("task-1");
  private static final Instant NOW = Instant.parse("2026-09-02T12:00:00Z");
  private static final String TOKEN = "a".repeat(64);
  private static final Actor REVIEWER =
      new Actor("reviewer-1", ActorKind.HUMAN, Set.of("reviewers"), Set.of("level-one"));
  private static final Actor SECOND_REVIEWER =
      new Actor("reviewer-2", ActorKind.HUMAN, Set.of("senior-reviewers"), Set.of("level-two"));
  private static final Actor SYSTEM = new Actor("scheduler", ActorKind.SYSTEM, Set.of(), Set.of());

  private final HumanTaskStateMachine machine = new HumanTaskStateMachine();
  private final ObjectMapper json =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

  @Test
  void createsAnUnassignedTaskWithoutImplicitExpiry() {
    HumanTaskTransition created =
        machine.decide(
            null,
            new HumanTaskCommand.Create(
                metadata("create", null, REVIEWER), definition(null, null)));

    HumanTaskState.Open open = assertInstanceOf(HumanTaskState.Open.class, created.state());
    assertNull(open.snapshot().definition().expiresAt());
    assertEquals(0, open.snapshot().contentRevision());
    assertEquals(1, open.revision());
    assertInstanceOf(HumanTaskEvent.TaskCreated.class, created.events().getFirst());
  }

  @Test
  void assignmentSurvivesClaimAndReleaseWhileLeaseRemainsFinite() {
    HumanTaskState state = create(definition(null, null));
    Assignment assignment = new Assignment(AssignmentKind.GROUP, "reviewers");
    state =
        decide(state, new HumanTaskCommand.Assign(metadata("assign", state, SYSTEM), assignment))
            .state();
    HumanTaskTransition claimed =
        decide(
            state,
            new HumanTaskCommand.BeginReview(
                metadata("claim", state, REVIEWER),
                "session-1",
                TOKEN,
                NOW.plus(10, ChronoUnit.MINUTES)));
    HumanTaskState.Claimed inReview =
        assertInstanceOf(HumanTaskState.Claimed.class, claimed.state());
    assertEquals(assignment, inReview.assignment());
    assertEquals(NOW.plus(10, ChronoUnit.MINUTES), inReview.reviewSession().expiresAt());

    HumanTaskTransition released =
        decide(
            inReview,
            new HumanTaskCommand.ReleaseReview(
                metadata("release", inReview, REVIEWER), "session-1", TOKEN, "Not acting now"));
    HumanTaskState.Assigned available =
        assertInstanceOf(HumanTaskState.Assigned.class, released.state());
    assertEquals(assignment, available.assignment());
  }

  @Test
  void supervisorReassignmentIsOneExplicitAuditEventAndDisplacesTheLease() {
    HumanTaskState claimed = claim(create(definition(null, null)), REVIEWER, "session-1");
    Assignment replacement = new Assignment(AssignmentKind.GROUP, "senior-reviewers");

    HumanTaskTransition reassigned =
        decide(
            claimed,
            new HumanTaskCommand.ReassignReview(
                metadata("reassign", claimed, SYSTEM), replacement, "Route to senior desk"));

    assertEquals(1, reassigned.events().size());
    HumanTaskEvent.ReviewReassigned event =
        assertInstanceOf(HumanTaskEvent.ReviewReassigned.class, reassigned.events().getFirst());
    assertEquals("session-1", event.displacedReviewSessionId());
    assertEquals(replacement, event.assignment());
    HumanTaskState.Assigned assigned =
        assertInstanceOf(HumanTaskState.Assigned.class, reassigned.state());
    assertEquals(replacement, assigned.assignment());
    assertEquals(claimed.revision() + 1, assigned.revision());
  }

  @Test
  void renewsLeaseAndRecordsCommentsWithoutChangingReviewOwnership() {
    HumanTaskState.Claimed claimed =
        assertInstanceOf(
            HumanTaskState.Claimed.class,
            claim(create(definition(null, null)), REVIEWER, "session-1"));
    Instant extendedExpiry = NOW.plus(20, ChronoUnit.MINUTES);
    HumanTaskTransition renewed =
        decide(
            claimed,
            new HumanTaskCommand.RenewReviewLease(
                metadata("renew", claimed, REVIEWER), "session-1", TOKEN, extendedExpiry));
    HumanTaskState.Claimed renewedState =
        assertInstanceOf(HumanTaskState.Claimed.class, renewed.state());
    assertEquals(extendedExpiry, renewedState.reviewSession().expiresAt());

    HumanTaskTransition commented =
        decide(
            renewedState,
            new HumanTaskCommand.AddComment(
                metadata("comment", renewedState, REVIEWER),
                "session-1",
                TOKEN,
                "Checked economics"));
    assertInstanceOf(HumanTaskEvent.CommentAdded.class, commented.events().getFirst());
    HumanTaskState.Claimed commentedState =
        assertInstanceOf(HumanTaskState.Claimed.class, commented.state());
    assertEquals(REVIEWER, commentedState.reviewSession().heldBy());
  }

  @Test
  void rejectsConcurrentReviewerAndStaleRevision() {
    HumanTaskState state = claim(create(definition(null, null)), REVIEWER, "session-1");
    HumanTaskTransitionException concurrent =
        assertThrows(
            HumanTaskTransitionException.class,
            () ->
                decide(
                    state,
                    new HumanTaskCommand.BeginReview(
                        metadata("other-claim", state, SECOND_REVIEWER),
                        "session-2",
                        "b".repeat(64),
                        NOW.plus(10, ChronoUnit.MINUTES))));
    assertEquals(Failure.ILLEGAL_TRANSITION, concurrent.failure());

    CommandMetadata stale =
        new CommandMetadata(TASK_ID, "stale", REVIEWER, NOW, state.revision() - 1);
    HumanTaskTransitionException conflict =
        assertThrows(
            HumanTaskTransitionException.class,
            () ->
                decide(state, new HumanTaskCommand.AddComment(stale, "session-1", TOKEN, "note")));
    assertEquals(Failure.REVISION_CONFLICT, conflict.failure());
  }

  @Test
  void savesForwardOnlyContentAndRejectsStaleContentRevision() {
    HumanTaskState state = claim(create(definition(null, null)), REVIEWER, "session-1");
    JsonNode corrected =
        JsonNodeFactory.instance.objectNode().put("amount", 20).put("currency", "USD");
    HumanTaskTransition saved =
        decide(
            state,
            new HumanTaskCommand.SaveRevision(
                metadata("save", state, REVIEWER),
                "session-1",
                TOKEN,
                0,
                DataReferences.inline(corrected),
                DataReferences.inline(JsonNodeFactory.instance.arrayNode()),
                "Correct amount"));
    assertEquals(1, saved.state().snapshot().contentRevision());
    assertEquals(
        20, saved.state().snapshot().currentContent().inlineValue().required("amount").intValue());
    assertEquals(10, state.snapshot().currentContent().inlineValue().required("amount").intValue());

    HumanTaskTransitionException stale =
        assertThrows(
            HumanTaskTransitionException.class,
            () ->
                decide(
                    saved.state(),
                    new HumanTaskCommand.SaveRevision(
                        metadata("stale-save", saved.state(), REVIEWER),
                        "session-1",
                        TOKEN,
                        0,
                        DataReferences.inline(corrected),
                        null,
                        null)));
    assertEquals(Failure.STALE_CONTENT, stale.failure());
  }

  @Test
  void advancesAcrossStagesAndResolvesWithExactReviewedContent() {
    HumanTaskState firstClaim = claim(create(definition(null, null)), REVIEWER, "session-1");
    HumanTaskTransition firstDecision =
        decide(
            firstClaim,
            new HumanTaskCommand.SubmitDecision(
                metadata("approve-one", firstClaim, REVIEWER),
                "session-1",
                TOKEN,
                0,
                "approve",
                null));
    assertEquals(
        List.of(
            HumanTaskEvent.DecisionRecorded.class,
            HumanTaskEvent.ReviewStageAdvanced.class,
            HumanTaskEvent.NextStageActivated.class),
        firstDecision.events().stream().map(Object::getClass).toList());
    HumanTaskState.Open secondStage =
        assertInstanceOf(HumanTaskState.Open.class, firstDecision.state());
    assertEquals(1, secondStage.snapshot().stageIndex());

    HumanTaskState secondClaim = claim(secondStage, SECOND_REVIEWER, "session-2");
    HumanTaskTransition completed =
        decide(
            secondClaim,
            new HumanTaskCommand.SubmitDecision(
                metadata("approve-two", secondClaim, SECOND_REVIEWER),
                "session-2",
                TOKEN,
                0,
                "approve",
                "Approved"));
    HumanTaskState.Approved approved =
        assertInstanceOf(HumanTaskState.Approved.class, completed.state());
    assertEquals("approve", approved.outcome().actionCode());
    assertEquals(approved.snapshot().currentContentSha256(), approved.outcome().contentSha256());
    assertEquals(2, approved.snapshot().decisions().size());
  }

  @Test
  void reworkAndReopenCreateANewReviewRoundWithoutChangingPriorDecision() {
    HumanTaskState claimed = claim(create(definition(null, null)), REVIEWER, "session-1");
    HumanTaskState.ReworkRequested rework =
        assertInstanceOf(
            HumanTaskState.ReworkRequested.class,
            decide(
                    claimed,
                    new HumanTaskCommand.SubmitDecision(
                        metadata("rework", claimed, REVIEWER),
                        "session-1",
                        TOKEN,
                        0,
                        "rework",
                        "Please correct"))
                .state());
    HumanTaskState.Open reopened =
        assertInstanceOf(
            HumanTaskState.Open.class,
            decide(
                    rework,
                    new HumanTaskCommand.Reopen(
                        metadata("reopen", rework, SYSTEM), "review", null, "Correction supplied"))
                .state());
    assertEquals(2, reopened.snapshot().reviewRound());
    assertEquals(1, reopened.snapshot().decisions().size());
  }

  @Test
  void leaseExpiryDoesNotExpireAnInfiniteTask() {
    HumanTaskState claimed = claim(create(definition(null, null)), REVIEWER, "session-1");
    Instant afterLease = NOW.plus(11, ChronoUnit.MINUTES);
    CommandMetadata expiryMetadata =
        new CommandMetadata(TASK_ID, "expire-lease", SYSTEM, afterLease, claimed.revision());
    HumanTaskState.Open reopened =
        assertInstanceOf(
            HumanTaskState.Open.class,
            decide(claimed, new HumanTaskCommand.ExpireReviewLease(expiryMetadata, "session-1"))
                .state());

    HumanTaskTransitionException noTaskExpiry =
        assertThrows(
            HumanTaskTransitionException.class,
            () ->
                decide(
                    reopened,
                    new HumanTaskCommand.ExpireTask(
                        new CommandMetadata(
                            TASK_ID, "expire-task", SYSTEM, afterLease, reopened.revision()),
                        "timer")));
    assertEquals(Failure.ILLEGAL_TRANSITION, noTaskExpiry.failure());
  }

  @Test
  void finiteTaskExpiresOnlyAtItsDeadlineAndCancellationIsIndependent() {
    Instant deadline = NOW.plus(2, ChronoUnit.HOURS);
    HumanTaskState finite = create(definition(null, deadline));
    HumanTaskTransitionException early =
        assertThrows(
            HumanTaskTransitionException.class,
            () ->
                decide(
                    finite,
                    new HumanTaskCommand.ExpireTask(
                        metadata("early-expiry", finite, SYSTEM), "deadline")));
    assertEquals(Failure.ILLEGAL_TRANSITION, early.failure());

    HumanTaskState.Expired expired =
        assertInstanceOf(
            HumanTaskState.Expired.class,
            decide(
                    finite,
                    new HumanTaskCommand.ExpireTask(
                        new CommandMetadata(TASK_ID, "expire", SYSTEM, deadline, finite.revision()),
                        "deadline"))
                .state());
    assertEquals("deadline", expired.reason());

    HumanTaskState cancellable = create(definition(null, null));
    HumanTaskState.Cancelled cancelled =
        assertInstanceOf(
            HumanTaskState.Cancelled.class,
            decide(
                    cancellable,
                    new HumanTaskCommand.Cancel(
                        metadata("cancel", cancellable, SYSTEM), "source withdrawn"))
                .state());
    assertEquals("source withdrawn", cancelled.reason());
  }

  @Test
  void escalationMovesActiveWorkToTheConfiguredStageAndAssignment() {
    HumanTaskState claimed = claim(create(definition(null, null)), REVIEWER, "session-1");
    Assignment senior = new Assignment(AssignmentKind.GROUP, "senior-reviewers");

    HumanTaskTransition escalated =
        decide(
            claimed,
            new HumanTaskCommand.Escalate(
                metadata("escalate", claimed, SYSTEM),
                "senior-review",
                senior,
                "material exception"));

    assertEquals(
        List.of(HumanTaskEvent.TaskEscalated.class, HumanTaskEvent.NextStageActivated.class),
        escalated.events().stream().map(Object::getClass).toList());
    HumanTaskState.Assigned assigned =
        assertInstanceOf(HumanTaskState.Assigned.class, escalated.state());
    assertEquals(1, assigned.snapshot().stageIndex());
    assertEquals(senior, assigned.assignment());
  }

  @Test
  void replayingEveryAcceptedEventProducesTheSameState() {
    HumanTaskState state = create(definition(null, null));
    HumanTaskTransition claim =
        decide(
            state,
            new HumanTaskCommand.BeginReview(
                metadata("claim", state, REVIEWER),
                "session-1",
                TOKEN,
                NOW.plus(10, ChronoUnit.MINUTES)));
    HumanTaskTransition decision =
        decide(
            claim.state(),
            new HumanTaskCommand.SubmitDecision(
                metadata("decline", claim.state(), REVIEWER),
                "session-1",
                TOKEN,
                0,
                "decline",
                "Invalid"));

    HumanTaskState replayed = null;
    HumanTaskTransition creation =
        machine.decide(
            null,
            new HumanTaskCommand.Create(
                metadata("replay-create", null, REVIEWER), definition(null, null)));
    for (HumanTaskEvent event : creation.events()) {
      replayed = machine.evolve(replayed, event);
    }
    for (HumanTaskEvent event : claim.events()) {
      replayed = machine.evolve(replayed, event);
    }
    for (HumanTaskEvent event : decision.events()) {
      replayed = machine.evolve(replayed, event);
    }
    assertEquals(decision.state(), replayed);
  }

  @Test
  void commandsEventsAndStatesHaveStablePolymorphicRoundTrips() throws Exception {
    HumanTaskCommand command =
        new HumanTaskCommand.Create(metadata("create", null, REVIEWER), definition(null, null));
    HumanTaskTransition transition = machine.decide(null, command);
    HumanTaskCommand restoredCommand =
        json.readValue(json.writeValueAsBytes(command), HumanTaskCommand.class);
    HumanTaskEvent restoredEvent =
        json.readValue(
            json.writeValueAsBytes(transition.events().getFirst()), HumanTaskEvent.class);
    HumanTaskState restoredState =
        json.readValue(json.writeValueAsBytes(transition.state()), HumanTaskState.class);

    assertEquals(json.valueToTree(command), json.valueToTree(restoredCommand));
    assertEquals(json.valueToTree(transition.events().getFirst()), json.valueToTree(restoredEvent));
    assertEquals(json.valueToTree(transition.state()), json.valueToTree(restoredState));
    assertEquals(
        json.valueToTree(transition.state()),
        json.valueToTree(machine.decide(null, restoredCommand).state()));
    assertEquals("create", json.valueToTree(command).required("type").textValue());
    assertEquals(
        "task-created",
        json.valueToTree(transition.events().getFirst()).required("type").textValue());
    assertEquals("open", json.valueToTree(transition.state()).required("type").textValue());
  }

  private HumanTaskState create(HumanTaskDefinition definition) {
    return machine
        .decide(null, new HumanTaskCommand.Create(metadata("create", null, REVIEWER), definition))
        .state();
  }

  private HumanTaskState claim(HumanTaskState state, Actor actor, String sessionId) {
    return decide(
            state,
            new HumanTaskCommand.BeginReview(
                metadata("claim-" + sessionId, state, actor),
                sessionId,
                TOKEN,
                NOW.plus(10, ChronoUnit.MINUTES)))
        .state();
  }

  private HumanTaskTransition decide(HumanTaskState state, HumanTaskCommand command) {
    return machine.decide(state, command);
  }

  private static CommandMetadata metadata(String commandId, HumanTaskState state, Actor actor) {
    return new CommandMetadata(
        TASK_ID, commandId, actor, NOW, state == null ? 0 : state.revision());
  }

  private static HumanTaskDefinition definition(Assignment assignment, Instant expiresAt) {
    ReviewAction firstApprove =
        new ReviewAction(
            "approve",
            "Approve",
            DispositionKind.APPROVE,
            new ActionTransition(TransitionKind.ADVANCE, "senior-review"),
            false);
    ReviewStage first =
        new ReviewStage(
            "review",
            "Review",
            Set.of(),
            Set.of("reviewers"),
            Set.of("level-one"),
            List.of(
                firstApprove,
                new ReviewAction(
                    "decline",
                    "Decline",
                    DispositionKind.DECLINE,
                    new ActionTransition(TransitionKind.RESOLVE, null),
                    true),
                new ReviewAction(
                    "rework",
                    "Request correction",
                    DispositionKind.OTHER,
                    new ActionTransition(TransitionKind.REWORK, "review"),
                    true)));
    ReviewStage second =
        new ReviewStage(
            "senior-review",
            "Senior review",
            Set.of(),
            Set.of("senior-reviewers"),
            Set.of("level-two"),
            List.of(
                new ReviewAction(
                    "approve",
                    "Approve",
                    DispositionKind.APPROVE,
                    new ActionTransition(TransitionKind.RESOLVE, null),
                    false),
                new ReviewAction(
                    "decline",
                    "Decline",
                    DispositionKind.DECLINE,
                    new ActionTransition(TransitionKind.RESOLVE, null),
                    true)));
    return new HumanTaskDefinition(
        TASK_ID,
        "trade-review",
        "Review trade",
        "Validate trade economics",
        10,
        new TaskSource(
            SourceKind.WORKFLOW, "trade-workflow", "execution-1", "/do/2/review", "correlation-1"),
        DataReferences.inline(
            JsonNodeFactory.instance.objectNode().put("amount", 10).put("currency", "USD")),
        new Presentation(PresentationKind.RAW_JSON, null, null, null, null),
        new ReviewPlan(List.of(first, second)),
        assignment,
        NOW.plus(1, ChronoUnit.HOURS),
        expiresAt,
        Map.of("amount", JsonNodeFactory.instance.numberNode(10)));
  }
}
