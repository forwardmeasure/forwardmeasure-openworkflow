/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.jaxrs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.AuthorizationDecision;
import com.forwardmeasure.openworkflow.authorization.AuthorizationDeniedException;
import com.forwardmeasure.openworkflow.authorization.AuthorizationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.data.DataReferences;
import com.forwardmeasure.openworkflow.humantask.api.model.DecisionRequest;
import com.forwardmeasure.openworkflow.humantask.api.model.HumanTask;
import com.forwardmeasure.openworkflow.humantask.api.model.ReasonRequest;
import com.forwardmeasure.openworkflow.humantask.api.model.ReviewSession;
import com.forwardmeasure.openworkflow.humantask.application.AuthorizedHumanTaskService;
import com.forwardmeasure.openworkflow.humantask.application.HmacReviewSessionCredentialIssuer;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskApplicationService;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskCommandReceipt;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskContentRevisionRecord;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskHistoryPage;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskListQuery;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskOutboxMessage;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskPage;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskQueryService;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskRepository;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskTransactionExecutor;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskView;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskCommand;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskCommand.CommandMetadata;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ActionTransition;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.Actor;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ActorKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.DispositionKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.Presentation;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.PresentationKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ReviewAction;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ReviewPlan;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ReviewStage;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.SourceKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.TaskSource;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.TransitionKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskEvent;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskId;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState;
import jakarta.ws.rs.core.Response;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class HumanTaskResourceTest {
  private static final HumanTaskId TASK_ID = new HumanTaskId("task-1");
  private static final Instant NOW = Instant.parse("2026-09-02T12:00:00Z");
  private static final ActiveOrganization ORGANIZATION =
      new ActiveOrganization(
          new TenantId(UUID.fromString("01234567-89ab-cdef-0123-456789abcdef")),
          "org-1",
          "reviewer-1",
          Set.of("reviewer"));
  private static final Actor ACTOR =
      new Actor("reviewer-1", ActorKind.HUMAN, Set.of(), Set.of("reviewer"));

  @Test
  void generatedApiJourneyIsIdempotentAndAuthorizesTheExactDecisionAction() {
    MemoryRepository repository = new MemoryRepository();
    HumanTaskApplicationService commands =
        new HumanTaskApplicationService(new MemoryTransactions(repository));
    commands.handle(create(), "1".repeat(64));
    RecordingAuthorization authorization = new RecordingAuthorization();
    HumanTaskResource resource = resource(repository, commands, authorization);

    try (Response first = resource.beginHumanTaskReview("\"1\"", "claim-1", TASK_ID.value(), null);
        Response replay =
            resource.beginHumanTaskReview("\"1\"", "claim-1", TASK_ID.value(), null)) {
      assertEquals(201, first.getStatus());
      assertEquals("\"2\"", first.getHeaderString("ETag"));
      ReviewSession firstSession = assertInstanceOf(ReviewSession.class, first.getEntity());
      ReviewSession replaySession = assertInstanceOf(ReviewSession.class, replay.getEntity());
      assertEquals(firstSession.getReviewSessionId(), replaySession.getReviewSessionId());
      assertEquals(firstSession.getReviewToken(), replaySession.getReviewToken());

      try (Response decided =
          resource.submitHumanTaskDecision(
              "\"2\"",
              "decision-1",
              firstSession.getReviewToken(),
              TASK_ID.value(),
              firstSession.getReviewSessionId(),
              new DecisionRequest(0L, "approve"))) {
        assertEquals(200, decided.getStatus());
        assertEquals("\"4\"", decided.getHeaderString("ETag"));
        assertEquals(
            "APPROVED",
            assertInstanceOf(HumanTask.class, decided.getEntity()).getStatus().toString());
        assertEquals(
            "human-task:decide:approve", authorization.requests.getLast().resolvedActionScope());
      }
    }
  }

  @Test
  void deniedAdministrativeMutationFailsBeforeStateChanges() {
    MemoryRepository repository = new MemoryRepository();
    HumanTaskApplicationService commands =
        new HumanTaskApplicationService(new MemoryTransactions(repository));
    commands.handle(create(), "1".repeat(64));
    RecordingAuthorization authorization = new RecordingAuthorization();
    authorization.permitted = false;
    HumanTaskResource resource = resource(repository, commands, authorization);

    assertThrows(
        AuthorizationDeniedException.class,
        () ->
            resource.cancelHumanTask(
                "\"1\"", "cancel-1", TASK_ID.value(), new ReasonRequest("stop")));
    assertInstanceOf(HumanTaskState.Open.class, repository.states.get(TASK_ID));
    assertEquals(1, repository.receipts.size());
  }

  private static HumanTaskResource resource(
      MemoryRepository repository,
      HumanTaskApplicationService commands,
      AuthorizationService authorization) {
    HumanTaskQueryService queries = new HumanTaskQueryService(new MemoryTransactions(repository));
    return new HumanTaskResource(
        new AuthorizedHumanTaskService(commands, queries, authorization),
        () -> ORGANIZATION,
        () -> "correlation-1",
        new HmacReviewSessionCredentialIssuer(
            "s".repeat(32).getBytes(java.nio.charset.StandardCharsets.UTF_8)),
        Clock.fixed(NOW, ZoneOffset.UTC),
        Duration.ofMinutes(10),
        new ObjectMapper().registerModule(new JavaTimeModule()));
  }

  private static HumanTaskCommand.Create create() {
    ReviewAction approve =
        new ReviewAction(
            "approve",
            "Approve",
            DispositionKind.APPROVE,
            new ActionTransition(TransitionKind.RESOLVE, null),
            false);
    HumanTaskDefinition definition =
        new HumanTaskDefinition(
            TASK_ID,
            "trade-review",
            "Review trade",
            "Check economics",
            10,
            new TaskSource(SourceKind.API, "request-1", null, null, null),
            DataReferences.inline(JsonNodeFactory.instance.objectNode().put("amount", 10)),
            new Presentation(PresentationKind.RAW_JSON, null, null, null, null),
            new ReviewPlan(
                List.of(
                    new ReviewStage(
                        "review",
                        "Review",
                        Set.of(),
                        Set.of(),
                        Set.of("reviewer"),
                        List.of(approve)))),
            null,
            null,
            null,
            Map.of("amount", JsonNodeFactory.instance.numberNode(10)));
    return new HumanTaskCommand.Create(
        new CommandMetadata(TASK_ID, "create-1", ACTOR, NOW.minusSeconds(1), 0), definition);
  }

  private static final class RecordingAuthorization implements AuthorizationService {
    private final List<AuthorizationRequest> requests = new ArrayList<>();
    private boolean permitted = true;

    @Override
    public AuthorizationDecision evaluate(AuthorizationRequest request) {
      requests.add(request);
      return new AuthorizationDecision(permitted, request.correlationId(), Map.of());
    }

    @Override
    public List<AuthorizationDecision> evaluateBatch(List<AuthorizationRequest> requests) {
      return requests.stream().map(this::evaluate).toList();
    }
  }

  private record MemoryTransactions(HumanTaskRepository repository)
      implements HumanTaskTransactionExecutor {
    @Override
    public <T> T execute(Function<HumanTaskRepository, T> work) {
      return work.apply(repository);
    }
  }

  private static final class MemoryRepository implements HumanTaskRepository {
    private final Map<HumanTaskId, HumanTaskState> states = new HashMap<>();
    private final Map<String, HumanTaskCommandReceipt> receipts = new HashMap<>();
    private Instant receivedAt;
    private Instant updatedAt;

    @Override
    public Optional<HumanTaskState> find(HumanTaskId taskId) {
      return Optional.ofNullable(states.get(taskId));
    }

    @Override
    public Optional<HumanTaskView> findView(HumanTaskId taskId) {
      return find(taskId).map(state -> new HumanTaskView(state, receivedAt, updatedAt));
    }

    @Override
    public HumanTaskPage list(HumanTaskListQuery query) {
      return new HumanTaskPage(List.of(), null);
    }

    @Override
    public HumanTaskHistoryPage history(HumanTaskId taskId, long afterSequence, int limit) {
      return new HumanTaskHistoryPage(List.of(), null);
    }

    @Override
    public List<HumanTaskContentRevisionRecord> revisions(HumanTaskId taskId) {
      return List.of();
    }

    @Override
    public Optional<HumanTaskCommandReceipt> findReceipt(String commandId) {
      return Optional.ofNullable(receipts.get(commandId));
    }

    @Override
    public void commit(
        HumanTaskState priorState,
        HumanTaskState resultingState,
        List<HumanTaskEvent> events,
        HumanTaskCommandReceipt receipt,
        List<HumanTaskOutboxMessage> outboxMessages) {
      states.put(resultingState.snapshot().definition().taskId(), resultingState);
      receipts.put(receipt.commandId(), receipt);
      if (receivedAt == null) receivedAt = events.getFirst().metadata().occurredAt();
      updatedAt = events.getLast().metadata().occurredAt();
    }
  }
}
