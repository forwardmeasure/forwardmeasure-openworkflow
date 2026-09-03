/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.data.DataReferences;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskApplicationService.HumanTaskIdempotencyException;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskCommand;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskCommand.CommandMetadata;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition;
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
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskEvent;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskId;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class HumanTaskApplicationServiceTest {
  private static final HumanTaskId TASK_ID = new HumanTaskId("task-1");
  private static final Instant NOW = Instant.parse("2026-09-02T12:00:00Z");
  private static final Actor ACTOR =
      new Actor("reviewer-1", ActorKind.HUMAN, Set.of("reviewers"), Set.of());
  private static final String REQUEST_DIGEST = "a".repeat(64);

  @Test
  void atomicallyCommitsStateEventReceiptAndCreationOutboxMessage() {
    MemoryRepository repository = new MemoryRepository();
    HumanTaskApplicationService service = service(repository);

    HumanTaskCommandResult result = service.handle(create("create"), REQUEST_DIGEST);

    assertInstanceOf(HumanTaskState.Open.class, result.state());
    assertEquals(1, repository.commits);
    assertEquals(1, repository.events.size());
    assertEquals(1, repository.outbox.size());
    assertEquals("human-task-created", repository.outbox.getFirst().messageType());
    assertEquals(result.state(), repository.receipts.get("create").resultingState());
  }

  @Test
  void returnsTheExactStoredResultForAnIdenticalRetryWithoutASecondCommit() {
    MemoryRepository repository = new MemoryRepository();
    HumanTaskApplicationService service = service(repository);
    HumanTaskCommandResult first = service.handle(create("create"), REQUEST_DIGEST);

    HumanTaskCommandResult retry = service.handle(create("create"), REQUEST_DIGEST);

    assertEquals(first.state(), retry.state());
    assertEquals(List.of(), retry.events());
    assertEquals(true, retry.replayed());
    assertEquals(1, repository.commits);
  }

  @Test
  void rejectsReuseOfACommandIdForDifferentRequestContent() {
    MemoryRepository repository = new MemoryRepository();
    HumanTaskApplicationService service = service(repository);
    service.handle(create("create"), REQUEST_DIGEST);

    assertThrows(
        HumanTaskIdempotencyException.class,
        () -> service.handle(create("create"), "b".repeat(64)));
    assertEquals(1, repository.commits);
  }

  @Test
  void nonIntegrationTransitionsDoNotCreateOutboxMessages() {
    MemoryRepository repository = new MemoryRepository();
    HumanTaskApplicationService service = service(repository);
    HumanTaskState created = service.handle(create("create"), REQUEST_DIGEST).state();
    Assignment assignment = new Assignment(AssignmentKind.GROUP, "reviewers");

    service.handle(
        new HumanTaskCommand.Assign(metadata("assign", created), assignment), "c".repeat(64));

    assertEquals(2, repository.commits);
    assertEquals(1, repository.outbox.size());
  }

  @Test
  void requestAndOutcomeContractsPreserveTaskCorrelation() {
    HumanTaskDefinition definition = definition();
    HumanTaskRequest request =
        new HumanTaskRequest(
            "request-1", REQUEST_DIGEST, TASK_ID, "execution-1", "approve", definition, ACTOR, NOW);
    var content = definition.originalContent();
    HumanTaskState.Outcome decision =
        new HumanTaskState.Outcome(
            TASK_ID,
            "decision-1",
            DispositionKind.APPROVE,
            "approve",
            1,
            "review",
            0,
            content.sha256(),
            content,
            ACTOR,
            NOW);
    HumanTaskOutcome outcome =
        new HumanTaskOutcome("outcome-1", TASK_ID, "execution-1", "approve", decision, NOW);

    assertEquals(TASK_ID, request.definition().taskId());
    assertEquals(TASK_ID, outcome.decision().taskId());
    assertEquals("execution-1", outcome.workflowCorrelation());
  }

  @Test
  void requestAndOutcomeContractsRejectMismatchedTaskIdentity() {
    HumanTaskDefinition definition = definition();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new HumanTaskRequest(
                "request-1",
                REQUEST_DIGEST,
                new HumanTaskId("other-task"),
                "execution-1",
                "approve",
                definition,
                ACTOR,
                NOW));

    var content = definition.originalContent();
    HumanTaskState.Outcome decision =
        new HumanTaskState.Outcome(
            TASK_ID,
            "decision-1",
            DispositionKind.APPROVE,
            "approve",
            1,
            "review",
            0,
            content.sha256(),
            content,
            ACTOR,
            NOW);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new HumanTaskOutcome(
                "outcome-1",
                new HumanTaskId("other-task"),
                "execution-1",
                "approve",
                decision,
                NOW));
  }

  @Test
  void requestPortCreatesOnceAndReturnsReplayForTheSameRequest() {
    MemoryRepository repository = new MemoryRepository();
    ApplicationHumanTaskRequestPort port = new ApplicationHumanTaskRequestPort(service(repository));
    HumanTaskRequest request =
        new HumanTaskRequest(
            "request-1",
            REQUEST_DIGEST,
            TASK_ID,
            "execution-1",
            "approve",
            definition(),
            ACTOR,
            NOW);

    HumanTaskAcceptance first = port.request(request).toCompletableFuture().join();
    HumanTaskAcceptance retry = port.request(request).toCompletableFuture().join();

    assertEquals(HumanTaskAcceptance.AcceptanceStatus.ACCEPTED, first.status());
    assertEquals(HumanTaskAcceptance.AcceptanceStatus.REPLAYED, retry.status());
    assertEquals(1, repository.commits);
  }

  private static HumanTaskApplicationService service(MemoryRepository repository) {
    HumanTaskTransactionExecutor transactions =
        new HumanTaskTransactionExecutor() {
          @Override
          public <T> T execute(Function<HumanTaskRepository, T> work) {
            return work.apply(repository);
          }
        };
    return new HumanTaskApplicationService(transactions);
  }

  private static HumanTaskCommand.Create create(String commandId) {
    return new HumanTaskCommand.Create(metadata(commandId, null), definition());
  }

  private static CommandMetadata metadata(String commandId, HumanTaskState state) {
    return new CommandMetadata(
        TASK_ID, commandId, ACTOR, NOW, state == null ? 0 : state.revision());
  }

  private static HumanTaskDefinition definition() {
    ReviewAction approve =
        new ReviewAction(
            "approve",
            "Approve",
            DispositionKind.APPROVE,
            new ActionTransition(TransitionKind.RESOLVE, null),
            false);
    ReviewStage stage =
        new ReviewStage(
            "review", "Review", Set.of(), Set.of("reviewers"), Set.of(), List.of(approve));
    return new HumanTaskDefinition(
        TASK_ID,
        "trade-review",
        "Review trade",
        "Check economics",
        10,
        new TaskSource(SourceKind.API, "request-1", null, null, null),
        DataReferences.inline(JsonNodeFactory.instance.objectNode().put("amount", 10)),
        new Presentation(PresentationKind.RAW_JSON, null, null, null, null),
        new ReviewPlan(List.of(stage)),
        null,
        null,
        null,
        Map.of());
  }

  private static final class MemoryRepository implements HumanTaskRepository {
    private final Map<HumanTaskId, HumanTaskState> states = new HashMap<>();
    private final Map<String, HumanTaskCommandReceipt> receipts = new HashMap<>();
    private final List<HumanTaskEvent> events = new ArrayList<>();
    private final List<HumanTaskOutboxMessage> outbox = new ArrayList<>();
    private int commits;

    @Override
    public Optional<HumanTaskState> find(HumanTaskId taskId) {
      return Optional.ofNullable(states.get(taskId));
    }

    @Override
    public Optional<HumanTaskView> findView(HumanTaskId taskId) {
      return find(taskId).map(state -> new HumanTaskView(state, NOW, NOW));
    }

    @Override
    public HumanTaskPage list(HumanTaskListQuery query) {
      return new HumanTaskPage(
          states.values().stream().map(state -> new HumanTaskView(state, NOW, NOW)).toList(), null);
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
        List<HumanTaskEvent> newEvents,
        HumanTaskCommandReceipt receipt,
        List<HumanTaskOutboxMessage> messages) {
      states.put(resultingState.snapshot().definition().taskId(), resultingState);
      receipts.put(receipt.commandId(), receipt);
      events.addAll(newEvents);
      outbox.addAll(messages);
      commits++;
    }
  }
}
