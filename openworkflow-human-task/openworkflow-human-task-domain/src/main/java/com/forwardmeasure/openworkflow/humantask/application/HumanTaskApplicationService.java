/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskCommand;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskEvent;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskStateMachine;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskTransition;
import java.util.List;
import java.util.Objects;

/** Transactional command application with durable idempotency and one transition authority. */
public final class HumanTaskApplicationService {
  private final HumanTaskTransactionExecutor transactions;
  private final HumanTaskStateMachine stateMachine;

  public HumanTaskApplicationService(HumanTaskTransactionExecutor transactions) {
    this(transactions, new HumanTaskStateMachine());
  }

  HumanTaskApplicationService(
      HumanTaskTransactionExecutor transactions, HumanTaskStateMachine stateMachine) {
    this.transactions = Objects.requireNonNull(transactions, "transactions");
    this.stateMachine = Objects.requireNonNull(stateMachine, "stateMachine");
  }

  public HumanTaskCommandResult handle(HumanTaskCommand command, String requestSha256) {
    Objects.requireNonNull(command, "command");
    requireDigest(requestSha256);
    return transactions.execute(
        repository -> {
          var existing = repository.findReceipt(command.metadata().commandId());
          if (existing.isPresent()) {
            HumanTaskCommandReceipt receipt = existing.orElseThrow();
            if (!receipt.requestSha256().equals(requestSha256)) {
              throw new HumanTaskIdempotencyException(
                  "Command id was already used for a different request");
            }
            return HumanTaskCommandResult.replayed(receipt.resultingState());
          }
          HumanTaskState prior = repository.find(command.metadata().taskId()).orElse(null);
          HumanTaskTransition transition = stateMachine.decide(prior, command);
          HumanTaskCommandReceipt receipt =
              new HumanTaskCommandReceipt(
                  command.metadata().commandId(),
                  requestSha256,
                  transition.state(),
                  command.metadata().occurredAt());
          repository.commit(
              prior,
              transition.state(),
              transition.events(),
              receipt,
              outboxMessages(transition.events(), transition.state()));
          return HumanTaskCommandResult.accepted(transition);
        });
  }

  private static List<HumanTaskOutboxMessage> outboxMessages(
      List<HumanTaskEvent> events, HumanTaskState state) {
    var source = state.snapshot().definition().source();
    return events.stream()
        .filter(HumanTaskApplicationService::requiresIntegrationDelivery)
        .map(
            event ->
                new HumanTaskOutboxMessage(
                    event.metadata().commandId() + ":" + event.getClass().getSimpleName(),
                    event.metadata().taskId(),
                    source.correlationId(),
                    source.workflowTaskPath(),
                    event instanceof HumanTaskEvent.TaskCreated
                        ? "human-task-created"
                        : "human-task-outcome",
                    event,
                    event.metadata().occurredAt()))
        .toList();
  }

  private static boolean requiresIntegrationDelivery(HumanTaskEvent event) {
    return event instanceof HumanTaskEvent.TaskCreated
        || event instanceof HumanTaskEvent.TaskApproved
        || event instanceof HumanTaskEvent.TaskRejected
        || event instanceof HumanTaskEvent.TaskResolved
        || event instanceof HumanTaskEvent.TaskCancelled
        || event instanceof HumanTaskEvent.TaskExpired;
  }

  private static void requireDigest(String value) {
    if (value == null || !value.matches("[0-9a-f]{64}")) {
      throw new IllegalArgumentException("requestSha256 must be lowercase SHA-256");
    }
  }

  public static final class HumanTaskIdempotencyException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public HumanTaskIdempotencyException(String message) {
      super(message);
    }
  }
}
