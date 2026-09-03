/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to You under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 */
package com.forwardmeasure.openworkflow.actor;

import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskOutcome;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskId;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState;
import java.time.Instant;
import java.util.Objects;

/** Pekko adapter envelope for a correlated, terminal Human Task outcome. */
public record PekkoHumanTaskOutcome(
    String outcomeId,
    ExecutionId executionId,
    HumanTaskId taskId,
    String workflowCorrelation,
    String taskPath,
    HumanTaskState.Outcome decision,
    Instant occurredAt) {
  public PekkoHumanTaskOutcome {
    requireText(outcomeId, "outcomeId");
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(taskId, "taskId");
    requireText(workflowCorrelation, "workflowCorrelation");
    requireText(taskPath, "taskPath");
    Objects.requireNonNull(decision, "decision");
    Objects.requireNonNull(occurredAt, "occurredAt");
    if (!taskId.equals(decision.taskId())) {
      throw new IllegalArgumentException("Outcome and decision task identifiers differ");
    }
  }

  public static PekkoHumanTaskOutcome from(ExecutionId executionId, HumanTaskOutcome outcome) {
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(outcome, "outcome");
    return new PekkoHumanTaskOutcome(
        outcome.outcomeId(),
        executionId,
        outcome.taskId(),
        outcome.workflowCorrelation(),
        outcome.taskPath(),
        outcome.decision(),
        outcome.occurredAt());
  }

  private static void requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
  }
}
