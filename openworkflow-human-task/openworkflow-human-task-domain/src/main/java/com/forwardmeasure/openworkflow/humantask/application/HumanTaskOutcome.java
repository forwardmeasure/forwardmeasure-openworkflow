/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to You under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskId;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState;
import java.time.Instant;
import java.util.Objects;

/** Engine-neutral terminal result correlated back to a waiting workflow branch. */
public record HumanTaskOutcome(
    String outcomeId,
    HumanTaskId taskId,
    String workflowCorrelation,
    String taskPath,
    HumanTaskState.Outcome decision,
    Instant occurredAt) {
  public HumanTaskOutcome {
    requireText(outcomeId, "outcomeId");
    Objects.requireNonNull(taskId, "taskId");
    requireText(workflowCorrelation, "workflowCorrelation");
    requireText(taskPath, "taskPath");
    Objects.requireNonNull(decision, "decision");
    Objects.requireNonNull(occurredAt, "occurredAt");
    if (!taskId.equals(decision.taskId())) {
      throw new IllegalArgumentException("Outcome and decision task identifiers differ");
    }
  }

  private static void requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
  }
}
