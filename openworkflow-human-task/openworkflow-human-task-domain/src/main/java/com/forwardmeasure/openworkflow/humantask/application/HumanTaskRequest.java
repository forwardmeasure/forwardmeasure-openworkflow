/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to You under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskId;
import java.time.Instant;
import java.util.Objects;

/** Engine-neutral, idempotent request to materialize one Human Task. */
public record HumanTaskRequest(
    String requestId,
    String requestSha256,
    HumanTaskId taskId,
    String workflowCorrelation,
    String taskPath,
    HumanTaskDefinition definition,
    HumanTaskDefinition.Actor actor,
    Instant requestedAt) {
  public HumanTaskRequest {
    requireText(requestId, "requestId");
    requireDigest(requestSha256, "requestSha256");
    Objects.requireNonNull(taskId, "taskId");
    requireText(workflowCorrelation, "workflowCorrelation");
    requireText(taskPath, "taskPath");
    Objects.requireNonNull(definition, "definition");
    Objects.requireNonNull(actor, "actor");
    Objects.requireNonNull(requestedAt, "requestedAt");
    if (!taskId.equals(definition.taskId())) {
      throw new IllegalArgumentException("Request and definition task identifiers differ");
    }
  }

  private static void requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
  }

  private static void requireDigest(String value, String name) {
    if (value == null || !value.matches("[0-9a-f]{64}")) {
      throw new IllegalArgumentException(name + " must be lowercase SHA-256");
    }
  }
}
