/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskEvent;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskId;
import java.time.Instant;
import java.util.Objects;

/** Durable integration message written atomically with an accepted Human Task transition. */
public record HumanTaskOutboxMessage(
    String messageId,
    HumanTaskId taskId,
    String workflowCorrelation,
    String taskPath,
    String messageType,
    HumanTaskEvent payload,
    Instant createdAt) {
  public HumanTaskOutboxMessage {
    requireText(messageId, "messageId");
    Objects.requireNonNull(taskId, "taskId");
    if ((workflowCorrelation == null) != (taskPath == null)) {
      throw new IllegalArgumentException("Workflow correlation and task path must coexist");
    }
    requireText(messageType, "messageType");
    Objects.requireNonNull(payload, "payload");
    Objects.requireNonNull(createdAt, "createdAt");
  }

  private static void requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
  }
}
