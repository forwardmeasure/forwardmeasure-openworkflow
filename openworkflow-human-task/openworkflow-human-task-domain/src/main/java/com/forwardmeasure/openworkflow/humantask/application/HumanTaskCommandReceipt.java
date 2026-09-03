/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState;
import java.time.Instant;
import java.util.Objects;

/** Durable idempotency result for one accepted Human Task command. */
public record HumanTaskCommandReceipt(
    String commandId, String requestSha256, HumanTaskState resultingState, Instant createdAt) {
  public HumanTaskCommandReceipt {
    if (commandId == null || commandId.isBlank()) throw new IllegalArgumentException("commandId");
    if (requestSha256 == null || !requestSha256.matches("[0-9a-f]{64}")) {
      throw new IllegalArgumentException("requestSha256 must be lowercase SHA-256");
    }
    Objects.requireNonNull(resultingState, "resultingState");
    Objects.requireNonNull(createdAt, "createdAt");
  }
}
