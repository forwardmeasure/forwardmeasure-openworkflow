/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to You under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskId;
import java.util.Objects;

/** Durable acknowledgement returned for an idempotent Human Task request. */
public record HumanTaskAcceptance(String requestId, HumanTaskId taskId, AcceptanceStatus status) {
  public HumanTaskAcceptance {
    requireText(requestId, "requestId");
    Objects.requireNonNull(taskId, "taskId");
    Objects.requireNonNull(status, "status");
  }

  public enum AcceptanceStatus {
    ACCEPTED,
    REPLAYED
  }

  private static void requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
  }
}
