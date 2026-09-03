/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState;
import java.time.Instant;
import java.util.Objects;

/** Read projection retaining the domain snapshot and its durable queue timestamps. */
public record HumanTaskView(HumanTaskState state, Instant receivedAt, Instant updatedAt) {
  public HumanTaskView {
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(receivedAt, "receivedAt");
    Objects.requireNonNull(updatedAt, "updatedAt");
  }
}
