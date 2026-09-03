/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskEvent;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskTransition;
import java.util.List;
import java.util.Objects;

/** Application response for either a newly accepted command or an idempotent replay. */
public record HumanTaskCommandResult(
    List<HumanTaskEvent> events, HumanTaskState state, boolean replayed) {
  public HumanTaskCommandResult {
    events = List.copyOf(events);
    Objects.requireNonNull(state, "state");
    if (!replayed && events.isEmpty()) {
      throw new IllegalArgumentException("A newly accepted command must emit events");
    }
    if (replayed && !events.isEmpty()) {
      throw new IllegalArgumentException("An idempotent replay must not report new events");
    }
  }

  static HumanTaskCommandResult accepted(HumanTaskTransition transition) {
    return new HumanTaskCommandResult(transition.events(), transition.state(), false);
  }

  static HumanTaskCommandResult replayed(HumanTaskState state) {
    return new HumanTaskCommandResult(List.of(), state, true);
  }
}
