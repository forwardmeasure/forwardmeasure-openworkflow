/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.domain;

import java.util.List;

/** Accepted event sequence and resulting state for one command. */
public record HumanTaskTransition(List<HumanTaskEvent> events, HumanTaskState state) {
  public HumanTaskTransition {
    events = List.copyOf(events);
    if (events.isEmpty()) {
      throw new IllegalArgumentException("An accepted command must emit at least one event");
    }
  }
}
