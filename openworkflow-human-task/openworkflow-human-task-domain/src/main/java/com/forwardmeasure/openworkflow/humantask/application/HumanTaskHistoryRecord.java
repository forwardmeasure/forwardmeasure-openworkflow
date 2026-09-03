/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskEvent;
import java.util.Objects;

/** One ordered, immutable event in a Human Task audit history. */
public record HumanTaskHistoryRecord(long sequence, HumanTaskEvent event) {
  public HumanTaskHistoryRecord {
    if (sequence < 1) throw new IllegalArgumentException("sequence must be positive");
    Objects.requireNonNull(event, "event");
  }
}
