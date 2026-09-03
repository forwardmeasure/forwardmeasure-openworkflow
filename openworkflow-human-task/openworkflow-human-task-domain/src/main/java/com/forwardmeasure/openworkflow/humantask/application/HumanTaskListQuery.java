/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import java.time.Instant;
import java.util.Set;

/** Bounded, keyset-paginated work-queue query. */
public record HumanTaskListQuery(
    Set<String> statuses,
    String taskType,
    String assignment,
    String reviewer,
    boolean overdue,
    String cursor,
    int limit,
    String sort,
    Direction direction,
    Instant now) {
  public HumanTaskListQuery {
    statuses = statuses == null ? Set.of() : Set.copyOf(statuses);
    if (limit < 1 || limit > 200) throw new IllegalArgumentException("limit must be 1 to 200");
    if (sort == null || sort.isBlank()) sort = "receivedAt";
    if (!sort.matches("[A-Za-z][A-Za-z0-9.]{0,199}")) {
      throw new IllegalArgumentException("sort is not a valid field name");
    }
    if (direction == null) direction = Direction.ASC;
    if (overdue && now == null) throw new IllegalArgumentException("overdue queries require now");
  }

  public enum Direction {
    ASC,
    DESC
  }
}
