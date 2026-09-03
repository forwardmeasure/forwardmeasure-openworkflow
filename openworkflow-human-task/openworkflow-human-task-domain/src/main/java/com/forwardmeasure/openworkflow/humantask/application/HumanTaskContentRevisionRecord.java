/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import com.forwardmeasure.openworkflow.data.DataReference;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskId;
import java.time.Instant;
import java.util.Objects;

/** Immutable content revision and its forward-only correction evidence. */
public record HumanTaskContentRevisionRecord(
    HumanTaskId taskId,
    long contentRevision,
    long basedOnRevision,
    String createdBy,
    Instant createdAt,
    String reviewSessionId,
    String beforeSha256,
    String afterSha256,
    DataReference jsonPatch,
    DataReference resultContent,
    String comment) {
  public HumanTaskContentRevisionRecord {
    Objects.requireNonNull(taskId, "taskId");
    if (contentRevision < 0 || basedOnRevision < 0) {
      throw new IllegalArgumentException("content revisions must not be negative");
    }
    Objects.requireNonNull(createdBy, "createdBy");
    Objects.requireNonNull(createdAt, "createdAt");
    Objects.requireNonNull(afterSha256, "afterSha256");
    Objects.requireNonNull(resultContent, "resultContent");
  }
}
