/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import java.util.Objects;

/** Save-revision result including the exact persisted immutable revision for replay parity. */
public record HumanTaskRevisionMutationResult(
    HumanTaskMutationResult mutation, HumanTaskContentRevisionRecord revision) {
  public HumanTaskRevisionMutationResult {
    Objects.requireNonNull(mutation, "mutation");
    Objects.requireNonNull(revision, "revision");
  }
}
