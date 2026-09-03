/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import java.util.Objects;

/** Accepted/replayed command result plus the exact persisted read projection. */
public record HumanTaskMutationResult(HumanTaskCommandResult command, HumanTaskView view) {
  public HumanTaskMutationResult {
    Objects.requireNonNull(command, "command");
    Objects.requireNonNull(view, "view");
  }
}
