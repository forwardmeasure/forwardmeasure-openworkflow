package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference;
import java.util.Objects;

/** Independently recoverable execution lane belonging to one fork branch. */
public record ForkBranchState(
    String name,
    String path,
    int declarationIndex,
    ForkBranchPhase phase,
    ExecutionCursor cursor,
    DataReference data,
    ForkRuntimeState activeFork,
    PendingInteraction pendingInteraction,
    Long completedOrder) {

  public ForkBranchState {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(path, "path");
    Objects.requireNonNull(phase, "phase");
    Objects.requireNonNull(cursor, "cursor");
    Objects.requireNonNull(data, "data");
    if (name.isBlank() || path.isBlank()) {
      throw new IllegalArgumentException("Fork branch name and path must not be blank");
    }
    if (declarationIndex < 0) {
      throw new IllegalArgumentException("declarationIndex must not be negative");
    }
    if ((phase == ForkBranchPhase.COMPLETED) != (completedOrder != null)) {
      throw new IllegalArgumentException("Only a completed branch has a completion order");
    }
    if (completedOrder != null && completedOrder < 0) {
      throw new IllegalArgumentException("completedOrder must not be negative");
    }
    if (phase.terminal() && (activeFork != null || pendingInteraction != null)) {
      throw new IllegalArgumentException("A terminal branch cannot contain an active nested fork");
    }
  }

  public static ForkBranchState pending(
      String name, String path, int declarationIndex, DataReference input) {
    return new ForkBranchState(
        name,
        path,
        declarationIndex,
        ForkBranchPhase.PENDING,
        ExecutionCursor.start(input),
        input,
        null,
        null,
        null);
  }

  public ForkBranchState running() {
    return new ForkBranchState(
        name,
        path,
        declarationIndex,
        ForkBranchPhase.RUNNING,
        cursor,
        data,
        activeFork,
        null,
        null);
  }

  public ForkBranchState progressed(
      ExecutionCursor changedCursor,
      DataReference changedData,
      ForkRuntimeState changedActiveFork,
      PendingInteraction changedInteraction) {
    boolean waiting =
        changedInteraction != null
            || (changedActiveFork != null && changedActiveFork.nextRunnableIndex() < 0);
    return new ForkBranchState(
        name,
        path,
        declarationIndex,
        waiting ? ForkBranchPhase.WAITING : ForkBranchPhase.RUNNING,
        changedCursor,
        changedData,
        changedActiveFork,
        changedInteraction,
        null);
  }

  public ForkBranchState completed(long order) {
    return new ForkBranchState(
        name, path, declarationIndex, ForkBranchPhase.COMPLETED, cursor, data, null, null, order);
  }

  public ForkBranchState abandoned() {
    return new ForkBranchState(
        name, path, declarationIndex, ForkBranchPhase.ABANDONED, cursor, data, null, null, null);
  }
}
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
