package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Durable scheduler and join state for one active Open Workflow fork. */
public record ForkRuntimeState(
    String taskPath,
    DataReference rawInput,
    DataReference input,
    ExecutionCursor parentCursor,
    boolean compete,
    List<ForkBranchState> branches,
    int nextBranchIndex,
    long nextCompletionOrder) {

  public ForkRuntimeState {
    Objects.requireNonNull(taskPath, "taskPath");
    Objects.requireNonNull(rawInput, "rawInput");
    Objects.requireNonNull(input, "input");
    Objects.requireNonNull(parentCursor, "parentCursor");
    branches = List.copyOf(Objects.requireNonNull(branches, "branches"));
    if (taskPath.isBlank() || branches.isEmpty()) {
      throw new IllegalArgumentException("A fork requires a path and at least one branch");
    }
    if (nextBranchIndex < 0 || nextBranchIndex >= branches.size()) {
      throw new IllegalArgumentException("nextBranchIndex must identify a branch");
    }
    if (nextCompletionOrder < 0) {
      throw new IllegalArgumentException("nextCompletionOrder must not be negative");
    }
  }

  public int nextRunnableIndex() {
    for (int offset = 0; offset < branches.size(); offset++) {
      int index = (nextBranchIndex + offset) % branches.size();
      ForkBranchPhase phase = branches.get(index).phase();
      if (phase == ForkBranchPhase.PENDING || phase == ForkBranchPhase.RUNNING) {
        return index;
      }
    }
    return -1;
  }

  public ForkRuntimeState replace(
      int index, ForkBranchState branch, int nextIndex, long completionOrder) {
    var changed = new ArrayList<>(branches);
    changed.set(index, Objects.requireNonNull(branch, "branch"));
    return new ForkRuntimeState(
        taskPath, rawInput, input, parentCursor, compete, changed, nextIndex, completionOrder);
  }

  public ForkRuntimeState replaceBranches(List<ForkBranchState> changedBranches) {
    return new ForkRuntimeState(
        taskPath,
        rawInput,
        input,
        parentCursor,
        compete,
        changedBranches,
        nextBranchIndex,
        nextCompletionOrder);
  }

  public boolean allCompleted() {
    return branches.stream().allMatch(branch -> branch.phase() == ForkBranchPhase.COMPLETED);
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
