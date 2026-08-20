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
package com.forwardmeasure.openworkflow.actor;

import java.util.List;
import java.util.Objects;

/** Durable round-robin lane set for one active Open Workflow fork task. */
public record ForkExecutionFrame(
    boolean compete, List<ForkBranchState> branches, int nextBranch, Integer winner) {
  public ForkExecutionFrame {
    branches = List.copyOf(Objects.requireNonNull(branches, "branches"));
    if (branches.isEmpty()) throw new IllegalArgumentException("A fork needs branches");
    if (nextBranch < 0 || nextBranch >= branches.size()) {
      throw new IllegalArgumentException("nextBranch is outside the fork");
    }
    if (winner != null
        && (!compete
            || winner < 0
            || winner >= branches.size()
            || !branches.get(winner).completed())) {
      throw new IllegalArgumentException("Invalid competing fork winner");
    }
  }

  public boolean complete() {
    return winner != null || branches.stream().allMatch(ForkBranchState::completed);
  }
}
