package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionFailure;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionPurgePolicyDecision;
import java.util.Objects;

/** Durable cutpoint while execution-owned data is being purged off-thread. */
public record ActiveExecutionPurgeState(
    String purgeId,
    ExecutionPhase terminalPhase,
    ExecutionFailure terminalFailure,
    ExecutionPurgePolicyDecision policyDecision,
    DataReference descriptor)
    implements PendingInteraction {

  public static final String TASK_PATH = "$purge";

  public ActiveExecutionPurgeState {
    requireText(purgeId, "purgeId");
    Objects.requireNonNull(terminalPhase, "terminalPhase");
    Objects.requireNonNull(policyDecision, "policyDecision");
    Objects.requireNonNull(descriptor, "descriptor");
    if (!terminalPhase.terminal()) {
      throw new IllegalArgumentException("A purge can preserve only a terminal execution phase");
    }
    if ((terminalPhase == ExecutionPhase.FAILED) != (terminalFailure != null)) {
      throw new IllegalArgumentException("Only a failed execution purge preserves a failure");
    }
  }

  @Override
  public String interactionId() {
    return purgeId;
  }

  @Override
  public String taskPath() {
    return TASK_PATH;
  }

  private static void requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
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
