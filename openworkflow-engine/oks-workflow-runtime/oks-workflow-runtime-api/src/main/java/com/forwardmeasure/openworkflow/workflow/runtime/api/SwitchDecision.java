package com.forwardmeasure.openworkflow.workflow.runtime.api;

import java.util.List;
import java.util.Objects;

/** Durable explanation of one switch task's selected control-flow edge. */
public record SwitchDecision(
    List<SwitchCaseEvaluation> cases, String selectedCase, String flowDirective) {

  public SwitchDecision {
    cases = List.copyOf(Objects.requireNonNull(cases, "cases"));
    Objects.requireNonNull(flowDirective, "flowDirective");
    if (cases.isEmpty()) {
      throw new IllegalArgumentException("A switch decision requires cases");
    }
    if (flowDirective.isBlank()) {
      throw new IllegalArgumentException("flowDirective must not be blank");
    }
    if (selectedCase != null
        && cases.stream()
            .noneMatch(
                value ->
                    value.name().equals(selectedCase) && Boolean.TRUE.equals(value.result()))) {
      throw new IllegalArgumentException("selectedCase must identify a matched case");
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
