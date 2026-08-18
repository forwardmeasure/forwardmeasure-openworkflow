package com.forwardmeasure.openworkflow.workflow.runtime.api;

import java.util.Objects;

/**
 * Auditable result for one ordered switch case.
 *
 * <p>A null result means the case was not evaluated because an earlier condition matched. A default
 * case has no condition and is marked true only when selected.
 */
public record SwitchCaseEvaluation(String name, String condition, Boolean result) {

  public SwitchCaseEvaluation {
    Objects.requireNonNull(name, "name");
    if (name.isBlank()) {
      throw new IllegalArgumentException("name must not be blank");
    }
    if (condition == null && result == null) {
      throw new IllegalArgumentException("A default case must record whether it was selected");
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
