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
package com.forwardmeasure.openworkflow.definition;

import java.util.List;
import java.util.Objects;

/** One ordered reusable extension applied to a concrete task. */
public record ExtensionApplicationPlan(
    String name, String target, String condition, List<PlanStep> before, List<PlanStep> after) {

  public ExtensionApplicationPlan {
    name = requireText(name, "name");
    target = requireText(target, "target");
    condition = condition == null || condition.isBlank() ? null : condition;
    before = before == null ? List.of() : List.copyOf(before);
    after = after == null ? List.of() : List.copyOf(after);
  }

  private static String requireText(String value, String field) {
    Objects.requireNonNull(value, field);
    if (value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
    return value;
  }
}
