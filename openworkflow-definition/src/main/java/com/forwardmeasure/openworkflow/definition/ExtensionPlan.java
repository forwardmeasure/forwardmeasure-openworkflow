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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Durable middleware plan around one normative task.
 *
 * <p>Conditions are evaluated once when the wrapper is entered. The selected before tasks, original
 * target and selected after tasks then form the wrapper's durable sequential scope. Consequently, a
 * normative {@code exit} from a before task skips the target and all remaining middleware.
 */
public record ExtensionPlan(PlanStep target, List<ExtensionApplicationPlan> applications) {

  public ExtensionPlan {
    Objects.requireNonNull(target, "target");
    applications = List.copyOf(Objects.requireNonNull(applications, "applications"));
    if (applications.isEmpty()) {
      throw new IllegalArgumentException("An extension wrapper requires at least one application");
    }
  }

  public List<PlanStep> selectedChildren(List<Boolean> applies) {
    Objects.requireNonNull(applies, "applies");
    if (applies.size() != applications.size()) {
      throw new IllegalArgumentException("Extension decisions do not match the compiled plan");
    }
    List<PlanStep> selected = new ArrayList<>();
    for (int index = 0; index < applications.size(); index++) {
      if (applies.get(index)) {
        selected.addAll(applications.get(index).before());
      }
    }
    selected.add(target);
    for (int index = 0; index < applications.size(); index++) {
      if (applies.get(index)) {
        selected.addAll(applications.get(index).after());
      }
    }
    return List.copyOf(selected);
  }

  public List<PlanStep> allChildren() {
    List<PlanStep> result = new ArrayList<>();
    applications.forEach(application -> result.addAll(application.before()));
    result.add(target);
    applications.forEach(application -> result.addAll(application.after()));
    return List.copyOf(result);
  }
}
