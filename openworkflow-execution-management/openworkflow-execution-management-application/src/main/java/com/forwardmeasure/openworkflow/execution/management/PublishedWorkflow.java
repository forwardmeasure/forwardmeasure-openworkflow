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
package com.forwardmeasure.openworkflow.execution.management;

import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import com.forwardmeasure.openworkflow.engine.api.DefinitionRevision;
import java.util.Objects;

/** Authorized, immutable publication resolved by the definition plane. */
public record PublishedWorkflow(
    DefinitionRevision revision, WorkflowPlan plan, String sourceDocument) {
  public PublishedWorkflow {
    Objects.requireNonNull(revision, "revision");
    Objects.requireNonNull(plan, "plan");
    if (sourceDocument != null && sourceDocument.isBlank()) {
      throw new IllegalArgumentException("sourceDocument must not be blank when present");
    }
    if (!revision.coordinates().equals(plan.coordinates())
        || !revision.definitionSha256().equals(plan.definitionSha256())
        || !revision.compilerSha256().equals(plan.compilerSha256())) {
      throw new IllegalArgumentException("published revision does not identify the supplied plan");
    }
  }

  public PublishedWorkflow(DefinitionRevision revision, WorkflowPlan plan) {
    this(revision, plan, null);
  }
}
