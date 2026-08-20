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
package com.forwardmeasure.openworkflow.engine.api;

import com.forwardmeasure.openworkflow.definition.WorkflowCoordinates;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable, published definition revision selected by execution admission.
 *
 * <p>{@code workflowId} is nullable: it identifies the owning {@code Workflow} for API responses
 * that need it (e.g. the execution contract's {@code workflowId} field), but isn't always available
 * where a {@link DefinitionRevision} is constructed (tests, engine-internal contexts with no
 * definition-plane lookup).
 */
public record DefinitionRevision(
    UUID revisionId,
    UUID workflowId,
    WorkflowCoordinates coordinates,
    String definitionSha256,
    String compilerSha256) {

  public DefinitionRevision {
    Objects.requireNonNull(revisionId, "revisionId");
    Objects.requireNonNull(coordinates, "coordinates");
    ContractSupport.requireSha256(definitionSha256, "definitionSha256");
    ContractSupport.requireSha256(compilerSha256, "compilerSha256");
  }

  public static DefinitionRevision from(UUID revisionId, WorkflowPlan plan) {
    return from(revisionId, null, plan);
  }

  public static DefinitionRevision from(UUID revisionId, UUID workflowId, WorkflowPlan plan) {
    Objects.requireNonNull(plan, "plan");
    return new DefinitionRevision(
        revisionId, workflowId, plan.coordinates(), plan.definitionSha256(), plan.compilerSha256());
  }
}
