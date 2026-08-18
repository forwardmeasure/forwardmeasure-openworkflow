package com.forwardmeasure.openworkflow.adapter.api;

import com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceReference;
import java.util.List;

/**
 * Resolves a digest-pinned resource from an admitted immutable definition.
 *
 * <p>Implementations resolve only from the definition bundle identified by {@code
 * definitionReference}; they must never re-fetch the original endpoint.
 */
@FunctionalInterface
public interface OperationResourceResolver {
  ResolvedWorkflowResource resolve(String definitionReference, WorkflowResourceReference resource);

  /**
   * Returns the complete immutable resource graph for the definition.
   *
   * <p>The primary resource must be present in the result. Implementations backed by older
   * single-resource stores remain valid for documents with no external references.
   */
  default List<ResolvedWorkflowResource> resolveAll(
      String definitionReference, WorkflowResourceReference primary) {
    return List.of(resolve(definitionReference, primary));
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
