package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference;
import java.util.Objects;

/** Signals that deterministic execution reached an off-thread data cutpoint. */
public final class WorkflowDataMaterializationRequiredException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private final transient DataReference reference;

  public WorkflowDataMaterializationRequiredException(DataReference reference) {
    super("Artifact-backed runtime data requires off-thread materialization");
    this.reference = Objects.requireNonNull(reference, "reference");
  }

  public DataReference reference() {
    return reference;
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
