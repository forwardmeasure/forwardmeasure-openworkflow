package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference;
import java.util.List;
import java.util.Objects;

/**
 * Durable state for one active {@code for} iteration.
 *
 * <p>The collection is materialised exactly once when the task starts. The iteration input captures
 * the flowing data presented to this iteration.
 */
public record ForIterationState(
    DataReference collection, int index, DataReference input, List<DataReference> outputs) {

  public ForIterationState {
    Objects.requireNonNull(collection, "collection");
    Objects.requireNonNull(input, "input");
    outputs = outputs == null ? List.of() : List.copyOf(outputs);
    if (index < 0) {
      throw new IllegalArgumentException("index must not be negative");
    }
  }

  public ForIterationState(DataReference collection, int index, DataReference input) {
    this(collection, index, input, List.of());
  }

  public ForIterationState next(DataReference nextInput) {
    return next(nextInput, null);
  }

  public ForIterationState next(DataReference nextInput, DataReference completedOutput) {
    var changed = new java.util.ArrayList<>(outputs);
    if (completedOutput != null) changed.add(completedOutput);
    return new ForIterationState(collection, Math.addExact(index, 1), nextInput, changed);
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
