package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.openworkflow.data.DataReference;
import com.forwardmeasure.openworkflow.data.DataReferences;
import java.util.Objects;

/**
 * Data boundary used by the deterministic Open Workflow reducer.
 *
 * <p>The controller uses {@link #inlineOnly()}, which performs no I/O and requests an off-thread
 * computation whenever an artifact must be read. A computation worker may provide a blocking
 * implementation because it does not run on a Kafka Streams processing thread.
 */
public interface WorkflowRuntimeDataAccess {

  JsonNode resolve(DataReference reference);

  DataReference reference(JsonNode value);

  /**
   * Creates the bounded, inline control envelope consumed by Kafka routing processors and operation
   * adapters. User workflow values belong behind nested {@link DataReference}s created by {@link
   * #reference(JsonNode)}; routing metadata itself must remain available without blocking a Kafka
   * Streams thread on external storage.
   */
  default DataReference controlReference(JsonNode value) {
    return DataReferences.inline(value);
  }

  static WorkflowRuntimeDataAccess inlineOnly() {
    return new WorkflowRuntimeDataAccess() {
      @Override
      public JsonNode resolve(DataReference reference) {
        Objects.requireNonNull(reference, "reference");
        if (reference.storage() != DataReference.Storage.INLINE) {
          throw new WorkflowDataMaterializationRequiredException(reference);
        }
        return reference.inlineValue().deepCopy();
      }

      @Override
      public DataReference reference(JsonNode value) {
        return DataReferences.inline(value);
      }
    };
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
