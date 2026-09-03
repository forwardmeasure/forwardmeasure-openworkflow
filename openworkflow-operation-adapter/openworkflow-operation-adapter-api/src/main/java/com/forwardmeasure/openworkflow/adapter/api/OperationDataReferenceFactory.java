package com.forwardmeasure.openworkflow.adapter.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.openworkflow.data.DataReference;
import com.forwardmeasure.openworkflow.data.DataReferenceJson;
import com.forwardmeasure.openworkflow.data.DataReferences;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import java.util.Objects;

/**
 * Host-supplied policy for capturing protocol output as workflow data.
 *
 * <p>Production hosts use a durable data store for oversized values. The bounded inline
 * implementation is retained for standalone adapter use and fails closed when no durable store was
 * configured.
 */
public interface OperationDataReferenceFactory {

  DataReference capture(OperationRequest request, JsonNode value);

  default DataReference captureProgress(OperationRequest request, JsonNode value) {
    return capture(request, value);
  }

  default DataReference captureEvent(OperationRequest request, JsonNode value) {
    return capture(request, value);
  }

  JsonNode resolve(OperationRequest request, DataReference reference);

  /**
   * Captures the initial input of a child workflow under the child's own execution identity.
   * Production stores therefore retain correct purge, tenancy and provenance boundaries for nested
   * runs.
   */
  default DataReference captureWorkflowInput(
      OperationRequest request, ExecutionKey childExecution, JsonNode value) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(childExecution, "childExecution");
    if (!request.requestedBy().tenantId().equals(childExecution.tenantId())) {
      throw new SecurityException("Child workflow input cannot cross tenants");
    }
    return DataReferences.inline(value);
  }

  /**
   * Resolves a descriptor field that is either directly inline or encoded as a sibling {@code
   * <name>Reference}. This keeps artifact bytes away from Kafka while allowing the blocking adapter
   * edge to consume them.
   */
  default JsonNode resolveDescriptorValue(OperationRequest request, String name) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(name, "name");
    JsonNode inline = request.descriptor().get(name);
    if (inline != null) {
      return inline.deepCopy();
    }
    JsonNode encoded = request.descriptor().required(name + "Reference");
    return resolve(request, DataReferenceJson.decode(encoded));
  }

  static OperationDataReferenceFactory boundedInline() {
    return new OperationDataReferenceFactory() {
      @Override
      public DataReference capture(OperationRequest request, JsonNode value) {
        Objects.requireNonNull(request, "request");
        return DataReferences.inline(value);
      }

      @Override
      public JsonNode resolve(OperationRequest request, DataReference reference) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(reference, "reference");
        if (reference.storage() != DataReference.Storage.INLINE) {
          throw new IllegalStateException("No durable workflow-data resolver is configured");
        }
        return reference.inlineValue().deepCopy();
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
