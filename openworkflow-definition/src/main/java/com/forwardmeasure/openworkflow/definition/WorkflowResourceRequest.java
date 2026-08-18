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

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.util.Objects;

/**
 * Publication-edge request for an Open Workflow external resource.
 *
 * <p>The endpoint configuration is retained so an authorised edge can apply authentication without
 * exposing credentials to an execution engine.
 */
public record WorkflowResourceRequest(
    URI uri, String name, JsonNode endpoint, WorkflowResourceKind kind) {

  public WorkflowResourceRequest {
    Objects.requireNonNull(uri, "uri");
    Objects.requireNonNull(kind, "kind");
    uri = uri.normalize();
    if (!uri.isAbsolute() || uri.getFragment() != null) {
      throw new IllegalArgumentException(
          "Workflow resource URI must be absolute and fragment-free");
    }
    name = name == null || name.isBlank() ? null : name;
    endpoint = endpoint == null ? null : endpoint.deepCopy();
  }

  @Override
  public JsonNode endpoint() {
    return endpoint == null ? null : endpoint.deepCopy();
  }
}
