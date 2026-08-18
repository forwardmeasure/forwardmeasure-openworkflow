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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Objects;

/** Canonical JSON representation of the immutable publication resource bundle. */
public final class WorkflowResourceBundleCodec {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final TypeReference<List<ResolvedWorkflowResource>> TYPE =
      new TypeReference<>() {};

  private WorkflowResourceBundleCodec() {}

  public static String encode(List<ResolvedWorkflowResource> resources) {
    try {
      return JSON.writeValueAsString(List.copyOf(Objects.requireNonNull(resources, "resources")));
    } catch (JsonProcessingException failure) {
      throw new IllegalArgumentException("Workflow resource bundle cannot be encoded", failure);
    }
  }

  public static List<ResolvedWorkflowResource> decode(String bundle) {
    try {
      return List.copyOf(JSON.readValue(Objects.requireNonNull(bundle, "bundle"), TYPE));
    } catch (JsonProcessingException failure) {
      throw new IllegalArgumentException("Stored workflow resource bundle is invalid", failure);
    }
  }
}
