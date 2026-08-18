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

import java.util.Objects;

/** Exact immutable child workflow selected while admitting its parent. */
public record ResolvedSubflow(
    WorkflowCoordinates coordinates, String sourceSha256, String definitionSha256) {

  public ResolvedSubflow {
    Objects.requireNonNull(coordinates, "coordinates");
    requireSha256(sourceSha256, "sourceSha256");
    requireSha256(definitionSha256, "definitionSha256");
  }

  public String canonical() {
    return component(coordinates.namespace())
        + component(coordinates.name())
        + component(coordinates.version())
        + component(coordinates.dsl())
        + sourceSha256
        + definitionSha256;
  }

  private static String component(String value) {
    return value.length() + ":" + value;
  }

  private static void requireSha256(String value, String name) {
    Objects.requireNonNull(value, name);
    if (!value.matches("[0-9a-f]{64}")) {
      throw new IllegalArgumentException(name + " must be lowercase SHA-256");
    }
  }
}
