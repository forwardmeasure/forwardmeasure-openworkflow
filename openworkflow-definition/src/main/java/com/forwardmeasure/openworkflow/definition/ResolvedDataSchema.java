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

/** Immutable JSON Schema attached to one precise workflow data-flow location. */
public record ResolvedDataSchema(
    String definitionPath, String format, URI resourceUri, String sha256, JsonNode document) {

  public ResolvedDataSchema {
    definitionPath = requireText(definitionPath, "definitionPath");
    format = requireText(format, "format");
    Objects.requireNonNull(sha256, "sha256");
    Objects.requireNonNull(document, "document");
    if (!sha256.matches("[0-9a-f]{64}")) {
      throw new IllegalArgumentException("sha256 must be lowercase SHA-256");
    }
    if (!document.isObject() && !document.isBoolean()) {
      throw new IllegalArgumentException("A JSON Schema must be an object or boolean");
    }
    if (resourceUri != null && !resourceUri.isAbsolute()) {
      throw new IllegalArgumentException("resourceUri must be absolute");
    }
    document = document.deepCopy();
  }

  public boolean external() {
    return resourceUri != null;
  }

  @Override
  public JsonNode document() {
    return document.deepCopy();
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
