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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** Durable, transport-neutral CloudEvents 1.0 envelope. */
public record WorkflowCloudEvent(
    String specVersion,
    String id,
    URI source,
    String type,
    String subject,
    Instant time,
    String dataContentType,
    JsonNode data,
    Map<String, JsonNode> extensions) {
  public WorkflowCloudEvent {
    specVersion = specVersion == null ? "1.0" : specVersion;
    if (!"1.0".equals(specVersion)) {
      throw new IllegalArgumentException("Only CloudEvents 1.0 is supported");
    }
    requireText(id, "id");
    Objects.requireNonNull(source, "source");
    requireText(type, "type");
    data = data == null ? NullNode.getInstance() : data.deepCopy();
    extensions =
        extensions == null
            ? Map.of()
            : extensions.entrySet().stream()
                .collect(
                    java.util.stream.Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        entry -> Objects.requireNonNull(entry.getValue()).deepCopy()));
  }

  private static void requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
  }

  /** CloudEvents dataschema, retained in the durable attribute map for v1 wire compatibility. */
  public URI dataSchema() {
    JsonNode value = extensions.get("dataschema");
    return value == null || value.isNull() ? null : URI.create(value.textValue());
  }
}
