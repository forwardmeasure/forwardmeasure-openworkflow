package com.forwardmeasure.openworkflow.adapter.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Ephemeral, edge-resolved secret material attached to an operation request.
 *
 * <p>This object is deliberately excluded from serialization. It owns copies of the provider
 * values, returns defensive copies, redacts values before a durable boundary and overwrites its
 * character arrays when closed.
 */
public final class ResolvedSecret implements AutoCloseable {
  private final String name;
  private final Map<String, char[]> values;
  private final AtomicBoolean closed = new AtomicBoolean();

  public ResolvedSecret(String name, Map<String, char[]> values) {
    if (name == null || name.isBlank()) {
      throw new IllegalArgumentException("Resolved secret name must not be blank");
    }
    this.name = name;
    Objects.requireNonNull(values, "values");
    Map<String, char[]> owned = new LinkedHashMap<>();
    values.forEach(
        (key, value) -> {
          if (key == null || key.isBlank() || value == null) {
            throw new IllegalArgumentException("Resolved secret values require names and values");
          }
          owned.put(key, value.clone());
        });
    this.values = owned;
  }

  public String name() {
    return name;
  }

  @JsonIgnore
  public Map<String, char[]> copyValues() {
    ensureOpen();
    Map<String, char[]> copy = new LinkedHashMap<>();
    values.forEach((key, value) -> copy.put(key, value.clone()));
    return copy;
  }

  @JsonIgnore
  public String redact(String value) {
    if (value == null) return null;
    ensureOpen();
    String redacted = value;
    for (String secret : sensitiveValues()) {
      redacted = redacted.replace(secret, "<redacted>");
    }
    return redacted;
  }

  @JsonIgnore
  public JsonNode redact(JsonNode value) {
    if (value == null) return null;
    ensureOpen();
    if (value.isTextual()) {
      return JsonNodeFactory.instance.textNode(redact(value.textValue()));
    }
    if (value.isArray()) {
      ArrayNode result = JsonNodeFactory.instance.arrayNode();
      value.forEach(child -> result.add(redact(child)));
      return result;
    }
    if (value.isObject()) {
      ObjectNode result = JsonNodeFactory.instance.objectNode();
      value.properties().forEach(entry -> result.set(entry.getKey(), redact(entry.getValue())));
      return result;
    }
    return value.deepCopy();
  }

  public boolean closed() {
    return closed.get();
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) return;
    values.values().forEach(value -> Arrays.fill(value, '\0'));
    values.clear();
  }

  private void ensureOpen() {
    if (closed.get()) {
      throw new IllegalStateException("Resolved secret material is already closed");
    }
  }

  private java.util.List<String> sensitiveValues() {
    ArrayList<String> result = new ArrayList<>();
    values
        .values()
        .forEach(
            value -> {
              String clear = new String(value);
              if (!clear.isEmpty()) result.add(clear);
            });
    result.sort(Comparator.comparingInt(String::length).reversed());
    return java.util.List.copyOf(result);
  }

  @Override
  public String toString() {
    return "ResolvedSecret[name=" + name + ", values=<redacted>, closed=" + closed + "]";
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
