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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Objects;

/**
 * Immutable, unevaluated plan for the governed Human Task extension.
 *
 * <p>The compiler owns this workflow-facing shape. Runtime materialization may evaluate its values,
 * but must not rediscover Human Task fields by interpreting the generic {@link CallPlan#arguments}
 * object.
 */
public record HumanTaskCallPlan(
    JsonNode title,
    JsonNode description,
    JsonNode input,
    JsonNode approvals,
    JsonNode dueAt,
    JsonNode dueAfter,
    JsonNode expiresAt,
    JsonNode expiresAfter,
    JsonNode presentation) {

  public HumanTaskCallPlan {
    title = requiredCopy(title, "title");
    approvals = requiredCopy(approvals, "approvals");
    description = copy(description);
    input = copy(input);
    dueAt = copy(dueAt);
    dueAfter = copy(dueAfter);
    expiresAt = copy(expiresAt);
    expiresAfter = copy(expiresAfter);
    presentation = copy(presentation);
  }

  /** Builds a plan after the compiler has validated the authoring shape. */
  static HumanTaskCallPlan fromValidated(JsonNode arguments) {
    Objects.requireNonNull(arguments, "arguments");
    return new HumanTaskCallPlan(
        arguments.required("title"),
        arguments.get("description"),
        arguments.get("input"),
        arguments.required("approvals"),
        arguments.get("dueAt"),
        arguments.get("dueAfter"),
        arguments.get("expiresAt"),
        arguments.get("expiresAfter"),
        arguments.get("presentation"));
  }

  /** Reconstructs the exact workflow-authoring object for expression materialization. */
  public ObjectNode arguments() {
    ObjectNode value = JsonNodeFactory.instance.objectNode();
    value.set("title", title());
    put(value, "description", description());
    put(value, "input", input());
    value.set("approvals", approvals());
    put(value, "dueAt", dueAt());
    put(value, "dueAfter", dueAfter());
    put(value, "expiresAt", expiresAt());
    put(value, "expiresAfter", expiresAfter());
    put(value, "presentation", presentation());
    return value;
  }

  @Override
  public JsonNode title() {
    return title.deepCopy();
  }

  @Override
  public JsonNode description() {
    return copy(description);
  }

  @Override
  public JsonNode input() {
    return copy(input);
  }

  @Override
  public JsonNode approvals() {
    return approvals.deepCopy();
  }

  @Override
  public JsonNode dueAt() {
    return copy(dueAt);
  }

  @Override
  public JsonNode dueAfter() {
    return copy(dueAfter);
  }

  @Override
  public JsonNode expiresAt() {
    return copy(expiresAt);
  }

  @Override
  public JsonNode expiresAfter() {
    return copy(expiresAfter);
  }

  @Override
  public JsonNode presentation() {
    return copy(presentation);
  }

  private static JsonNode requiredCopy(JsonNode value, String name) {
    return Objects.requireNonNull(value, name).deepCopy();
  }

  private static JsonNode copy(JsonNode value) {
    return value == null ? null : value.deepCopy();
  }

  private static void put(ObjectNode target, String name, JsonNode value) {
    if (value != null) {
      target.set(name, value);
    }
  }
}
