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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/** Validated internal tenant identity; it is not a database schema name. */
public record TenantId(UUID value) {
  public TenantId {
    Objects.requireNonNull(value, "value");
  }

  /** Reads both canonical UUID identities and legacy durable DID identities. */
  public TenantId(String value) {
    this(parseCompatible(value));
  }

  /** Reads the canonical scalar form and the legacy {@code {"value": ...}} durable form. */
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public static TenantId fromJson(JsonNode value) {
    Objects.requireNonNull(value, "value");
    JsonNode identity = value.isObject() ? value.get("value") : value;
    if (identity == null || !identity.isTextual()) {
      throw new IllegalArgumentException("tenant identity must be a UUID string");
    }
    return new TenantId(identity.textValue());
  }

  private static UUID parseCompatible(String value) {
    String checked = ContractSupport.requireText(value, "value");
    try {
      return UUID.fromString(checked);
    } catch (IllegalArgumentException ignored) {
      return UUID.nameUUIDFromBytes(checked.getBytes(StandardCharsets.UTF_8));
    }
  }

  public static TenantId parse(String value) {
    return new TenantId(UUID.fromString(ContractSupport.requireText(value, "value")));
  }

  @Override
  @JsonValue
  public String toString() {
    return value.toString();
  }
}
