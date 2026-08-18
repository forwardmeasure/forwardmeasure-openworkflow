package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Objects;

/** Immutable tenant DID supplied by an authenticated ingress adapter. */
public record OksTenantId(Did value) implements Comparable<OksTenantId> {

  public OksTenantId {
    Objects.requireNonNull(value, "value");
  }

  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public static OksTenantId parse(String value) {
    return new OksTenantId(Did.parse(value));
  }

  /** MicroProfile Config implicit-converter entry point. */
  public static OksTenantId of(String value) {
    return parse(value);
  }

  @Override
  public int compareTo(OksTenantId other) {
    return value.compareTo(other.value);
  }

  @JsonValue
  @Override
  public String toString() {
    return value.toString();
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
