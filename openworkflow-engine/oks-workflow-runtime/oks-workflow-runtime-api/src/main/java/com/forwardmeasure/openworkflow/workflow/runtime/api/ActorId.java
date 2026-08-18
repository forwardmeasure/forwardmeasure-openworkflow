package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Objects;

/** Stable actor DID supplied by the integrating identity and persistence layer. */
public record ActorId(Did value) implements Comparable<ActorId> {

  public ActorId {
    Objects.requireNonNull(value, "value");
  }

  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public static ActorId parse(String value) {
    return new ActorId(Did.parse(value));
  }

  @Override
  public int compareTo(ActorId other) {
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
