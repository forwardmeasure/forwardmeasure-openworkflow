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
import java.util.Objects;

/**
 * Immutable compiled representation of the specification's duration union.
 *
 * <p>Expressions remain definition-owned and are evaluated only by the deterministic runtime
 * reducer. Literal and inline forms require no runtime schema inspection.
 */
public record DurationPlan(Kind kind, JsonNode value) {
  public DurationPlan {
    Objects.requireNonNull(kind, "kind");
    Objects.requireNonNull(value, "value");
    value = value.deepCopy();
    if (kind == Kind.INLINE && !value.isObject()) {
      throw new IllegalArgumentException("An inline duration must be an object");
    }
    if (kind != Kind.INLINE && !value.isTextual()) {
      throw new IllegalArgumentException("A literal or expression duration must be text");
    }
  }

  @Override
  public JsonNode value() {
    return value.deepCopy();
  }

  public enum Kind {
    INLINE,
    LITERAL,
    EXPRESSION
  }
}
