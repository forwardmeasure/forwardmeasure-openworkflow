package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Objects;

/**
 * Transport-neutral business correlation identity.
 *
 * <p>This is neither a W3C trace identifier nor an Open Workflow event correlation expression.
 * HTTP, Kafka, A2A and other ingress adapters may map their transport metadata into this value, but
 * the durable runtime does not depend on that transport.
 */
public record BusinessCorrelationId(@JsonValue String value) {

  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public BusinessCorrelationId {
    Objects.requireNonNull(value, "value");
    if (value.isBlank()
        || value.length() > 256
        || value.chars().anyMatch(Character::isISOControl)) {
      throw new IllegalArgumentException(
          "Business correlation identity must be printable, "
              + "non-blank text no longer than 256 characters");
    }
  }

  public static BusinessCorrelationId parse(String value) {
    return new BusinessCorrelationId(value);
  }

  public static BusinessCorrelationId parseNullable(String value) {
    return value == null ? null : parse(value);
  }

  @Override
  public String toString() {
    return value;
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
