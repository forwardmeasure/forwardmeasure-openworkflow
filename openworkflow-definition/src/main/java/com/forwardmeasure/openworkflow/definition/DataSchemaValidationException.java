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
import java.util.List;
import java.util.Objects;

/** A runtime value failed a schema pinned into the immutable workflow plan. */
@SuppressWarnings("serial")
public final class DataSchemaValidationException extends RuntimeException {
  private final ResolvedDataSchema schema;
  private final JsonNode rejectedValue;
  private final List<SchemaViolation> violations;

  public DataSchemaValidationException(
      ResolvedDataSchema schema, JsonNode rejectedValue, List<SchemaViolation> violations) {
    super(message(schema, violations));
    this.schema = Objects.requireNonNull(schema, "schema");
    this.rejectedValue = Objects.requireNonNull(rejectedValue, "rejectedValue").deepCopy();
    this.violations = List.copyOf(Objects.requireNonNull(violations, "violations"));
    if (this.violations.isEmpty()) {
      throw new IllegalArgumentException("A validation exception requires at least one violation");
    }
  }

  public ResolvedDataSchema schema() {
    return schema;
  }

  public List<SchemaViolation> violations() {
    return violations;
  }

  public JsonNode rejectedValue() {
    return rejectedValue.deepCopy();
  }

  private static String message(ResolvedDataSchema schema, List<SchemaViolation> violations) {
    Objects.requireNonNull(schema, "schema");
    Objects.requireNonNull(violations, "violations");
    return "Data does not satisfy "
        + schema.definitionPath()
        + ": "
        + (violations.isEmpty() ? "unknown validation failure" : violations.getFirst().message());
  }
}
