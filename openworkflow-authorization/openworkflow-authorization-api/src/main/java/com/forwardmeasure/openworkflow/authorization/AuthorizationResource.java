/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.forwardmeasure.openworkflow.authorization;

import java.util.Map;
import java.util.Objects;

public record AuthorizationResource(String type, String id, Map<String, Object> properties) {
  public AuthorizationResource {
    type = requireText(type, "type");
    id = requireText(id, "id");
    properties = Map.copyOf(Objects.requireNonNull(properties, "properties"));
  }

  public static AuthorizationResource definition(String id) {
    return new AuthorizationResource(
        "openworkflow-definition", "definitions", Map.of("definition_key", requireText(id, "id")));
  }

  public static AuthorizationResource execution(String id) {
    return new AuthorizationResource(
        "openworkflow-execution", "executions", Map.of("execution_id", requireText(id, "id")));
  }

  public static AuthorizationResource operation(
      String executionId, String operationId, String operationKind) {
    return new AuthorizationResource(
        "openworkflow-operation",
        "operations",
        Map.of(
            "execution_id", requireText(executionId, "executionId"),
            "operation_id", requireText(operationId, "operationId"),
            "operation_kind", requireText(operationKind, "operationKind")));
  }

  public static AuthorizationResource audit(String id) {
    return new AuthorizationResource(
        "openworkflow-audit", "audit", Map.of("query_id", requireText(id, "id")));
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
