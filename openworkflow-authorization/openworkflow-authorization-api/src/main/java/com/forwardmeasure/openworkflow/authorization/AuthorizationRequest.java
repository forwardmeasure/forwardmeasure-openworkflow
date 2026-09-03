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

public record AuthorizationRequest(
    ActiveOrganization organization,
    AuthorizationResource resource,
    AuthorizationAction action,
    String correlationId,
    Map<String, Object> context) {
  public AuthorizationRequest {
    Objects.requireNonNull(organization, "organization");
    Objects.requireNonNull(resource, "resource");
    Objects.requireNonNull(action, "action");
    if (correlationId == null || correlationId.isBlank()) {
      throw new IllegalArgumentException("correlationId must not be blank");
    }
    context = Map.copyOf(Objects.requireNonNull(context, "context"));
  }

  /** Resolves the AuthZEN wire action, including a reviewed Human Task disposition code. */
  public String resolvedActionScope() {
    if (action != AuthorizationAction.HUMAN_TASK_DECIDE) {
      return action.scope();
    }
    Object value = context.get("action_code");
    if (!(value instanceof String actionCode)
        || !actionCode.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,199}")) {
      throw new IllegalArgumentException(
          "Human Task decision authorization requires a valid action_code context value");
    }
    return action.scope() + ":" + actionCode;
  }
}
