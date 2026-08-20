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
package com.forwardmeasure.openworkflow.definition.management;

import java.io.Serial;
import java.util.Objects;

/**
 * The complete set of failures this capability's business logic decides on its own. Deliberately
 * does not cover 400 (bean validation on the generated request DTOs already handles malformed
 * input), 401/403 ({@link
 * com.forwardmeasure.openworkflow.authorization.AuthorizationDeniedException} already covers
 * authorization denial), or 500 (the uncaught-exception fallback). Carries a status classification,
 * not an HTTP type, so this class has no {@code jakarta.ws.rs} dependency; {@code -jaxrs} maps
 * {@link Status} to an actual response.
 */
public final class DefinitionManagementException extends RuntimeException {
  @Serial private static final long serialVersionUID = 1L;

  public enum Status {
    NOT_FOUND,
    CONFLICT,
    PRECONDITION_FAILED,
    UNPROCESSABLE_ENTITY
  }

  private final Status status;

  private DefinitionManagementException(Status status, String message) {
    super(message);
    this.status = Objects.requireNonNull(status, "status");
  }

  public static DefinitionManagementException notFound(String message) {
    return new DefinitionManagementException(Status.NOT_FOUND, message);
  }

  public static DefinitionManagementException conflict(String message) {
    return new DefinitionManagementException(Status.CONFLICT, message);
  }

  public static DefinitionManagementException preconditionFailed(String message) {
    return new DefinitionManagementException(Status.PRECONDITION_FAILED, message);
  }

  public static DefinitionManagementException unprocessableEntity(String message) {
    return new DefinitionManagementException(Status.UNPROCESSABLE_ENTITY, message);
  }

  public Status status() {
    return status;
  }
}
