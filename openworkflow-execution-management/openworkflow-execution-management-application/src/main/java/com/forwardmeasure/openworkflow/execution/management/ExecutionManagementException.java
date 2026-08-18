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
package com.forwardmeasure.openworkflow.execution.management;

/** Stable application failure mapped by every framework binding. */
public final class ExecutionManagementException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private final Kind kind;

  public ExecutionManagementException(Kind kind, String message) {
    super(message);
    this.kind = java.util.Objects.requireNonNull(kind, "kind");
  }

  public Kind kind() {
    return kind;
  }

  public enum Kind {
    NOT_FOUND,
    NOT_PUBLISHED,
    FORBIDDEN,
    STALE_VERSION,
    ENGINE_UNAVAILABLE
  }
}
