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

import java.util.Objects;
import java.util.UUID;

/** Tenant-qualified product identity for one workflow execution. */
public record ExecutionId(TenantId tenantId, UUID value) {
  public ExecutionId {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(value, "value");
  }

  public String canonicalValue() {
    return tenantId + ":" + value;
  }

  /** Stable Pekko sharding identity; retained as an alias of the canonical product identity. */
  public String entityId() {
    return canonicalValue();
  }

  public static ExecutionId parse(String value) {
    ContractSupport.requireText(value, "value");
    int separator = value.indexOf(':');
    if (separator < 1 || separator == value.length() - 1) {
      throw new IllegalArgumentException(
          "execution identity must be <tenant UUID>:<execution UUID>");
    }
    return new ExecutionId(
        TenantId.parse(value.substring(0, separator)),
        UUID.fromString(value.substring(separator + 1)));
  }

  public static ExecutionId fromEntityId(String value) {
    return parse(value);
  }
}
