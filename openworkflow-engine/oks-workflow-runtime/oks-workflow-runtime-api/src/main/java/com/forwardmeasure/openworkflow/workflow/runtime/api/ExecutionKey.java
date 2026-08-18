package com.forwardmeasure.openworkflow.workflow.runtime.api;

import java.util.Objects;

/**
 * Canonical Kafka aggregate key. Length-prefixing makes the encoding reversible without reserving a
 * delimiter in either identifier.
 */
public record ExecutionKey(OksTenantId tenantId, WorkflowExecutionId executionId)
    implements Comparable<ExecutionKey> {

  public ExecutionKey {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(executionId, "executionId");
  }

  public String canonical() {
    String tenant = tenantId.toString();
    return tenant.length() + ":" + tenant + executionId.value();
  }

  public static ExecutionKey parse(String value) {
    Objects.requireNonNull(value, "value");
    int separator = value.indexOf(':');
    if (separator < 1) {
      throw new IllegalArgumentException("Invalid execution key");
    }
    final int tenantLength;
    try {
      tenantLength = Integer.parseInt(value.substring(0, separator));
    } catch (NumberFormatException invalid) {
      throw new IllegalArgumentException("Invalid execution key", invalid);
    }
    int tenantStart = separator + 1;
    int tenantEnd = tenantStart + tenantLength;
    if (tenantLength < 1 || tenantEnd >= value.length()) {
      throw new IllegalArgumentException("Invalid execution key");
    }
    return new ExecutionKey(
        OksTenantId.parse(value.substring(tenantStart, tenantEnd)),
        new WorkflowExecutionId(value.substring(tenantEnd)));
  }

  @Override
  public int compareTo(ExecutionKey other) {
    int tenant = tenantId.compareTo(other.tenantId);
    return tenant == 0 ? executionId.compareTo(other.executionId) : tenant;
  }

  @Override
  public String toString() {
    return canonical();
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
