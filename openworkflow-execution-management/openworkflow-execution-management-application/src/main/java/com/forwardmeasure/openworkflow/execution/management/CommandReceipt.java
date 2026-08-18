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

import com.forwardmeasure.openworkflow.engine.api.ActorId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Durable accepted or rejected command audit. */
public record CommandReceipt(
    UUID commandId,
    ExecutionId executionId,
    String commandType,
    long expectedVersion,
    boolean accepted,
    String reason,
    ActorId actorId,
    String correlationId,
    Instant createdAt) {
  public CommandReceipt {
    Objects.requireNonNull(commandId, "commandId");
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(commandType, "commandType");
    if (expectedVersion < 0) {
      throw new IllegalArgumentException("expectedVersion must not be negative");
    }
    Objects.requireNonNull(actorId, "actorId");
    Objects.requireNonNull(correlationId, "correlationId");
    Objects.requireNonNull(createdAt, "createdAt");
  }
}
