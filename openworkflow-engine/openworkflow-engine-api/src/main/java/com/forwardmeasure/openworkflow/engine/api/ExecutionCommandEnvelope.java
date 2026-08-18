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

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Authenticated command metadata plus the trusted, already selected engine identity. */
public record ExecutionCommandEnvelope(
    UUID commandId,
    String correlationId,
    TenantActorContext context,
    EngineId selectedEngine,
    long expectedVersion,
    Instant issuedAt,
    ExecutionCommand command) {

  public ExecutionCommandEnvelope {
    Objects.requireNonNull(commandId, "commandId");
    ContractSupport.requireText(correlationId, "correlationId");
    Objects.requireNonNull(context, "context");
    Objects.requireNonNull(selectedEngine, "selectedEngine");
    if (expectedVersion < 0) {
      throw new IllegalArgumentException("expectedVersion must not be negative");
    }
    Objects.requireNonNull(issuedAt, "issuedAt");
    Objects.requireNonNull(command, "command");
    if (!context.tenantId().equals(command.executionId().tenantId())) {
      throw new IllegalArgumentException("command execution and actor context must share a tenant");
    }
  }
}
