package com.forwardmeasure.openworkflow.workflow.runtime.api;

import java.time.Instant;
import java.util.Objects;

/** One human-task outcome correlated to an active durable workflow wait. */
public record ObserveHumanTaskCommand(
    String commandId,
    ExecutionKey key,
    String humanTaskId,
    BusinessCorrelationId correlationId,
    HumanTaskObservation observation,
    ActorContext actor,
    Instant requestedAt)
    implements ExecutionCommand {

  public ObserveHumanTaskCommand {
    ControlExecutionCommand.requireCommand(commandId, key, actor, requestedAt);
    requireText(humanTaskId, "humanTaskId");
    Objects.requireNonNull(correlationId, "correlationId");
    Objects.requireNonNull(observation, "observation");
    if (actor.actorType() != ActorType.SYSTEM) {
      throw new IllegalArgumentException("Human-task observations require a system actor");
    }
    if (!observation.actor().tenantId().equals(key.tenantId())) {
      throw new IllegalArgumentException(
          "Human-task outcome actor and workflow tenants must match");
    }
  }

  private static void requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
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
