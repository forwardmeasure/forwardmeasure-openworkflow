package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.Objects;

/**
 * Trusted worker result for one durable, off-thread workflow computation.
 *
 * <p>The transition contains only Kafka-safe state and immutable data references. The controller
 * validates its digest, computation identity, execution identity and definition identity before
 * applying it.
 */
public record ObserveWorkflowComputationCommand(
    String commandId,
    ExecutionKey key,
    String computationId,
    long basisRevision,
    String transitionSha256,
    JsonNode transition,
    ActorContext actor,
    Instant requestedAt)
    implements ExecutionCommand {

  public ObserveWorkflowComputationCommand {
    ControlExecutionCommand.requireCommand(commandId, key, actor, requestedAt);
    requireText(computationId, "computationId");
    if (basisRevision < 1) {
      throw new IllegalArgumentException("basisRevision must be positive");
    }
    Objects.requireNonNull(transitionSha256, "transitionSha256");
    if (!transitionSha256.matches("[0-9a-f]{64}")) {
      throw new IllegalArgumentException("transitionSha256 must be lowercase SHA-256");
    }
    Objects.requireNonNull(transition, "transition");
    if (!transition.isObject()) {
      throw new IllegalArgumentException("transition must be a JSON object");
    }
    KafkaRecordLimits.requireRuntimeTransition(
        transition.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
    if (actor.actorType() != ActorType.SYSTEM) {
      throw new IllegalArgumentException(
          "A workflow computation observation requires " + "a registered system actor");
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
