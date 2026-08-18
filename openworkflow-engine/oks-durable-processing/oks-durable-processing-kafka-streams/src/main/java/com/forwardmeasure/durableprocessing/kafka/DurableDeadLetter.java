package com.forwardmeasure.durableprocessing.kafka;

import java.time.Instant;
import java.util.Objects;

/** Deterministic command rejection isolated from the processing thread. */
public record DurableDeadLetter(
    String deadLetterId,
    String aggregateKey,
    String commandId,
    String commandSha256,
    String failureType,
    String message,
    Instant rejectedAt) {

  public DurableDeadLetter {
    requireText(deadLetterId, "deadLetterId");
    requireText(aggregateKey, "aggregateKey");
    requireText(commandId, "commandId");
    requireText(commandSha256, "commandSha256");
    requireText(failureType, "failureType");
    requireText(message, "message");
    Objects.requireNonNull(rejectedAt, "rejectedAt");
    if (!commandSha256.matches("[0-9a-f]{64}")) {
      throw new IllegalArgumentException("commandSha256 must be lowercase SHA-256");
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
