package com.forwardmeasure.durableprocessing.api;

import java.time.Instant;
import java.util.Objects;

/**
 * Queryable, compacted processing outcome for one durable command.
 *
 * <p>The outcome is separate from the idempotency fingerprint. A receipt says that an identity was
 * seen; this record says what the durable processor did with it.
 */
public record DurableCommandOutcome(
    String aggregateKey,
    String commandId,
    String commandType,
    DurableCommandOutcomeStatus status,
    long aggregateRevision,
    Instant requestedAt,
    Instant processedAt,
    String rejectionType,
    String rejectionMessage) {

  public DurableCommandOutcome {
    requireText(aggregateKey, "aggregateKey");
    requireText(commandId, "commandId");
    requireText(commandType, "commandType");
    Objects.requireNonNull(status, "status");
    Objects.requireNonNull(requestedAt, "requestedAt");
    Objects.requireNonNull(processedAt, "processedAt");
    if (aggregateRevision < 0) {
      throw new IllegalArgumentException("aggregateRevision must not be negative");
    }
    boolean rejected = status == DurableCommandOutcomeStatus.REJECTED;
    if (rejected != (rejectionType != null)) {
      throw new IllegalArgumentException("Only a rejected command has rejection details");
    }
    if (rejectionType != null) {
      requireText(rejectionType, "rejectionType");
      requireText(rejectionMessage, "rejectionMessage");
    } else if (rejectionMessage != null) {
      throw new IllegalArgumentException("A rejection message requires a rejection type");
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
