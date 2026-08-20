package com.forwardmeasure.durableprocessing.api;

import java.time.Instant;
import java.util.Objects;

/** Deterministic metadata supplied to one domain decision. */
public record DurableProcessContext(
    String aggregateKey,
    String commandId,
    long currentRevision,
    long nextRevision,
    Instant requestedAt) {

  public DurableProcessContext {
    requireText(aggregateKey, "aggregateKey");
    requireText(commandId, "commandId");
    if (currentRevision < 0) {
      throw new IllegalArgumentException("currentRevision must not be negative");
    }
    if (nextRevision != currentRevision + 1) {
      throw new IllegalArgumentException("nextRevision must follow currentRevision");
    }
    Objects.requireNonNull(requestedAt, "requestedAt");
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
