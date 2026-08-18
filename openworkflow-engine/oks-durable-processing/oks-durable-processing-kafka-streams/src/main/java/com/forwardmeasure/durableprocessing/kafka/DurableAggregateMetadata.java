package com.forwardmeasure.durableprocessing.kafka;

import java.time.Instant;
import java.util.Objects;

/** Kernel-owned metadata stored atomically beside caller-owned state. */
public record DurableAggregateMetadata(
    String aggregateKey, long revision, Instant startedAt, Instant updatedAt) {

  public DurableAggregateMetadata {
    Objects.requireNonNull(aggregateKey, "aggregateKey");
    if (aggregateKey.isBlank()) {
      throw new IllegalArgumentException("aggregateKey must not be blank");
    }
    if (revision < 1) {
      throw new IllegalArgumentException("revision must be positive");
    }
    Objects.requireNonNull(startedAt, "startedAt");
    Objects.requireNonNull(updatedAt, "updatedAt");
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
