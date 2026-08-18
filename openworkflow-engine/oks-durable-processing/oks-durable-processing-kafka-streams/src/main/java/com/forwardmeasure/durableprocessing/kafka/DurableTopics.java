package com.forwardmeasure.durableprocessing.kafka;

import java.util.Objects;
import java.util.Set;

/** Topics required by one durable processing topology. */
public record DurableTopics(String commands, String history, String outbox, String deadLetters) {

  public DurableTopics {
    requireText(commands, "commands");
    requireText(history, "history");
    requireText(outbox, "outbox");
    requireText(deadLetters, "deadLetters");
    if (Set.of(commands, history, outbox, deadLetters).size() != 4) {
      throw new IllegalArgumentException("Durable processing topics must be distinct");
    }
  }

  public static DurableTopics withPrefix(String prefix) {
    requireText(prefix, "prefix");
    return new DurableTopics(
        prefix + ".commands", prefix + ".history", prefix + ".outbox", prefix + ".dead-letters");
  }

  /** Compacted status topic derived from the command authority's name. */
  public String commandOutcomes() {
    return commands + ".outcomes";
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
