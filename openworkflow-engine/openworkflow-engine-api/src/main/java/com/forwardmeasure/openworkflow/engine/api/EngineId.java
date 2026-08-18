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

/** Stable provider identity persisted when an execution is admitted. */
public record EngineId(String value) implements Comparable<EngineId> {
  public static final EngineId KAFKA_STREAMS = new EngineId("kafka-streams");
  public static final EngineId PEKKO = new EngineId("pekko");

  public EngineId {
    ContractSupport.requireText(value, "value");
    if (!value.matches("[a-z][a-z0-9-]{0,62}")) {
      throw new IllegalArgumentException("engine id must be a lowercase kebab-case identifier");
    }
  }

  @Override
  public int compareTo(EngineId other) {
    return value.compareTo(other.value);
  }
}
