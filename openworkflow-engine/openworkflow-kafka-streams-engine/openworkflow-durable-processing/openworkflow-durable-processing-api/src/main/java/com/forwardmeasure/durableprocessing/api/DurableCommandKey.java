package com.forwardmeasure.durableprocessing.api;

import java.util.Objects;

/** Canonical, collision-free key for a command associated with an aggregate. */
public final class DurableCommandKey {

  private DurableCommandKey() {}

  public static String canonical(String aggregateKey, String commandId) {
    requireText(aggregateKey, "aggregateKey");
    requireText(commandId, "commandId");
    return aggregateKey.length() + ":" + aggregateKey + commandId.length() + ":" + commandId;
  }

  public static String aggregatePrefix(String aggregateKey) {
    requireText(aggregateKey, "aggregateKey");
    return aggregateKey.length() + ":" + aggregateKey;
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
