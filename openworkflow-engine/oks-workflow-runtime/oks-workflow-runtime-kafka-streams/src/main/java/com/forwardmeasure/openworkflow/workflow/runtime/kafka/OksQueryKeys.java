package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;

/** Lexicographically ordered keys for append-only query projections. */
public final class OksQueryKeys {
  private static final char SEPARATOR = '\u0000';

  private OksQueryKeys() {}

  public static String history(ExecutionKey key, long sequence) {
    return key.canonical() + SEPARATOR + "%020d".formatted(sequence);
  }

  public static String effect(ExecutionKey key, String effectId) {
    return key.canonical() + SEPARATOR + effectId;
  }

  public static String definitionHistory(String definitionKey, String eventId) {
    return definitionKey + SEPARATOR + eventId;
  }

  public static String rangeStart(String canonicalKey) {
    return canonicalKey + SEPARATOR;
  }

  public static String rangeEnd(String canonicalKey) {
    return canonicalKey + SEPARATOR + '\uffff';
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
