package com.forwardmeasure.openworkflow.workflow.runtime.api;

/**
 * Application-level Kafka payload ceilings.
 *
 * <p>The runtime ceiling leaves headroom below Kafka's conventional 1 MiB topic limit for keys,
 * headers, record batches and protocol overhead. Definition records use a separate, explicitly
 * provisioned topic ceiling because immutable source and resolved contracts are publication
 * artifacts, not flowing workflow data.
 */
public final class KafkaRecordLimits {
  public static final int RUNTIME_TRANSITION_BYTES = 512 * 1024;
  public static final int DEFINITION_PAYLOAD_BYTES = (8 * 1024 * 1024) - (16 * 1024);
  public static final int DEFINITION_TOPIC_MESSAGE_BYTES = 8 * 1024 * 1024;

  private KafkaRecordLimits() {}

  public static void requireRuntimeTransition(int actualBytes) {
    requireWithin("Workflow transition", actualBytes, RUNTIME_TRANSITION_BYTES);
  }

  public static void requireDefinitionPayload(int actualBytes) {
    requireWithin("Workflow definition record", actualBytes, DEFINITION_PAYLOAD_BYTES);
  }

  private static void requireWithin(String kind, int actualBytes, int maximumBytes) {
    if (actualBytes < 0) {
      throw new IllegalArgumentException("actualBytes must not be negative");
    }
    if (actualBytes > maximumBytes) {
      throw new KafkaRecordLimitException(kind, actualBytes, maximumBytes);
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
