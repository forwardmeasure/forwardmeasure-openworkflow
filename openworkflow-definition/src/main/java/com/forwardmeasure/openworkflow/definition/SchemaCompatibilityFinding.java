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
package com.forwardmeasure.openworkflow.definition;

import java.util.Objects;

/** One compile-time result for a declared workflow data-flow edge. */
public record SchemaCompatibilityFinding(
    String producer,
    String producerSchemaPath,
    String consumer,
    String consumerSchemaPath,
    SchemaCompatibilityStatus status,
    String reason) {

  public SchemaCompatibilityFinding {
    producer = requireText(producer, "producer");
    producerSchemaPath = requireText(producerSchemaPath, "producerSchemaPath");
    consumer = requireText(consumer, "consumer");
    consumerSchemaPath = requireText(consumerSchemaPath, "consumerSchemaPath");
    Objects.requireNonNull(status, "status");
    reason = requireText(reason, "reason");
  }

  public String diagnostic() {
    return producerSchemaPath + " -> " + consumerSchemaPath + " [" + status + "] " + reason;
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
