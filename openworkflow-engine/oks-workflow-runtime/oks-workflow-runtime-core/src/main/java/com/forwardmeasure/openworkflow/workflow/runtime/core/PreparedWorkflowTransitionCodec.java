package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.forwardmeasure.openworkflow.workflow.runtime.api.KafkaRecordLimits;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Canonical JSON codec and integrity check for computed transitions. */
public final class PreparedWorkflowTransitionCodec {
  private final ObjectMapper json;

  public PreparedWorkflowTransitionCodec(ObjectMapper json) {
    this.json =
        Objects.requireNonNull(json, "json")
            .copy()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  }

  public Encoded encode(PreparedWorkflowTransition transition) {
    Objects.requireNonNull(transition, "transition");
    JsonNode value = json.valueToTree(transition);
    byte[] encoded = canonicalBytes(value);
    KafkaRecordLimits.requireRuntimeTransition(encoded.length);
    return new Encoded(value, sha256(encoded));
  }

  public PreparedWorkflowTransition decode(JsonNode value, String expectedSha256) {
    Objects.requireNonNull(value, "value");
    requireSha256(expectedSha256);
    String actual = sha256(canonicalBytes(value));
    if (!MessageDigest.isEqual(
        actual.getBytes(java.nio.charset.StandardCharsets.US_ASCII),
        expectedSha256.getBytes(java.nio.charset.StandardCharsets.US_ASCII))) {
      throw new SecurityException("Workflow computation transition digest does not match");
    }
    try {
      return json.treeToValue(value, PreparedWorkflowTransition.class);
    } catch (JsonProcessingException invalid) {
      throw new IllegalArgumentException("Workflow computation transition is invalid", invalid);
    }
  }

  private byte[] canonicalBytes(JsonNode value) {
    try {
      return json.writeValueAsBytes(value);
    } catch (JsonProcessingException invalid) {
      throw new IllegalArgumentException("Workflow computation transition is not JSON", invalid);
    }
  }

  private static String sha256(byte[] value) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("JVM does not provide SHA-256", impossible);
    }
  }

  private static void requireSha256(String value) {
    Objects.requireNonNull(value, "expectedSha256");
    if (!value.matches("[0-9a-f]{64}")) {
      throw new IllegalArgumentException("expectedSha256 must be lowercase SHA-256");
    }
  }

  public record Encoded(JsonNode value, String sha256) {
    public Encoded {
      Objects.requireNonNull(value, "value");
      requireSha256(sha256);
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
