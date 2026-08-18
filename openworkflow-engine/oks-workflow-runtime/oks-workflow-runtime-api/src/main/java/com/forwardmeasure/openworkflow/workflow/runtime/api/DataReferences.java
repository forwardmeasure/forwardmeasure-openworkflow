package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Creates immutable, content-addressed runtime data references. */
public final class DataReferences {
  /**
   * Keeps a data value and its surrounding snapshot/history envelope below Kafka's conventional
   * one-megabyte record boundary. Large business objects travel as immutable application-level URIs
   * in the JSON value.
   */
  public static final int MAX_INLINE_BYTES = 32 * 1024;

  private static final ObjectMapper CANONICAL =
      new ObjectMapper().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

  private DataReferences() {}

  public static DataReference inline(JsonNode value) {
    Objects.requireNonNull(value, "value");
    final byte[] bytes;
    try {
      bytes = CANONICAL.writeValueAsBytes(value);
    } catch (JsonProcessingException failure) {
      throw new IllegalArgumentException("Runtime data is not serializable JSON", failure);
    }
    if (bytes.length > MAX_INLINE_BYTES) {
      throw new RuntimeDataLimitException(bytes.length, MAX_INLINE_BYTES);
    }
    return new DataReference(
        DataReference.Storage.INLINE,
        value.deepCopy(),
        null,
        "application/json",
        bytes.length,
        sha256(bytes));
  }

  private static String sha256(byte[] value) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
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
