package com.forwardmeasure.durableprocessing.kafka;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/** Explicit deterministic JSON serde for caller-owned durable contracts. */
public final class JacksonSerde<T> implements Serde<T> {
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
  private final Class<T> type;

  public JacksonSerde(Class<T> type) {
    this.type = Objects.requireNonNull(type, "type");
  }

  @Override
  public Serializer<T> serializer() {
    return (topic, value) -> {
      if (value == null) return null;
      try {
        return MAPPER.writeValueAsBytes(value);
      } catch (Exception failure) {
        throw new SerializationException("Unable to serialize " + type.getName(), failure);
      }
    };
  }

  @Override
  public Deserializer<T> deserializer() {
    return (topic, value) -> {
      if (value == null) return null;
      try {
        return MAPPER.readValue(value, type);
      } catch (Exception failure) {
        throw new SerializationException("Unable to deserialize " + type.getName(), failure);
      }
    };
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public void close() {}
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
