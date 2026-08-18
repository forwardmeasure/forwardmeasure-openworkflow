package com.forwardmeasure.openworkflow.actor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Objects;

/** Lossless JSON-data boundary to the official CloudEvents Java SDK. */
public final class CloudEventsMapper {
  private final ObjectMapper json;

  public CloudEventsMapper(ObjectMapper json) {
    this.json = Objects.requireNonNull(json, "json");
  }

  public CloudEvent toSdk(WorkflowCloudEvent source) {
    CloudEventBuilder builder =
        CloudEventBuilder.v1()
            .withId(source.id())
            .withSource(source.source())
            .withType(source.type());
    if (source.subject() != null) builder.withSubject(source.subject());
    if (source.time() != null) {
      builder.withTime(OffsetDateTime.ofInstant(source.time(), ZoneOffset.UTC));
    }
    if (source.dataSchema() != null) builder.withDataSchema(source.dataSchema());
    if (!source.data().isNull()) {
      try {
        builder.withData(
            source.dataContentType(),
            source.data().isBinary()
                ? source.data().binaryValue()
                : json.writeValueAsBytes(source.data()));
      } catch (IOException failure) {
        throw new IllegalArgumentException("CloudEvent data is not JSON serializable", failure);
      }
    }
    source
        .extensions()
        .forEach(
            (name, value) -> {
              if (name.equals("dataschema")) return;
              if (value.isBoolean()) builder.withExtension(name, value.booleanValue());
              else if (value.isIntegralNumber()) builder.withExtension(name, value.longValue());
              else if (value.isFloatingPointNumber())
                builder.withExtension(name, value.doubleValue());
              else builder.withExtension(name, value.asText());
            });
    return builder.build();
  }

  public WorkflowCloudEvent fromSdk(CloudEvent source) {
    JsonNode data = json.nullNode();
    if (source.getData() != null) {
      try {
        byte[] bytes = source.getData().toBytes();
        data =
            isJson(source.getDataContentType())
                ? json.readTree(bytes)
                : json.getNodeFactory().binaryNode(bytes);
      } catch (IOException failure) {
        throw new IllegalArgumentException("CloudEvent data is not JSON", failure);
      }
    }
    var extensions = new LinkedHashMap<String, JsonNode>();
    for (String name : source.getExtensionNames()) {
      Object value = source.getExtension(name);
      extensions.put(name, json.valueToTree(value));
    }
    if (source.getDataSchema() != null) {
      extensions.put(
          "dataschema", json.getNodeFactory().textNode(source.getDataSchema().toString()));
    }
    return new WorkflowCloudEvent(
        source.getSpecVersion().toString(),
        source.getId(),
        source.getSource(),
        source.getType(),
        source.getSubject(),
        source.getTime() == null ? null : source.getTime().toInstant(),
        source.getDataContentType(),
        data,
        extensions);
  }

  private static boolean isJson(String contentType) {
    if (contentType == null) return true;
    String mediaType = contentType.toLowerCase(java.util.Locale.ROOT).split(";", 2)[0].trim();
    return mediaType.equals("application/json") || mediaType.endsWith("+json");
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
