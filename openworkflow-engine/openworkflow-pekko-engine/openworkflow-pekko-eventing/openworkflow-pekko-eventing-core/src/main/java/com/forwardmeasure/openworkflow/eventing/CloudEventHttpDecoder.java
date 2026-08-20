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
package com.forwardmeasure.openworkflow.eventing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.actor.CloudEventsMapper;
import com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Decodes both structured-content and binary-content CloudEvents HTTP bindings. */
public final class CloudEventHttpDecoder {
  private final CloudEventsMapper mapper;

  public CloudEventHttpDecoder(ObjectMapper json) {
    mapper = new CloudEventsMapper(Objects.requireNonNull(json, "json"));
  }

  public WorkflowCloudEvent decode(
      String contentType, Map<String, List<String>> headers, byte[] body) {
    Objects.requireNonNull(headers, "headers");
    body = body == null ? new byte[0] : body.clone();
    String mediaType = mediaType(contentType);
    if (JsonFormat.CONTENT_TYPE.equalsIgnoreCase(mediaType)) {
      var format = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
      if (format == null) {
        throw new IllegalStateException("CloudEvents JSON event format is unavailable");
      }
      return mapper.fromSdk(format.deserialize(body));
    }

    var normalized =
        headers.entrySet().stream()
            .collect(
                java.util.stream.Collectors.toMap(
                    entry -> entry.getKey().toLowerCase(Locale.ROOT),
                    entry -> List.copyOf(entry.getValue()),
                    (left, ignored) -> left));
    String specVersion = required(normalized, "ce-specversion");
    if (!"1.0".equals(specVersion)) {
      throw new IllegalArgumentException("Only CloudEvents specversion 1.0 is supported");
    }
    var builder =
        CloudEventBuilder.v1()
            .withId(required(normalized, "ce-id"))
            .withSource(URI.create(required(normalized, "ce-source")))
            .withType(required(normalized, "ce-type"));
    optional(normalized, "ce-subject").ifPresent(builder::withSubject);
    optional(normalized, "ce-time")
        .ifPresent(value -> builder.withTime(OffsetDateTime.parse(value)));
    optional(normalized, "ce-dataschema")
        .ifPresent(value -> builder.withDataSchema(URI.create(value)));
    normalized.forEach(
        (name, values) -> {
          if (name.startsWith("ce-") && !STANDARD_ATTRIBUTES.contains(name) && !values.isEmpty()) {
            builder.withExtension(name.substring(3), values.getFirst());
          }
        });
    if (!bodyIsEmpty(body)) {
      builder.withData(mediaType == null ? null : mediaType, body);
    }
    return mapper.fromSdk(builder.build());
  }

  private static final java.util.Set<String> STANDARD_ATTRIBUTES =
      java.util.Set.of(
          "ce-specversion",
          "ce-id",
          "ce-source",
          "ce-type",
          "ce-subject",
          "ce-time",
          "ce-dataschema",
          "ce-datacontenttype");

  private static boolean bodyIsEmpty(byte[] body) {
    return body.length == 0;
  }

  private static String required(Map<String, List<String>> headers, String name) {
    return optional(headers, name)
        .orElseThrow(() -> new IllegalArgumentException("Missing CloudEvents header " + name));
  }

  private static java.util.Optional<String> optional(
      Map<String, List<String>> headers, String name) {
    List<String> values = headers.get(name);
    return values == null || values.isEmpty() || values.getFirst().isBlank()
        ? java.util.Optional.empty()
        : java.util.Optional.of(values.getFirst());
  }

  private static String mediaType(String contentType) {
    if (contentType == null || contentType.isBlank()) return null;
    int parameters = contentType.indexOf(';');
    return (parameters < 0 ? contentType : contentType.substring(0, parameters))
        .strip()
        .toLowerCase(Locale.ROOT);
  }
}
