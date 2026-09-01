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
package com.forwardmeasure.openworkflow.workflow.runtime.kafka.jaxrs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Decodes both structured-content and binary-content CloudEvents HTTP bindings (CloudEvents HTTP
 * Protocol Binding 1.0) into the plain JSON envelope {@link
 * com.forwardmeasure.openworkflow.workflow.runtime.api.InboundCloudEvent} actually requires - a
 * Jackson {@link JsonNode} object carrying at minimum {@code specversion}/{@code id}/{@code
 * source}/{@code type} as text (see that record's compact constructor).
 *
 * <p>Deliberately not the Pekko-side {@code CloudEventHttpDecoder}: that one builds the richer
 * {@code io.cloudevents.CloudEvent}/{@code WorkflowCloudEvent} object model, which this engine has
 * no use for - the Kafka Streams ingress only ever needs the raw envelope to embed inline (via
 * {@code DataReferences.inline}) on the {@code inbound-events} topic, so this decoder produces that
 * directly with plain Jackson and carries no new external CloudEvents SDK dependency. The decoding
 * logic (structured mode via {@code application/cloudevents+json}, binary mode via {@code ce-*}
 * headers) mirrors that class's shape one-for-one.
 */
final class OksCloudEventHttpDecoder {
  private static final String STRUCTURED_MEDIA_TYPE = "application/cloudevents+json";
  private static final Set<String> STANDARD_ATTRIBUTES =
      Set.of(
          "ce-specversion",
          "ce-id",
          "ce-source",
          "ce-type",
          "ce-subject",
          "ce-time",
          "ce-dataschema",
          "ce-datacontenttype");

  private final ObjectMapper json;

  OksCloudEventHttpDecoder(ObjectMapper json) {
    this.json = Objects.requireNonNull(json, "json");
  }

  JsonNode decode(String contentType, Map<String, List<String>> headers, byte[] body) {
    Objects.requireNonNull(headers, "headers");
    byte[] content = body == null ? new byte[0] : body;
    String mediaType = mediaType(contentType);
    if (STRUCTURED_MEDIA_TYPE.equalsIgnoreCase(mediaType)) {
      return decodeStructured(content);
    }
    return decodeBinary(normalize(headers), mediaType, content);
  }

  private JsonNode decodeStructured(byte[] content) {
    if (content.length == 0) {
      throw new IllegalArgumentException("A structured-mode CloudEvent body must not be empty");
    }
    JsonNode envelope;
    try {
      envelope = json.readTree(content);
    } catch (Exception malformed) {
      throw new IllegalArgumentException("Malformed CloudEvent JSON", malformed);
    }
    if (envelope == null || !envelope.isObject()) {
      throw new IllegalArgumentException("A structured-mode CloudEvent body must be a JSON object");
    }
    return envelope;
  }

  private JsonNode decodeBinary(
      Map<String, List<String>> headers, String mediaType, byte[] content) {
    String specVersion = required(headers, "ce-specversion");
    if (!"1.0".equals(specVersion)) {
      throw new IllegalArgumentException("Only CloudEvents specversion 1.0 is supported");
    }
    ObjectNode envelope = json.createObjectNode();
    envelope.put("specversion", specVersion);
    envelope.put("id", required(headers, "ce-id"));
    envelope.put("source", required(headers, "ce-source"));
    envelope.put("type", required(headers, "ce-type"));
    optional(headers, "ce-subject").ifPresent(value -> envelope.put("subject", value));
    optional(headers, "ce-time").ifPresent(value -> envelope.put("time", value));
    optional(headers, "ce-dataschema").ifPresent(value -> envelope.put("dataschema", value));
    optional(headers, "ce-datacontenttype")
        .ifPresent(value -> envelope.put("datacontenttype", value));
    headers.forEach(
        (name, values) -> {
          if (name.startsWith("ce-") && !STANDARD_ATTRIBUTES.contains(name) && !values.isEmpty()) {
            envelope.put(name.substring(3), values.getFirst());
          }
        });
    if (content.length > 0) {
      attachData(envelope, mediaType, content);
    }
    return envelope;
  }

  private void attachData(ObjectNode envelope, String mediaType, byte[] content) {
    if (mediaType == null || mediaType.equalsIgnoreCase("application/json")) {
      try {
        envelope.set("data", json.readTree(content));
        return;
      } catch (Exception notJson) {
        // Not actually JSON despite the content type - fall through to base64 below.
      }
    }
    envelope.put("data_base64", Base64.getEncoder().encodeToString(content));
  }

  private static Map<String, List<String>> normalize(Map<String, List<String>> headers) {
    return headers.entrySet().stream()
        .collect(
            Collectors.toMap(
                entry -> entry.getKey().toLowerCase(Locale.ROOT),
                entry -> List.copyOf(entry.getValue()),
                (left, ignored) -> left));
  }

  private static String required(Map<String, List<String>> headers, String name) {
    return optional(headers, name)
        .orElseThrow(() -> new IllegalArgumentException("Missing CloudEvents header " + name));
  }

  private static Optional<String> optional(Map<String, List<String>> headers, String name) {
    List<String> values = headers.get(name);
    return values == null || values.isEmpty() || values.getFirst().isBlank()
        ? Optional.empty()
        : Optional.of(values.getFirst());
  }

  private static String mediaType(String contentType) {
    if (contentType == null || contentType.isBlank()) return null;
    int parameters = contentType.indexOf(';');
    return (parameters < 0 ? contentType : contentType.substring(0, parameters))
        .strip()
        .toLowerCase(Locale.ROOT);
  }
}
