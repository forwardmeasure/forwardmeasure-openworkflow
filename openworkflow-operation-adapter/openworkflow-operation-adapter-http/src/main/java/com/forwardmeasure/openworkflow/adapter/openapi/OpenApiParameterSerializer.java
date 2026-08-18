package com.forwardmeasure.openworkflow.adapter.openapi;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * OpenAPI 3 parameter serialization for the four HTTP parameter locations.
 *
 * <p>The serializer returns already percent-encoded path/query/cookie values. Header values are
 * deliberately not percent encoded. It implements the location defaults and the style/explode
 * combinations defined by OpenAPI 3.x; invalid style/location/value combinations fail closed.
 */
final class OpenApiParameterSerializer {
  private OpenApiParameterSerializer() {}

  static String path(String name, JsonNode value, JsonNode declaration) {
    String style = declaration.path("style").asText("simple");
    boolean explode = declaration.path("explode").asBoolean(false);
    return switch (style) {
      case "simple" -> encodeComposite(value, explode, ",", ",", false);
      case "label" ->
          "." + encodeComposite(value, explode, explode ? "." : ",", explode ? "." : ",", false);
      case "matrix" -> matrix(name, value, explode);
      default -> throw invalid(name, "path", style);
    };
  }

  static List<QueryValue> query(String name, JsonNode value, JsonNode declaration) {
    String style = declaration.path("style").asText("form");
    boolean explode =
        declaration.has("explode") ? declaration.path("explode").asBoolean() : "form".equals(style);
    boolean allowReserved = declaration.path("allowReserved").asBoolean(false);
    return switch (style) {
      case "form" -> formQuery(name, value, explode, allowReserved);
      case "spaceDelimited" ->
          List.of(
              new QueryValue(
                  percentEncode(name, false),
                  array(value, name).stream()
                      .map(item -> percentEncode(item, allowReserved))
                      .reduce((left, right) -> left + "%20" + right)
                      .orElse("")));
      case "pipeDelimited" ->
          List.of(
              new QueryValue(
                  percentEncode(name, false),
                  array(value, name).stream()
                      .map(item -> percentEncode(item, allowReserved))
                      .reduce((left, right) -> left + "%7C" + right)
                      .orElse("")));
      case "deepObject" -> deepObject(name, value, allowReserved);
      default -> throw invalid(name, "query", style);
    };
  }

  static String header(String name, JsonNode value, JsonNode declaration) {
    String style = declaration.path("style").asText("simple");
    if (!"simple".equals(style)) {
      throw invalid(name, "header", style);
    }
    boolean explode = declaration.path("explode").asBoolean(false);
    return rawComposite(value, explode, ",", ",");
  }

  static List<QueryValue> cookie(String name, JsonNode value, JsonNode declaration) {
    String style = declaration.path("style").asText("form");
    if (!"form".equals(style)) {
      throw invalid(name, "cookie", style);
    }
    boolean explode = declaration.has("explode") ? declaration.path("explode").asBoolean() : true;
    return formQuery(name, value, explode, false);
  }

  /**
   * Serializes an OpenAPI 3 parameter declared with {@code content} instead of {@code schema}. The
   * OpenAPI specification permits exactly one media type in this form.
   */
  static String content(String name, String location, JsonNode value, JsonNode declaration) {
    JsonNode content = declaration.path("content");
    if (!content.isObject() || content.size() != 1) {
      throw new IllegalArgumentException(
          "OpenAPI content parameter " + name + " must define exactly one media type");
    }
    String mediaType = content.fieldNames().next();
    String serialized;
    String normalized = mediaType.toLowerCase(java.util.Locale.ROOT);
    if ("application/json".equals(normalized) || normalized.endsWith("+json")) {
      serialized = value.toString();
    } else if (normalized.startsWith("text/")) {
      serialized = rawScalar(value, name);
    } else {
      throw new IllegalArgumentException(
          "OpenAPI content parameter " + name + " uses unsupported media type " + mediaType);
    }
    if ("header".equals(location)) {
      return serialized;
    }
    return percentEncode(
        serialized, "query".equals(location) && declaration.path("allowReserved").asBoolean(false));
  }

  static String swagger2(String name, String location, JsonNode value, JsonNode declaration) {
    if (!value.isArray()) {
      return rawScalar(value, name);
    }
    String format = declaration.path("collectionFormat").asText("csv");
    String delimiter =
        switch (format) {
          case "csv" -> ",";
          case "ssv" -> " ";
          case "tsv" -> "\t";
          case "pipes" -> "|";
          case "multi" ->
              throw new IllegalArgumentException(
                  "Swagger 2 multi-valued "
                      + location
                      + " parameter "
                      + name
                      + " requires repeated query/form fields");
          default ->
              throw new IllegalArgumentException(
                  "Unsupported Swagger 2 collectionFormat " + format + " for " + name);
        };
    return String.join(delimiter, array(value, name));
  }

  static List<QueryValue> swagger2Query(String name, JsonNode value, JsonNode declaration) {
    if (value.isArray() && "multi".equals(declaration.path("collectionFormat").asText())) {
      List<QueryValue> values = new ArrayList<>();
      for (String item : array(value, name)) {
        values.add(new QueryValue(percentEncode(name, false), percentEncode(item, false)));
      }
      return List.copyOf(values);
    }
    return List.of(
        new QueryValue(
            percentEncode(name, false),
            percentEncode(swagger2(name, "query", value, declaration), false)));
  }

  /**
   * Swagger 2 {@code formData} serialization before the enclosing form media type is encoded.
   * Unlike query serialization, multipart fields must retain their literal value.
   */
  static List<QueryValue> swagger2Form(String name, JsonNode value, JsonNode declaration) {
    if (value.isArray() && "multi".equals(declaration.path("collectionFormat").asText())) {
      List<QueryValue> values = new ArrayList<>();
      for (String item : array(value, name)) {
        values.add(new QueryValue(name, item));
      }
      return List.copyOf(values);
    }
    return List.of(new QueryValue(name, swagger2(name, "formData", value, declaration)));
  }

  private static List<QueryValue> formQuery(
      String name, JsonNode value, boolean explode, boolean allowReserved) {
    if (value.isObject()) {
      List<QueryValue> result = new ArrayList<>();
      if (explode) {
        fields(value)
            .forEach(
                entry ->
                    result.add(
                        new QueryValue(
                            percentEncode(entry.getKey(), false),
                            encode(entry.getValue(), allowReserved))));
      } else {
        result.add(
            new QueryValue(
                percentEncode(name, false),
                encodeComposite(value, false, ",", ",", allowReserved)));
      }
      return List.copyOf(result);
    }
    if (value.isArray() && explode) {
      List<QueryValue> result = new ArrayList<>();
      for (String item : array(value, name)) {
        result.add(new QueryValue(percentEncode(name, false), percentEncode(item, allowReserved)));
      }
      return List.copyOf(result);
    }
    return List.of(
        new QueryValue(
            percentEncode(name, false), encodeComposite(value, false, ",", ",", allowReserved)));
  }

  private static List<QueryValue> deepObject(String name, JsonNode value, boolean allowReserved) {
    if (!value.isObject()) {
      throw new IllegalArgumentException(
          "OpenAPI deepObject parameter " + name + " requires an object value");
    }
    List<QueryValue> result = new ArrayList<>();
    fields(value)
        .forEach(
            entry ->
                result.add(
                    new QueryValue(
                        percentEncode(name + "[" + entry.getKey() + "]", false),
                        encode(entry.getValue(), allowReserved))));
    return List.copyOf(result);
  }

  private static String matrix(String name, JsonNode value, boolean explode) {
    if (value.isObject() && explode) {
      StringBuilder result = new StringBuilder();
      fields(value)
          .forEach(
              entry ->
                  result
                      .append(';')
                      .append(percentEncode(entry.getKey(), false))
                      .append('=')
                      .append(encode(entry.getValue(), false)));
      return result.toString();
    }
    if (value.isArray() && explode) {
      StringBuilder result = new StringBuilder();
      for (String item : array(value, name)) {
        result
            .append(';')
            .append(percentEncode(name, false))
            .append('=')
            .append(percentEncode(item, false));
      }
      return result.toString();
    }
    return ";" + percentEncode(name, false) + "=" + encodeComposite(value, false, ",", ",", false);
  }

  private static String encodeComposite(
      JsonNode value,
      boolean explode,
      String arrayDelimiter,
      String objectDelimiter,
      boolean allowReserved) {
    if (value.isObject()) {
      List<String> components = new ArrayList<>();
      fields(value)
          .forEach(
              entry -> {
                if (explode) {
                  components.add(
                      percentEncode(entry.getKey(), false)
                          + "="
                          + encode(entry.getValue(), allowReserved));
                } else {
                  components.add(percentEncode(entry.getKey(), false));
                  components.add(encode(entry.getValue(), allowReserved));
                }
              });
      return String.join(objectDelimiter, components);
    }
    if (value.isArray()) {
      return value
          .valueStream()
          .map(item -> encode(item, allowReserved))
          .reduce((left, right) -> left + arrayDelimiter + right)
          .orElse("");
    }
    return encode(value, allowReserved);
  }

  private static String rawComposite(
      JsonNode value, boolean explode, String arrayDelimiter, String objectDelimiter) {
    if (value.isObject()) {
      List<String> components = new ArrayList<>();
      fields(value)
          .forEach(
              entry -> {
                if (explode) {
                  components.add(
                      entry.getKey() + "=" + rawScalar(entry.getValue(), entry.getKey()));
                } else {
                  components.add(entry.getKey());
                  components.add(rawScalar(entry.getValue(), entry.getKey()));
                }
              });
      return String.join(objectDelimiter, components);
    }
    if (value.isArray()) {
      return String.join(arrayDelimiter, array(value, "header"));
    }
    return rawScalar(value, "header");
  }

  private static List<Map.Entry<String, JsonNode>> fields(JsonNode value) {
    List<Map.Entry<String, JsonNode>> result = new ArrayList<>();
    Iterator<Map.Entry<String, JsonNode>> iterator = value.properties().iterator();
    iterator.forEachRemaining(result::add);
    return result;
  }

  private static List<String> array(JsonNode value, String name) {
    if (!value.isArray()) {
      throw new IllegalArgumentException("OpenAPI parameter " + name + " requires an array value");
    }
    List<String> values = new ArrayList<>();
    value.forEach(item -> values.add(rawScalar(item, name)));
    return List.copyOf(values);
  }

  private static String encode(JsonNode value, boolean allowReserved) {
    return percentEncode(rawScalar(value, "parameter"), allowReserved);
  }

  private static String rawScalar(JsonNode value, String name) {
    if (value == null || !value.isValueNode() || value.isNull()) {
      throw new IllegalArgumentException(
          "OpenAPI parameter " + name + " requires a non-null scalar value");
    }
    return value.asText();
  }

  private static IllegalArgumentException invalid(String name, String location, String style) {
    return new IllegalArgumentException(
        "OpenAPI " + location + " parameter " + name + " does not support style " + style);
  }

  private static String percentEncode(String value, boolean allowReserved) {
    ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    for (byte octet : value.getBytes(StandardCharsets.UTF_8)) {
      int unsigned = octet & 0xff;
      if (unreserved(unsigned) || allowReserved && reserved(unsigned)) {
        encoded.write(unsigned);
      } else {
        encoded.write('%');
        encoded.write(Character.toUpperCase(Character.forDigit(unsigned >>> 4, 16)));
        encoded.write(Character.toUpperCase(Character.forDigit(unsigned & 0xf, 16)));
      }
    }
    return encoded.toString(StandardCharsets.US_ASCII);
  }

  private static boolean unreserved(int value) {
    return value >= 'a' && value <= 'z'
        || value >= 'A' && value <= 'Z'
        || value >= '0' && value <= '9'
        || value == '-'
        || value == '.'
        || value == '_'
        || value == '~';
  }

  private static boolean reserved(int value) {
    return ":/?#[]@!$&'()*+,;=".indexOf(value) >= 0;
  }

  record QueryValue(String name, String value) {}
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
