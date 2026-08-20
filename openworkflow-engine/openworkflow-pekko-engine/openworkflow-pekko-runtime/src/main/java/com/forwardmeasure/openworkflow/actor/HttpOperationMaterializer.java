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
package com.forwardmeasure.openworkflow.actor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.forwardmeasure.openworkflow.definition.CallPlan;
import com.forwardmeasure.openworkflow.definition.PlanStep;
import com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import com.forwardmeasure.openworkflow.engine.api.AuthenticationExpressionContext;
import com.forwardmeasure.openworkflow.engine.api.HttpOperationDescriptor;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Deterministically turns evaluated call arguments and pinned OpenAPI bytes into an effect. */
final class HttpOperationMaterializer {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

  private HttpOperationMaterializer() {}

  static HttpOperationDescriptor materialize(
      WorkflowPlan plan,
      PlanStep step,
      JsonNode arguments,
      JsonNode input,
      String operationId,
      AuthenticationExpressionContext authenticationContext) {
    CallPlan call = step.callPlan();
    return call.kind() == CallPlan.Kind.HTTP
        ? direct(call, arguments, input, operationId, authenticationContext)
        : openApi(plan, call, arguments, input, operationId, authenticationContext);
  }

  private static HttpOperationDescriptor direct(
      CallPlan call,
      JsonNode args,
      JsonNode input,
      String operationId,
      AuthenticationExpressionContext authenticationContext) {
    String endpoint =
        args.required("endpoint").isObject()
            ? args.required("endpoint").required("uri").asText()
            : args.required("endpoint").asText();
    endpoint = expand(endpoint, input);
    URI uri = addQuery(URI.create(endpoint), args.path("query"));
    return descriptor(
        operationId,
        HttpOperationDescriptor.Kind.HTTP,
        args.required("method").asText(),
        uri,
        headers(args.path("headers")),
        args.path("body"),
        args.path("output").asText("content"),
        args.path("redirect").asBoolean(false),
        null,
        null,
        call,
        authenticationContext);
  }

  private static HttpOperationDescriptor openApi(
      WorkflowPlan plan,
      CallPlan call,
      JsonNode args,
      JsonNode input,
      String operationId,
      AuthenticationExpressionContext authenticationContext) {
    ResolvedWorkflowResource resource =
        plan.resources().stream()
            .filter(
                candidate ->
                    candidate.uri().equals(call.resource().uri())
                        && candidate.sha256().equals(call.resource().sha256()))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Pinned OpenAPI document is absent from the executable plan"));
    JsonNode document = parse(resource);
    String wanted = args.required("operationId").asText();
    LocatedOperation located = locate(document, wanted);
    JsonNode parameters = args.path("parameters");
    String server = firstServer(located.operation(), located.pathItem(), document);
    String path = located.path();
    var query = new LinkedHashMap<String, JsonNode>();
    var requestHeaders = new LinkedHashMap<String, String>();
    var consumed = new java.util.HashSet<String>();
    JsonNode body = NullNode.getInstance();
    for (JsonNode parameter : combinedParameters(located.pathItem(), located.operation())) {
      String name = parameter.path("name").asText();
      JsonNode value = parameters.get(name);
      if (value == null) continue;
      consumed.add(name);
      switch (parameter.path("in").asText()) {
        case "path" -> path = path.replace("{" + name + "}", encoded(scalar(value)));
        case "query" -> query.put(name, value);
        case "header" -> requestHeaders.put(name, scalar(value));
        case "cookie" ->
            requestHeaders.merge(
                "Cookie", name + "=" + scalar(value), (left, right) -> left + "; " + right);
        case "body" -> body = value;
        default ->
            throw new IllegalArgumentException("Unsupported OpenAPI parameter location at " + name);
      }
    }
    if (located.operation().has("requestBody")) {
      if (args.has("body")) {
        body = args.get("body");
      } else if (parameters.isObject()) {
        var object = JSON.createObjectNode();
        parameters
            .properties()
            .iterator()
            .forEachRemaining(
                entry -> {
                  if (!consumed.contains(entry.getKey()))
                    object.set(entry.getKey(), entry.getValue());
                });
        body = object;
      }
      String mediaType = firstContentType(located.operation().path("requestBody").path("content"));
      if (mediaType != null) requestHeaders.putIfAbsent("Content-Type", mediaType);
    } else if (!body.isNull()) {
      JsonNode consumes =
          located.operation().path("consumes").isArray()
              ? located.operation().path("consumes")
              : document.path("consumes");
      if (consumes.isArray() && !consumes.isEmpty()) {
        requestHeaders.putIfAbsent("Content-Type", consumes.get(0).asText());
      }
    }
    URI uri = addQuery(URI.create(expand(server + path, input)), object(query));
    return descriptor(
        operationId,
        HttpOperationDescriptor.Kind.OPEN_API,
        located.method(),
        uri,
        requestHeaders,
        body,
        args.path("output").asText("content"),
        args.path("redirect").asBoolean(false),
        call.resource(),
        wanted,
        call,
        authenticationContext);
  }

  private static HttpOperationDescriptor descriptor(
      String id,
      HttpOperationDescriptor.Kind kind,
      String method,
      URI uri,
      Map<String, String> headers,
      JsonNode body,
      String output,
      boolean redirect,
      com.forwardmeasure.openworkflow.definition.WorkflowResourceReference resource,
      String openApiOperationId,
      CallPlan call,
      AuthenticationExpressionContext authenticationContext) {
    return new HttpOperationDescriptor(
        id,
        kind,
        method,
        uri,
        headers,
        body,
        switch (output.toLowerCase(Locale.ROOT)) {
          case "raw" -> HttpOperationDescriptor.Output.RAW;
          case "response" -> HttpOperationDescriptor.Output.RESPONSE;
          default -> HttpOperationDescriptor.Output.CONTENT;
        },
        redirect,
        resource,
        openApiOperationId,
        call.authentication(),
        call.authentication() != null && !call.authentication().secretBacked()
            ? authenticationContext
            : null);
  }

  private static JsonNode parse(ResolvedWorkflowResource resource) {
    try {
      return (resource.mediaType().toLowerCase(Locale.ROOT).contains("yaml") ? YAML : JSON)
          .readTree(resource.content());
    } catch (Exception failure) {
      throw new IllegalArgumentException("Pinned OpenAPI document cannot be parsed", failure);
    }
  }

  private static LocatedOperation locate(JsonNode document, String operationId) {
    for (Iterator<Map.Entry<String, JsonNode>> paths =
            document.path("paths").properties().iterator();
        paths.hasNext(); ) {
      Map.Entry<String, JsonNode> path = paths.next();
      for (String method :
          java.util.List.of("get", "put", "post", "delete", "options", "head", "patch", "trace")) {
        JsonNode operation = path.getValue().path(method);
        if (operationId.equals(operation.path("operationId").asText())) {
          return new LocatedOperation(
              path.getKey(), method.toUpperCase(Locale.ROOT), path.getValue(), operation);
        }
      }
    }
    throw new IllegalArgumentException("OpenAPI operationId was not found: " + operationId);
  }

  private static String firstServer(JsonNode operation, JsonNode path, JsonNode document) {
    for (JsonNode candidate : java.util.List.of(operation, path, document)) {
      JsonNode servers = candidate.path("servers");
      if (servers.isArray() && !servers.isEmpty()) return servers.get(0).required("url").asText();
    }
    if ("2.0".equals(document.path("swagger").asText())) {
      String scheme =
          document.path("schemes").isArray() && !document.path("schemes").isEmpty()
              ? document.path("schemes").get(0).asText()
              : "https";
      String host = document.path("host").asText();
      if (host.isBlank()) throw new IllegalArgumentException("Swagger 2.0 document has no host");
      String basePath = document.path("basePath").asText("");
      return scheme + "://" + host + basePath;
    }
    throw new IllegalArgumentException("OpenAPI operation has no server URL");
  }

  private static java.util.List<JsonNode> combinedParameters(JsonNode path, JsonNode operation) {
    var result = new java.util.ArrayList<JsonNode>();
    path.path("parameters").forEach(result::add);
    operation.path("parameters").forEach(result::add);
    return result;
  }

  private static String firstContentType(JsonNode content) {
    Iterator<String> names = content.fieldNames();
    return names.hasNext() ? names.next() : null;
  }

  private static Map<String, String> headers(JsonNode node) {
    var result = new LinkedHashMap<String, String>();
    if (node.isObject())
      node.properties()
          .iterator()
          .forEachRemaining(entry -> result.put(entry.getKey(), scalar(entry.getValue())));
    return result;
  }

  private static URI addQuery(URI uri, JsonNode query) {
    if (!query.isObject() || query.isEmpty()) return uri;
    var values = new java.util.ArrayList<String>();
    query
        .properties()
        .iterator()
        .forEachRemaining(
            entry -> {
              if (entry.getValue().isArray())
                entry
                    .getValue()
                    .forEach(
                        value ->
                            values.add(encoded(entry.getKey()) + "=" + encoded(scalar(value))));
              else values.add(encoded(entry.getKey()) + "=" + encoded(scalar(entry.getValue())));
            });
    String existing = uri.getRawQuery();
    String combined =
        existing == null || existing.isEmpty()
            ? String.join("&", values)
            : existing + "&" + String.join("&", values);
    StringBuilder result =
        new StringBuilder()
            .append(uri.getScheme())
            .append("://")
            .append(uri.getRawAuthority())
            .append(uri.getRawPath() == null ? "" : uri.getRawPath())
            .append('?')
            .append(combined);
    if (uri.getRawFragment() != null) {
      result.append('#').append(uri.getRawFragment());
    }
    return URI.create(result.toString());
  }

  private static JsonNode object(Map<String, JsonNode> values) {
    var result = JSON.createObjectNode();
    values.forEach(result::set);
    return result;
  }

  private static String expand(String template, JsonNode input) {
    String result = template;
    if (input.isObject())
      for (Iterator<Map.Entry<String, JsonNode>> fields = input.properties().iterator();
          fields.hasNext(); ) {
        Map.Entry<String, JsonNode> field = fields.next();
        result = result.replace("{" + field.getKey() + "}", encoded(scalar(field.getValue())));
      }
    if (result.matches(".*\\{[^}]+}.*")) {
      throw new IllegalArgumentException("Unresolved URI-template variable in " + result);
    }
    return result;
  }

  private static String scalar(JsonNode value) {
    return value.isValueNode() ? value.asText() : value.toString();
  }

  private static String encoded(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
  }

  private record LocatedOperation(
      String path, String method, JsonNode pathItem, JsonNode operation) {}
}
