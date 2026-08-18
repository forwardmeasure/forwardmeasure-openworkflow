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
package com.forwardmeasure.openworkflow.engine.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.forwardmeasure.openworkflow.definition.AuthenticationPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceReference;
import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Immutable, credential-free HTTP effect materialized before journal persistence. */
public record HttpOperationDescriptor(
    String operationId,
    Kind kind,
    String method,
    URI uri,
    Map<String, String> headers,
    JsonNode body,
    Output output,
    boolean redirectAsError,
    WorkflowResourceReference openApiDocument,
    String openApiOperationId,
    AuthenticationPlan authentication,
    AuthenticationExpressionContext authenticationContext,
    @com.fasterxml.jackson.annotation.JsonInclude(
            com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        ActorIdentity requestedBy) {

  public HttpOperationDescriptor(
      String operationId,
      Kind kind,
      String method,
      URI uri,
      Map<String, String> headers,
      JsonNode body,
      Output output,
      boolean redirectAsError,
      WorkflowResourceReference openApiDocument,
      String openApiOperationId,
      AuthenticationPlan authentication,
      AuthenticationExpressionContext authenticationContext) {
    this(
        operationId,
        kind,
        method,
        uri,
        headers,
        body,
        output,
        redirectAsError,
        openApiDocument,
        openApiOperationId,
        authentication,
        authenticationContext,
        null);
  }

  public enum Kind {
    HTTP,
    OPEN_API
  }

  public enum Output {
    RAW,
    CONTENT,
    RESPONSE
  }

  public HttpOperationDescriptor {
    operationId = requireText(operationId, "operationId");
    Objects.requireNonNull(kind, "kind");
    method = requireText(method, "method").toUpperCase(Locale.ROOT);
    uri = Objects.requireNonNull(uri, "uri").normalize();
    if (!uri.isAbsolute()
        || !("http".equalsIgnoreCase(uri.getScheme())
            || "https".equalsIgnoreCase(uri.getScheme()))) {
      throw new IllegalArgumentException("HTTP operation URI must be absolute HTTP(S)");
    }
    headers =
        headers == null
            ? Map.of()
            : headers.entrySet().stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        entry -> requireText(entry.getKey(), "header name"),
                        entry -> Objects.requireNonNull(entry.getValue(), "header value")));
    body = body == null ? NullNode.getInstance() : body.deepCopy();
    output = output == null ? Output.CONTENT : output;
    if (kind == Kind.HTTP && (openApiDocument != null || openApiOperationId != null)) {
      throw new IllegalArgumentException("Direct HTTP operations cannot carry OpenAPI identity");
    }
    if (kind == Kind.OPEN_API
        && (openApiDocument == null
            || openApiOperationId == null
            || openApiOperationId.isBlank())) {
      throw new IllegalArgumentException(
          "OpenAPI operations require a pinned document and operationId");
    }
    if (authentication != null && !authentication.secretBacked() && authenticationContext == null) {
      throw new IllegalArgumentException(
          "Expression-backed authentication requires a runtime context");
    }
    if ((authentication == null || authentication.secretBacked())
        && authenticationContext != null) {
      throw new IllegalArgumentException(
          "Only expression-backed authentication can carry a runtime context");
    }
  }

  public HttpOperationDescriptor(
      String operationId,
      Kind kind,
      String method,
      URI uri,
      Map<String, String> headers,
      JsonNode body,
      Output output,
      boolean redirectAsError,
      WorkflowResourceReference openApiDocument,
      String openApiOperationId,
      AuthenticationPlan authentication) {
    this(
        operationId,
        kind,
        method,
        uri,
        headers,
        body,
        output,
        redirectAsError,
        openApiDocument,
        openApiOperationId,
        authentication,
        null,
        null);
  }

  public HttpOperationDescriptor requestedBy(ActorIdentity actor) {
    return new HttpOperationDescriptor(
        operationId,
        kind,
        method,
        uri,
        headers,
        body,
        output,
        redirectAsError,
        openApiDocument,
        openApiOperationId,
        authentication,
        authenticationContext,
        Objects.requireNonNull(actor));
  }

  @Override
  public Map<String, String> headers() {
    return Map.copyOf(headers);
  }

  @Override
  public JsonNode body() {
    return body.deepCopy();
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
    return value;
  }
}
