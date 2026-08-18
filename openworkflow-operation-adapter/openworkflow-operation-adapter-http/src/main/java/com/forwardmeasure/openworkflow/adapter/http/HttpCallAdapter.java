package com.forwardmeasure.openworkflow.adapter.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.adapter.api.OperationAdapter;
import com.forwardmeasure.openworkflow.adapter.api.OperationDataReferenceFactory;
import com.forwardmeasure.openworkflow.adapter.api.OperationProgressSink;
import com.forwardmeasure.openworkflow.adapter.api.OperationRequest;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservation;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservationStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowError;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Standards-oriented asynchronous executor for Open Workflow HTTP calls. */
public final class HttpCallAdapter implements OperationAdapter, AutoCloseable {
  public static final String COMMUNICATION_ERROR =
      "https://open-workflow-specification.org/spec/1.0.0/errors/communication";
  public static final String EXPRESSION_ERROR =
      "https://open-workflow-specification.org/spec/1.0.0/errors/expression";
  public static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";
  public static final String OPERATION_ID_HEADER = "X-OKS-Operation-Id";
  public static final String TENANT_DID_HEADER = "X-OKS-Tenant-DID";
  public static final String ACTOR_DID_HEADER = "X-OKS-Actor-DID";
  public static final String IDENTITY_PROVIDER_HEADER = "X-OKS-Identity-Provider";
  public static final String SUBJECT_IDENTIFIER_HEADER = "X-OKS-Subject-Identifier";
  public static final String WORKFLOW_RUN_ID_HEADER = "X-OKS-Workflow-Run-Id";
  public static final String CORRELATION_ID_HEADER = "X-OKS-Correlation-Id";
  private static final Pattern TEMPLATE = Pattern.compile("\\{([^{}]+)}");

  private final HttpClient client;
  private final ObjectMapper json;
  private final OperationDataReferenceFactory dataReferences;
  private final HttpEndpointPolicy endpointPolicy;
  private final Map<String, ActiveInvocation> inFlight = new ConcurrentHashMap<>();
  private final AtomicBoolean closed = new AtomicBoolean();

  public HttpCallAdapter() {
    this(
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NEVER)
            .connectTimeout(Duration.ofSeconds(30))
            .build(),
        new ObjectMapper(),
        OperationDataReferenceFactory.boundedInline(),
        HttpEndpointPolicy.allowAll());
  }

  public HttpCallAdapter(HttpClient client, ObjectMapper json) {
    this(
        client, json, OperationDataReferenceFactory.boundedInline(), HttpEndpointPolicy.allowAll());
  }

  public HttpCallAdapter(
      HttpClient client, ObjectMapper json, OperationDataReferenceFactory dataReferences) {
    this(client, json, dataReferences, HttpEndpointPolicy.allowAll());
  }

  public HttpCallAdapter(
      HttpClient client,
      ObjectMapper json,
      OperationDataReferenceFactory dataReferences,
      HttpEndpointPolicy endpointPolicy) {
    this.client = Objects.requireNonNull(client, "client");
    this.json = Objects.requireNonNull(json, "json");
    this.dataReferences = Objects.requireNonNull(dataReferences, "dataReferences");
    this.endpointPolicy = Objects.requireNonNull(endpointPolicy, "endpointPolicy");
  }

  @Override
  public boolean supports(OperationRequest request) {
    return "call".equals(request.operationKind())
        && "HTTP".equals(request.descriptor().path("callKind").textValue());
  }

  @Override
  public CompletionStage<OperationObservation> execute(
      OperationRequest request, OperationProgressSink progress) {
    Objects.requireNonNull(progress, "progress");
    if (!supports(request)) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("Operation is not an HTTP call"));
    }
    if (closed.get()) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("HTTP call adapter is closed"));
    }
    try {
      Prepared prepared = prepare(request);
      CompletableFuture<HttpResponse<byte[]>> first =
          client.sendAsync(prepared.request(), HttpResponse.BodyHandlers.ofByteArray());
      CompletableFuture<OperationObservation> result = new CompletableFuture<>();
      ActiveInvocation active = new ActiveInvocation(request, result);
      active.track(first);
      ActiveInvocation existing = inFlight.putIfAbsent(request.operationId(), active);
      if (existing != null) {
        first.cancel(true);
        return CompletableFuture.completedFuture(
            failed(request, 409, "HTTP operation is already in flight", request.operationId()));
      }
      if (closed.get()) {
        inFlight.remove(request.operationId(), active);
        active.cancel("HTTP adapter shut down");
        return result;
      }
      CompletableFuture<HttpResponse<byte[]>> invocation =
          first.thenCompose(response -> digestRetry(request, prepared, response, active));
      active.track(invocation);
      invocation.whenComplete(
          (response, failure) -> {
            if (failure != null) {
              result.complete(failed(request, 500, "HTTP transport failed", rootMessage(failure)));
            } else {
              try {
                result.complete(observeResponse(request, prepared, response));
              } catch (RuntimeException invalidResponse) {
                result.complete(
                    failed(
                        request,
                        500,
                        "HTTP response processing failed",
                        rootMessage(invalidResponse)));
              }
            }
          });
      return result.whenComplete(
          (ignored, failure) -> inFlight.remove(request.operationId(), active));
    } catch (AdapterFailure failure) {
      return CompletableFuture.completedFuture(
          new OperationObservation(OperationObservationStatus.FAILED, null, failure.error(), null));
    } catch (RuntimeException failure) {
      return CompletableFuture.completedFuture(
          failed(request, 500, "HTTP request preparation failed", rootMessage(failure)));
    }
  }

  @Override
  public CompletionStage<Void> cancel(OperationRequest request) {
    ActiveInvocation invocation = inFlight.remove(request.operationId());
    if (invocation != null) {
      invocation.cancel("HTTP call cancelled");
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Cancels local transports without claiming that a remote server has rolled back a request it may
   * already have accepted. The Kafka host does not commit that effect during shutdown, so a
   * replacement host will replay it with the same stable idempotency key.
   */
  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    for (Map.Entry<String, ActiveInvocation> entry : Map.copyOf(inFlight).entrySet()) {
      if (inFlight.remove(entry.getKey(), entry.getValue())) {
        entry.getValue().cancel("HTTP adapter shut down");
      }
    }
  }

  private Prepared prepare(OperationRequest request) {
    JsonNode arguments = dataReferences.resolveDescriptorValue(request, "arguments");
    JsonNode input = dataReferences.resolveDescriptorValue(request, "taskInput");
    String method = requiredText(arguments, "method").toUpperCase(Locale.ROOT);
    JsonNode endpoint = arguments.required("endpoint");
    String template;
    if (endpoint.isTextual()) {
      template = endpoint.textValue();
    } else {
      template = requiredText(endpoint, "uri");
    }
    if (endpoint.isObject() && endpoint.has("authentication")) {
      throw communication(
          request,
          500,
          "Unresolved HTTP authentication",
          "HTTP authentication must be resolved at the authorised " + "adapter edge");
    }
    if (request.descriptor().has("authentication") && request.authentication() == null) {
      throw communication(
          request,
          500,
          "Unresolved HTTP authentication",
          "HTTP authentication must be resolved at the authorised " + "adapter edge");
    }
    URI logicalUri =
        URI.create(appendQuery(interpolate(template, input, request), arguments.get("query")));
    URI uri =
        Objects.requireNonNull(
            endpointPolicy.resolve(request, method, logicalUri),
            "HTTP endpoint policy returned null");
    endpointPolicy.authorize(request, method, uri);
    HttpRequest.Builder builder = HttpRequest.newBuilder(uri);
    ObjectNode requestHeaders = JsonNodeFactory.instance.objectNode();
    JsonNode headers = arguments.get("headers");
    if (headers != null) {
      headers
          .properties()
          .iterator()
          .forEachRemaining(
              entry -> {
                String value = scalarText(entry.getValue(), "HTTP header " + entry.getKey());
                builder.header(entry.getKey(), value);
                requestHeaders.put(entry.getKey(), value);
              });
    }
    if (request.descriptor().path("propagateTrustedIdentity").asBoolean(false)) {
      verifyTrustedIdentityHeaders(request, requestHeaders);
    }
    if (request.authentication() != null) {
      if (containsHeader(requestHeaders, "authorization")) {
        throw communication(
            request,
            500,
            "Conflicting HTTP authentication",
            "A workflow cannot provide both an Authorization "
                + "header and a resolved authentication policy");
      }
      request
          .authentication()
          .authorizationHeader()
          .ifPresent(authorization -> builder.header("Authorization", authorization));
    }
    if (!containsHeader(requestHeaders, IDEMPOTENCY_KEY_HEADER)) {
      builder.header(IDEMPOTENCY_KEY_HEADER, request.operationId());
      requestHeaders.put(IDEMPOTENCY_KEY_HEADER, request.operationId());
    }
    byte[] body = body(arguments.get("body"));
    if (body.length > 0
        && !containsHeader(requestHeaders, "content-type")
        && arguments.get("body") != null
        && !arguments.get("body").isTextual()) {
      builder.header("Content-Type", "application/json");
      requestHeaders.put("Content-Type", "application/json");
    }
    builder.method(
        method,
        body.length == 0
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofByteArray(body));
    return new Prepared(
        builder.build(),
        method,
        requestHeaders,
        arguments.path("output").asText("content"),
        arguments.path("redirect").asBoolean(false),
        body);
  }

  private static void verifyTrustedIdentityHeaders(OperationRequest request, ObjectNode headers) {
    requireHeader(headers, OPERATION_ID_HEADER, request.operationId());
    requireHeader(headers, TENANT_DID_HEADER, request.requestedBy().tenantId().toString());
    requireHeader(headers, ACTOR_DID_HEADER, request.requestedBy().actorId().toString());
    String identityProvider = request.requestedBy().identityProvider();
    String subjectIdentifier = request.requestedBy().subjectIdentifier();
    if (identityProvider == null || subjectIdentifier == null) {
      throw new SecurityException(
          "Trusted HTTP identity propagation requires persisted IdP coordinates");
    }
    requireHeader(headers, IDENTITY_PROVIDER_HEADER, identityProvider);
    requireHeader(headers, SUBJECT_IDENTIFIER_HEADER, subjectIdentifier);
    requireHeader(headers, WORKFLOW_RUN_ID_HEADER, request.executionKey().executionId().value());
  }

  private static void requireHeader(ObjectNode headers, String name, String expected) {
    String actual = null;
    var fields = headers.properties().iterator();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      if (field.getKey().equalsIgnoreCase(name)) {
        actual = field.getValue().asText();
        break;
      }
    }
    if (!expected.equals(actual)) {
      throw new SecurityException(
          "Trusted HTTP header is absent or does not match the authenticated invocation: " + name);
    }
  }

  private CompletionStage<HttpResponse<byte[]>> digestRetry(
      OperationRequest request,
      Prepared prepared,
      HttpResponse<byte[]> response,
      ActiveInvocation active) {
    if (request.authentication() == null
        || request.authentication().kind()
            != com.forwardmeasure.openworkflow.adapter.api.ResolvedAuthentication.Kind.DIGEST
        || response.statusCode() != 401) {
      return CompletableFuture.completedFuture(response);
    }
    String challenge =
        response.headers().allValues("www-authenticate").stream()
            .filter(value -> value.regionMatches(true, 0, "Digest ", 0, 7))
            .findFirst()
            .orElse(null);
    if (challenge == null) {
      return CompletableFuture.completedFuture(response);
    }
    final String authorization;
    try {
      authorization =
          DigestAuthentication.authorize(
              request.authentication(),
              challenge,
              prepared.method(),
              prepared.request().uri(),
              prepared.body());
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
    HttpRequest authenticated =
        HttpRequest.newBuilder(
                prepared.request(), (name, value) -> !"authorization".equalsIgnoreCase(name))
            .header("Authorization", authorization)
            .build();
    CompletableFuture<HttpResponse<byte[]>> retry =
        client.sendAsync(authenticated, HttpResponse.BodyHandlers.ofByteArray());
    active.track(retry);
    return retry;
  }

  private OperationObservation observeResponse(
      OperationRequest request, Prepared prepared, HttpResponse<byte[]> response) {
    int status = response.statusCode();
    boolean accepted =
        status >= 200 && status <= 299
            || prepared.acceptRedirects() && status >= 300 && status <= 399;
    if (!accepted) {
      return failed(
          request,
          status,
          "HTTP call returned status " + status,
          response.body().length == 0 ? null : new String(response.body(), StandardCharsets.UTF_8));
    }
    JsonNode content =
        content(response.body(), response.headers().firstValue("content-type").orElse(""));
    JsonNode output =
        switch (prepared.output()) {
          case "raw" ->
              JsonNodeFactory.instance.textNode(
                  Base64.getEncoder().encodeToString(response.body()));
          case "content" -> content;
          case "response" -> response(prepared, response, content);
          default ->
              throw new IllegalStateException(
                  "Schema admitted unsupported HTTP output " + prepared.output());
        };
    return new OperationObservation(
        OperationObservationStatus.SUCCEEDED, dataReferences.capture(request, output), null, null);
  }

  private ObjectNode response(Prepared prepared, HttpResponse<byte[]> response, JsonNode content) {
    ObjectNode result = JsonNodeFactory.instance.objectNode();
    ObjectNode request = result.putObject("request");
    request.put("method", prepared.method().toLowerCase(Locale.ROOT));
    request.put("uri", prepared.request().uri().toString());
    request.set("headers", prepared.requestHeaders().deepCopy());
    result.put("statusCode", response.statusCode());
    result.set("headers", responseHeaders(response));
    result.set("content", content);
    return result;
  }

  private static ObjectNode responseHeaders(HttpResponse<byte[]> response) {
    ObjectNode headers = JsonNodeFactory.instance.objectNode();
    response.headers().map().forEach((name, values) -> headers.put(name, String.join(",", values)));
    return headers;
  }

  private JsonNode content(byte[] body, String contentType) {
    if (body.length == 0) {
      return JsonNodeFactory.instance.nullNode();
    }
    String mediaType = contentType.split(";", 2)[0].trim().toLowerCase(Locale.ROOT);
    if ("application/json".equals(mediaType) || mediaType.endsWith("+json")) {
      try {
        return json.readTree(body);
      } catch (Exception failure) {
        return JsonNodeFactory.instance.textNode(Base64.getEncoder().encodeToString(body));
      }
    }
    if (mediaType.startsWith("text/")
        || mediaType.endsWith("+xml")
        || "application/xml".equals(mediaType)) {
      return JsonNodeFactory.instance.textNode(new String(body, StandardCharsets.UTF_8));
    }
    return JsonNodeFactory.instance.textNode(Base64.getEncoder().encodeToString(body));
  }

  private byte[] body(JsonNode value) {
    if (value == null || value.isNull()) return new byte[0];
    if (value.isTextual()) {
      return value.textValue().getBytes(StandardCharsets.UTF_8);
    }
    try {
      return json.writeValueAsBytes(value);
    } catch (Exception failure) {
      throw new IllegalArgumentException("HTTP body cannot be serialized", failure);
    }
  }

  private String appendQuery(String endpoint, JsonNode query) {
    if (query == null || query.isNull() || query.isEmpty()) {
      return endpoint;
    }
    StringBuilder result = new StringBuilder(endpoint);
    result.append(endpoint.contains("?") ? '&' : '?');
    boolean first = true;
    var fields = query.properties().iterator();
    while (fields.hasNext()) {
      var entry = fields.next();
      if (!first) result.append('&');
      first = false;
      result
          .append(percentEncode(entry.getKey()))
          .append('=')
          .append(percentEncode(scalarText(entry.getValue(), "HTTP query " + entry.getKey())));
    }
    return result.toString();
  }

  private String interpolate(String template, JsonNode input, OperationRequest request) {
    Matcher matcher = TEMPLATE.matcher(template);
    StringBuffer result = new StringBuffer();
    while (matcher.find()) {
      JsonNode value = input.path(matcher.group(1));
      if (value.isContainerNode()) {
        throw expression(request, "URI template value " + matcher.group(1) + " must be scalar");
      }
      matcher.appendReplacement(
          result,
          Matcher.quoteReplacement(
              value.isMissingNode() || value.isNull()
                  ? ""
                  : percentEncode(scalarText(value, "URI template " + matcher.group(1)))));
    }
    matcher.appendTail(result);
    return result.toString();
  }

  private static String percentEncode(String value) {
    ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    for (byte octet : value.getBytes(StandardCharsets.UTF_8)) {
      int unsigned = octet & 0xff;
      if (unsigned >= 'a' && unsigned <= 'z'
          || unsigned >= 'A' && unsigned <= 'Z'
          || unsigned >= '0' && unsigned <= '9'
          || unsigned == '-'
          || unsigned == '.'
          || unsigned == '_'
          || unsigned == '~') {
        encoded.write(unsigned);
      } else {
        encoded.write('%');
        encoded.write(Character.toUpperCase(Character.forDigit(unsigned >>> 4, 16)));
        encoded.write(Character.toUpperCase(Character.forDigit(unsigned & 0xf, 16)));
      }
    }
    return encoded.toString(StandardCharsets.US_ASCII);
  }

  private static boolean containsHeader(ObjectNode headers, String expected) {
    var names = headers.fieldNames();
    while (names.hasNext()) {
      if (expected.equalsIgnoreCase(names.next())) return true;
    }
    return false;
  }

  private static String requiredText(JsonNode value, String name) {
    JsonNode property = value.required(name);
    if (!property.isTextual() || property.textValue().isBlank()) {
      throw new IllegalArgumentException(name + " must be non-blank text");
    }
    return property.textValue();
  }

  private static String scalarText(JsonNode value, String name) {
    if (value.isTextual()) return value.textValue();
    if (value.isNumber() || value.isBoolean() || value.isNull()) {
      return value.isNull() ? "" : value.asText();
    }
    throw new IllegalArgumentException(name + " must be scalar");
  }

  private static OperationObservation failed(
      OperationRequest request, int status, String title, String detail) {
    return new OperationObservation(
        OperationObservationStatus.FAILED,
        null,
        new WorkflowError(
            COMMUNICATION_ERROR,
            status,
            request.descriptor().required("taskPath").textValue(),
            title,
            detail),
        null);
  }

  private static AdapterFailure communication(
      OperationRequest request, int status, String title, String detail) {
    return new AdapterFailure(
        new WorkflowError(
            COMMUNICATION_ERROR,
            status,
            request.descriptor().required("taskPath").textValue(),
            title,
            detail));
  }

  private static AdapterFailure expression(OperationRequest request, String detail) {
    return new AdapterFailure(
        new WorkflowError(
            EXPRESSION_ERROR,
            400,
            request.descriptor().required("taskPath").textValue(),
            "URI template evaluation failed",
            detail));
  }

  private static String rootMessage(Throwable failure) {
    Throwable root = failure;
    while (root.getCause() != null) root = root.getCause();
    return root.getMessage() == null ? root.getClass().getName() : root.getMessage();
  }

  private static OperationObservation cancelled(OperationRequest request, String detail) {
    return new OperationObservation(
        OperationObservationStatus.CANCELLED,
        null,
        new WorkflowError(
            COMMUNICATION_ERROR, 499, request.operationId(), "HTTP call cancelled", detail),
        null);
  }

  private static final class ActiveInvocation {
    private final OperationRequest request;
    private final CompletableFuture<OperationObservation> result;
    private final Set<CompletableFuture<?>> transports = ConcurrentHashMap.newKeySet();

    private ActiveInvocation(
        OperationRequest request, CompletableFuture<OperationObservation> result) {
      this.request = request;
      this.result = result;
    }

    private void track(CompletableFuture<?> transport) {
      transports.add(transport);
      transport.whenComplete((ignored, failure) -> transports.remove(transport));
    }

    private void cancel(String detail) {
      result.complete(cancelled(request, detail));
      transports.forEach(transport -> transport.cancel(true));
      transports.clear();
    }
  }

  private record Prepared(
      HttpRequest request,
      String method,
      ObjectNode requestHeaders,
      String output,
      boolean acceptRedirects,
      byte[] body) {
    private Prepared {
      body = body.clone();
    }

    @Override
    public byte[] body() {
      return body.clone();
    }
  }

  private static final class AdapterFailure extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final transient WorkflowError error;

    AdapterFailure(WorkflowError error) {
      super(error.detail());
      this.error = error;
    }

    WorkflowError error() {
      return error;
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
