package com.forwardmeasure.openworkflow.operation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.definition.AuthenticationPlan;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.HttpOperationDescriptor;
import com.forwardmeasure.openworkflow.expression.ExpressionMode;
import com.forwardmeasure.openworkflow.expression.JqRuntimeExpressionEvaluator;
import com.forwardmeasure.openworkflow.expression.RuntimeExpressionArguments;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** JDK HTTP adapter with no redirect following and an explicit egress/secret boundary. */
public final class JdkHttpOperationExecutor implements HttpOperationExecutor {
  private static final Set<String> RESTRICTED_HEADERS =
      Set.of("host", "content-length", "connection", "upgrade");
  private final HttpClient client;
  private final ObjectMapper json;
  private final Duration timeout;
  private final SecretProvider secrets;
  private final HttpEgressPolicy egress;
  private final JqRuntimeExpressionEvaluator expressions = new JqRuntimeExpressionEvaluator();

  public JdkHttpOperationExecutor(
      ObjectMapper json, Duration timeout, SecretProvider secrets, HttpEgressPolicy egress) {
    this(
        HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NEVER)
            .connectTimeout(timeout)
            .build(),
        json,
        timeout,
        secrets,
        egress);
  }

  public JdkHttpOperationExecutor(
      HttpClient client,
      ObjectMapper json,
      Duration timeout,
      SecretProvider secrets,
      HttpEgressPolicy egress) {
    this.client = Objects.requireNonNull(client, "client");
    this.json = Objects.requireNonNull(json, "json");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.secrets = Objects.requireNonNull(secrets, "secrets");
    this.egress = Objects.requireNonNull(egress, "egress");
    if (timeout.isZero() || timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must be positive");
    }
  }

  @Override
  public CompletionStage<HttpOperationResult> execute(
      ExecutionId executionId, HttpOperationDescriptor operation) {
    try {
      egress.authorize(executionId.tenantId(), operation.uri());
      byte[] body = body(operation);
      ResolvedAuthentication authentication = resolveAuthentication(executionId, operation);
      CompletionStage<HttpResponse<byte[]>> responseStage;
      if (authentication != null
          && (authentication.kind() == AuthenticationPlan.Kind.OAUTH2
              || authentication.kind() == AuthenticationPlan.Kind.OIDC)
          && authentication.configuration() != null) {
        responseStage =
            acquireToken(executionId, operation, authentication)
                .thenCompose(token -> send(operation, executionId, body, "Bearer " + token));
      } else if (authentication != null
          && authentication.kind() == AuthenticationPlan.Kind.DIGEST) {
        responseStage = sendDigest(operation, executionId, body, authentication);
      } else {
        responseStage =
            send(
                operation,
                executionId,
                body,
                authentication == null ? null : authentication.authorization());
      }
      return responseStage
          .<HttpOperationResult>thenApply(response -> result(operation, response))
          .exceptionally(
              failure ->
                  HttpOperationResult.failure(
                      problem(502, "HTTP transport failure", rootMessage(failure), operation)));
    } catch (Exception failure) {
      return CompletableFuture.completedFuture(
          HttpOperationResult.failure(
              problem(
                  failure instanceof SecurityException ? 403 : 500,
                  "HTTP operation rejected",
                  rootMessage(failure),
                  operation)));
    }
  }

  private CompletionStage<HttpResponse<byte[]>> send(
      HttpOperationDescriptor operation,
      ExecutionId executionId,
      byte[] body,
      String authorization) {
    return client.sendAsync(
        request(operation, executionId, body, authorization),
        HttpResponse.BodyHandlers.ofByteArray());
  }

  private HttpRequest request(
      HttpOperationDescriptor operation,
      ExecutionId executionId,
      byte[] body,
      String authorization) {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(operation.uri())
            .timeout(timeout)
            .header("Idempotency-Key", operation.operationId())
            .header("X-OpenWorkflow-Tenant", executionId.tenantId().value().toString());
    operation
        .headers()
        .forEach(
            (name, value) -> {
              if (RESTRICTED_HEADERS.contains(name.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException("Restricted HTTP header: " + name);
              }
              request.header(name, value);
            });
    if (authorization != null) request.header("Authorization", authorization);
    if (body.length > 0 && header(operation, "content-type") == null) {
      request.header("Content-Type", "application/json");
    }
    request.method(
        operation.method(),
        body.length == 0
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofByteArray(body));
    return request.build();
  }

  private byte[] body(HttpOperationDescriptor operation) throws Exception {
    JsonNode body = operation.body();
    if (body.isNull()) return new byte[0];
    String mediaType = header(operation, "content-type");
    if (mediaType != null
        && mediaType.toLowerCase(Locale.ROOT).startsWith("application/x-www-form-urlencoded")
        && body.isObject()) {
      var fields = new java.util.ArrayList<String>();
      body.properties()
          .forEach(
              entry -> {
                if (entry.getValue().isArray()) {
                  entry
                      .getValue()
                      .forEach(
                          value -> fields.add(form(entry.getKey()) + "=" + form(value.asText())));
                } else {
                  fields.add(form(entry.getKey()) + "=" + form(entry.getValue().asText()));
                }
              });
      return String.join("&", fields).getBytes(StandardCharsets.UTF_8);
    }
    if (mediaType != null
        && !mediaType.toLowerCase(Locale.ROOT).contains("json")
        && body.isTextual()) {
      return body.textValue().getBytes(StandardCharsets.UTF_8);
    }
    return json.writeValueAsBytes(body);
  }

  private static String header(HttpOperationDescriptor operation, String wanted) {
    return operation.headers().entrySet().stream()
        .filter(entry -> entry.getKey().equalsIgnoreCase(wanted))
        .map(java.util.Map.Entry::getValue)
        .findFirst()
        .orElse(null);
  }

  private static String form(String value) {
    return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private ResolvedAuthentication resolveAuthentication(
      ExecutionId executionId, HttpOperationDescriptor operation) {
    AuthenticationPlan authentication = operation.authentication();
    if (authentication == null) return null;
    if (!authentication.secretBacked()) {
      JsonNode configuration =
          materializeAuthentication(executionId, authentication, operation.authenticationContext());
      return switch (authentication.kind()) {
        case BASIC ->
            new ResolvedAuthentication(
                authentication.kind(),
                basic(
                    requiredText(configuration, "username"),
                    requiredText(configuration, "password")),
                null,
                null,
                null);
        case BEARER ->
            new ResolvedAuthentication(
                authentication.kind(),
                "Bearer " + requiredText(configuration, "token"),
                null,
                null,
                null);
        case DIGEST ->
            new ResolvedAuthentication(
                authentication.kind(),
                null,
                requiredText(configuration, "username"),
                requiredText(configuration, "password"),
                null);
        case OAUTH2, OIDC ->
            new ResolvedAuthentication(authentication.kind(), null, null, null, configuration);
      };
    }
    char[] value = secrets.resolve(executionId.tenantId(), authentication.secretName());
    try {
      String credential = new String(value);
      if (authentication.kind() == AuthenticationPlan.Kind.DIGEST) {
        int delimiter = credential.indexOf(':');
        if (delimiter < 1 || delimiter == credential.length() - 1) {
          throw new IllegalArgumentException("Digest secret must contain username:password");
        }
        return new ResolvedAuthentication(
            authentication.kind(),
            null,
            credential.substring(0, delimiter),
            credential.substring(delimiter + 1),
            null);
      }
      return new ResolvedAuthentication(
          authentication.kind(),
          authentication.kind() == AuthenticationPlan.Kind.BASIC
              ? basic(credential)
              : "Bearer " + credential,
          null,
          null,
          null);
    } finally {
      Arrays.fill(value, '\0');
    }
  }

  private JsonNode materializeAuthentication(
      ExecutionId executionId,
      AuthenticationPlan authentication,
      com.forwardmeasure.openworkflow.engine.api.AuthenticationExpressionContext context) {
    if (context == null) {
      throw new IllegalArgumentException("Expression-backed authentication context is absent");
    }
    ObjectNode resolvedSecrets = JsonNodeFactory.instance.objectNode();
    var sensitive = new java.util.ArrayList<char[]>();
    try {
      for (String reference : authentication.secretReferences()) {
        char[] value = secrets.resolve(executionId.tenantId(), reference);
        sensitive.add(value);
        resolvedSecrets.put(reference, new String(value));
      }
      RuntimeExpressionArguments arguments =
          new RuntimeExpressionArguments(
              context.context(),
              context.input(),
              context.output(),
              resolvedSecrets,
              context.authorization(),
              context.task(),
              context.workflow(),
              context.runtime(),
              context.variables());
      return expressions.evaluateTemplate(
          authentication.expressionConfiguration(),
          context.input(),
          arguments,
          ExpressionMode.STRICT);
    } finally {
      sensitive.forEach(value -> Arrays.fill(value, '\0'));
      resolvedSecrets.removeAll();
    }
  }

  private static String requiredText(JsonNode configuration, String name) {
    JsonNode value = configuration.get(name);
    if (value == null || !value.isTextual() || value.textValue().isBlank()) {
      throw new IllegalArgumentException(
          "Authentication field '" + name + "' must evaluate to non-blank text");
    }
    return value.textValue();
  }

  private static String basic(String username, String password) {
    return basic(username + ":" + password);
  }

  private static String basic(String credential) {
    return "Basic "
        + Base64.getEncoder().encodeToString(credential.getBytes(StandardCharsets.UTF_8));
  }

  private CompletionStage<HttpResponse<byte[]>> sendDigest(
      HttpOperationDescriptor operation,
      ExecutionId executionId,
      byte[] body,
      ResolvedAuthentication authentication) {
    return send(operation, executionId, body, null)
        .thenCompose(
            response -> {
              if (response.statusCode() != 401) {
                return CompletableFuture.completedFuture(response);
              }
              String challenge =
                  response.headers().allValues("www-authenticate").stream()
                      .filter(value -> value.regionMatches(true, 0, "Digest ", 0, 7))
                      .findFirst()
                      .orElseThrow(
                          () ->
                              new IllegalArgumentException(
                                  "Digest endpoint returned 401 without a Digest challenge"));
              String authorization =
                  digestAuthorization(operation, body, authentication, challenge.substring(7));
              return send(operation, executionId, body, authorization);
            });
  }

  private String digestAuthorization(
      HttpOperationDescriptor operation,
      byte[] body,
      ResolvedAuthentication authentication,
      String challenge) {
    Map<String, String> parameters = challengeParameters(challenge);
    String realm = required(parameters, "realm");
    String nonce = required(parameters, "nonce");
    String opaque = parameters.get("opaque");
    String algorithmToken = parameters.getOrDefault("algorithm", "MD5");
    String algorithm =
        switch (algorithmToken.toUpperCase(Locale.ROOT)) {
          case "MD5", "MD5-SESS" -> "MD5";
          case "SHA-256", "SHA-256-SESS" -> "SHA-256";
          case "SHA-512-256", "SHA-512-256-SESS" -> "SHA-512/256";
          default ->
              throw new IllegalArgumentException("Unsupported Digest algorithm: " + algorithmToken);
        };
    String qop = selectQop(parameters.get("qop"));
    String uri = operation.uri().getRawPath();
    if (uri == null || uri.isEmpty()) uri = "/";
    if (operation.uri().getRawQuery() != null) {
      uri += "?" + operation.uri().getRawQuery();
    }
    String cnonce = digestHex("SHA-256", operation.operationId() + "|" + nonce).substring(0, 32);
    String ha1 =
        digestHex(
            algorithm, authentication.username() + ":" + realm + ":" + authentication.password());
    if (algorithmToken.toLowerCase(Locale.ROOT).endsWith("-sess")) {
      ha1 = digestHex(algorithm, ha1 + ":" + nonce + ":" + cnonce);
    }
    String entityHash = digestHex(algorithm, body);
    String ha2 =
        digestHex(
            algorithm,
            operation.method() + ":" + uri + ("auth-int".equals(qop) ? ":" + entityHash : ""));
    String response =
        qop == null
            ? digestHex(algorithm, ha1 + ":" + nonce + ":" + ha2)
            : digestHex(
                algorithm, ha1 + ":" + nonce + ":00000001:" + cnonce + ":" + qop + ":" + ha2);
    StringBuilder header =
        new StringBuilder("Digest username=\"")
            .append(quoted(authentication.username()))
            .append("\", realm=\"")
            .append(quoted(realm))
            .append("\", nonce=\"")
            .append(quoted(nonce))
            .append("\", uri=\"")
            .append(quoted(uri))
            .append("\", response=\"")
            .append(response)
            .append('"')
            .append(", algorithm=")
            .append(algorithmToken);
    if (opaque != null) header.append(", opaque=\"").append(quoted(opaque)).append('"');
    if (qop != null)
      header
          .append(", qop=")
          .append(qop)
          .append(", nc=00000001, cnonce=\"")
          .append(cnonce)
          .append('"');
    return header.toString();
  }

  private CompletionStage<String> acquireToken(
      ExecutionId executionId,
      HttpOperationDescriptor operation,
      ResolvedAuthentication authentication) {
    JsonNode configuration = authentication.configuration();
    java.net.URI authority = java.net.URI.create(uriText(configuration.required("authority")));
    CompletionStage<java.net.URI> endpoint;
    if (authentication.kind() == AuthenticationPlan.Kind.OIDC) {
      String base = authority.toString().replaceAll("/+$", "");
      java.net.URI discovery = java.net.URI.create(base + "/.well-known/openid-configuration");
      egress.authorize(executionId.tenantId(), discovery);
      HttpRequest request = HttpRequest.newBuilder(discovery).timeout(timeout).GET().build();
      endpoint =
          client
              .sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
              .thenApply(
                  response -> {
                    if (response.statusCode() < 200 || response.statusCode() >= 300) {
                      throw new IllegalArgumentException(
                          "OIDC discovery returned status " + response.statusCode());
                    }
                    try {
                      return java.net.URI.create(
                          json.readTree(response.body()).required("token_endpoint").asText());
                    } catch (Exception failure) {
                      throw new IllegalArgumentException(
                          "OIDC discovery document is invalid", failure);
                    }
                  });
    } else {
      String tokenPath = configuration.path("endpoints").path("token").asText("/oauth2/token");
      endpoint = CompletableFuture.completedFuture(authority.resolve(tokenPath));
    }
    return endpoint.thenCompose(
        tokenEndpoint -> requestToken(executionId, operation, configuration, tokenEndpoint));
  }

  private CompletionStage<String> requestToken(
      ExecutionId executionId,
      HttpOperationDescriptor operation,
      JsonNode configuration,
      java.net.URI tokenEndpoint) {
    egress.authorize(executionId.tenantId(), tokenEndpoint);
    Map<String, java.util.List<String>> fields = tokenFields(configuration);
    JsonNode clientConfiguration = configuration.path("client");
    String clientId = optionalText(clientConfiguration, "id");
    String clientSecret = optionalText(clientConfiguration, "secret");
    String clientAssertion = optionalText(clientConfiguration, "assertion");
    String method = clientConfiguration.path("authentication").asText("client_secret_post");
    String authorization = null;
    if (clientId != null) add(fields, "client_id", clientId);
    switch (method) {
      case "client_secret_basic" -> {
        if (clientId == null || clientSecret == null) {
          throw new IllegalArgumentException("client_secret_basic requires client id and secret");
        }
        authorization = basic(clientId, clientSecret);
        fields.remove("client_id");
      }
      case "client_secret_post" -> {
        if (clientSecret != null) add(fields, "client_secret", clientSecret);
      }
      case "client_secret_jwt", "private_key_jwt" -> {
        if (clientAssertion == null)
          throw new IllegalArgumentException(method + " requires client.assertion");
        add(fields, "client_assertion", clientAssertion);
        add(
            fields,
            "client_assertion_type",
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer");
      }
      case "none" -> {}
      default ->
          throw new IllegalArgumentException("Unsupported OAuth client authentication: " + method);
    }
    String encoding =
        configuration.path("request").path("encoding").asText("application/x-www-form-urlencoded");
    byte[] payload;
    try {
      payload =
          "application/json".equalsIgnoreCase(encoding)
              ? json.writeValueAsBytes(fieldsToJson(fields))
              : formBody(fields).getBytes(StandardCharsets.UTF_8);
    } catch (Exception failure) {
      throw new IllegalArgumentException("OAuth token request cannot be encoded", failure);
    }
    HttpRequest.Builder request =
        HttpRequest.newBuilder(tokenEndpoint)
            .timeout(timeout)
            .header("Content-Type", encoding)
            .header("Accept", "application/json")
            .header("Idempotency-Key", operation.operationId() + "-token")
            .header("X-OpenWorkflow-Tenant", executionId.tenantId().value().toString());
    if (authorization != null) request.header("Authorization", authorization);
    request.POST(HttpRequest.BodyPublishers.ofByteArray(payload));
    return client
        .sendAsync(request.build(), HttpResponse.BodyHandlers.ofByteArray())
        .thenApply(
            response -> {
              if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalArgumentException(
                    "OAuth token endpoint returned status " + response.statusCode());
              }
              try {
                JsonNode token = json.readTree(response.body());
                String type = token.path("token_type").asText("Bearer");
                if (!"bearer".equalsIgnoreCase(type)) {
                  throw new IllegalArgumentException("Unsupported OAuth token type: " + type);
                }
                return requiredText(token, "access_token");
              } catch (Exception failure) {
                throw new IllegalArgumentException("OAuth token response is invalid", failure);
              }
            });
  }

  private static Map<String, java.util.List<String>> tokenFields(JsonNode configuration) {
    Map<String, java.util.List<String>> fields = new LinkedHashMap<>();
    String grant = requiredText(configuration, "grant");
    add(fields, "grant_type", grant);
    if (configuration.path("scopes").isArray()) {
      add(fields, "scope", join(configuration.path("scopes")));
    }
    configuration.path("audiences").forEach(value -> add(fields, "audience", value.asText()));
    switch (grant) {
      case "password" -> {
        add(fields, "username", requiredText(configuration, "username"));
        add(fields, "password", requiredText(configuration, "password"));
      }
      case "urn:ietf:params:oauth:grant-type:token-exchange" -> {
        JsonNode subject = configuration.required("subject");
        add(fields, "subject_token", requiredText(subject, "token"));
        add(fields, "subject_token_type", requiredText(subject, "type"));
        if (configuration.has("actor")) {
          add(fields, "actor_token", requiredText(configuration.path("actor"), "token"));
          add(fields, "actor_token_type", requiredText(configuration.path("actor"), "type"));
        }
      }
      case "refresh_token" -> {
        JsonNode subject = configuration.path("subject");
        add(fields, "refresh_token", requiredText(subject, "token"));
      }
      case "authorization_code" ->
          throw new IllegalArgumentException(
              "authorization_code requires an interactive authorization provider");
      case "client_credentials" -> {}
      default -> throw new IllegalArgumentException("Unsupported OAuth grant: " + grant);
    }
    return fields;
  }

  private static Map<String, String> challengeParameters(String challenge) {
    Map<String, String> result = new LinkedHashMap<>();
    java.util.regex.Matcher matcher =
        java.util.regex.Pattern.compile(
                "([A-Za-z][A-Za-z0-9_-]*)\\s*=\\s*(?:\\\"((?:\\\\.|[^\\\"])*)\\\"|([^,\\s]+))")
            .matcher(challenge);
    while (matcher.find()) {
      result.put(
          matcher.group(1).toLowerCase(Locale.ROOT),
          matcher.group(2) != null ? matcher.group(2).replace("\\\"", "\"") : matcher.group(3));
    }
    return result;
  }

  private static String selectQop(String offered) {
    if (offered == null || offered.isBlank()) return null;
    java.util.List<String> values =
        Arrays.stream(offered.split(","))
            .map(String::trim)
            .map(value -> value.toLowerCase(Locale.ROOT))
            .toList();
    if (values.contains("auth")) return "auth";
    if (values.contains("auth-int")) return "auth-int";
    throw new IllegalArgumentException("Unsupported Digest qop: " + offered);
  }

  private static String digestHex(String algorithm, String value) {
    return digestHex(algorithm, value.getBytes(StandardCharsets.ISO_8859_1));
  }

  private static String digestHex(String algorithm, byte[] value) {
    try {
      return java.util.HexFormat.of().formatHex(MessageDigest.getInstance(algorithm).digest(value));
    } catch (Exception failure) {
      throw new IllegalArgumentException("Digest algorithm is unavailable: " + algorithm, failure);
    }
  }

  private static String uriText(JsonNode value) {
    if (value.isTextual()) return value.asText();
    if (value.isObject() && value.hasNonNull("uri")) {
      return value.required("uri").asText();
    }
    throw new IllegalArgumentException("Authentication authority must be a URI");
  }

  private static String optionalText(JsonNode node, String name) {
    JsonNode value = node.get(name);
    return value == null || value.isNull() || value.asText().isBlank() ? null : value.asText();
  }

  private static void add(Map<String, java.util.List<String>> fields, String name, String value) {
    fields.computeIfAbsent(name, ignored -> new java.util.ArrayList<>()).add(value);
  }

  private static String join(JsonNode values) {
    var result = new java.util.ArrayList<String>();
    values.forEach(value -> result.add(value.asText()));
    return String.join(" ", result);
  }

  private static String formBody(Map<String, java.util.List<String>> fields) {
    var values = new java.util.ArrayList<String>();
    fields.forEach(
        (name, entries) -> entries.forEach(value -> values.add(form(name) + "=" + form(value))));
    return String.join("&", values);
  }

  private static ObjectNode fieldsToJson(Map<String, java.util.List<String>> fields) {
    ObjectNode value = JsonNodeFactory.instance.objectNode();
    fields.forEach(
        (name, entries) -> {
          if (entries.size() == 1) value.put(name, entries.getFirst());
          else {
            var array = value.putArray(name);
            entries.forEach(array::add);
          }
        });
    return value;
  }

  private static String required(Map<String, String> values, String name) {
    String value = values.get(name);
    if (value == null || value.isBlank())
      throw new IllegalArgumentException("Digest challenge is missing " + name);
    return value;
  }

  private static String quoted(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private record ResolvedAuthentication(
      AuthenticationPlan.Kind kind,
      String authorization,
      String username,
      String password,
      JsonNode configuration) {}

  private HttpOperationResult result(
      HttpOperationDescriptor operation, HttpResponse<byte[]> response) {
    int status = response.statusCode();
    boolean failed = status >= 400 || operation.redirectAsError() && status >= 300 && status < 400;
    JsonNode content = content(response);
    if (failed)
      return HttpOperationResult.failure(
          problem(
              status,
              "HTTP response status " + status,
              content.isTextual() ? content.asText() : content.toString(),
              operation));
    return HttpOperationResult.success(
        switch (operation.output()) {
          case RAW ->
              JsonNodeFactory.instance.textNode(
                  new String(response.body(), StandardCharsets.UTF_8));
          case CONTENT -> content;
          case RESPONSE -> response(operation, response, content);
        });
  }

  private JsonNode content(HttpResponse<byte[]> response) {
    if (response.body().length == 0) return JsonNodeFactory.instance.nullNode();
    String mediaType =
        response.headers().firstValue("content-type").orElse("text/plain").toLowerCase(Locale.ROOT);
    if (mediaType.contains("json"))
      try {
        return json.readTree(response.body());
      } catch (Exception ignored) {
        // Invalid declared JSON is retained as text rather than discarded.
      }
    return JsonNodeFactory.instance.textNode(new String(response.body(), StandardCharsets.UTF_8));
  }

  private static JsonNode response(
      HttpOperationDescriptor operation, HttpResponse<byte[]> response, JsonNode content) {
    ObjectNode value = JsonNodeFactory.instance.objectNode();
    ObjectNode request =
        value
            .putObject("request")
            .put("method", operation.method())
            .put("uri", operation.uri().toString());
    ObjectNode requestHeaders = request.putObject("headers");
    operation.headers().forEach(requestHeaders::put);
    value.put("statusCode", response.statusCode());
    ObjectNode headers = value.putObject("headers");
    response
        .headers()
        .map()
        .forEach(
            (name, values) -> {
              var array = headers.putArray(name);
              values.forEach(array::add);
            });
    value.set("content", content);
    return value;
  }

  private static ObjectNode problem(
      int status, String title, String detail, HttpOperationDescriptor operation) {
    return JsonNodeFactory.instance
        .objectNode()
        .put("type", "urn:openworkflow:http:status:" + status)
        .put("status", status)
        .put("title", title)
        .put("detail", detail == null ? title : detail)
        .put("instance", "urn:openworkflow:operation:" + operation.operationId());
  }

  private static String rootMessage(Throwable failure) {
    Throwable current = failure;
    while (current.getCause() != null) current = current.getCause();
    return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
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
