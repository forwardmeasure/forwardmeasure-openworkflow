package com.forwardmeasure.openworkflow.operation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.definition.AuthenticationPlan;
import com.forwardmeasure.openworkflow.engine.api.AuthenticationExpressionContext;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import java.net.URI;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Shared OAuth/OIDC acquisition and HTTP Digest challenge implementation. */
public final class HttpAuthenticationSupport {
  private final HttpClient client;
  private final ObjectMapper json;
  private final Duration timeout;
  private final HttpEgressPolicy egress;
  private final ProtocolAuthenticationResolver credentials;

  public HttpAuthenticationSupport(
      HttpClient client,
      ObjectMapper json,
      Duration timeout,
      HttpEgressPolicy egress,
      SecretProvider secrets) {
    this.client = Objects.requireNonNull(client, "client");
    this.json = Objects.requireNonNull(json, "json");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    this.egress = Objects.requireNonNull(egress, "egress");
    this.credentials =
        new ProtocolAuthenticationResolver(Objects.requireNonNull(secrets, "secrets"));
    if (timeout.isZero() || timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must be positive");
    }
  }

  public CompletionStage<Credential> resolve(
      ExecutionId executionId,
      AuthenticationPlan authentication,
      AuthenticationExpressionContext context,
      String operationId) {
    Objects.requireNonNull(executionId, "executionId");
    if (authentication == null) {
      return CompletableFuture.completedFuture(null);
    }
    if (!authentication.secretBacked()
        && (authentication.kind() == AuthenticationPlan.Kind.OAUTH2
            || authentication.kind() == AuthenticationPlan.Kind.OIDC)) {
      JsonNode configuration =
          credentials.materializeConfiguration(executionId, authentication, context);
      if (configuration.path("token").isTextual()
          && !configuration.path("token").asText().isBlank()) {
        return CompletableFuture.completedFuture(
            new Credential(
                authentication.kind(),
                "Bearer " + configuration.required("token").asText(),
                null,
                null));
      }
      return acquireToken(
              executionId,
              authentication.kind(),
              configuration,
              requireText(operationId, "operationId"))
          .thenApply(token -> new Credential(authentication.kind(), "Bearer " + token, null, null));
    }
    ProtocolAuthenticationResolver.Credential resolved =
        credentials.resolve(executionId, authentication, context);
    return CompletableFuture.completedFuture(
        new Credential(
            resolved.kind(), resolved.authorization(),
            resolved.username(), resolved.password()));
  }

  public String digestAuthorization(
      String method,
      URI uri,
      byte[] body,
      Credential credential,
      String challenge,
      String operationId) {
    Objects.requireNonNull(credential, "credential");
    if (credential.kind() != AuthenticationPlan.Kind.DIGEST) {
      throw new IllegalArgumentException("Digest credential is required");
    }
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
    String requestTarget = uri.getRawPath();
    if (requestTarget == null || requestTarget.isEmpty()) requestTarget = "/";
    if (uri.getRawQuery() != null) requestTarget += "?" + uri.getRawQuery();
    String cnonce = digestHex("SHA-256", operationId + "|" + nonce).substring(0, 32);
    String ha1 =
        digestHex(algorithm, credential.username() + ":" + realm + ":" + credential.password());
    if (algorithmToken.toLowerCase(Locale.ROOT).endsWith("-sess")) {
      ha1 = digestHex(algorithm, ha1 + ":" + nonce + ":" + cnonce);
    }
    String entityHash = digestHex(algorithm, body);
    String ha2 =
        digestHex(
            algorithm,
            method + ":" + requestTarget + ("auth-int".equals(qop) ? ":" + entityHash : ""));
    String response =
        qop == null
            ? digestHex(algorithm, ha1 + ":" + nonce + ":" + ha2)
            : digestHex(
                algorithm, ha1 + ":" + nonce + ":00000001:" + cnonce + ":" + qop + ":" + ha2);
    StringBuilder header =
        new StringBuilder("Digest username=\"")
            .append(quoted(credential.username()))
            .append("\", realm=\"")
            .append(quoted(realm))
            .append("\", nonce=\"")
            .append(quoted(nonce))
            .append("\", uri=\"")
            .append(quoted(requestTarget))
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
      AuthenticationPlan.Kind kind,
      JsonNode configuration,
      String operationId) {
    URI authority = URI.create(uriText(configuration.required("authority")));
    CompletionStage<URI> endpoint;
    if (kind == AuthenticationPlan.Kind.OIDC) {
      URI discovery =
          URI.create(
              authority.toString().replaceAll("/+$", "") + "/.well-known/openid-configuration");
      egress.authorize(executionId.tenantId(), discovery);
      endpoint =
          client
              .sendAsync(
                  HttpRequest.newBuilder(discovery).timeout(timeout).GET().build(),
                  HttpResponse.BodyHandlers.ofByteArray())
              .thenApply(
                  response -> {
                    if (response.statusCode() < 200 || response.statusCode() >= 300) {
                      throw new IllegalArgumentException(
                          "OIDC discovery returned status " + response.statusCode());
                    }
                    try {
                      return URI.create(
                          json.readTree(response.body()).required("token_endpoint").asText());
                    } catch (Exception failure) {
                      throw new IllegalArgumentException(
                          "OIDC discovery document is invalid", failure);
                    }
                  });
    } else {
      endpoint =
          CompletableFuture.completedFuture(
              authority.resolve(
                  configuration.path("endpoints").path("token").asText("/oauth2/token")));
    }
    return endpoint.thenCompose(
        tokenEndpoint -> requestToken(executionId, configuration, tokenEndpoint, operationId));
  }

  private CompletionStage<String> requestToken(
      ExecutionId executionId, JsonNode configuration, URI tokenEndpoint, String operationId) {
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
        if (clientId == null || clientSecret == null)
          throw new IllegalArgumentException("client_secret_basic requires client id and secret");
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
            .header("Idempotency-Key", operationId + "-token")
            .header("X-OpenWorkflow-Tenant", executionId.tenantId().value().toString());
    if (authorization != null) request.header("Authorization", authorization);
    return client
        .sendAsync(
            request.POST(HttpRequest.BodyPublishers.ofByteArray(payload)).build(),
            HttpResponse.BodyHandlers.ofByteArray())
        .thenApply(
            response -> {
              if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalArgumentException(
                    "OAuth token endpoint returned status " + response.statusCode());
              }
              try {
                JsonNode token = json.readTree(response.body());
                if (!"bearer".equalsIgnoreCase(token.path("token_type").asText("Bearer"))) {
                  throw new IllegalArgumentException("Unsupported OAuth token type");
                }
                return requiredText(token, "access_token");
              } catch (Exception failure) {
                throw new IllegalArgumentException("OAuth token response is invalid", failure);
              }
            });
  }

  private static Map<String, java.util.List<String>> tokenFields(JsonNode config) {
    Map<String, java.util.List<String>> fields = new LinkedHashMap<>();
    String grant = requiredText(config, "grant");
    add(fields, "grant_type", grant);
    if (config.path("scopes").isArray()) add(fields, "scope", join(config.path("scopes")));
    config.path("audiences").forEach(value -> add(fields, "audience", value.asText()));
    switch (grant) {
      case "password" -> {
        add(fields, "username", requiredText(config, "username"));
        add(fields, "password", requiredText(config, "password"));
      }
      case "urn:ietf:params:oauth:grant-type:token-exchange" -> {
        JsonNode subject = config.required("subject");
        add(fields, "subject_token", requiredText(subject, "token"));
        add(fields, "subject_token_type", requiredText(subject, "type"));
        if (config.has("actor")) {
          add(fields, "actor_token", requiredText(config.path("actor"), "token"));
          add(fields, "actor_token_type", requiredText(config.path("actor"), "type"));
        }
      }
      case "refresh_token" ->
          add(fields, "refresh_token", requiredText(config.path("subject"), "token"));
      case "authorization_code" ->
          throw new IllegalArgumentException(
              "authorization_code requires an interactive authorization provider");
      case "client_credentials" -> {}
      default -> throw new IllegalArgumentException("Unsupported OAuth grant: " + grant);
    }
    return fields;
  }

  private static Map<String, String> challengeParameters(String challenge) {
    String value =
        challenge.regionMatches(true, 0, "Digest ", 0, 7) ? challenge.substring(7) : challenge;
    Map<String, String> result = new LinkedHashMap<>();
    var matcher =
        java.util.regex.Pattern.compile(
                "([A-Za-z][A-Za-z0-9_-]*)\\s*=\\s*(?:\\\"((?:\\\\.|[^\\\"])*)\\\"|([^,\\s]+))")
            .matcher(value);
    while (matcher.find())
      result.put(
          matcher.group(1).toLowerCase(Locale.ROOT),
          matcher.group(2) != null ? matcher.group(2).replace("\\\"", "\"") : matcher.group(3));
    return result;
  }

  private static String selectQop(String offered) {
    if (offered == null || offered.isBlank()) return null;
    var values =
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

  private static String basic(String username, String password) {
    return "Basic "
        + Base64.getEncoder()
            .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  private static String uriText(JsonNode value) {
    if (value.isTextual()) return value.asText();
    if (value.isObject() && value.hasNonNull("uri")) return value.required("uri").asText();
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

  private static String form(String value) {
    return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8);
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

  private static String requiredText(JsonNode configuration, String name) {
    JsonNode value = configuration.get(name);
    if (value == null || !value.isTextual() || value.textValue().isBlank()) {
      throw new IllegalArgumentException(
          "Authentication field '" + name + "' must evaluate to non-blank text");
    }
    return value.textValue();
  }

  private static String required(Map<String, String> values, String name) {
    String value = values.get(name);
    if (value == null || value.isBlank())
      throw new IllegalArgumentException("Digest challenge is missing " + name);
    return value;
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
    return value;
  }

  private static String quoted(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  public record Credential(
      AuthenticationPlan.Kind kind, String authorization, String username, String password) {
    public Credential {
      Objects.requireNonNull(kind, "kind");
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
