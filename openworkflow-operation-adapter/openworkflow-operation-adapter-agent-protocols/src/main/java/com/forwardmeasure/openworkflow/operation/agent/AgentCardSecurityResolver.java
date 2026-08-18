package com.forwardmeasure.openworkflow.operation.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.operation.HttpAuthenticationSupport;
import com.forwardmeasure.openworkflow.operation.SecretProvider;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Selects the first satisfiable AgentCard security requirement at the tenant edge. */
final class AgentCardSecurityResolver {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final SecretProvider secrets;
  private final HttpClient defaultClient;
  private final AgentCardHttpClientProvider mutualTls;

  AgentCardSecurityResolver(SecretProvider secrets) {
    this(secrets, HttpClient.newHttpClient(), AgentCardHttpClientProvider.rejecting());
  }

  AgentCardSecurityResolver(
      SecretProvider secrets, HttpClient defaultClient, AgentCardHttpClientProvider mutualTls) {
    this.secrets = Objects.requireNonNull(secrets, "secrets");
    this.defaultClient = Objects.requireNonNull(defaultClient, "defaultClient");
    this.mutualTls = Objects.requireNonNull(mutualTls, "mutualTls");
  }

  Selection select(TenantId tenantId, ProtocolOperationDescriptor operation) {
    if (operation.kind() != ProtocolOperationDescriptor.Kind.A2A
        || operation.protocolSchema() == null) {
      return Selection.anonymous(operation.endpoint(), defaultClient);
    }
    JsonNode card;
    try {
      card = JSON.readTree(operation.protocolSchema());
    } catch (Exception failure) {
      throw new IllegalArgumentException("Pinned AgentCard is invalid", failure);
    }
    JsonNode requirements =
        card.has("security") ? card.path("security") : card.path("securityRequirements");
    if (requirements.isMissingNode()
        || requirements.isNull()
        || (requirements.isArray() && requirements.isEmpty())) {
      return Selection.anonymous(operation.endpoint(), defaultClient);
    }
    if (!requirements.isArray())
      throw new IllegalArgumentException("AgentCard security requirements must be an array");
    JsonNode schemes = card.path("securitySchemes");
    if (!schemes.isObject())
      throw new IllegalArgumentException(
          "AgentCard security requirements have no securitySchemes map");
    var failures = new ArrayList<String>();
    for (JsonNode candidate : requirements) {
      try {
        return selectRequirement(
            tenantId, operation.endpoint(), schemes, requirementSchemes(candidate));
      } catch (RuntimeException unavailable) {
        failures.add(unavailable.getMessage());
      }
    }
    throw new SecurityException(
        "No AgentCard security requirement is satisfiable: " + String.join("; ", failures));
  }

  private Selection selectRequirement(
      TenantId tenantId, URI endpoint, JsonNode definitions, JsonNode requirement) {
    if (!requirement.isObject())
      throw new IllegalArgumentException("AgentCard security requirement must be an object");
    if (requirement.isEmpty()) return Selection.anonymous(endpoint, defaultClient);
    Map<String, String> headers = new LinkedHashMap<>();
    Map<String, String> query = new LinkedHashMap<>();
    var cookies = new ArrayList<String>();
    var names = new ArrayList<String>();
    requirement.properties().forEach(entry -> names.add(entry.getKey()));
    HttpClient selectedClient = defaultClient;
    HttpAuthenticationSupport.Credential digest = null;
    for (String name : names) {
      JsonNode definition = definitions.path(name);
      if (definition.isMissingNode())
        throw new IllegalArgumentException("AgentCard security scheme is undefined: " + name);
      Scheme scheme = scheme(definition);
      if (scheme.kind().equals("mutualtls")) {
        if (selectedClient != defaultClient)
          throw new IllegalArgumentException(
              "An AgentCard requirement cannot select multiple mTLS clients");
        selectedClient = Objects.requireNonNull(mutualTls.client(tenantId, name), "mTLS client");
      } else {
        char[] sensitive = secrets.resolve(tenantId, name);
        try {
          HttpAuthenticationSupport.Credential selected =
              apply(name, scheme, new String(sensitive), headers, query, cookies);
          if (selected != null) {
            if (digest != null
                || headers.keySet().stream()
                    .anyMatch(header -> header.equalsIgnoreCase("Authorization"))) {
              throw new IllegalArgumentException("AgentCard schemes conflict on Authorization");
            }
            digest = selected;
          }
        } finally {
          Arrays.fill(sensitive, '\0');
        }
      }
    }
    if (!cookies.isEmpty()) putUnique(headers, "Cookie", String.join("; ", cookies));
    return new Selection(
        withQuery(endpoint, query),
        Map.copyOf(headers),
        List.copyOf(names),
        selectedClient,
        digest);
  }

  private static HttpAuthenticationSupport.Credential apply(
      String schemeName,
      Scheme scheme,
      String secret,
      Map<String, String> headers,
      Map<String, String> query,
      List<String> cookies) {
    return switch (scheme.kind()) {
      case "apikey" -> {
        String parameter = requiredText(scheme.value(), "name");
        switch (requiredText(scheme.value(), "in", "location").toLowerCase(Locale.ROOT)) {
          case "header" -> putUnique(headers, parameter, secret);
          case "query" -> putUnique(query, parameter, secret);
          case "cookie" -> cookies.add(parameter + "=" + secret);
          default ->
              throw new IllegalArgumentException("Unsupported API key location for " + schemeName);
        }
        yield null;
      }
      case "http" -> {
        String authentication = requiredText(scheme.value(), "scheme");
        if (authentication.equalsIgnoreCase("basic")) {
          if (!secret.contains(":"))
            throw new IllegalArgumentException(
                "Basic AgentCard secret must contain username:password for " + schemeName);
          putUnique(
              headers,
              "Authorization",
              "Basic "
                  + Base64.getEncoder().encodeToString(secret.getBytes(StandardCharsets.UTF_8)));
        } else if (authentication.equalsIgnoreCase("digest")) {
          int separator = secret.indexOf(':');
          if (separator < 1 || separator == secret.length() - 1) {
            throw new IllegalArgumentException(
                "Digest AgentCard secret must contain username:password for " + schemeName);
          }
          yield new HttpAuthenticationSupport.Credential(
              com.forwardmeasure.openworkflow.definition.AuthenticationPlan.Kind.DIGEST,
              null,
              secret.substring(0, separator),
              secret.substring(separator + 1));
        } else {
          putUnique(headers, "Authorization", authentication + " " + secret);
        }
        yield null;
      }
      case "oauth2", "openidconnect" -> {
        putUnique(headers, "Authorization", "Bearer " + secret);
        yield null;
      }
      default ->
          throw new IllegalArgumentException(
              "Unsupported AgentCard security scheme " + schemeName + " of type " + scheme.kind());
    };
  }

  private static Scheme scheme(JsonNode configured) {
    if (configured.hasNonNull("type")) {
      return new Scheme(
          configured.required("type").asText().replace("-", "").toLowerCase(Locale.ROOT),
          configured);
    }
    for (Map.Entry<String, String> wrapper :
        Map.of(
                "apiKeySecurityScheme", "apikey",
                "httpAuthSecurityScheme", "http",
                "oauth2SecurityScheme", "oauth2",
                "openIdConnectSecurityScheme", "openidconnect",
                "mtlsSecurityScheme", "mutualtls")
            .entrySet()) {
      if (configured.path(wrapper.getKey()).isObject()) {
        return new Scheme(wrapper.getValue(), configured.path(wrapper.getKey()));
      }
    }
    throw new IllegalArgumentException("AgentCard security scheme has no supported type");
  }

  private static JsonNode requirementSchemes(JsonNode requirement) {
    JsonNode wrapped = requirement.path("schemes");
    return wrapped.isObject() ? wrapped : requirement;
  }

  private static URI withQuery(URI endpoint, Map<String, String> additions) {
    if (additions.isEmpty()) return endpoint;
    StringBuilder query =
        new StringBuilder(endpoint.getRawQuery() == null ? "" : endpoint.getRawQuery());
    additions.forEach(
        (name, value) -> {
          if (!query.isEmpty()) query.append('&');
          query
              .append(URLEncoder.encode(name, StandardCharsets.UTF_8))
              .append('=')
              .append(URLEncoder.encode(value, StandardCharsets.UTF_8));
        });
    try {
      return new URI(
          endpoint.getScheme(),
          endpoint.getRawAuthority(),
          endpoint.getRawPath(),
          query.toString(),
          endpoint.getRawFragment());
    } catch (Exception failure) {
      throw new IllegalArgumentException("AgentCard API-key query is invalid", failure);
    }
  }

  private static void putUnique(Map<String, String> values, String name, String value) {
    String existing =
        values.keySet().stream()
            .filter(candidate -> candidate.equalsIgnoreCase(name))
            .findFirst()
            .orElse(null);
    if (existing != null)
      throw new IllegalArgumentException("AgentCard security schemes conflict on " + name);
    values.put(name, value);
  }

  private static String requiredText(JsonNode value, String... names) {
    for (String name : names) {
      JsonNode candidate = value.path(name);
      if (candidate.isTextual() && !candidate.asText().isBlank()) {
        return candidate.asText();
      }
    }
    throw new IllegalArgumentException(
        "AgentCard security scheme is missing " + String.join("/", names));
  }

  private record Scheme(String kind, JsonNode value) {}

  record Selection(
      URI endpoint,
      Map<String, String> headers,
      List<String> schemes,
      HttpClient client,
      HttpAuthenticationSupport.Credential digest) {
    Selection {
      Objects.requireNonNull(endpoint, "endpoint");
      headers = Map.copyOf(headers);
      schemes = List.copyOf(schemes);
      Objects.requireNonNull(client, "client");
    }

    static Selection anonymous(URI endpoint, HttpClient client) {
      return new Selection(endpoint, Map.of(), List.of(), client, null);
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
