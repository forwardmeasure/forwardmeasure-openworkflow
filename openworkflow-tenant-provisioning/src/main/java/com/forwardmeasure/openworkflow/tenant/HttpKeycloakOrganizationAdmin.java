/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.forwardmeasure.openworkflow.tenant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/** Keycloak 26.7 Admin REST adapter for shared client roles and Organizations. */
public final class HttpKeycloakOrganizationAdmin implements KeycloakOrganizationAdmin {
  private final HttpClient client;
  private final ObjectMapper mapper;
  private final KeycloakAdminTokenSupplier tokens;
  private final KeycloakAdminConfiguration configuration;
  private final URI adminBase;

  public HttpKeycloakOrganizationAdmin(
      HttpClient client,
      ObjectMapper mapper,
      KeycloakAdminTokenSupplier tokens,
      KeycloakAdminConfiguration configuration) {
    this.client = Objects.requireNonNull(client, "client");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
    this.tokens = Objects.requireNonNull(tokens, "tokens");
    this.configuration = Objects.requireNonNull(configuration, "configuration");
    this.adminBase =
        configuration.serverUri().resolve("/admin/realms/" + configuration.realm() + "/");
  }

  @Override
  public Set<String> sharedClientRoles() {
    JsonNode roles =
        send(
                "GET",
                adminBase.resolve("clients/" + configuration.sharedClientUuid() + "/roles"),
                null,
                200)
            .body();
    Set<String> names = new TreeSet<>();
    roles.forEach(role -> names.add(requiredText(role, "name")));
    return Set.copyOf(names);
  }

  @Override
  public void createSharedClientRole(String role) {
    send(
        "POST",
        adminBase.resolve("clients/" + configuration.sharedClientUuid() + "/roles"),
        Map.of("name", role),
        201,
        204);
  }

  @Override
  public Optional<OrganizationState> organizationByAlias(String alias) {
    String query = URLEncoder.encode("alias:" + alias, StandardCharsets.UTF_8);
    JsonNode organizations =
        send(
                "GET",
                URI.create(
                    adminBase.resolve("organizations").toString()
                        + "?q="
                        + query
                        + "&briefRepresentation=false"),
                null,
                200)
            .body();
    for (JsonNode organization : organizations) {
      if (alias.equals(organization.path("alias").asText())) {
        return Optional.of(toState(organization));
      }
    }
    return Optional.empty();
  }

  @Override
  public OrganizationState createOrganization(
      String name, String alias, Map<String, String> attributes) {
    Response created =
        send(
            "POST",
            adminBase.resolve("organizations"),
            Map.of(
                "name",
                name,
                "alias",
                alias,
                "enabled",
                true,
                "attributes",
                attributePayload(attributes)),
            201);
    String location =
        created
            .location()
            .orElseThrow(
                () ->
                    new KeycloakAdminException(
                        "Keycloak did not return the created Organization location"));
    return toState(send("GET", URI.create(location), null, 200).body());
  }

  @Override
  public void updateOrganization(OrganizationState organization) {
    String id = KeycloakAdminConfiguration.validateSegment(organization.id(), "organization.id");
    send(
        "PUT",
        adminBase.resolve("organizations/" + id),
        Map.of(
            "id", organization.id(),
            "name", organization.name(),
            "alias", organization.alias(),
            "enabled", organization.enabled(),
            "attributes", attributePayload(organization.attributes())),
        204);
  }

  @Override
  public Set<String> organizationRoleGroups(String organizationId) {
    String id = KeycloakAdminConfiguration.validateSegment(organizationId, "organization.id");
    JsonNode groups =
        send("GET", adminBase.resolve("organizations/" + id + "/groups"), null, 200).body();
    Set<String> names = new TreeSet<>();
    groups.forEach(group -> names.add(requiredText(group, "name")));
    return Set.copyOf(names);
  }

  @Override
  public void createOrganizationRoleGroup(String organizationId, String role) {
    String id = KeycloakAdminConfiguration.validateSegment(organizationId, "organization.id");
    send(
        "POST",
        adminBase.resolve("organizations/" + id + "/groups"),
        Map.of("name", role),
        201,
        204,
        409);
  }

  private Response send(String method, URI uri, Object body, int... expectedStatuses) {
    String token = tokens.bearerToken();
    if (token == null || token.isBlank()) {
      throw new KeycloakAdminException("Keycloak administration token is missing");
    }
    try {
      HttpRequest.BodyPublisher publisher =
          body == null
              ? HttpRequest.BodyPublishers.noBody()
              : HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body));
      HttpRequest request =
          HttpRequest.newBuilder(uri)
              .timeout(configuration.requestTimeout())
              .header("Authorization", "Bearer " + token)
              .header("Content-Type", "application/json")
              .method(method, publisher)
              .build();
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      for (int expected : expectedStatuses) {
        if (response.statusCode() == expected) {
          JsonNode json =
              response.body().isBlank()
                  ? mapper.createObjectNode()
                  : mapper.readTree(response.body());
          return new Response(json, response.headers().firstValue("Location"));
        }
      }
      throw new KeycloakAdminException(
          "Keycloak Admin REST returned HTTP " + response.statusCode());
    } catch (JsonProcessingException exception) {
      throw new KeycloakAdminException("Keycloak Admin REST returned unusable JSON", exception);
    } catch (IOException exception) {
      throw new KeycloakAdminException("Keycloak Admin REST is unavailable", exception);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new KeycloakAdminException("Keycloak Admin REST request was interrupted", exception);
    }
  }

  private OrganizationState toState(JsonNode node) {
    Map<String, String> attributes = new LinkedHashMap<>();
    node.path("attributes")
        .properties()
        .forEach(
            entry -> {
              JsonNode value = entry.getValue();
              attributes.put(
                  entry.getKey(), value.isArray() ? value.path(0).asText() : value.asText());
            });
    return new OrganizationState(
        requiredText(node, "id"),
        requiredText(node, "name"),
        requiredText(node, "alias"),
        node.path("enabled").asBoolean(false),
        attributes);
  }

  private static Map<String, Object> attributePayload(Map<String, String> attributes) {
    Map<String, Object> result = new LinkedHashMap<>();
    attributes.forEach((key, value) -> result.put(key, new String[] {value}));
    return Map.copyOf(result);
  }

  private static String requiredText(JsonNode node, String field) {
    String value = node.path(field).asText();
    if (value.isBlank()) {
      throw new KeycloakAdminException("Keycloak response is missing " + field);
    }
    return value;
  }

  private record Response(JsonNode body, Optional<String> location) {}
}
