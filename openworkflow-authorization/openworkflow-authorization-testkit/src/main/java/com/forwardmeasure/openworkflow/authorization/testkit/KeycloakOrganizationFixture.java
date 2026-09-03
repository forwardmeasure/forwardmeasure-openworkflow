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
package com.forwardmeasure.openworkflow.authorization.testkit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.testcontainers.keycloak.KeycloakTestContainer;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A real, running Keycloak fixture with the Organizations feature genuinely wired for the {@code
 * organization} claim shape {@code KeycloakOrganizationClaims.extract()} expects - not a stub, not
 * a hand-built claims map. Provisions one Organization per call via the same Admin REST sequence
 * confirmed live against Keycloak 26.7.2 in forwardmeasure-platform's own bootstrap-admin.sh, then
 * mints a real signed JWT for a real user who is really a member of it.
 */
public final class KeycloakOrganizationFixture implements AutoCloseable {

  public static final String REALM = "openworkflow-authz-test";
  public static final String CLIENT_ID = "openworkflow-test-client";
  public static final String USERNAME = "openworkflow-test-user";
  public static final String PASSWORD = "openworkflow-test-password";

  private static final String REALM_RESOURCE = "/openworkflow-authz-test-realm.json";
  private static final Pattern ACCESS_TOKEN =
      Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");

  private final KeycloakTestContainer container;
  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
  private final ObjectMapper mapper = new ObjectMapper();
  private volatile String adminToken;
  private volatile String clientUuid;

  private KeycloakOrganizationFixture(KeycloakTestContainer container) {
    this.container = container;
  }

  public static KeycloakOrganizationFixture start() {
    KeycloakTestContainer container = new KeycloakTestContainer(REALM, realmJson()).start();
    return new KeycloakOrganizationFixture(container);
  }

  public URI issuer() {
    return container.issuer();
  }

  /**
   * Provisions one Organization (with the {@code forwardmeasure.tenant-id} attribute {@code
   * KeycloakOrganizationClaims} reads), one client role on {@value #CLIENT_ID}, one Organization
   * Group mapping that role, and adds {@value #USERNAME} as a member of both the Organization and
   * the group. Uses the real, dedicated Organization Groups API ({@code
   * organizations/{orgId}/groups/...}), not a plain realm Group.
   */
  public String provisionTenant(String organizationAlias, UUID tenantId, String roleName) {
    ensureClientRole(roleName);
    String organizationId = createOrganization(organizationAlias, tenantId);
    String groupId = createOrganizationGroup(organizationId, roleName);
    mapRoleOntoOrganizationGroup(organizationId, groupId, roleName);
    String userId = requireUserId();
    addOrganizationMember(organizationId, userId);
    addOrganizationGroupMember(organizationId, groupId, userId);
    return organizationId;
  }

  public String mintUserToken() {
    return container.passwordToken(CLIENT_ID, USERNAME, PASSWORD);
  }

  @Override
  public void close() {
    container.close();
  }

  private void ensureClientRole(String roleName) {
    Response existing =
        send(
            "GET",
            adminBase().resolve("clients/" + clientUuid() + "/roles/" + roleName),
            null,
            200,
            404);
    if (existing.status() == 404) {
      send(
          "POST",
          adminBase().resolve("clients/" + clientUuid() + "/roles"),
          Map.of("name", roleName),
          201,
          204,
          409);
    }
  }

  private String createOrganization(String alias, UUID tenantId) {
    Response created =
        send(
            "POST",
            adminBase().resolve("organizations"),
            Map.of(
                "name",
                alias,
                "alias",
                alias,
                "enabled",
                true,
                "attributes",
                Map.of("forwardmeasure.tenant-id", List.of(tenantId.toString()))),
            201);
    return created
        .location()
        .map(KeycloakOrganizationFixture::lastPathSegment)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Keycloak did not return the created Organization location"));
  }

  private String createOrganizationGroup(String organizationId, String groupName) {
    send(
        "POST",
        adminBase().resolve("organizations/" + organizationId + "/groups"),
        Map.of("name", groupName),
        201,
        204,
        409);
    JsonNode groups =
        send("GET", adminBase().resolve("organizations/" + organizationId + "/groups"), null, 200)
            .body();
    for (JsonNode group : groups) {
      if (groupName.equals(group.path("name").asText())) {
        return requiredText(group, "id");
      }
    }
    throw new IllegalStateException(
        "Keycloak did not return the created Organization Group " + groupName);
  }

  private void mapRoleOntoOrganizationGroup(
      String organizationId, String groupId, String roleName) {
    JsonNode role =
        send(
                "GET",
                adminBase().resolve("clients/" + clientUuid() + "/roles/" + roleName),
                null,
                200)
            .body();
    send(
        "POST",
        adminBase()
            .resolve(
                "organizations/"
                    + organizationId
                    + "/groups/"
                    + groupId
                    + "/role-mappings/clients/"
                    + clientUuid()),
        List.of(role),
        201,
        204,
        409);
  }

  private String requireUserId() {
    String query = URLEncoder.encode(USERNAME, StandardCharsets.UTF_8);
    JsonNode users =
        send(
                "GET",
                URI.create(
                    adminBase().resolve("users").toString() + "?username=" + query + "&exact=true"),
                null,
                200)
            .body();
    for (JsonNode user : users) {
      if (USERNAME.equals(user.path("username").asText())) {
        return requiredText(user, "id");
      }
    }
    throw new IllegalStateException(
        "Keycloak test user " + USERNAME + " was not found - realm import may have failed");
  }

  private void addOrganizationMember(String organizationId, String userId) {
    // Bare JSON string body, not a JSON object - confirmed live (any other shape returns HTTP
    // 415).
    send(
        "POST",
        adminBase().resolve("organizations/" + organizationId + "/members"),
        userId,
        201,
        204,
        409);
  }

  private void addOrganizationGroupMember(String organizationId, String groupId, String userId) {
    send(
        "PUT",
        adminBase()
            .resolve(
                "organizations/" + organizationId + "/groups/" + groupId + "/members/" + userId),
        null,
        204,
        409);
  }

  private String clientUuid() {
    String resolved = clientUuid;
    if (resolved != null) {
      return resolved;
    }
    String query = URLEncoder.encode(CLIENT_ID, StandardCharsets.UTF_8);
    JsonNode clients =
        send(
                "GET",
                URI.create(adminBase().resolve("clients").toString() + "?clientId=" + query),
                null,
                200)
            .body();
    for (JsonNode client : clients) {
      if (CLIENT_ID.equals(client.path("clientId").asText())) {
        resolved = requiredText(client, "id");
        clientUuid = resolved;
        return resolved;
      }
    }
    throw new IllegalStateException(
        "Keycloak client " + CLIENT_ID + " was not found - realm import may have failed");
  }

  private URI adminBase() {
    return URI.create(baseUrl() + "/admin/realms/" + REALM + "/");
  }

  private String baseUrl() {
    String issuer = issuer().toString();
    String suffix = "/realms/" + REALM;
    if (!issuer.endsWith(suffix)) {
      throw new IllegalStateException("Unexpected Keycloak issuer shape: " + issuer);
    }
    return issuer.substring(0, issuer.length() - suffix.length());
  }

  private String adminBearerToken() {
    String resolved = adminToken;
    if (resolved != null) {
      return resolved;
    }
    try {
      HttpRequest request =
          HttpRequest.newBuilder(
                  URI.create(baseUrl() + "/realms/master/protocol/openid-connect/token"))
              .header("Content-Type", "application/x-www-form-urlencoded")
              .POST(
                  HttpRequest.BodyPublishers.ofString(
                      "grant_type=password&client_id=admin-cli&username=admin&password=admin-integration-only"))
              .build();
      HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != 200) {
        throw new IllegalStateException(
            "Keycloak admin token request returned HTTP "
                + response.statusCode()
                + ": "
                + response.body());
      }
      Matcher matcher = ACCESS_TOKEN.matcher(response.body());
      if (!matcher.find()) {
        throw new IllegalStateException("Keycloak admin token response has no access_token");
      }
      resolved = matcher.group(1);
      adminToken = resolved;
      return resolved;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted while obtaining a Keycloak admin token", interrupted);
    } catch (IOException failure) {
      throw new UncheckedIOException("Unable to obtain a Keycloak admin token", failure);
    }
  }

  private Response send(String method, URI uri, Object body, int... expectedStatuses) {
    try {
      HttpRequest.BodyPublisher publisher =
          body == null
              ? HttpRequest.BodyPublishers.noBody()
              : HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body));
      HttpRequest request =
          HttpRequest.newBuilder(uri)
              .timeout(Duration.ofSeconds(30))
              .header("Authorization", "Bearer " + adminBearerToken())
              .header("Content-Type", "application/json")
              .method(method, publisher)
              .build();
      HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
      for (int expected : expectedStatuses) {
        if (response.statusCode() == expected) {
          JsonNode json =
              response.body().isBlank()
                  ? mapper.createObjectNode()
                  : mapper.readTree(response.body());
          return new Response(
              response.statusCode(), json, response.headers().firstValue("Location"));
        }
      }
      throw new IllegalStateException(
          "Keycloak Admin REST "
              + method
              + " "
              + uri
              + " returned HTTP "
              + response.statusCode()
              + ": "
              + response.body());
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted during a Keycloak Admin REST call", interrupted);
    } catch (IOException failure) {
      throw new UncheckedIOException("Keycloak Admin REST is unavailable", failure);
    }
  }

  private static String requiredText(JsonNode node, String field) {
    String value = node.path(field).asText();
    if (value.isBlank()) {
      throw new IllegalStateException("Keycloak response is missing " + field);
    }
    return value;
  }

  private static String lastPathSegment(String uri) {
    return uri.substring(uri.lastIndexOf('/') + 1);
  }

  private static String realmJson() {
    try (InputStream stream =
        KeycloakOrganizationFixture.class.getResourceAsStream(REALM_RESOURCE)) {
      if (stream == null) {
        throw new IllegalStateException("Missing " + REALM_RESOURCE);
      }
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException failure) {
      throw new UncheckedIOException("Unable to load " + REALM_RESOURCE, failure);
    }
  }

  private record Response(int status, JsonNode body, Optional<String> location) {}
}
