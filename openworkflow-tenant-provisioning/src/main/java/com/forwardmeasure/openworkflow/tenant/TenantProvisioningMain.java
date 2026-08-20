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
package com.forwardmeasure.openworkflow.tenant;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.jpa.tenancy.TenantId;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;

/** Bounded Kubernetes Keycloak reconciliation-job entry point. */
public final class TenantProvisioningMain {
  private TenantProvisioningMain() {}

  public static void main(String[] arguments) {
    URI server = URI.create(required("KEYCLOAK_URL"));
    String realm = required("KEYCLOAK_REALM");
    HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    ObjectMapper mapper = new ObjectMapper();
    KeycloakAdminTokenSupplier tokens =
        () ->
            adminToken(
                client,
                mapper,
                server,
                required("KEYCLOAK_ADMIN_USERNAME"),
                required("KEYCLOAK_ADMIN_PASSWORD"));
    var admin =
        new HttpKeycloakOrganizationAdmin(
            client,
            mapper,
            tokens,
            new KeycloakAdminConfiguration(
                server, realm, required("KEYCLOAK_SHARED_CLIENT_ID"), Duration.ofSeconds(10)));
    var reconciler = new TenantOrganizationReconciler(admin);
    var packs =
        Arrays.stream(optional("FORWARDMEASURE_CAPABILITY_PACKS", "openworkflow").split(","))
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .map(CapabilityPack::named)
            .toList();
    Arrays.stream(required("OPENWORKFLOW_TENANTS").split(","))
        .map(String::trim)
        .filter(value -> !value.isEmpty())
        .map(value -> value.split(":", 3))
        .forEach(
            parts -> {
              if (parts.length != 3) {
                throw new IllegalArgumentException(
                    "Tenant entries must be UUID:alias:display-name");
              }
              var request =
                  new TenantProvisioningRequest(TenantId.parse(parts[0]), parts[2], parts[1]);
              packs.forEach(pack -> reconciler.reconcileCapabilityPack(request, pack));
            });
  }

  private static String adminToken(
      HttpClient client, ObjectMapper mapper, URI server, String username, String password) {
    String form =
        "client_id=admin-cli&grant_type=password&username="
            + encode(username)
            + "&password="
            + encode(password);
    HttpRequest request =
        HttpRequest.newBuilder(server.resolve("/realms/master/protocol/openid-connect/token"))
            .timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(form))
            .build();
    try {
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != 200) {
        throw new KeycloakAdminException(
            "Keycloak token endpoint returned HTTP " + response.statusCode());
      }
      JsonNode token = mapper.readTree(response.body()).get("access_token");
      if (token == null || !token.isTextual() || token.textValue().isBlank()) {
        throw new KeycloakAdminException("Keycloak token response is unusable");
      }
      return token.textValue();
    } catch (IOException exception) {
      throw new KeycloakAdminException("Keycloak token endpoint is unavailable", exception);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new KeycloakAdminException("Keycloak token request was interrupted", exception);
    }
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private static String required(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException(name + " is required");
    }
    return value;
  }

  private static String optional(String name, String fallback) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? fallback : value;
  }
}
