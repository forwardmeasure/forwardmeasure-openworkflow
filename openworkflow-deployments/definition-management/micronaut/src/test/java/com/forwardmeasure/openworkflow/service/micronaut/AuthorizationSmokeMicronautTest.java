/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.service.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.authorization.testkit.KeycloakOrganizationFixture;
import com.forwardmeasure.testcontainers.postgresql.PostgreSqlTestContainer;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Proves the full, real chain from "a browser presents a Bearer token" to "a REST resource sees the
 * right tenant/actor/roles" for the Micronaut binding - the gap described in {@code
 * docs/live-jwt-authorization-testkit-plan.md}: before this test, nothing in fowf ever sent a real,
 * Keycloak-signed JWT through any of the three framework bindings; only {@code
 * KeycloakOrganizationClaimsTest} exercised the claim-parsing logic in isolation, against a
 * hand-built Java {@code Map}. This test uses a genuine testcontainers-backed Keycloak instance
 * with the Organizations feature actually enabled - no mocks, no hand-built claims map.
 *
 * <p>{@code transactional = false}: by default {@code @MicronautTest} wraps every {@code @Test}
 * method in a Hibernate transaction (a convenience for tests that touch persistence and want
 * automatic rollback) via {@code DefaultTestTransactionExecutionListener}, which begins that
 * transaction - and therefore resolves a tenant schema - before the test body runs at all.
 * Confirmed missing the hard way: with the default {@code transactional = true}, both tests below
 * failed with {@code IllegalArgumentException: Tenant schema must be public or t_{32 lowercase hex
 * characters}} even though neither test method touches persistence, because no HTTP request (and
 * therefore no {@code ActiveOrganizationProvider}-backed tenant context) exists yet at that point.
 * This test drives real HTTP requests against the real embedded server instead, so it needs no
 * transaction of its own.
 */
@MicronautTest(transactional = false)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AuthorizationSmokeMicronautTest implements TestPropertyProvider {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final UUID TENANT_ID = UUID.randomUUID();
  private static final String ROLE = "workflow-administrator";

  private static KeycloakOrganizationFixture fixture;
  private static PostgreSqlTestContainer postgres;
  private static String organizationId;
  private static String token;

  @Inject
  @Client("/")
  HttpClient http;

  /**
   * Called by the Micronaut JUnit5 extension before the application context is built, so the
   * Keycloak and PostgreSQL containers must be started (and the tenant Organization provisioned)
   * here rather than in a JUnit lifecycle callback. Unlike Quarkus/Spring, micronaut-security-jwt
   * does not do issuer-based OIDC auto-discovery - it needs an explicit JWKS URL (confirmed against
   * this leaf's own real {@code application.yaml}, which already configures {@code
   * micronaut.security.token.jwt.signatures.jwks.keycloak.url} the same way). {@code
   * ForwardMeasureJpaFactory.tenantConnectionProvider} is an eager ({@code @Context}) singleton, so
   * the datasource properties below must also point at a real, reachable database before this
   * getProperties() call returns - see the pom.xml comment on the PostgreSQL testcontainer
   * dependency for why.
   */
  @Override
  public Map<String, String> getProperties() {
    fixture = KeycloakOrganizationFixture.start();
    organizationId = fixture.provisionTenant("acme-micronaut", TENANT_ID, ROLE);
    token = fixture.mintUserToken();
    postgres = new PostgreSqlTestContainer().start();
    Map<String, String> properties = new HashMap<>();
    properties.put("micronaut.security.enabled", "true");
    properties.put("micronaut.security.authentication", "bearer");
    properties.put(
        "micronaut.security.token.jwt.signatures.jwks.keycloak.url",
        fixture.issuer().toString() + "/protocol/openid-connect/certs");
    properties.put(
        "openworkflow.authorization.organization-client-id", KeycloakOrganizationFixture.CLIENT_ID);
    // Unrelated to this test's own JWT round trip: openworkflow.authorization.client-secret
    // (this service's OWN outbound AuthZEN OAuth identity, a completely different thing - see
    // MicronautActiveOrganizationProvider's javadoc) has no default in the checked-in
    // application.yaml and was never previously supplied by any test in this leaf. A placeholder
    // is fine since nothing in these two tests exercises the outbound AuthZEN client-credentials
    // call.
    properties.put("openworkflow.authorization.client-secret", "unused-test-secret");
    properties.put("datasources.default.url", postgres.hostJdbcUrl());
    properties.put("datasources.default.username", postgres.username());
    properties.put("datasources.default.password", postgres.password());
    // See AuthorizationSmokeResource's javadoc: SecuredAnnotationRule never fires for a route
    // registered through micronaut-jaxrs-server's bridge (confirmed via SecurityFilter debug
    // logging - it never sees the class's annotations), so an authenticated request with no rule
    // provider deciding is denied 403 by Micronaut's fail-closed default. InterceptUrlMapRule
    // instead matches on the raw request path/method, which works regardless of how the route was
    // registered.
    properties.put(
        "micronaut.security.intercept-url-map[0].pattern",
        "/internal/v1/smoke/active-organization");
    properties.put("micronaut.security.intercept-url-map[0].http-method", "GET");
    properties.put("micronaut.security.intercept-url-map[0].access[0]", "isAuthenticated()");
    return properties;
  }

  @AfterAll
  static void closeFixtures() {
    if (fixture != null) {
      fixture.close();
    }
    if (postgres != null) {
      postgres.close();
    }
  }

  @Test
  void activeOrganization_roundTripsRealKeycloakSignedJwtThroughMicronautSecurity()
      throws Exception {
    String body =
        http.toBlocking()
            .exchange(
                HttpRequest.GET("/internal/v1/smoke/active-organization").bearerAuth(token),
                String.class)
            .body();
    JsonNode json = MAPPER.readTree(body);
    assertEquals(TENANT_ID.toString(), json.path("tenantId").asText());
    assertEquals(organizationId, json.path("organizationId").asText());
    assertFalse(json.path("actorId").asText().isBlank());
    boolean hasRole = false;
    for (JsonNode role : json.path("organizationRoles")) {
      hasRole |= ROLE.equals(role.asText());
    }
    assertTrue(hasRole, "expected organizationRoles to contain " + ROLE + ": " + json);
  }

  @Test
  void activeOrganization_unauthenticatedRequestIsRejected() {
    HttpClientResponseException thrown =
        assertThrows(
            HttpClientResponseException.class,
            () ->
                http.toBlocking()
                    .exchange(
                        HttpRequest.GET("/internal/v1/smoke/active-organization"), String.class));
    assertEquals(401, thrown.getStatus().getCode());
  }
}
