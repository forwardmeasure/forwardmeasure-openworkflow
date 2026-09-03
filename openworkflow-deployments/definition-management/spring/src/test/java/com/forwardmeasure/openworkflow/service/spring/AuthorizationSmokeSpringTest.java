/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.service.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.authorization.testkit.KeycloakOrganizationFixture;
import com.forwardmeasure.testcontainers.postgresql.PostgreSqlTestContainer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.jersey.autoconfigure.ResourceConfigCustomizer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Proves the full, real chain from "a browser presents a Bearer token" to "a REST resource sees the
 * right tenant/actor/roles" for the Spring binding - the gap described in {@code
 * docs/live-jwt-authorization-testkit-plan.md}: before this test, nothing in fowf ever sent a real,
 * Keycloak-signed JWT through any of the three framework bindings; only {@code
 * KeycloakOrganizationClaimsTest} exercised the claim-parsing logic in isolation, against a
 * hand-built Java {@code Map}. This test uses a genuine testcontainers-backed Keycloak instance
 * with the Organizations feature actually enabled - no mocks, no hand-built claims map.
 *
 * <p>Follows this repository's own established pattern for a real Spring HTTP round trip ({@code
 * StudioControllerTest}: {@code @LocalServerPort} + plain {@code java.net.http.HttpClient}) rather
 * than adding a new rest-assured test dependency this module has never needed before.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(AuthorizationSmokeSpringTest.SmokeResourceConfig.class)
class AuthorizationSmokeSpringTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final KeycloakOrganizationFixture FIXTURE = KeycloakOrganizationFixture.start();
  private static final UUID TENANT_ID = UUID.randomUUID();
  private static final String ROLE = "workflow-administrator";
  private static final String ORGANIZATION_ID =
      FIXTURE.provisionTenant("acme-spring", TENANT_ID, ROLE);
  private static final String TOKEN = FIXTURE.mintUserToken();
  // AuthorizationSmokeResource has no persistence dependency of its own, but the shared
  // OpenWorkflowDefinitionManagementSpringBinding @Configuration class this test also boots
  // eagerly builds every OTHER production resource too (WorkflowManagementResource, etc.), each
  // needing a real EntityManagerFactory - see the pom.xml comment on this dependency for why a
  // real, disposable Postgres is needed just to let the context refresh, not to exercise
  // persistence in this test.
  private static final PostgreSqlTestContainer POSTGRES = new PostgreSqlTestContainer().start();

  @LocalServerPort int port;
  private final HttpClient http = HttpClient.newHttpClient();

  /**
   * Points Spring Security's resource-server JWT verification, {@code organization-client-id}, and
   * the datasource at the fixtures' disposable realm/client/database - dynamic, test-scoped
   * configuration only; the checked-in {@code application.yaml} is untouched. {@code FIXTURE}'s and
   * {@code POSTGRES}'s static field initializers above already started both containers (Java
   * guarantees a class's static initializers complete before any of its own static methods,
   * including this one, can run), so both are available here.
   */
  @DynamicPropertySource
  static void registerKeycloak(DynamicPropertyRegistry registry) {
    registry.add(
        "spring.security.oauth2.resourceserver.jwt.issuer-uri", () -> FIXTURE.issuer().toString());
    registry.add(
        "openworkflow.authorization.organization-client-id",
        () -> KeycloakOrganizationFixture.CLIENT_ID);
    registry.add("spring.datasource.url", POSTGRES::hostJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::username);
    registry.add("spring.datasource.password", POSTGRES::password);
    // Unrelated to this test's own JWT round trip: openworkflow.authorization.client-secret
    // (this service's OWN outbound AuthZEN OAuth identity, a completely different thing - see
    // SpringActiveOrganizationProvider's javadoc) has no default in the checked-in
    // application.yaml and was never previously supplied by any test in this leaf (it had zero
    // Spring context tests before this one). A placeholder is fine since nothing in these two
    // tests exercises the outbound AuthZEN client-credentials call.
    registry.add("openworkflow.authorization.client-secret", () -> "unused-test-secret");
  }

  @AfterAll
  static void closeFixtures() {
    FIXTURE.close();
    POSTGRES.close();
  }

  @Test
  void activeOrganization_roundTripsRealKeycloakSignedJwtThroughSpringSecurity() throws Exception {
    HttpResponse<String> response = get(TOKEN);
    assertEquals(200, response.statusCode());
    JsonNode json = MAPPER.readTree(response.body());
    assertEquals(TENANT_ID.toString(), json.path("tenantId").asText());
    assertEquals(ORGANIZATION_ID, json.path("organizationId").asText());
    assertFalse(json.path("actorId").asText().isBlank());
    boolean hasRole = false;
    for (JsonNode role : json.path("organizationRoles")) {
      hasRole |= ROLE.equals(role.asText());
    }
    assertTrue(hasRole, () -> "expected organizationRoles to contain " + ROLE + ": " + json);
  }

  @Test
  void activeOrganization_unauthenticatedRequestIsRejected() throws Exception {
    HttpResponse<String> response = get(null);
    assertEquals(401, response.statusCode());
  }

  private HttpResponse<String> get(String token) throws Exception {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + port + "/internal/v1/smoke/active-organization"))
            .GET();
    if (token != null) {
      builder.header("Authorization", "Bearer " + token);
    }
    return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
  }

  /**
   * Registers {@link AuthorizationSmokeResource} into the single Jersey {@code ResourceConfig},
   * mirroring how {@code OpenWorkflowDefinitionManagementSpringBinding} registers every production
   * resource - register-by-instance is safe here since this resource has no {@code
   * EntityManager}/persistence dependency to force into premature construction.
   */
  @TestConfiguration
  static class SmokeResourceConfig {
    @Bean
    AuthorizationSmokeResource authorizationSmokeResource(
        ActiveOrganizationProvider organizations) {
      return new AuthorizationSmokeResource(organizations);
    }

    @Bean
    ResourceConfigCustomizer authorizationSmokeResourceConfigCustomizer(
        AuthorizationSmokeResource resource) {
      return resourceConfig -> resourceConfig.register(resource);
    }
  }
}
