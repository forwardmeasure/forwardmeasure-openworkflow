/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.service.quarkus;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;

import com.forwardmeasure.openworkflow.authorization.testkit.KeycloakOrganizationFixture;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTest;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Proves the full, real chain from "a browser presents a Bearer token" to "a REST resource sees the
 * right tenant/actor/roles" for the Quarkus binding - the gap described in {@code
 * docs/live-jwt-authorization-testkit-plan.md}: before this test, nothing in fowf ever sent a real,
 * Keycloak-signed JWT through any of the three framework bindings; only {@code
 * KeycloakOrganizationClaimsTest} exercised the claim-parsing logic in isolation, against a
 * hand-built Java {@code Map}. This test uses a genuine testcontainers-backed Keycloak instance
 * with the Organizations feature actually enabled - no mocks, no hand-built claims map.
 */
@QuarkusTest
@QuarkusTestResource(AuthorizationSmokeQuarkusTest.KeycloakResource.class)
class AuthorizationSmokeQuarkusTest {

  @Test
  void activeOrganization_roundTripsRealKeycloakSignedJwtThroughQuarkusOidc() {
    given()
        .header("Authorization", "Bearer " + KeycloakResource.token)
        .when()
        .get("/internal/v1/smoke/active-organization")
        .then()
        .statusCode(200)
        .body("tenantId", equalTo(KeycloakResource.TENANT_ID.toString()))
        .body("organizationId", equalTo(KeycloakResource.organizationId))
        .body("actorId", notNullValue())
        .body("organizationRoles", hasItem(KeycloakResource.ROLE));
  }

  @Test
  void activeOrganization_unauthenticatedRequestIsRejected() {
    given().when().get("/internal/v1/smoke/active-organization").then().statusCode(401);
  }

  /**
   * Starts a genuine, per-test-class Keycloak container with the Organizations feature enabled (see
   * {@link KeycloakOrganizationFixture}), provisions one tenant Organization for the fixed test
   * user, and points quarkus-oidc at that container's issuer. Dynamic, test-scoped configuration
   * only - the checked-in {@code application.yaml} is untouched.
   */
  public static final class KeycloakResource implements QuarkusTestResourceLifecycleManager {
    static final UUID TENANT_ID = UUID.randomUUID();
    static final String ROLE = "workflow-administrator";

    static volatile String organizationId;
    static volatile String token;

    private KeycloakOrganizationFixture fixture;

    @Override
    public Map<String, String> start() {
      fixture = KeycloakOrganizationFixture.start();
      organizationId = fixture.provisionTenant("acme-quarkus", TENANT_ID, ROLE);
      token = fixture.mintUserToken();
      return Map.of(
          "quarkus.oidc.auth-server-url",
          fixture.issuer().toString(),
          "quarkus.oidc.client-id",
          KeycloakOrganizationFixture.CLIENT_ID,
          "quarkus.oidc.application-type",
          "service",
          "openworkflow.authorization.organization-client-id",
          KeycloakOrganizationFixture.CLIENT_ID,
          // Unrelated to this test's own JWT round trip: openworkflow.authorization.client-secret
          // (this service's OWN outbound AuthZEN OAuth identity, a completely different thing -
          // see QuarkusActiveOrganizationProvider's javadoc) has no default in the checked-in
          // application.yaml and was never previously supplied by any @QuarkusTest in this leaf
          // (it had zero test classes before this one). The whole application context still has
          // to boot for this resource to be reachable, so it needs a value from somewhere; a
          // placeholder is fine since nothing in these two tests exercises the outbound AuthZEN
          // client-credentials call.
          "openworkflow.authorization.client-secret",
          "unused-test-secret");
    }

    @Override
    public void stop() {
      if (fixture != null) {
        fixture.close();
      }
    }
  }
}
