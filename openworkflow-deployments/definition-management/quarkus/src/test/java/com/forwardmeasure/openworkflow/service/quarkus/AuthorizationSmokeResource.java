/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.service.quarkus;

import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import jakarta.annotation.security.RolesAllowed;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.Set;

/**
 * Test-only JAX-RS resource proving that a real, Keycloak-signed JWT round-trips through
 * quarkus-oidc verification and {@link
 * com.forwardmeasure.openworkflow.binding.quarkus.QuarkusActiveOrganizationProvider} into the exact
 * tenant/actor/role identity {@code KeycloakOrganizationClaims.extract()} produced when the token
 * was minted. Lives in test sources only - never shipped in the production jar.
 *
 * <p>{@code @RolesAllowed("**")} is what actually rejects an unauthenticated caller with HTTP 401
 * here: this deployment has no global {@code quarkus.http.auth.permission.*} policy, so without an
 * explicit security annotation an anonymous request would reach {@link
 * ActiveOrganizationProvider#current()} and fail with a raw {@code NullPointerException} (500), not
 * the clean 401 a real caller needs to see.
 */
@ApplicationScoped
@Path("/internal/v1/smoke/active-organization")
public class AuthorizationSmokeResource {
  private final ActiveOrganizationProvider organizations;

  @Inject
  public AuthorizationSmokeResource(ActiveOrganizationProvider organizations) {
    this.organizations = organizations;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @RolesAllowed("**")
  public ActiveOrganizationView current() {
    ActiveOrganization organization = organizations.current();
    return new ActiveOrganizationView(
        organization.tenantId().value().toString(),
        organization.organizationId(),
        organization.actorId(),
        organization.organizationRoles());
  }

  public record ActiveOrganizationView(
      String tenantId, String organizationId, String actorId, Set<String> organizationRoles) {}
}
