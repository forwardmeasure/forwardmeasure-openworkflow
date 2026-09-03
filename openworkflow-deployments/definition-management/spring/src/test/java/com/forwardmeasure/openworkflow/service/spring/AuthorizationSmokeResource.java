/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.service.spring;

import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.Objects;
import java.util.Set;

/**
 * Test-only JAX-RS resource proving that a real, Keycloak-signed JWT round-trips through Spring
 * Security's resource-server JWT verification and {@link
 * com.forwardmeasure.openworkflow.binding.spring.SpringActiveOrganizationProvider} into the exact
 * tenant/actor/role identity {@code KeycloakOrganizationClaims.extract()} produced when the token
 * was minted. Lives in test sources only - never shipped in the production jar. Framework-neutral
 * on its own (a plain JAX-RS POJO, like every production resource in this codebase); registered
 * into the shared Jersey {@code ResourceConfig} by {@code
 * AuthorizationSmokeSpringTest.SmokeResourceConfig} rather than by any framework annotation here.
 * Rejecting an unauthenticated caller with HTTP 401 needs no per-resource annotation: {@code
 * OpenWorkflowSpringBinding}'s {@code SecurityFilterChain} already requires {@code
 * anyRequest().authenticated()} for the whole application.
 */
@Path("/internal/v1/smoke/active-organization")
public class AuthorizationSmokeResource {
  private final ActiveOrganizationProvider organizations;

  public AuthorizationSmokeResource(ActiveOrganizationProvider organizations) {
    this.organizations = Objects.requireNonNull(organizations, "organizations");
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
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
