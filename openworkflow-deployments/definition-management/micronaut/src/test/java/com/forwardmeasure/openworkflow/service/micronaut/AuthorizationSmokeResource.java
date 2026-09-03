/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.service.micronaut;

import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import io.micronaut.serde.annotation.Serdeable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Objects;
import java.util.Set;

/**
 * Test-only JAX-RS resource proving that a real, Keycloak-signed JWT round-trips through
 * micronaut-security-jwt verification and {@link
 * com.forwardmeasure.openworkflow.binding.micronaut.MicronautActiveOrganizationProvider} into the
 * exact tenant/actor/role identity {@code KeycloakOrganizationClaims.extract()} produced when the
 * token was minted. Lives in test sources only - never shipped in the production jar.
 *
 * <p>{@code @Singleton} on the class is all Micronaut needs for routing (confirmed against this
 * codebase's real {@code MicronautWorkflowDefinitionResource}/{@code
 * MicronautStudioAuthorizationResource}: {@code micronaut-jaxrs-server} inherits routing directly
 * from the JAX-RS {@code @Path}/{@code @GET} annotations).
 *
 * <p>Neither a bare class (no security annotation at all) nor an explicit
 * {@code @Secured(SecurityRule.IS_AUTHENTICATED)} on this class was enough to let an authenticated
 * caller through - both returned HTTP 403, confirmed via {@code SecurityFilter} debug logging:
 * {@code io.micronaut.security.rules.SecuredAnnotationRule} is registered but never fires for this
 * route ("No rule provider authorized or rejected the request" even with {@code @Secured} present),
 * because {@code micronaut-jaxrs-server}'s route-bridging does not expose JAX-RS resource
 * annotations the way {@code SecuredAnnotationRule} expects to find them on a normal
 * {@code @Controller} method. With no rule provider deciding, Micronaut's fail-closed default
 * denies an authenticated-but-unruled request as 403 (versus 401 for no authentication at all) -
 * the plan's "secure by default, no annotation needed" claim does not hold through this specific
 * bridge. The actual, verified fix is a path-based {@code micronaut.security.intercept-url-map}
 * entry (set dynamically in {@code AuthorizationSmokeMicronautTest}), which {@code
 * InterceptUrlMapRule} evaluates against the raw request URI/method rather than reflecting on
 * annotations, so it works regardless of how the route was registered. The production resources
 * this plan cited as precedent must be getting their actual enforcement from somewhere else in the
 * real deployment (an {@code intercept-url-map} entry, an API gateway, or similar) - not from being
 * left bare, contrary to what a quick source read suggested.
 *
 * <p>{@code current()} returns {@code Response.ok(...).build()} rather than the view record
 * directly, matching this codebase's own established convention (every real production JAX-RS
 * resource, e.g. {@code WorkflowManagementResource}, does the same). Either return shape actually
 * works once the classpath conflict below is fixed - see the {@code jersey-common} exclusion on
 * this leaf's own {@code openworkflow-micronaut-binding} dependency in {@code pom.xml}. Before that
 * fix, EVERY {@code jakarta.ws.rs.core.Response} built anywhere in this leaf - explicit or implicit
 * - resolved to Jersey's own {@code OutboundJaxrsResponse} instead of Micronaut's, because {@code
 * jersey-common} bundles a competing {@code META-INF/services/jakarta.ws.rs.ext.RuntimeDelegate}
 * registration that wins the standard ServiceLoader-based lookup; {@code micronaut-jaxrs-server}
 * then failed to cast that foreign object to its own {@code JaxRsMutableResponse}, producing HTTP
 * 500 for every request regardless of what the resource method returned.
 *
 * <p>{@code @Serdeable} on {@link ActiveOrganizationView} is required too: unlike this leaf's
 * existing production DTOs (which reuse already-serde-processed generated OpenAPI model types),
 * this is a fresh record with no prior serde processing, and {@code micronaut-serde-jackson}
 * refuses to serialize a type it has no compile-time introspection for.
 */
@Singleton
@Path("/internal/v1/smoke/active-organization")
public class AuthorizationSmokeResource {
  private final ActiveOrganizationProvider organizations;

  @Inject
  public AuthorizationSmokeResource(ActiveOrganizationProvider organizations) {
    this.organizations = Objects.requireNonNull(organizations, "organizations");
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response current() {
    ActiveOrganization organization = organizations.current();
    ActiveOrganizationView view =
        new ActiveOrganizationView(
            organization.tenantId().value().toString(),
            organization.organizationId(),
            organization.actorId(),
            organization.organizationRoles());
    return Response.ok(view).build();
  }

  @Serdeable
  public record ActiveOrganizationView(
      String tenantId, String organizationId, String actorId, Set<String> organizationRoles) {}
}
