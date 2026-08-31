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
package com.forwardmeasure.openworkflow.eventing.jaxrs;

import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.authorization.AuthorizationAction;
import com.forwardmeasure.openworkflow.authorization.AuthorizationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationResource;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * AuthZEN-backed {@link AuthenticatedActorProvider}, mirroring {@code AuthzenExecutionAuthorizer}
 * exactly - trusted request identity in, {@link AuthorizationService#requireAuthorized} out. {@link
 * #authorize} is generic (any action/resourceType string) because {@link CloudEventIngressResource}
 * - the only caller today - covers three distinct authorization decisions (routing in general,
 * delivery to an execution, delivery to a schedule) with one interface; the mapping to real {@link
 * AuthorizationAction}/{@link AuthorizationResource} values lives here rather than in the
 * interface, which stays free of the closed action vocabulary.
 */
public final class AuthzenAuthenticatedActorProvider implements AuthenticatedActorProvider {
  private final ActiveOrganizationProvider organizations;
  private final AuthorizationService authorization;

  public AuthzenAuthenticatedActorProvider(
      ActiveOrganizationProvider organizations, AuthorizationService authorization) {
    this.organizations = Objects.requireNonNull(organizations, "organizations");
    this.authorization = Objects.requireNonNull(authorization, "authorization");
  }

  @Override
  public ActorIdentity currentActor() {
    return toActorIdentity(organizations.current());
  }

  @Override
  public ActorIdentity authorize(String action, String resourceType, String resourceId) {
    Objects.requireNonNull(action, "action");
    Objects.requireNonNull(resourceType, "resourceType");
    Objects.requireNonNull(resourceId, "resourceId");
    ActiveOrganization organization = organizations.current();
    authorization.requireAuthorized(
        new AuthorizationRequest(
            organization,
            resource(resourceType, resourceId),
            action(action),
            UUID.randomUUID().toString(),
            Map.of("active_organization_id", organization.organizationId())));
    return toActorIdentity(organization);
  }

  private static AuthorizationAction action(String action) {
    return switch (action) {
      case "event.route" -> AuthorizationAction.EVENT_ROUTE;
      case "event.deliver" -> AuthorizationAction.EVENT_DELIVER;
      default -> throw new IllegalArgumentException("Unrecognized CloudEvent action: " + action);
    };
  }

  private static AuthorizationResource resource(String resourceType, String resourceId) {
    return switch (resourceType) {
      case "event-target" -> AuthorizationResource.eventTarget();
      case "execution" -> AuthorizationResource.execution(resourceId);
      case "schedule" -> AuthorizationResource.schedule(resourceId);
      default ->
          throw new IllegalArgumentException("Unrecognized CloudEvent resource: " + resourceType);
    };
  }

  private static ActorIdentity toActorIdentity(ActiveOrganization organization) {
    return new ActorIdentity(
        new TenantId(organization.tenantId().value()),
        actorDid(organization.actorId()),
        organization.organizationId(),
        organization.organizationRoles(),
        UUID.randomUUID().toString());
  }

  private static String actorDid(String actorId) {
    return actorId.startsWith("did:") ? actorId : "did:forwardmeasure:actor:" + actorId;
  }
}
