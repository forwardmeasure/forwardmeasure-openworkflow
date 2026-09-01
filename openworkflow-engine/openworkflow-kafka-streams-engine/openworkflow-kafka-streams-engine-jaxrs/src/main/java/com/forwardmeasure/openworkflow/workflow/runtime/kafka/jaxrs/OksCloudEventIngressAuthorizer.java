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
package com.forwardmeasure.openworkflow.workflow.runtime.kafka.jaxrs;

import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.authorization.AuthorizationAction;
import com.forwardmeasure.openworkflow.authorization.AuthorizationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationResource;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.BusinessCorrelationId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OksTenantId;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Authorizes one CloudEvent-ingress HTTP request and turns the trusted {@link ActiveOrganization}
 * it resolves into the Kafka Streams runtime's own {@link ActorContext}/{@link OksTenantId} types.
 *
 * <p>Mirrors {@code AuthzenAuthenticatedActorProvider} (Pekko eventing JAX-RS) exactly in spirit -
 * trusted request identity in, {@link AuthorizationService#requireAuthorized} out - but that class
 * cannot be reused directly: it builds an {@code engine-api} {@code ActorIdentity}, not the
 * Kafka-Streams-runtime-api {@link ActorContext} {@link
 * com.forwardmeasure.openworkflow.workflow.runtime.api.InboundCloudEvent} requires, and this engine
 * has exactly one authorization decision to make (route a CloudEvent), not three, so the generic
 * string-typed {@code authorize(action, resourceType, resourceId)} indirection it uses is
 * unnecessary here.
 *
 * <p>Tenant/actor DID conversion follows {@code OksKafkaCommandGateway}'s established
 * "did:forwardmeasure:tenant:"/"did:forwardmeasure:actor:" convention exactly, so the resulting
 * actor is durably compatible with every other Kafka Streams ingress path (the same convention
 * {@code OksKafkaCommandGateway.actor()} already uses to turn an engine-api tenant/actor id into
 * these same runtime-api types).
 */
final class OksCloudEventIngressAuthorizer {
  private final ActiveOrganizationProvider organizations;
  private final AuthorizationService authorization;
  private final Clock clock;

  OksCloudEventIngressAuthorizer(
      ActiveOrganizationProvider organizations, AuthorizationService authorization, Clock clock) {
    this.organizations = Objects.requireNonNull(organizations, "organizations");
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  /**
   * Authorizes the general CloudEvents routing decision (the ingress endpoint as a whole, not one
   * execution or schedule in particular - there is no Kafka Streams equivalent of the Pekko
   * engine's per-execution/per-schedule direct-delivery paths; every accepted event is routed by
   * {@code receive:}/{@code listen:} subscription matching instead) and returns the accepted actor
   * context for the resulting {@code InboundCloudEvent}.
   */
  ActorContext authorizeEventRoute() {
    ActiveOrganization organization = organizations.current();
    String correlationId = UUID.randomUUID().toString();
    authorization.requireAuthorized(
        new AuthorizationRequest(
            organization,
            AuthorizationResource.eventTarget(),
            AuthorizationAction.EVENT_ROUTE,
            correlationId,
            Map.of("active_organization_id", organization.organizationId())));
    return new ActorContext(
        tenantId(organization),
        actorId(organization),
        ActorType.SERVICE,
        null,
        null,
        BusinessCorrelationId.parse(correlationId),
        organization.organizationRoles(),
        null,
        Instant.now(clock),
        null,
        null,
        organization.organizationId());
  }

  private static OksTenantId tenantId(ActiveOrganization organization) {
    return OksTenantId.parse("did:forwardmeasure:tenant:" + organization.tenantId().value());
  }

  private static ActorId actorId(ActiveOrganization organization) {
    String value = organization.actorId();
    if (!value.startsWith("did:")) {
      value = "did:forwardmeasure:actor:" + value;
    }
    return ActorId.parse(value);
  }
}
