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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.AuthorizationAction;
import com.forwardmeasure.openworkflow.authorization.AuthorizationDecision;
import com.forwardmeasure.openworkflow.authorization.AuthorizationDeniedException;
import com.forwardmeasure.openworkflow.authorization.AuthorizationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationResource;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class AuthzenAuthenticatedActorProviderTest {
  private static final ActiveOrganization ORGANIZATION =
      new ActiveOrganization(
          new com.forwardmeasure.jpa.tenancy.TenantId(
              UUID.fromString("01234567-89ab-cdef-0123-456789abcdef")),
          "org-1",
          "actor-1",
          Set.of("workflow-author"));

  @Test
  void currentActorReflectsTheActiveOrganizationAndPrefixesTheActorIdAsADid() {
    var provider =
        new AuthzenAuthenticatedActorProvider(
            () -> ORGANIZATION, new RecordingAuthorizationService());

    ActorIdentity actor = provider.currentActor();

    assertEquals(ORGANIZATION.tenantId().value(), actor.tenantId().value());
    assertEquals("did:forwardmeasure:actor:actor-1", actor.actorDid());
    assertEquals("org-1", actor.organizationId());
    assertEquals(Set.of("workflow-author"), actor.organizationRoles());
  }

  @Test
  void currentActorLeavesAnAlreadyPrefixedActorIdUntouched() {
    ActiveOrganization alreadyADid =
        new ActiveOrganization(
            ORGANIZATION.tenantId(), "org-1", "did:web:example.com:actor:actor-1", Set.of());
    var provider =
        new AuthzenAuthenticatedActorProvider(
            () -> alreadyADid, new RecordingAuthorizationService());

    assertEquals("did:web:example.com:actor:actor-1", provider.currentActor().actorDid());
  }

  @Test
  void authorizeMapsEventRouteAndEventTargetToTheRoutingDecisionCloudEventIngressActuallySends() {
    // Exactly the (action, resourceType, resourceId) triple CloudEventIngressResource#route sends.
    RecordingAuthorizationService authorization = new RecordingAuthorizationService();
    var provider = new AuthzenAuthenticatedActorProvider(() -> ORGANIZATION, authorization);

    ActorIdentity actor = provider.authorize("event.route", "event-target", "*");

    assertEquals(AuthorizationAction.EVENT_ROUTE, authorization.lastRequest.action());
    assertEquals(AuthorizationResource.eventTarget(), authorization.lastRequest.resource());
    assertEquals("did:forwardmeasure:actor:actor-1", actor.actorDid());
  }

  @Test
  void authorizeMapsEventDeliverAndExecutionToTheExecutionDeliveryDecisionCloudEventIngressSends() {
    // Exactly the triple CloudEventIngressResource#execution sends.
    RecordingAuthorizationService authorization = new RecordingAuthorizationService();
    var provider = new AuthzenAuthenticatedActorProvider(() -> ORGANIZATION, authorization);

    provider.authorize("event.deliver", "execution", "exec-42");

    assertEquals(AuthorizationAction.EVENT_DELIVER, authorization.lastRequest.action());
    assertEquals(AuthorizationResource.execution("exec-42"), authorization.lastRequest.resource());
  }

  @Test
  void authorizeMapsEventDeliverAndScheduleToTheScheduleDeliveryDecisionCloudEventIngressSends() {
    // Exactly the triple CloudEventIngressResource#schedule sends.
    RecordingAuthorizationService authorization = new RecordingAuthorizationService();
    var provider = new AuthzenAuthenticatedActorProvider(() -> ORGANIZATION, authorization);

    provider.authorize("event.deliver", "schedule", "sched-7");

    assertEquals(AuthorizationAction.EVENT_DELIVER, authorization.lastRequest.action());
    assertEquals(AuthorizationResource.schedule("sched-7"), authorization.lastRequest.resource());
  }

  @Test
  void authorizeSendsTheActiveOrganizationIdAsAuthorizationContext() {
    RecordingAuthorizationService authorization = new RecordingAuthorizationService();
    var provider = new AuthzenAuthenticatedActorProvider(() -> ORGANIZATION, authorization);

    provider.authorize("event.route", "event-target", "*");

    assertEquals(Map.of("active_organization_id", "org-1"), authorization.lastRequest.context());
  }

  @Test
  void authorizeRejectsAnUnrecognizedAction() {
    var provider =
        new AuthzenAuthenticatedActorProvider(
            () -> ORGANIZATION, new RecordingAuthorizationService());

    assertThrows(
        IllegalArgumentException.class,
        () -> provider.authorize("event.unknown", "event-target", "*"));
  }

  @Test
  void authorizeRejectsAnUnrecognizedResourceType() {
    var provider =
        new AuthzenAuthenticatedActorProvider(
            () -> ORGANIZATION, new RecordingAuthorizationService());

    assertThrows(
        IllegalArgumentException.class,
        () -> provider.authorize("event.route", "unknown-resource", "*"));
  }

  @Test
  void authorizeRejectsNullArguments() {
    var provider =
        new AuthzenAuthenticatedActorProvider(
            () -> ORGANIZATION, new RecordingAuthorizationService());

    assertThrows(NullPointerException.class, () -> provider.authorize(null, "event-target", "*"));
    assertThrows(NullPointerException.class, () -> provider.authorize("event.route", null, "*"));
    assertThrows(
        NullPointerException.class, () -> provider.authorize("event.route", "event-target", null));
  }

  @Test
  void authorizePropagatesADeniedDecisionFromTheAuthorizationService() {
    var provider =
        new AuthzenAuthenticatedActorProvider(
            () -> ORGANIZATION, new DenyingAuthorizationService());

    assertThrows(
        AuthorizationDeniedException.class,
        () -> provider.authorize("event.route", "event-target", "*"));
  }

  /** Permits every request and records the last one, mirroring a real AuthZEN PDP that allows. */
  private static final class RecordingAuthorizationService implements AuthorizationService {
    private AuthorizationRequest lastRequest;

    @Override
    public AuthorizationDecision evaluate(AuthorizationRequest request) {
      this.lastRequest = request;
      return new AuthorizationDecision(true, request.correlationId(), Map.of());
    }

    @Override
    public List<AuthorizationDecision> evaluateBatch(List<AuthorizationRequest> requests) {
      throw new UnsupportedOperationException("not used by AuthzenAuthenticatedActorProvider");
    }
  }

  /** Denies every request, mirroring a real AuthZEN PDP that rejects. */
  private static final class DenyingAuthorizationService implements AuthorizationService {
    @Override
    public AuthorizationDecision evaluate(AuthorizationRequest request) {
      return new AuthorizationDecision(false, request.correlationId(), Map.of());
    }

    @Override
    public List<AuthorizationDecision> evaluateBatch(List<AuthorizationRequest> requests) {
      throw new UnsupportedOperationException("not used by AuthzenAuthenticatedActorProvider");
    }
  }
}
