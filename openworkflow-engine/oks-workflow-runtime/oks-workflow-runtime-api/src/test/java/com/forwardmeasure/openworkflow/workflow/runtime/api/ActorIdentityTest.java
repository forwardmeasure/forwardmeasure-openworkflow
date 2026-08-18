package com.forwardmeasure.openworkflow.workflow.runtime.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ActorIdentityTest {
  private static final OksTenantId TENANT = OksTenantId.parse("did:web:tenant.example.com");
  private static final Instant AUTHENTICATED = Instant.parse("2026-07-28T12:00:00Z");

  @Test
  void roundTripsCanonicalActorDid() {
    var actor = actor("2ab3aea3-0972-4eac-8a9d-bcd4a5f0cc45");

    assertEquals(actor, ActorId.parse(actor.toString()));
  }

  @Test
  void distinctActorDidsAreNotTheSameActor() {
    var first = actor("2ab3aea3-0972-4eac-8a9d-bcd4a5f0cc45");
    var second = actor("93b2603e-b3f0-4637-a26c-2288795fcba4");

    assertNotEquals(first, second);
  }

  @Test
  void makerCheckerUsesStableActorIdentityNotDisplayName() {
    var actor = actor("2ab3aea3-0972-4eac-8a9d-bcd4a5f0cc45");
    var maker = context(actor, "Original Name");
    var renamedChecker = context(actor, "Renamed User");
    var independentChecker =
        context(actor("93b2603e-b3f0-4637-a26c-2288795fcba4"), "Original Name");

    assertTrue(maker.sameActor(renamedChecker));
    assertFalse(maker.sameActor(independentChecker));
  }

  @Test
  void rejectsSelfDelegation() {
    var actor = actor("2ab3aea3-0972-4eac-8a9d-bcd4a5f0cc45");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ActorContext(
                TENANT, actor, ActorType.HUMAN, "User", "client", Set.of(), actor, AUTHENTICATED));
  }

  private static ActorContext context(ActorId actor, String displayName) {
    return new ActorContext(
        TENANT,
        actor,
        ActorType.HUMAN,
        displayName,
        "client",
        Set.of("approver"),
        null,
        AUTHENTICATED);
  }

  @Test
  void sameActorDidInAnotherTenantIsADifferentActorScope() {
    var actor = actor("2ab3aea3-0972-4eac-8a9d-bcd4a5f0cc45");
    var firstTenant = context(actor, "User");
    var secondTenant =
        new ActorContext(
            OksTenantId.parse("did:web:other-tenant.example.com"),
            actor,
            ActorType.HUMAN,
            "User",
            "client",
            Set.of(),
            null,
            AUTHENTICATED);

    assertFalse(firstTenant.sameActor(secondTenant));
  }

  @Test
  void persistedIdentityCoordinatesAreAtomicAndRetainedWithCorrelation() {
    ActorContext principal =
        new ActorContext(
            TENANT,
            actor("2ab3aea3-0972-4eac-8a9d-bcd4a5f0cc45"),
            ActorType.HUMAN,
            "User",
            "client",
            null,
            Set.of("approver"),
            null,
            AUTHENTICATED,
            "https://auth.example.com/realms/forwardmeasure",
            "2ab3aea3-0972-4eac-8a9d-bcd4a5f0cc45");

    ActorContext correlated = principal.withCorrelationId(BusinessCorrelationId.parse("request-1"));

    assertEquals(principal.identityProvider(), correlated.identityProvider());
    assertEquals(principal.subjectIdentifier(), correlated.subjectIdentifier());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ActorContext(
                TENANT,
                actor("93b2603e-b3f0-4637-a26c-2288795fcba4"),
                ActorType.HUMAN,
                "User",
                "client",
                null,
                Set.of(),
                null,
                AUTHENTICATED,
                "https://auth.example.com/realms/forwardmeasure",
                null));
  }

  private static ActorId actor(String value) {
    return ActorId.parse("did:web:tenant.example.com:actors:" + value);
  }
}
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
