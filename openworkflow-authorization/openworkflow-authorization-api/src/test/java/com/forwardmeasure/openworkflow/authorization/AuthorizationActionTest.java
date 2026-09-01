/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.forwardmeasure.openworkflow.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class AuthorizationActionTest {
  @Test
  void eventRouteAndEventDeliverExposeTheirDocumentedScopes() {
    // These are the CloudEvent-ingress actions AuthzenAuthenticatedActorProvider maps
    // "event.route"/"event.deliver" onto - the scope string is what actually crosses the wire to
    // the AuthZEN PDP, so its exact value matters.
    assertEquals("event:route", AuthorizationAction.EVENT_ROUTE.scope());
    assertEquals("event:deliver", AuthorizationAction.EVENT_DELIVER.scope());
  }

  @Test
  void eventScopesFollowTheSameDomainColonVerbConventionAsEveryOtherAction() {
    for (AuthorizationAction action : AuthorizationAction.values()) {
      assertTrue(
          action.scope().matches("[a-z]+:[a-z]+"),
          () -> action + " scope \"" + action.scope() + "\" must be \"domain:verb\"");
    }
  }

  @Test
  void eventRouteAndEventDeliverAreDistinctFromEveryOtherAction() {
    long eventScopedActions =
        Arrays.stream(AuthorizationAction.values())
            .filter(action -> action.scope().startsWith("event:"))
            .count();
    assertEquals(2, eventScopedActions);
  }
}
