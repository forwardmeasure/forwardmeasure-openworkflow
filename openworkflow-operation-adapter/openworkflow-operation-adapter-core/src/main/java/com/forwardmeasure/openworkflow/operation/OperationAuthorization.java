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
package com.forwardmeasure.openworkflow.operation;

import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.AuthorizationAction;
import com.forwardmeasure.openworkflow.authorization.AuthorizationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationResource;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import java.util.Map;
import java.util.Objects;

/** Shared fail-closed AuthZEN check for durable Pekko operation intents. */
final class OperationAuthorization {
  private static final String ACTOR_DID_PREFIX = "did:forwardmeasure:actor:";
  private final AuthorizationService authorization;

  OperationAuthorization(AuthorizationService authorization) {
    this.authorization = Objects.requireNonNull(authorization, "authorization");
  }

  void require(ExecutionId executionId, String operationId, String kind, ActorIdentity actor) {
    Objects.requireNonNull(actor, "Durable operation intent has no trusted actor context");
    if (actor.organizationId() == null || actor.correlationId() == null) {
      throw new SecurityException("Durable operation actor has no Organization context");
    }
    authorization.requireAuthorized(
        new AuthorizationRequest(
            new ActiveOrganization(
                TenantId.parse(actor.tenantId().value().toString()),
                actor.organizationId(),
                authorizationSubject(actor.actorDid()),
                actor.organizationRoles()),
            AuthorizationResource.operation(executionId.entityId(), operationId, kind),
            AuthorizationAction.OPERATION_EXECUTE,
            actor.correlationId(),
            Map.of("engine", "pekko")));
  }

  private static String authorizationSubject(String actorDid) {
    if (!actorDid.startsWith(ACTOR_DID_PREFIX) || actorDid.length() == ACTOR_DID_PREFIX.length()) {
      throw new SecurityException("Durable operation actor has no recoverable identity subject");
    }
    return actorDid.substring(ACTOR_DID_PREFIX.length());
  }
}
