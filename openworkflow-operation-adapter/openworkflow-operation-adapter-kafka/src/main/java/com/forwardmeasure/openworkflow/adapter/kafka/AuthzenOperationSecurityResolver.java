/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.adapter.kafka;

import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.openworkflow.adapter.api.OperationRequest;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.AuthorizationAction;
import com.forwardmeasure.openworkflow.authorization.AuthorizationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationResource;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Authorizes committed operation effects before delegating ephemeral credential resolution. */
public final class AuthzenOperationSecurityResolver implements OperationSecurityResolver {
  private static final String TENANT_PREFIX = "did:forwardmeasure:tenant:";
  private static final String ACTOR_PREFIX = "did:forwardmeasure:actor:";

  private final AuthorizationService authorization;
  private final OperationSecurityResolver credentials;

  public AuthzenOperationSecurityResolver(
      AuthorizationService authorization, OperationSecurityResolver credentials) {
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.credentials = Objects.requireNonNull(credentials, "credentials");
  }

  @Override
  public CompletionStage<SecuredOperationRequest> secure(OperationRequest request) {
    Objects.requireNonNull(request, "request");
    try {
      var actor = request.requestedBy();
      if (actor.organizationId() == null) {
        throw new SecurityException("Durable actor context has no trusted Organization ID");
      }
      if (actor.correlationId() == null) {
        throw new SecurityException("Durable actor context has no business correlation ID");
      }
      String durableTenant = actor.tenantId().toString();
      if (!durableTenant.startsWith(TENANT_PREFIX)) {
        throw new SecurityException("Durable actor tenant is not a ForwardMeasure tenant DID");
      }
      var organization =
          new ActiveOrganization(
              TenantId.parse(durableTenant.substring(TENANT_PREFIX.length())),
              actor.organizationId(),
              authorizationSubject(actor.actorId().toString()),
              actor.roles());
      authorization.requireAuthorized(
          new AuthorizationRequest(
              organization,
              AuthorizationResource.operation(
                  request.executionKey().canonical(),
                  request.operationId(),
                  request.operationKind()),
              AuthorizationAction.OPERATION_EXECUTE,
              actor.correlationId().value(),
              Map.of(
                  "definition_reference", request.definitionReference(),
                  "effect_id", request.effectId())));
      return credentials.secure(request);
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }

  private static String authorizationSubject(String actorDid) {
    if (!actorDid.startsWith(ACTOR_PREFIX) || actorDid.length() == ACTOR_PREFIX.length()) {
      throw new SecurityException("Durable actor has no recoverable identity subject");
    }
    return actorDid.substring(ACTOR_PREFIX.length());
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
