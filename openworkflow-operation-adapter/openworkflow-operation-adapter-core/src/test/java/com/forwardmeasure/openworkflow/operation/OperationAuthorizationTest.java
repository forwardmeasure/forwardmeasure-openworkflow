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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.forwardmeasure.openworkflow.authorization.AuthorizationDecision;
import com.forwardmeasure.openworkflow.authorization.AuthorizationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class OperationAuthorizationTest {
  private static final TenantId TENANT =
      new TenantId(UUID.fromString("b29dca8d-c849-4863-b99e-1b81244d95a7"));
  private static final ExecutionId EXECUTION =
      new ExecutionId(TENANT, UUID.fromString("55c9d8a9-e9a8-4fbd-b068-13c501bff40c"));

  @Test
  void submitsDurableOrganizationContextToAuthzen() {
    AtomicReference<AuthorizationRequest> observed = new AtomicReference<>();
    var authorization =
        new OperationAuthorization(
            service(
                request -> {
                  observed.set(request);
                  return new AuthorizationDecision(true, request.correlationId(), Map.of());
                }));

    authorization.require(
        EXECUTION,
        "operation-1",
        "GRPC",
        new ActorIdentity(
            TENANT,
            "did:forwardmeasure:actor:operator",
            "organization-1",
            Set.of("workflow-execution-controller"),
            "correlation-1"));

    assertEquals("operation:execute", observed.get().action().scope());
    assertEquals("organization-1", observed.get().organization().organizationId());
    assertEquals("operator", observed.get().organization().actorId());
    assertEquals(EXECUTION.entityId(), observed.get().resource().properties().get("execution_id"));
  }

  @Test
  void rejectsLegacyIntentWithoutCallingAuthzen() {
    AtomicReference<AuthorizationRequest> observed = new AtomicReference<>();
    var authorization =
        new OperationAuthorization(
            service(
                request -> {
                  observed.set(request);
                  return new AuthorizationDecision(true, request.correlationId(), Map.of());
                }));

    assertThrows(
        SecurityException.class,
        () ->
            authorization.require(
                EXECUTION,
                "operation-1",
                "HTTP",
                new ActorIdentity(TENANT, "did:forwardmeasure:actor:legacy")));
    assertEquals(null, observed.get());
  }

  private static AuthorizationService service(
      java.util.function.Function<AuthorizationRequest, AuthorizationDecision> evaluation) {
    return new AuthorizationService() {
      @Override
      public AuthorizationDecision evaluate(AuthorizationRequest request) {
        return evaluation.apply(request);
      }

      @Override
      public List<AuthorizationDecision> evaluateBatch(List<AuthorizationRequest> requests) {
        return requests.stream().map(evaluation).toList();
      }
    };
  }
}
