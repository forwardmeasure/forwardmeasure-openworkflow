/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.adapter.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.adapter.api.OperationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationAction;
import com.forwardmeasure.openworkflow.authorization.AuthorizationDecision;
import com.forwardmeasure.openworkflow.authorization.AuthorizationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.BusinessCorrelationId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OksTenantId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowExecutionId;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class AuthzenOperationSecurityResolverTest {
  private static final String TENANT = "134b09a7-1c36-4b89-86e7-a28c88bc5cef";

  @Test
  void authorizesOrganizationScopedOperationBeforeResolvingCredentials() {
    var observed = new AtomicReference<AuthorizationRequest>();
    var resolver =
        new AuthzenOperationSecurityResolver(
            permitting(observed),
            request ->
                java.util.concurrent.CompletableFuture.completedFuture(
                    new SecuredOperationRequest(request)));

    try (var secured = resolver.secure(request("organization-1")).toCompletableFuture().join()) {
      assertEquals("operation-1", secured.request().operationId());
    }

    assertEquals(AuthorizationAction.OPERATION_EXECUTE, observed.get().action());
    assertEquals("openworkflow-operation", observed.get().resource().type());
    assertEquals("organization-1", observed.get().organization().organizationId());
    assertEquals("actor-1", observed.get().organization().actorId());
    assertEquals(TENANT, observed.get().organization().tenantId().toString());
  }

  @Test
  void failsClosedWhenLegacyDurableActorHasNoOrganization() {
    var resolver =
        new AuthzenOperationSecurityResolver(
            permitting(new AtomicReference<>()), OperationSecurityResolver.rejecting());

    CompletionException failure =
        assertThrows(
            CompletionException.class,
            () -> resolver.secure(request(null)).toCompletableFuture().join());

    assertInstanceOf(SecurityException.class, failure.getCause());
  }

  private static AuthorizationService permitting(AtomicReference<AuthorizationRequest> observed) {
    return new AuthorizationService() {
      @Override
      public AuthorizationDecision evaluate(AuthorizationRequest request) {
        observed.set(request);
        return new AuthorizationDecision(true, request.correlationId(), Map.of());
      }

      @Override
      public List<AuthorizationDecision> evaluateBatch(List<AuthorizationRequest> requests) {
        return requests.stream().map(this::evaluate).toList();
      }
    };
  }

  private static OperationRequest request(String organizationId) {
    OksTenantId tenant = OksTenantId.parse("did:forwardmeasure:tenant:" + TENANT);
    ExecutionKey key = new ExecutionKey(tenant, new WorkflowExecutionId("execution-1"));
    var descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("executionKey", key.canonical());
    descriptor.put("taskPath", "invoke");
    descriptor.put("operationId", "operation-1");
    descriptor.put("operationKind", "call");
    descriptor.put("definitionReference", "definition-1");
    descriptor.put("callKind", "HTTP");
    var actor =
        new ActorContext(
            tenant,
            ActorId.parse("did:forwardmeasure:actor:actor-1"),
            ActorType.HUMAN,
            null,
            null,
            BusinessCorrelationId.parse("correlation-1"),
            Set.of("workflow-execution-controller"),
            null,
            Instant.parse("2026-08-18T12:00:00Z"),
            null,
            null,
            organizationId);
    return new OperationRequest(
        "operation-1",
        "call",
        "definition-1",
        descriptor,
        null,
        actor,
        "effect-1",
        Instant.parse("2026-08-18T12:00:01Z"));
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
