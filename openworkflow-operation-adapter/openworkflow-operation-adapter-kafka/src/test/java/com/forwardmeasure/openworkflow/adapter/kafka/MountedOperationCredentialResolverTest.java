/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.adapter.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.adapter.api.OperationDataReferenceFactory;
import com.forwardmeasure.openworkflow.adapter.api.OperationRequest;
import com.forwardmeasure.openworkflow.adapter.api.ResolvedAuthentication;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.BusinessCorrelationId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OksTenantId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowExecutionId;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class MountedOperationCredentialResolverTest {
  private static final String TENANT = "134b09a7-1c36-4b89-86e7-a28c88bc5cef";

  @Test
  void resolvesAndDestroysTenantMountedAuthenticationAndSupplementalSecrets() {
    var resolver =
        new MountedOperationCredentialResolver(
            (tenant, name) ->
                switch (name) {
                  case "basic-secret" -> "service:password".toCharArray();
                  case "environment" -> "environment-value".toCharArray();
                  default -> throw new IllegalArgumentException(name);
                },
            OperationDataReferenceFactory.boundedInline());
    OperationRequest request = request();
    var descriptor = (com.fasterxml.jackson.databind.node.ObjectNode) request.descriptor();
    descriptor.putObject("authentication").put("kind", "BASIC").put("secretName", "basic-secret");
    descriptor.putArray("secretReferences").add("environment");

    var secured = resolver.secure(request).toCompletableFuture().join();
    assertEquals(ResolvedAuthentication.Kind.BASIC, secured.request().authentication().kind());
    assertEquals(
        "Basic c2VydmljZTpwYXNzd29yZA==",
        secured.request().authentication().authorizationHeader().orElseThrow());
    assertEquals(
        "environment-value",
        new String(
            secured.request().secret("environment").orElseThrow().copyValues().get("value")));

    var authentication = secured.request().authentication();
    var supplemental = secured.request().secret("environment").orElseThrow();
    secured.close();
    assertTrue(authentication.closed());
    assertTrue(supplemental.closed());
  }

  @Test
  void evaluatesCredentialTemplatesWithOnlyEphemeralSecretValues() {
    var resolver =
        new MountedOperationCredentialResolver(
            (tenant, name) -> "expression-token".toCharArray(),
            OperationDataReferenceFactory.boundedInline());
    OperationRequest request = request();
    var descriptor = (com.fasterxml.jackson.databind.node.ObjectNode) request.descriptor();
    descriptor
        .putObject("authentication")
        .put("kind", "BEARER")
        .putObject("expressions")
        .put("token", "${ $secrets.token.value }");
    descriptor.withObject("authentication").putArray("secretReferences").add("token");

    try (var secured = resolver.secure(request).toCompletableFuture().join()) {
      assertEquals(
          "Bearer expression-token",
          secured.request().authentication().authorizationHeader().orElseThrow());
    }
  }

  private static OperationRequest request() {
    OksTenantId tenant = OksTenantId.parse("did:forwardmeasure:tenant:" + TENANT);
    ExecutionKey key = new ExecutionKey(tenant, new WorkflowExecutionId("execution-1"));
    var descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("executionKey", key.canonical());
    descriptor.put("taskPath", "invoke");
    descriptor.put("operationId", "operation-1");
    descriptor.put("operationKind", "call");
    descriptor.put("definitionReference", "definition-1");
    descriptor.put("callKind", "HTTP");
    descriptor.set("taskInput", JsonNodeFactory.instance.objectNode());
    descriptor.set("workflowContext", JsonNodeFactory.instance.objectNode());
    var actor =
        new ActorContext(
            tenant,
            ActorId.parse("did:forwardmeasure:actor:actor-1"),
            ActorType.HUMAN,
            null,
            null,
            BusinessCorrelationId.parse("correlation-1"),
            Set.of(),
            null,
            Instant.parse("2026-08-18T12:00:00Z"),
            null,
            null,
            "organization-1");
    return new OperationRequest(
        "operation-1",
        "call",
        "definition-1",
        descriptor,
        null,
        actor,
        "effect-1",
        Instant.parse("2026-08-18T12:00:01Z"),
        null,
        Map.of(),
        java.util.List.of());
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
