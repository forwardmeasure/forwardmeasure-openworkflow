package com.forwardmeasure.openworkflow.operation.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceKind;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceReference;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.net.URI;
import java.net.http.HttpClient;
import org.junit.jupiter.api.Test;

final class AgentCardSecurityResolverTest {
  private static final TenantId TENANT =
      new TenantId("did:web:forwardmeasure.com:tenant:agent-card");

  @Test
  void selectsTheFirstSatisfiableAlternativeInDeclaredOrder() {
    String card =
        """
        {"url":"https://agent.example.test/rpc",
         "securitySchemes":{
           "missing":{"type":"apiKey","in":"header","name":"X-Missing"},
           "bearer":{"type":"http","scheme":"Bearer"}},
         "security":[{"missing":[]},{"bearer":["tasks:read"]}]}
        """;
    var selection =
        new AgentCardSecurityResolver(
                (tenant, name) -> {
                  if (!name.equals("bearer")) throw new SecurityException("unavailable " + name);
                  return "tenant-token".toCharArray();
                })
            .select(TENANT, descriptor(card));

    assertEquals(java.util.List.of("bearer"), selection.schemes());
    assertEquals("Bearer tenant-token", selection.headers().get("Authorization"));
  }

  @Test
  void appliesEverySchemeInAConjunctiveRequirementIncludingQueryKeys() {
    String card =
        """
        {"url":"https://agent.example.test/rpc?existing=true",
         "securitySchemes":{
           "headerKey":{"apiKeySecurityScheme":{"location":"header","name":"X-Agent-Key"}},
           "queryKey":{"apiKeySecurityScheme":{"location":"query","name":"access_key"}}},
         "securityRequirements":[{"schemes":{
           "headerKey":{"list":[]},"queryKey":{"list":[]}}}]}
        """;
    var selection =
        new AgentCardSecurityResolver((tenant, name) -> (name + "-secret").toCharArray())
            .select(
                TENANT,
                descriptor(card, URI.create("https://agent.example.test/rpc?existing=true")));

    assertEquals("headerKey-secret", selection.headers().get("X-Agent-Key"));
    assertEquals("existing=true&access_key=queryKey-secret", selection.endpoint().getRawQuery());
    assertEquals(java.util.List.of("headerKey", "queryKey"), selection.schemes());
  }

  @Test
  void rejectsAProtectedCardWhenNoAlternativeCanBeSatisfied() {
    String card =
        """
        {"url":"https://agent.example.test/rpc",
         "securitySchemes":{"key":{"type":"apiKey","in":"header","name":"X-Key"}},
         "security":[{"key":[]}]}
        """;
    var resolver =
        new AgentCardSecurityResolver(
            (tenant, name) -> {
              throw new SecurityException("absent " + name);
            });

    assertThrows(SecurityException.class, () -> resolver.select(TENANT, descriptor(card)));
  }

  @Test
  void materializesDigestChallengeCredentialsWithoutPersistingThem() {
    String card =
        """
        {"url":"https://agent.example.test/rpc",
         "securitySchemes":{"digest":{"type":"http","scheme":"Digest"}},
         "security":[{"digest":[]}]}
        """;
    var selection =
        new AgentCardSecurityResolver((tenant, name) -> "agent:password".toCharArray())
            .select(TENANT, descriptor(card));

    assertEquals("agent", selection.digest().username());
    assertEquals("password", selection.digest().password());
  }

  @Test
  void selectsTenantMutualTlsClientWithoutRequestingAStringSecret() {
    String card =
        """
        {"url":"https://agent.example.test/rpc",
         "securitySchemes":{"tenantCert":{"type":"mutualTLS"}},
         "security":[{"tenantCert":[]}]}
        """;
    HttpClient ordinary = HttpClient.newHttpClient();
    HttpClient tenantClient = HttpClient.newBuilder().build();
    var selection =
        new AgentCardSecurityResolver(
                (tenant, name) -> {
                  throw new AssertionError("mTLS is not a string secret");
                },
                ordinary,
                (tenant, name) -> {
                  assertEquals(TENANT, tenant);
                  assertEquals("tenantCert", name);
                  return tenantClient;
                })
            .select(TENANT, descriptor(card));

    assertSame(tenantClient, selection.client());
  }

  private static ProtocolOperationDescriptor descriptor(String card) {
    return descriptor(card, URI.create("https://agent.example.test/rpc"));
  }

  private static ProtocolOperationDescriptor descriptor(String card, URI endpoint) {
    return new ProtocolOperationDescriptor(
        "agent-card-security",
        ProtocolOperationDescriptor.Kind.A2A,
        ProtocolOperationDescriptor.Mode.RPC_UNARY,
        new WorkflowResourceReference(
            WorkflowResourceKind.A2A_AGENT_CARD,
            URI.create("https://agent.example.test/.well-known/agent-card.json"),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        "a2a-jsonrpc",
        endpoint,
        "tasks/get",
        JsonNodeFactory.instance.objectNode().put("id", "t-1"),
        null,
        null,
        null,
        card);
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
