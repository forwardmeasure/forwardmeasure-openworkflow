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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.jpa.tenancy.TenantId;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.AuthorizationDecision;
import com.forwardmeasure.openworkflow.authorization.AuthorizationDeniedException;
import com.forwardmeasure.openworkflow.authorization.AuthorizationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.workflow.runtime.api.InboundCloudEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.OksInboundCloudEventGateway;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

/**
 * Fast, no-broker unit coverage of {@link OksCloudEventIngressResource}'s own logic - decode
 * failures, the inline size limit, and authorization denial - complementing {@code
 * OksCloudEventIngressResourceKafkaIntegrationTest}'s real-broker proof of the happy path. {@link
 * OksInboundCloudEventGateway} is mocked directly (Mockito's inline mock maker, already relied on
 * elsewhere in this reactor - see {@code KafkaEngineQuarkusBindingTest}'s {@code mockConstruction}
 * of the equally-final {@code KafkaStreamsEngineRuntime} - handles mocking a {@code final} class).
 */
class OksCloudEventIngressResourceTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Clock CLOCK =
      Clock.fixed(Instant.parse("2026-08-01T00:00:00Z"), ZoneOffset.UTC);

  private final OksInboundCloudEventGateway gateway = mock(OksInboundCloudEventGateway.class);
  private final ActiveOrganization organization =
      new ActiveOrganization(new TenantId(UUID.randomUUID()), "org-1", "actor-1", Set.of());

  @Test
  void malformedStructuredBodyIsRejectedBeforeTouchingTheGateway() {
    var resource =
        new OksCloudEventIngressResource(
            gateway, () -> organization, permissiveAuthorization(), JSON, CLOCK);

    OksCloudEventIngressException thrown =
        assertThrows(
            OksCloudEventIngressException.class,
            () ->
                resource
                    .ingest(
                        "application/cloudevents+json",
                        Map.of(),
                        "not json".getBytes(StandardCharsets.UTF_8))
                    .toCompletableFuture()
                    .join());
    assertEquals(OksCloudEventIngressException.Kind.MALFORMED, thrown.kind());
    verify(gateway, never()).publish(any());
  }

  @Test
  void missingRequiredCloudEventAttributeIsRejectedAsMalformed() {
    var resource =
        new OksCloudEventIngressResource(
            gateway, () -> organization, permissiveAuthorization(), JSON, CLOCK);

    // Valid JSON, but missing "type" - InboundCloudEvent's own compact constructor rejects this.
    String body = "{\"specversion\":\"1.0\",\"id\":\"e-1\",\"source\":\"urn:test\"}";
    OksCloudEventIngressException thrown =
        assertThrows(
            OksCloudEventIngressException.class,
            () ->
                resource
                    .ingest(
                        "application/cloudevents+json",
                        Map.of(),
                        body.getBytes(StandardCharsets.UTF_8))
                    .toCompletableFuture()
                    .join());
    assertEquals(OksCloudEventIngressException.Kind.MALFORMED, thrown.kind());
    verify(gateway, never()).publish(any());
  }

  @Test
  void oversizedInlineDataIsRejectedAsTooLarge() {
    var resource =
        new OksCloudEventIngressResource(
            gateway, () -> organization, permissiveAuthorization(), JSON, CLOCK);

    String oversizedData = "x".repeat(64 * 1024);
    String body =
        """
        {"specversion":"1.0","id":"e-1","source":"urn:test","type":"t","data":"%s"}
        """
            .formatted(oversizedData);
    OksCloudEventIngressException thrown =
        assertThrows(
            OksCloudEventIngressException.class,
            () ->
                resource
                    .ingest(
                        "application/cloudevents+json",
                        Map.of(),
                        body.getBytes(StandardCharsets.UTF_8))
                    .toCompletableFuture()
                    .join());
    assertEquals(OksCloudEventIngressException.Kind.TOO_LARGE, thrown.kind());
    verify(gateway, never()).publish(any());
  }

  @Test
  void deniedAuthorizationNeverReachesTheGateway() {
    AuthorizationService denying =
        new AuthorizationService() {
          @Override
          public AuthorizationDecision evaluate(AuthorizationRequest request) {
            return new AuthorizationDecision(false, request.correlationId(), Map.of());
          }

          @Override
          public List<AuthorizationDecision> evaluateBatch(List<AuthorizationRequest> requests) {
            return requests.stream().map(this::evaluate).toList();
          }
        };
    var resource =
        new OksCloudEventIngressResource(gateway, () -> organization, denying, JSON, CLOCK);

    String body = "{\"specversion\":\"1.0\",\"id\":\"e-1\",\"source\":\"urn:test\",\"type\":\"t\"}";
    assertThrows(
        AuthorizationDeniedException.class,
        () ->
            resource
                .ingest(
                    "application/cloudevents+json", Map.of(), body.getBytes(StandardCharsets.UTF_8))
                .toCompletableFuture()
                .join());
    verify(gateway, never()).publish(any());
  }

  @Test
  void acceptedCloudEventIsPublishedThroughTheGatewayExactlyOnce()
      throws ExecutionException, InterruptedException {
    when(gateway.publish(any())).thenReturn(CompletableFuture.completedFuture(null));
    var resource =
        new OksCloudEventIngressResource(
            gateway, () -> organization, permissiveAuthorization(), JSON, CLOCK);

    String body = "{\"specversion\":\"1.0\",\"id\":\"e-1\",\"source\":\"urn:test\",\"type\":\"t\"}";
    var response =
        resource
            .ingest("application/cloudevents+json", Map.of(), body.getBytes(StandardCharsets.UTF_8))
            .toCompletableFuture()
            .get();

    assertEquals(202, response.getStatus());
    verify(gateway, times(1)).publish(any(InboundCloudEvent.class));
  }

  private static AuthorizationService permissiveAuthorization() {
    return new AuthorizationService() {
      @Override
      public AuthorizationDecision evaluate(AuthorizationRequest request) {
        return new AuthorizationDecision(true, request.correlationId(), Map.of());
      }

      @Override
      public List<AuthorizationDecision> evaluateBatch(List<AuthorizationRequest> requests) {
        return requests.stream().map(this::evaluate).toList();
      }
    };
  }
}
