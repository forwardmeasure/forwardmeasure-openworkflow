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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.actor.ScheduleId;
import com.forwardmeasure.openworkflow.actor.ScheduleReply;
import com.forwardmeasure.openworkflow.actor.WorkflowReply;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent;
import com.forwardmeasure.openworkflow.eventing.CloudEventHttpDecoder;
import com.forwardmeasure.openworkflow.eventing.CloudEventIngress;
import com.forwardmeasure.openworkflow.eventing.CloudEventRouteResult;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

final class CloudEventIngressResourceTest {
  private static final TenantId TENANT =
      new TenantId("did:web:forwardmeasure.com:tenant:event-http");
  private static final Instant RECEIVED = Instant.parse("2026-08-15T12:00:00Z");

  @Test
  void qualifiesExecutionAndScheduleRoutesWithAuthenticatedTenant() {
    var execution = new AtomicReference<ExecutionId>();
    var schedule = new AtomicReference<ScheduleId>();
    var offered = new AtomicReference<WorkflowCloudEvent>();
    var routedTenant = new AtomicReference<TenantId>();
    CloudEventIngress ingress =
        new CloudEventIngress() {
          @Override
          public CompletionStage<WorkflowReply> deliver(
              ExecutionId id, WorkflowCloudEvent event, Instant receivedAt) {
            execution.set(id);
            offered.set(event);
            assertEquals(RECEIVED, receivedAt);
            return new CompletableFuture<>();
          }

          @Override
          public CompletionStage<ScheduleReply> deliver(
              ScheduleId id, WorkflowCloudEvent event, Instant receivedAt) {
            schedule.set(id);
            offered.set(event);
            assertEquals(RECEIVED, receivedAt);
            return new CompletableFuture<>();
          }

          @Override
          public CompletionStage<CloudEventRouteResult> route(
              TenantId tenant, WorkflowCloudEvent event, Instant receivedAt) {
            routedTenant.set(tenant);
            offered.set(event);
            assertEquals(RECEIVED, receivedAt);
            return new CompletableFuture<>();
          }
        };
    var resource =
        new CloudEventIngressResource(
            ingress,
            new AuthenticatedActorProvider() {
              @Override
              public ActorIdentity currentActor() {
                return new ActorIdentity(TENANT, "did:web:forwardmeasure.com:actor:broker");
              }

              @Override
              public ActorIdentity authorize(
                  String action, String resourceType, String resourceId) {
                return currentActor();
              }
            },
            new CloudEventHttpDecoder(new ObjectMapper()),
            Clock.fixed(RECEIVED, ZoneOffset.UTC));
    byte[] event =
        """
        {"specversion":"1.0","id":"http-1","source":"urn:test",
         "type":"example.http.v1","data":{"value":7}}
        """
            .getBytes(StandardCharsets.UTF_8);
    UUID executionValue = UUID.randomUUID();

    resource.execution(executionValue, "application/cloudevents+json", Map.of(), event);
    assertEquals(new ExecutionId(TENANT, executionValue), execution.get());
    assertEquals("http-1", offered.get().id());

    resource.schedule(
        "forwardmeasure",
        "orders",
        "1.0.0",
        "1.0.3",
        "application/cloudevents+json",
        Map.of(),
        event);
    assertEquals(TENANT, schedule.get().tenantId());
    assertEquals("orders", schedule.get().definition().name());
    assertEquals("1.0.3", schedule.get().definition().dsl());

    resource.route("application/cloudevents+json", Map.of(), event);
    assertEquals(TENANT, routedTenant.get());
    assertEquals("http-1", offered.get().id());
  }
}
