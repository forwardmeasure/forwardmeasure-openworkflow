package com.forwardmeasure.openworkflow.operation.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.forwardmeasure.openworkflow.definition.AuthenticationPlan;
import com.forwardmeasure.openworkflow.operation.HttpAuthenticationSupport;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.junit.jupiter.api.Test;

final class WorkflowOAuthLoginCallbackHandlerTest {
  @Test
  void retainsEphemeralSupplierForRefreshAfterRegistryHandleIsRemoved() throws Exception {
    var calls = new AtomicInteger();
    String handle =
        WorkflowOAuthLoginCallbackHandler.register(
            new WorkflowOAuthLoginCallbackHandler.Context(
                () ->
                    new HttpAuthenticationSupport.Credential(
                        AuthenticationPlan.Kind.BEARER,
                        "Bearer token-" + calls.incrementAndGet(),
                        null,
                        null),
                Set.of("events.read"),
                "tenant-worker",
                Clock.fixed(Instant.parse("2026-08-15T12:00:00Z"), ZoneOffset.UTC),
                Duration.ofMinutes(5)));
    var handler = new WorkflowOAuthLoginCallbackHandler();
    handler.configure(
        Map.of(WorkflowOAuthLoginCallbackHandler.CONTEXT_CONFIG, handle), "OAUTHBEARER", List.of());
    WorkflowOAuthLoginCallbackHandler.unregister(handle);

    var first = new OAuthBearerTokenCallback();
    var refreshed = new OAuthBearerTokenCallback();
    handler.handle(new javax.security.auth.callback.Callback[] {first});
    handler.handle(new javax.security.auth.callback.Callback[] {refreshed});

    assertEquals("token-1", first.token().value());
    assertEquals("token-2", refreshed.token().value());
    assertEquals(Set.of("events.read"), refreshed.token().scope());
    assertEquals("tenant-worker", refreshed.token().principalName());
    assertEquals(
        Instant.parse("2026-08-15T12:05:00Z").toEpochMilli(), refreshed.token().lifetimeMs());
    handler.close();
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
