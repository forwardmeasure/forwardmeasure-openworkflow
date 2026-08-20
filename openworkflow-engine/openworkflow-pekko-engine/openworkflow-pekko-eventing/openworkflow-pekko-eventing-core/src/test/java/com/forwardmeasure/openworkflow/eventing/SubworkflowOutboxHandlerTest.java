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
package com.forwardmeasure.openworkflow.eventing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.actor.SubworkflowCoordinatorReply;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.ResolvedSubflow;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.pekko.persistence.query.Sequence;
import org.apache.pekko.projection.eventsourced.EventEnvelope;
import org.junit.jupiter.api.Test;

class SubworkflowOutboxHandlerTest {
  private static final Instant AT = Instant.parse("2026-08-15T12:00:00Z");

  @Test
  void verifiesPinnedDefinitionBeforeDurableCoordinatorLaunch() {
    var tenant = new TenantId("did:web:forwardmeasure.com:tenant:subflow-outbox");
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: forwardmeasure
          name: child
          version: '1.0.0'
        do:
          - finish: { set: { child: true } }
        """
            .getBytes(StandardCharsets.UTF_8);
    var plan = new OpenWorkflowCompiler().compile(source);
    var actor = new ActorIdentity(tenant, "did:web:forwardmeasure.com:actor:engine");
    SubworkflowPlanResolver resolver =
        (resolvedTenant, resolvedActor, subflow) -> {
          assertEquals(tenant, resolvedTenant);
          assertEquals(actor, resolvedActor);
          assertEquals(plan.coordinates(), subflow.coordinates());
          assertEquals(plan.sourceSha256(), subflow.sourceSha256());
          assertEquals(plan.definitionSha256(), subflow.definitionSha256());
          return plan;
        };
    var parent = new ExecutionId(tenant, UUID.randomUUID());
    var child = new ExecutionId(tenant, UUID.randomUUID());
    var requested =
        new EngineEvent.SubworkflowRequested(
            UUID.randomUUID(),
            "/child",
            JsonNodeFactory.instance.objectNode(),
            JsonNodeFactory.instance.objectNode(),
            1,
            child.value().toString(),
            child,
            actor,
            new ResolvedSubflow(plan.coordinates(), plan.sourceSha256(), plan.definitionSha256()),
            JsonNodeFactory.instance.objectNode(),
            true,
            null,
            null,
            AT);
    var launched =
        new java.util.concurrent.atomic.AtomicReference<SubworkflowOutboxHandler.LaunchRequest>();
    var handler =
        new SubworkflowOutboxHandler(
            resolver,
            (request, childPlan) -> {
              launched.set(request);
              assertEquals(plan, childPlan);
              return CompletableFuture.completedFuture(
                  new SubworkflowCoordinatorReply(child, 1, true));
            });

    handler
        .process(
            EventEnvelope.<EngineEvent>create(
                Sequence.apply(1L),
                "workflow-execution|" + parent.entityId(),
                2,
                requested,
                AT.toEpochMilli()))
        .toCompletableFuture()
        .join();

    assertEquals(parent, launched.get().parentExecutionId());
    assertEquals(child, launched.get().childExecutionId());
  }

  @Test
  void launchesTheSameDurableCoordinatorForAForkOwnedChildIntent() {
    var tenant = new TenantId("did:web:forwardmeasure.com:tenant:fork-subflow-outbox");
    byte[] source =
        """
        document:
          dsl: '1.0.3'
          namespace: forwardmeasure
          name: fork-child
          version: '1.0.0'
        do:
          - finish: { set: { child: true } }
        """
            .getBytes(StandardCharsets.UTF_8);
    var plan = new OpenWorkflowCompiler().compile(source);
    var actor = new ActorIdentity(tenant, "did:web:forwardmeasure.com:actor:engine");
    var parent = new ExecutionId(tenant, UUID.randomUUID());
    var child = new ExecutionId(tenant, UUID.randomUUID());
    var pin = new ResolvedSubflow(plan.coordinates(), plan.sourceSha256(), plan.definitionSha256());
    var input = JsonNodeFactory.instance.objectNode().put("caseId", "fork-1");
    var requested =
        new EngineEvent.ForkBranchSubworkflowRequested(
            UUID.randomUUID(),
            "/parallel",
            List.of(0, 1),
            "/parallel/child",
            input,
            input,
            3,
            child.value().toString(),
            child,
            actor,
            pin,
            input,
            true,
            null,
            null,
            false,
            AT);
    var launched =
        new java.util.concurrent.atomic.AtomicReference<SubworkflowOutboxHandler.LaunchRequest>();
    var handler =
        new SubworkflowOutboxHandler(
            (resolvedTenant, resolvedActor, resolvedSubflow) -> {
              assertEquals(tenant, resolvedTenant);
              assertEquals(actor, resolvedActor);
              assertEquals(pin, resolvedSubflow);
              return plan;
            },
            (request, childPlan) -> {
              launched.set(request);
              assertEquals(plan, childPlan);
              return CompletableFuture.completedFuture(
                  new SubworkflowCoordinatorReply(child, 1, true));
            });

    handler
        .process(
            EventEnvelope.<EngineEvent>create(
                Sequence.apply(1L),
                "workflow-execution|" + parent.entityId(),
                2,
                requested,
                AT.toEpochMilli()))
        .toCompletableFuture()
        .join();

    assertEquals(parent, launched.get().parentExecutionId());
    assertEquals(child, launched.get().childExecutionId());
    assertEquals(input, launched.get().childInput());
  }
}
