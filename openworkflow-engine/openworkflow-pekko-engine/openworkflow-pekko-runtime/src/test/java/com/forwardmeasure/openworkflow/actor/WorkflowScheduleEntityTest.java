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
package com.forwardmeasure.openworkflow.actor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.persistence.testkit.javadsl.EventSourcedBehaviorTestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class WorkflowScheduleEntityTest {
  private static final Instant AT = Instant.parse("2030-08-15T12:00:00Z");
  private static ActorTestKit actorTestKit;

  @BeforeAll
  static void start() {
    actorTestKit = ActorTestKit.create(EventSourcedBehaviorTestKit.config());
  }

  @AfterAll
  static void stop() {
    actorTestKit.shutdownTestKit();
  }

  @Test
  void persistsEveryCronAfterAndAtLeastOnceDispatchAcrossRecovery() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:schedule");
    var actor = new ActorIdentity(tenant, "did:web:forwardmeasure.com:actor:scheduler");
    var plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: durable-schedule
                  version: '1.0.0'
                schedule:
                  every: PT1H
                  cron: '0 0 * * *'
                  after: PT2M
                do:
                  - work:
                      set:
                        accepted: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var scheduleId = new ScheduleId(tenant, plan.coordinates());
    assertEquals(scheduleId, ScheduleId.fromEntityId(scheduleId.entityId()));
    var dispatch = actorTestKit.<ScheduledExecutionRequest>createTestProbe();
    var kit =
        EventSourcedBehaviorTestKit.create(
            actorTestKit.system(),
            WorkflowScheduleEntity.create(scheduleId, dispatch.ref()),
            EventSourcedBehaviorTestKit.enabledSerializationSettings());

    UUID registerId = UUID.randomUUID();
    var registered =
        kit.<ScheduleReply>runCommand(
            replyTo ->
                new ScheduleCommand.Register(
                    registerId,
                    scheduleId,
                    actor,
                    plan,
                    JsonNodeFactory.instance.objectNode().put("source", "schedule"),
                    AT,
                    replyTo));
    assertInstanceOf(ScheduleEvent.Registered.class, registered.events().getFirst());
    var active = assertInstanceOf(ScheduleState.Active.class, registered.state());
    assertEquals(AT.plusSeconds(3600), active.nextEvery());
    assertEquals(Instant.parse("2030-08-16T00:00:00Z"), active.nextCron());
    assertEquals(1, registered.replyOfType(ScheduleReply.Accepted.class).revision());

    var launched =
        kit.runCommand(
            new ScheduleCommand.Due(scheduleId, ScheduleTriggerKind.EVERY, active.nextEvery()));
    assertInstanceOf(ScheduleEvent.LaunchRequested.class, launched.events().getFirst());
    var request = dispatch.receiveMessage();
    assertEquals(ScheduleTriggerKind.EVERY, request.trigger());
    assertEquals(active.nextEvery(), request.scheduledAt());
    var afterEvery = assertInstanceOf(ScheduleState.Active.class, launched.state());
    assertEquals(AT.plusSeconds(7200), afterEvery.nextEvery());
    assertTrue(
        afterEvery.pending().stream()
            .anyMatch(pending -> pending.executionId().equals(request.executionId())));

    var recovered = assertInstanceOf(ScheduleState.Active.class, kit.restart().state());
    assertEquals(afterEvery, recovered);
    assertEquals(request, dispatch.receiveMessage());
    var acknowledged =
        kit.runCommand(
            new ScheduleCommand.DispatchAcknowledged(
                scheduleId, request.executionId(), AT.plusSeconds(3601)));
    assertTrue(
        assertInstanceOf(ScheduleState.Active.class, acknowledged.state()).pending().isEmpty());

    var scheduledAfter =
        kit.runCommand(
            new ScheduleCommand.ExecutionCompleted(
                scheduleId, request.executionId(), AT.plusSeconds(10_000)));
    var afterState = assertInstanceOf(ScheduleState.Active.class, scheduledAfter.state());
    Instant afterDeadline = AT.plusSeconds(10_120);
    assertTrue(afterState.afterDeadlines().contains(afterDeadline));
    var duplicateCompletion =
        kit.runCommand(
            new ScheduleCommand.ExecutionCompleted(
                scheduleId, request.executionId(), AT.plusSeconds(20_000)));
    assertTrue(duplicateCompletion.hasNoEvents());

    var afterLaunch =
        kit.runCommand(
            new ScheduleCommand.Due(scheduleId, ScheduleTriggerKind.AFTER, afterDeadline));
    var afterRequest = dispatch.receiveMessage();
    assertEquals(ScheduleTriggerKind.AFTER, afterRequest.trigger());
    assertTrue(
        !assertInstanceOf(ScheduleState.Active.class, afterLaunch.state())
            .afterDeadlines()
            .contains(afterDeadline));

    var stale =
        kit.runCommand(
            new ScheduleCommand.Due(scheduleId, ScheduleTriggerKind.EVERY, active.nextEvery()));
    assertTrue(stale.hasNoEvents());
  }

  @Test
  void pekkoTimerAutomaticallyEmitsAnEveryLaunch() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:live-schedule");
    var actor = new ActorIdentity(tenant, "did:web:forwardmeasure.com:actor:scheduler");
    var plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: live-schedule
                  version: '1.0.0'
                schedule:
                  every: PT0.05S
                do:
                  - work:
                      set:
                        accepted: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var scheduleId = new ScheduleId(tenant, plan.coordinates());
    var dispatch = actorTestKit.<ScheduledExecutionRequest>createTestProbe();
    var kit =
        EventSourcedBehaviorTestKit.create(
            actorTestKit.system(),
            WorkflowScheduleEntity.create(scheduleId, dispatch.ref()),
            EventSourcedBehaviorTestKit.enabledSerializationSettings());
    Instant registeredAt = Instant.now();
    kit.<ScheduleReply>runCommand(
        replyTo ->
            new ScheduleCommand.Register(
                UUID.randomUUID(),
                scheduleId,
                actor,
                plan,
                JsonNodeFactory.instance.objectNode(),
                registeredAt,
                replyTo));

    ScheduledExecutionRequest request = dispatch.receiveMessage(java.time.Duration.ofSeconds(2));
    assertEquals(ScheduleTriggerKind.EVERY, request.trigger());
    assertEquals(registeredAt.plusMillis(50), request.scheduledAt());
  }

  @Test
  void eventSchedulePersistsCorrelationWindowAndDispatchesTenantQualifiedInput() {
    var tenant =
        com.forwardmeasure.openworkflow.actor.TestTenantIds.tenant(
            "did:web:forwardmeasure.com:tenant:event-schedule");
    var actor = new ActorIdentity(tenant, "did:web:forwardmeasure.com:actor:scheduler");
    var plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: event-schedule
                  version: '1.0.0'
                schedule:
                  on:
                    all:
                      - with:
                          type: evidence.first.v1
                        correlate:
                          caseId:
                            from: .caseId
                      - with:
                          type: evidence.second.v1
                        correlate:
                          caseId:
                            from: .caseId
                  read: data
                do:
                  - work:
                      set:
                        accepted: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var scheduleId = new ScheduleId(tenant, plan.coordinates());
    var dispatch = actorTestKit.<ScheduledExecutionRequest>createTestProbe();
    var kit =
        EventSourcedBehaviorTestKit.create(
            actorTestKit.system(),
            WorkflowScheduleEntity.create(scheduleId, dispatch.ref()),
            EventSourcedBehaviorTestKit.enabledSerializationSettings());
    kit.<ScheduleReply>runCommand(
        replyTo ->
            new ScheduleCommand.Register(
                UUID.randomUUID(),
                scheduleId,
                actor,
                plan,
                JsonNodeFactory.instance.objectNode(),
                AT,
                replyTo));

    var first =
        new com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent(
            "1.0",
            "first",
            java.net.URI.create("https://events.test"),
            "evidence.first.v1",
            null,
            AT,
            "application/json",
            JsonNodeFactory.instance.objectNode().put("caseId", "case-7").put("value", 1),
            java.util.Map.of());
    var partial =
        kit.<ScheduleReply>runCommand(
            replyTo ->
                new ScheduleCommand.EventReceived(scheduleId, first, AT.plusSeconds(1), replyTo));
    assertInstanceOf(ScheduleEvent.EventAccepted.class, partial.events().getFirst());
    assertEquals(
        1,
        assertInstanceOf(ScheduleState.Active.class, partial.state())
            .eventWindow()
            .accepted()
            .size());
    assertEquals(scheduleId, partial.replyOfType(ScheduleReply.Accepted.class).scheduleId());
    assertEquals(partial.state(), kit.restart().state());

    var wrong =
        new com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent(
            "1.0",
            "wrong",
            java.net.URI.create("https://events.test"),
            "evidence.second.v1",
            null,
            AT,
            "application/json",
            JsonNodeFactory.instance.objectNode().put("caseId", "other"),
            java.util.Map.of());
    assertTrue(
        kit.runCommand(new ScheduleCommand.EventReceived(scheduleId, wrong, AT.plusSeconds(2)))
            .hasNoEvents());
    var second =
        new com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent(
            "1.0",
            "second",
            java.net.URI.create("https://events.test"),
            "evidence.second.v1",
            null,
            AT,
            "application/json",
            JsonNodeFactory.instance.objectNode().put("caseId", "case-7").put("value", 2),
            java.util.Map.of());
    var triggered =
        kit.runCommand(new ScheduleCommand.EventReceived(scheduleId, second, AT.plusSeconds(3)));
    ScheduleEvent.EventAccepted accepted =
        assertInstanceOf(ScheduleEvent.EventAccepted.class, triggered.events().getFirst());
    assertEquals(ScheduleTriggerKind.EVENT, accepted.request().trigger());
    ScheduledExecutionRequest request = dispatch.receiveMessage();
    assertEquals(tenant, request.executionId().tenantId());
    assertEquals(2, request.input().size());
    assertEquals(1, request.input().get(0).required("value").intValue());
    assertEquals(2, request.input().get(1).required("value").intValue());
    var active = assertInstanceOf(ScheduleState.Active.class, triggered.state());
    assertTrue(active.eventWindow().accepted().isEmpty());
    assertTrue(active.pending().contains(request));
    assertEquals(triggered.state(), kit.restart().state());
    assertEquals(request, dispatch.receiveMessage());
    assertTrue(
        kit.runCommand(new ScheduleCommand.EventReceived(scheduleId, second, AT.plusSeconds(4)))
            .hasNoEvents());
  }
}
