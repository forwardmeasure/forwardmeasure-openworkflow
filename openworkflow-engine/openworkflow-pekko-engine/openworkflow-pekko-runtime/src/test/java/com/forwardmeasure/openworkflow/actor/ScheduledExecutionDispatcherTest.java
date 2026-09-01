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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.WorkflowCoordinates;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionStatus;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.testkit.typed.javadsl.TestProbe;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.cluster.MemberStatus;
import org.apache.pekko.cluster.typed.Cluster;
import org.apache.pekko.cluster.typed.Join;
import org.apache.pekko.persistence.testkit.javadsl.EventSourcedBehaviorTestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * {@link ScheduledExecutionDispatcher} is the only production caller that turns a durable {@link
 * ScheduledExecutionRequest} into the {@code WorkflowCommand.Start} that actually launches an
 * execution - before it existed, {@link WorkflowScheduleSharding#initialize} had no dispatch target
 * for this at all, so every event-triggered schedule firing was silently dropped.
 *
 * <p>{@link WorkflowSharding} is a concrete class backed by real Pekko Cluster Sharding (not an
 * interface, so it cannot be faked), and the only way to prove the dispatcher routed the correct
 * {@code Start} to the correct execution id's entity ref is to actually ask that entity afterward.
 * These tests therefore stand up a real, single-node Pekko cluster exactly the way {@code
 * RealSubworkflowRecoveryTest} (openworkflow-pekko-persistence-contract-tests) does for the same
 * reason, but with the in-memory {@link EventSourcedBehaviorTestKit} persistence plugin instead of
 * a real database, since only the routing/dispatch behavior is under test here.
 */
class ScheduledExecutionDispatcherTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(20);
  private static final Instant SCHEDULED_AT = Instant.parse("2026-08-15T12:00:00Z");

  private static ActorTestKit actors;
  private static WorkflowSharding workflows;
  private static ActorRef<ScheduledExecutionRequest> dispatcher;

  @BeforeAll
  static void start() throws Exception {
    Config config =
        EventSourcedBehaviorTestKit.config()
            .withValue("pekko.actor.provider", ConfigValueFactory.fromAnyRef("cluster"))
            .withValue(
                "pekko.remote.artery.canonical.hostname",
                ConfigValueFactory.fromAnyRef("127.0.0.1"))
            .withValue(
                "pekko.remote.artery.bind.hostname", ConfigValueFactory.fromAnyRef("127.0.0.1"))
            .withValue("pekko.remote.artery.canonical.port", ConfigValueFactory.fromAnyRef(0))
            .withValue("pekko.remote.artery.bind.port", ConfigValueFactory.fromAnyRef(0))
            .withValue("pekko.cluster.seed-nodes", ConfigValueFactory.fromIterable(List.of()))
            .resolve();
    actors = ActorTestKit.create("scheduled-execution-dispatcher-test", config);
    joinSelfAndAwaitUp(actors);
    workflows = WorkflowSharding.initialize(actors.system());
    dispatcher = ScheduledExecutionDispatcher.spawn(actors.system(), workflows);
  }

  @AfterAll
  static void stop() {
    actors.shutdownTestKit();
  }

  @Test
  void dispatchedRequestStartsOnlyItsOwnExecutionsEntity() throws InterruptedException {
    var tenant = TestTenantIds.tenant("did:web:forwardmeasure.com:tenant:scheduled-dispatch-a");
    var executionId = new ExecutionId(tenant, UUID.randomUUID());
    var untouched = new ExecutionId(tenant, UUID.randomUUID());

    dispatcher.tell(request(tenant, executionId, "scheduled-dispatch-a"));

    WorkflowReply.StateSnapshot snapshot = awaitStarted(executionId);
    assertEquals(executionId, snapshot.executionId());
    assertEquals(ExecutionStatus.RUNNING, snapshot.status());
    assertEquals(1, snapshot.revision());

    // An execution id that was never handed to the dispatcher must stay untouched - proves the
    // dispatcher routes to the *requested* execution id rather than starting every entity it can
    // reach on this node. Its own dedicated probe, since a probe that already polled a different
    // execution id can still have a stale in-flight reply queued for it.
    var untouchedProbe = TestProbe.<WorkflowReply>create(actors.system());
    workflows
        .entityRef(untouched)
        .tell(new WorkflowCommand.GetState(untouched, untouchedProbe.ref()));
    var untouchedSnapshot =
        untouchedProbe.expectMessageClass(WorkflowReply.StateSnapshot.class, TIMEOUT);
    assertEquals(ExecutionStatus.NEW, untouchedSnapshot.status());
    assertEquals(0, untouchedSnapshot.revision());
  }

  @Test
  void concurrentRequestsForDifferentTenantsAreRoutedToTheirOwnDistinctEntities()
      throws InterruptedException {
    var tenantX = TestTenantIds.tenant("did:web:forwardmeasure.com:tenant:scheduled-dispatch-x");
    var tenantY = TestTenantIds.tenant("did:web:forwardmeasure.com:tenant:scheduled-dispatch-y");
    var executionX = new ExecutionId(tenantX, UUID.randomUUID());
    var executionY = new ExecutionId(tenantY, UUID.randomUUID());

    dispatcher.tell(request(tenantX, executionX, "scheduled-dispatch-x"));
    dispatcher.tell(request(tenantY, executionY, "scheduled-dispatch-y"));

    WorkflowReply.StateSnapshot snapshotX = awaitStarted(executionX);
    WorkflowReply.StateSnapshot snapshotY = awaitStarted(executionY);

    assertEquals(executionX, snapshotX.executionId());
    assertEquals(executionY, snapshotY.executionId());
  }

  private static ScheduledExecutionRequest request(
      TenantId tenant, ExecutionId executionId, String workflowName) {
    var scheduleId =
        new ScheduleId(
            tenant, new WorkflowCoordinates("forwardmeasure", workflowName, "1.0.0", "1.0.3"));
    var actor = new ActorIdentity(tenant, "did:forwardmeasure:actor:" + workflowName);
    return new ScheduledExecutionRequest(
        scheduleId,
        executionId,
        actor,
        plan(workflowName),
        JsonNodeFactory.instance.objectNode().put("seed", workflowName),
        ScheduleTriggerKind.EVERY,
        SCHEDULED_AT);
  }

  private static WorkflowPlan plan(String name) {
    return new OpenWorkflowCompiler()
        .compile(
            ("""
            document:
              dsl: '1.0.3'
              namespace: forwardmeasure
              name: %s
              version: '1.0.0'
            do:
              - finish: { set: { done: true } }
            """
                    .formatted(name))
                .getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Polls GetState until the dispatched Start has actually been persisted for this execution. Uses
   * a probe of its own - a probe already used to poll a different execution id can still have a
   * stale in-flight reply queued for it (an earlier retry's answer arriving just after this method
   * already returned), which a later caller would otherwise read as if it were their own reply.
   */
  private static WorkflowReply.StateSnapshot awaitStarted(ExecutionId executionId)
      throws InterruptedException {
    var probe = TestProbe.<WorkflowReply>create(actors.system());
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      workflows.entityRef(executionId).tell(new WorkflowCommand.GetState(executionId, probe.ref()));
      try {
        WorkflowReply reply = probe.receiveMessage(Duration.ofMillis(300));
        if (reply instanceof WorkflowReply.StateSnapshot snapshot && snapshot.revision() >= 1) {
          return snapshot;
        }
      } catch (AssertionError notYetReplied) {
        // no reply within this short window yet - retry until the overall deadline.
      }
      Thread.sleep(20);
    }
    throw new AssertionError("expected " + executionId + " to have been started by the dispatcher");
  }

  private static void joinSelfAndAwaitUp(ActorTestKit testKit) throws Exception {
    Cluster cluster = Cluster.get(testKit.system());
    cluster.manager().tell(Join.create(cluster.selfMember().address()));
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      MemberStatus status = cluster.selfMember().status();
      if (status.equals(MemberStatus.up()) || status.equals(MemberStatus.weaklyUp())) {
        return;
      }
      Thread.sleep(50);
    }
    throw new AssertionError("single-node test cluster did not become Up");
  }
}
