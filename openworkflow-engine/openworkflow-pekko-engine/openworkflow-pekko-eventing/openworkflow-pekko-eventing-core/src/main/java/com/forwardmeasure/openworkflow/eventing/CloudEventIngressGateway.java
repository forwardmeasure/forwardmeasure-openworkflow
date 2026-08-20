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

import com.forwardmeasure.openworkflow.actor.ScheduleCommand;
import com.forwardmeasure.openworkflow.actor.ScheduleId;
import com.forwardmeasure.openworkflow.actor.ScheduleReply;
import com.forwardmeasure.openworkflow.actor.WorkflowCommand;
import com.forwardmeasure.openworkflow.actor.WorkflowReply;
import com.forwardmeasure.openworkflow.actor.WorkflowScheduleSharding;
import com.forwardmeasure.openworkflow.actor.WorkflowSharding;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;

/** Tenant-qualified ingress routing; subscription discovery remains a separate adapter concern. */
public final class CloudEventIngressGateway implements CloudEventIngress {
  private final WorkflowSharding workflows;
  private final WorkflowScheduleSharding schedules;
  private final ActorSystem<?> system;
  private final Duration timeout;
  private final CloudEventSubscriptionRepository subscriptions;
  private final int maximumTargets;

  public CloudEventIngressGateway(
      WorkflowSharding workflows,
      WorkflowScheduleSharding schedules,
      ActorSystem<?> system,
      Duration timeout) {
    this(workflows, schedules, system, timeout, null, 10_000);
  }

  public CloudEventIngressGateway(
      WorkflowSharding workflows,
      WorkflowScheduleSharding schedules,
      ActorSystem<?> system,
      Duration timeout,
      CloudEventSubscriptionRepository subscriptions,
      int maximumTargets) {
    this.workflows = Objects.requireNonNull(workflows, "workflows");
    this.schedules = Objects.requireNonNull(schedules, "schedules");
    this.system = Objects.requireNonNull(system, "system");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    if (timeout.isZero() || timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must be positive");
    }
    this.subscriptions = subscriptions;
    if (maximumTargets < 1) {
      throw new IllegalArgumentException("maximumTargets must be positive");
    }
    this.maximumTargets = maximumTargets;
  }

  @Override
  public CompletionStage<WorkflowReply> deliver(
      ExecutionId executionId, WorkflowCloudEvent event, Instant receivedAt) {
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(event, "event");
    Objects.requireNonNull(receivedAt, "receivedAt");
    return AskPattern.ask(
        workflows.entityRef(executionId),
        replyTo -> new WorkflowCommand.CloudEventReceived(executionId, event, receivedAt, replyTo),
        timeout,
        system.scheduler());
  }

  @Override
  public CompletionStage<ScheduleReply> deliver(
      ScheduleId scheduleId, WorkflowCloudEvent event, Instant receivedAt) {
    Objects.requireNonNull(scheduleId, "scheduleId");
    Objects.requireNonNull(event, "event");
    Objects.requireNonNull(receivedAt, "receivedAt");
    return AskPattern.ask(
        schedules.entityRef(scheduleId),
        replyTo -> new ScheduleCommand.EventReceived(scheduleId, event, receivedAt, replyTo),
        timeout,
        system.scheduler());
  }

  @Override
  public CompletionStage<CloudEventRouteResult> route(
      TenantId tenantId, WorkflowCloudEvent event, Instant receivedAt) {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(event, "event");
    Objects.requireNonNull(receivedAt, "receivedAt");
    if (subscriptions == null) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("CloudEvent subscription discovery is not configured"));
    }
    return subscriptions
        .candidates(tenantId, event.type(), maximumTargets + 1)
        .thenCompose(
            targets -> {
              if (targets.size() > maximumTargets) {
                return CompletableFuture.failedFuture(
                    new IllegalStateException("CloudEvent target limit exceeded for tenant"));
              }
              var deliveries =
                  targets.stream().map(target -> deliver(target, event, receivedAt)).toList();
              return CompletableFuture.allOf(
                      deliveries.stream()
                          .map(CompletionStage::toCompletableFuture)
                          .toArray(CompletableFuture[]::new))
                  .handle((ignored, failure) -> summarize(deliveries));
            });
  }

  private CompletionStage<RouteOutcome> deliver(
      CloudEventSubscription target, WorkflowCloudEvent event, Instant receivedAt) {
    CompletionStage<?> stage =
        switch (target.targetKind()) {
          case EXECUTION -> deliver(target.executionId(), event, receivedAt);
          case SCHEDULE -> deliver(target.scheduleId(), event, receivedAt);
        };
    return stage.handle(
        (reply, failure) -> {
          if (failure != null) return new RouteOutcome(false, "delivery_failed");
          if (reply instanceof WorkflowReply.Accepted || reply instanceof ScheduleReply.Accepted) {
            return new RouteOutcome(true, null);
          }
          if (reply instanceof WorkflowReply.Rejected rejected) {
            return new RouteOutcome(false, rejected.code());
          }
          return new RouteOutcome(false, ((ScheduleReply.Rejected) reply).code());
        });
  }

  private static CloudEventRouteResult summarize(
      java.util.List<CompletionStage<RouteOutcome>> deliveries) {
    int accepted = 0;
    var codes = new java.util.ArrayList<String>();
    for (CompletionStage<RouteOutcome> delivery : deliveries) {
      RouteOutcome outcome = delivery.toCompletableFuture().join();
      if (outcome.accepted()) accepted++;
      else codes.add(outcome.code());
    }
    return new CloudEventRouteResult(deliveries.size(), accepted, codes);
  }

  private record RouteOutcome(boolean accepted, String code) {}
}
