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

import java.util.UUID;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Props;
import org.apache.pekko.actor.typed.javadsl.Behaviors;

/**
 * Turns a durable, at-least-once {@link ScheduledExecutionRequest} - emitted whenever a standing
 * event-triggered schedule matches an incoming CloudEvent - into the {@code WorkflowCommand.Start}
 * that actually launches the new execution. {@link WorkflowScheduleSharding#initialize} requires a
 * dispatch target for exactly this; before this class existed, every deployment either had no
 * production caller at all, or would have had to pass {@code system.ignoreRef()} - silently
 * dropping every event-triggered schedule firing, the same "recorded but never acted on" shape as
 * the CloudEvent and subworkflow outboxes.
 */
public final class ScheduledExecutionDispatcher {
  private ScheduledExecutionDispatcher() {}

  public static ActorRef<ScheduledExecutionRequest> spawn(
      ActorSystem<?> system, WorkflowSharding workflows) {
    ActorRef<WorkflowReply> ignored = system.ignoreRef();
    return system.systemActorOf(
        Behaviors.receiveMessage(
            (ScheduledExecutionRequest request) -> {
              workflows
                  .entityRef(request.executionId())
                  .tell(
                      new WorkflowCommand.Start(
                          UUID.randomUUID(),
                          request.executionId(),
                          request.actor(),
                          request.plan(),
                          request.input(),
                          request.scheduledAt(),
                          ignored));
              return Behaviors.same();
            }),
        "scheduled-execution-dispatcher",
        Props.empty());
  }
}
