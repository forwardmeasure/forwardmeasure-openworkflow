package com.forwardmeasure.openworkflow.workflow.runtime.api;

import java.time.Instant;

/** Durable wall-clock observation that resumes one scheduled wait task. */
public record FireTimerCommand(
    String commandId, ExecutionKey key, String timerId, ActorContext actor, Instant requestedAt)
    implements ExecutionCommand {

  public FireTimerCommand {
    ControlExecutionCommand.requireCommand(commandId, key, actor, requestedAt);
    if (timerId == null || timerId.isBlank()) {
      throw new IllegalArgumentException("timerId must not be blank");
    }
    if (actor.actorType() != ActorType.SYSTEM) {
      throw new IllegalArgumentException("A timer command requires a system actor");
    }
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
