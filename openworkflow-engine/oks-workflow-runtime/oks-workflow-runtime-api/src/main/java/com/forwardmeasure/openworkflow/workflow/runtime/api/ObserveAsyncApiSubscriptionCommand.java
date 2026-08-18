package com.forwardmeasure.openworkflow.workflow.runtime.api;

import java.time.Instant;
import java.util.Objects;

/** Structured terminal transport failure for one durable AsyncAPI subscription. */
public record ObserveAsyncApiSubscriptionCommand(
    String commandId,
    ExecutionKey key,
    String subscriptionId,
    WorkflowError error,
    ActorContext actor,
    Instant requestedAt)
    implements ExecutionCommand {

  public ObserveAsyncApiSubscriptionCommand {
    ControlExecutionCommand.requireCommand(commandId, key, actor, requestedAt);
    if (subscriptionId == null || subscriptionId.isBlank()) {
      throw new IllegalArgumentException("subscriptionId must not be blank");
    }
    Objects.requireNonNull(error, "error");
    if (actor.actorType() != ActorType.SYSTEM) {
      throw new IllegalArgumentException("An AsyncAPI observation requires a system actor");
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
