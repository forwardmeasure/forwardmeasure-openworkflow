package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.forwardmeasure.openworkflow.data.DataReference;
import java.time.Instant;

/** A CloudEvent durably routed to one active Open Workflow subscription. */
public record ReceiveEventCommand(
    String commandId,
    ExecutionKey key,
    String subscriptionId,
    DataReference event,
    ActorContext actor,
    Instant requestedAt)
    implements ExecutionCommand {

  public ReceiveEventCommand {
    ControlExecutionCommand.requireCommand(commandId, key, actor, requestedAt);
    if (subscriptionId == null || subscriptionId.isBlank()) {
      throw new IllegalArgumentException("subscriptionId must not be blank");
    }
    if (event == null
        || event.storage() != DataReference.Storage.INLINE
        || !event.inlineValue().isObject()) {
      throw new IllegalArgumentException(
          "A received CloudEvent requires an inline object envelope");
    }
    var envelope = event.inlineValue();
    for (String attribute : java.util.List.of("specversion", "id", "source", "type")) {
      if (!envelope.path(attribute).isTextual() || envelope.path(attribute).textValue().isBlank()) {
        throw new IllegalArgumentException("CloudEvent attribute " + attribute + " is required");
      }
    }
    if (!"1.0".equals(envelope.path("specversion").textValue())) {
      throw new IllegalArgumentException("Only CloudEvents specversion 1.0 is supported");
    }
    if (actor.actorType() != ActorType.SYSTEM) {
      throw new IllegalArgumentException("A received event command requires a system actor");
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
