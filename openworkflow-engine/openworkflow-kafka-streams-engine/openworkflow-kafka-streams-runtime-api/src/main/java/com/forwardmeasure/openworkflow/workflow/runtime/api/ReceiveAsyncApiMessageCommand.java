package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.forwardmeasure.openworkflow.data.DataReference;
import java.time.Instant;

/**
 * One broker message correlated to a durable AsyncAPI subscription.
 *
 * <p>The source position is an opaque, stable transport identity (for Kafka,
 * topic/partition/offset). It is used for workflow-level deduplication and audit independently of a
 * transport consumer's local state.
 */
public record ReceiveAsyncApiMessageCommand(
    String commandId,
    ExecutionKey key,
    String subscriptionId,
    String sourcePosition,
    DataReference message,
    ActorContext actor,
    Instant requestedAt)
    implements ExecutionCommand {

  public ReceiveAsyncApiMessageCommand {
    ControlExecutionCommand.requireCommand(commandId, key, actor, requestedAt);
    requireText(subscriptionId, "subscriptionId");
    requireText(sourcePosition, "sourcePosition");
    if (message == null
        || (message.storage() == DataReference.Storage.INLINE
            && !message.inlineValue().isObject())) {
      throw new IllegalArgumentException("An AsyncAPI message requires a JSON object");
    }
    if (actor.actorType() != ActorType.SYSTEM) {
      throw new IllegalArgumentException("An AsyncAPI message command requires a system actor");
    }
  }

  private static void requireText(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
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
