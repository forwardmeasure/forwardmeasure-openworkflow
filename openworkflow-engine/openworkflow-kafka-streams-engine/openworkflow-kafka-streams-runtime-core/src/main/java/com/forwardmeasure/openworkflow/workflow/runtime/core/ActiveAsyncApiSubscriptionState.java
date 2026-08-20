package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/** Authoritative durable state for one AsyncAPI subscription call. */
public record ActiveAsyncApiSubscriptionState(
    String subscriptionId,
    String taskPath,
    DataReference rawInput,
    DataReference taskInput,
    ExecutionCursor resumeCursor,
    DataReference descriptor,
    List<DataReference> messages,
    Set<String> seenSourcePositions,
    String deadlineTimerId,
    Instant deadlineAt,
    boolean completionReady)
    implements PendingInteraction {

  public ActiveAsyncApiSubscriptionState {
    requireText(subscriptionId, "subscriptionId");
    requireText(taskPath, "taskPath");
    Objects.requireNonNull(rawInput, "rawInput");
    Objects.requireNonNull(taskInput, "taskInput");
    Objects.requireNonNull(resumeCursor, "resumeCursor");
    Objects.requireNonNull(descriptor, "descriptor");
    messages = messages == null ? List.of() : List.copyOf(messages);
    seenSourcePositions =
        seenSourcePositions == null
            ? Set.of()
            : Collections.unmodifiableSortedSet(new TreeSet<>(seenSourcePositions));
    if ((deadlineTimerId == null) != (deadlineAt == null)) {
      throw new IllegalArgumentException("AsyncAPI deadline ID and due time must both be present");
    }
  }

  @Override
  public String interactionId() {
    return subscriptionId;
  }

  private static void requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
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
