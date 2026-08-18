package com.forwardmeasure.durableprocessing.api;

import java.util.List;
import java.util.Objects;

/** Kernel decision before a storage implementation commits it. */
public record DurableDecision<S, C, E, O>(
    DurableDisposition disposition,
    DurableAggregate<S> aggregate,
    List<E> events,
    List<C> followUpCommands,
    List<O> outbox,
    String receiptFingerprint,
    boolean stateChanged,
    boolean stateRemoved) {

  public DurableDecision {
    Objects.requireNonNull(disposition, "disposition");
    events = List.copyOf(Objects.requireNonNull(events, "events"));
    followUpCommands = List.copyOf(Objects.requireNonNull(followUpCommands, "followUpCommands"));
    outbox = List.copyOf(Objects.requireNonNull(outbox, "outbox"));
    if (disposition == DurableDisposition.APPLIED && aggregate == null && !stateRemoved) {
      throw new IllegalArgumentException(
          "An applied decision requires aggregate state unless it " + "removed that state");
    }
    if (stateRemoved
        && (disposition != DurableDisposition.APPLIED || aggregate != null || !stateChanged)) {
      throw new IllegalArgumentException("Only an applied state change can remove an aggregate");
    }
    if (disposition != DurableDisposition.APPLIED
        && (!events.isEmpty()
            || !followUpCommands.isEmpty()
            || !outbox.isEmpty()
            || stateChanged
            || stateRemoved)) {
      throw new IllegalArgumentException(
          "A non-applied decision cannot change state or emit output");
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
