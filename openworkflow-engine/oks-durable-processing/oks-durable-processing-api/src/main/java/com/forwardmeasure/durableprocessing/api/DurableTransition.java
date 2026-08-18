package com.forwardmeasure.durableprocessing.api;

import java.util.List;
import java.util.Objects;

/** Domain result committed atomically by the durability implementation. */
public record DurableTransition<S, C, E, O>(
    S state,
    List<E> events,
    List<C> followUpCommands,
    List<O> outbox,
    boolean stateChanged,
    boolean stateRemoved) {

  public DurableTransition {
    if ((stateRemoved && state != null) || (!stateRemoved && state == null)) {
      throw new IllegalArgumentException(
          "A removed transition has no state; every other " + "transition requires state");
    }
    events = List.copyOf(Objects.requireNonNull(events, "events"));
    followUpCommands = List.copyOf(Objects.requireNonNull(followUpCommands, "followUpCommands"));
    outbox = List.copyOf(Objects.requireNonNull(outbox, "outbox"));
    if (stateRemoved && !stateChanged) {
      throw new IllegalArgumentException("Removing durable state is a state change");
    }
    if (stateRemoved && !followUpCommands.isEmpty()) {
      throw new IllegalArgumentException("A removed aggregate cannot emit follow-up commands");
    }
    if (!stateChanged && (!events.isEmpty() || !followUpCommands.isEmpty() || !outbox.isEmpty())) {
      throw new IllegalArgumentException(
          "An unchanged transition cannot emit events or follow-ups");
    }
  }

  public static <S, C, E, O> DurableTransition<S, C, E, O> changed(
      S state, List<E> events, List<C> followUps) {
    return new DurableTransition<>(state, events, followUps, List.of(), true, false);
  }

  public static <S, C, E, O> DurableTransition<S, C, E, O> changed(
      S state, List<E> events, List<C> followUps, List<O> outbox) {
    return new DurableTransition<>(state, events, followUps, outbox, true, false);
  }

  public static <S, C, E, O> DurableTransition<S, C, E, O> unchanged(S state) {
    return new DurableTransition<>(state, List.of(), List.of(), List.of(), false, false);
  }

  public static <S, C, E, O> DurableTransition<S, C, E, O> removed(List<E> events, List<O> outbox) {
    return new DurableTransition<>(null, events, List.of(), outbox, true, true);
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
