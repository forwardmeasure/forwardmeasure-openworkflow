package com.forwardmeasure.openworkflow.actor;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Durable parent-task data needed while executing a nested {@code do} scope. */
public record TaskExecutionFrame(
    String taskPath,
    JsonNode rawInput,
    JsonNode input,
    JsonNode collection,
    int iterationIndex,
    String itemVariable,
    String indexVariable,
    Instant waitDeadline,
    Instant timeoutDeadline,
    int attempt,
    TryPhase tryPhase,
    JsonNode error,
    Instant retryStartedAt,
    Duration attemptsElapsed,
    Instant attemptStartedAt,
    ForkExecutionFrame fork,
    EventExecutionFrame event,
    List<Boolean> extensionDecisions) {

  public enum TryPhase {
    BODY,
    RETRY_DELAY,
    CATCH
  }

  public TaskExecutionFrame(String taskPath, JsonNode rawInput, JsonNode input) {
    this(
        taskPath, rawInput, input, null, -1, null, null, null, null, 0, null, null, null, null,
        null, null, null, List.of());
  }

  public static TaskExecutionFrame extending(
      String taskPath, JsonNode rawInput, JsonNode input, List<Boolean> decisions) {
    return new TaskExecutionFrame(
        taskPath, rawInput, input, null, -1, null, null, null, null, 0, null, null, null, null,
        null, null, null, decisions);
  }

  /** Backward-compatible canonical shape for non-extension frames. */
  public TaskExecutionFrame(
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      JsonNode collection,
      int iterationIndex,
      String itemVariable,
      String indexVariable,
      Instant waitDeadline,
      Instant timeoutDeadline,
      int attempt,
      TryPhase tryPhase,
      JsonNode error,
      Instant retryStartedAt,
      Duration attemptsElapsed,
      Instant attemptStartedAt,
      ForkExecutionFrame fork,
      EventExecutionFrame event) {
    this(
        taskPath,
        rawInput,
        input,
        collection,
        iterationIndex,
        itemVariable,
        indexVariable,
        waitDeadline,
        timeoutDeadline,
        attempt,
        tryPhase,
        error,
        retryStartedAt,
        attemptsElapsed,
        attemptStartedAt,
        fork,
        event,
        List.of());
  }

  public TaskExecutionFrame(
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      JsonNode collection,
      int iterationIndex,
      String itemVariable,
      String indexVariable) {
    this(
        taskPath,
        rawInput,
        input,
        collection,
        iterationIndex,
        itemVariable,
        indexVariable,
        null,
        null,
        0,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  public static TaskExecutionFrame waiting(
      String taskPath, JsonNode rawInput, JsonNode input, Instant deadline) {
    return new TaskExecutionFrame(
        taskPath,
        rawInput,
        input,
        null,
        -1,
        null,
        null,
        Objects.requireNonNull(deadline, "deadline"),
        null,
        0,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  public TaskExecutionFrame(
      String taskPath,
      JsonNode rawInput,
      JsonNode input,
      JsonNode collection,
      int iterationIndex,
      String itemVariable,
      String indexVariable,
      Instant waitDeadline) {
    this(
        taskPath,
        rawInput,
        input,
        collection,
        iterationIndex,
        itemVariable,
        indexVariable,
        waitDeadline,
        null,
        0,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  public TaskExecutionFrame {
    Objects.requireNonNull(taskPath, "taskPath");
    if (taskPath.isBlank()) throw new IllegalArgumentException("taskPath must not be blank");
    rawInput = Objects.requireNonNull(rawInput, "rawInput").deepCopy();
    input = Objects.requireNonNull(input, "input").deepCopy();
    if (collection == null || collection.isNull()) {
      collection = null;
      iterationIndex = -1;
      itemVariable = null;
      indexVariable = null;
    } else {
      if (!collection.isArray()) {
        throw new IllegalArgumentException("iteration collection must be an array");
      }
      collection = collection.deepCopy();
      if (iterationIndex < 0 || iterationIndex >= collection.size()) {
        throw new IllegalArgumentException("iteration index is outside the collection");
      }
      if (itemVariable == null
          || itemVariable.isBlank()
          || indexVariable == null
          || indexVariable.isBlank()) {
        throw new IllegalArgumentException("iteration variables must not be blank");
      }
    }
    error = error == null || error.isNull() ? null : error.deepCopy();
    extensionDecisions = extensionDecisions == null ? List.of() : List.copyOf(extensionDecisions);
    if (fork != null
        && (collection != null || waitDeadline != null || tryPhase != null || event != null)) {
      throw new IllegalArgumentException(
          "A fork frame cannot also be an iteration, wait, or try frame");
    }
    if (attempt < 0) {
      throw new IllegalArgumentException("attempt must not be negative");
    }
    if (tryPhase == null) {
      if (attempt != 0
          || error != null
          || retryStartedAt != null
          || attemptsElapsed != null
          || attemptStartedAt != null) {
        throw new IllegalArgumentException("Non-try frames cannot carry retry state");
      }
    } else if (attempt < 1) {
      throw new IllegalArgumentException("Try frames require a positive attempt");
    } else {
      Objects.requireNonNull(retryStartedAt, "retryStartedAt");
      attemptsElapsed = Objects.requireNonNull(attemptsElapsed, "attemptsElapsed");
      Objects.requireNonNull(attemptStartedAt, "attemptStartedAt");
      if (attemptsElapsed.isNegative()) {
        throw new IllegalArgumentException("attemptsElapsed must not be negative");
      }
    }
    if (tryPhase == TryPhase.RETRY_DELAY && waitDeadline == null) {
      throw new IllegalArgumentException("A retry delay requires a deadline");
    }
    if (event != null
        && (collection != null || waitDeadline != null || tryPhase != null || fork != null)) {
      throw new IllegalArgumentException(
          "An event frame cannot also be an iteration, wait, try, or fork frame");
    }
    if (!extensionDecisions.isEmpty()
        && (collection != null
            || waitDeadline != null
            || tryPhase != null
            || fork != null
            || event != null)) {
      throw new IllegalArgumentException(
          "An extension frame cannot also be an iteration, wait, try, fork, or event frame");
    }
  }

  public boolean iterating() {
    return collection != null;
  }

  public boolean waiting() {
    return waitDeadline != null;
  }

  public TaskExecutionFrame advance(int nextIndex) {
    if (!iterating()) throw new IllegalStateException("task frame is not an iteration");
    return new TaskExecutionFrame(
        taskPath,
        rawInput,
        input,
        collection,
        nextIndex,
        itemVariable,
        indexVariable,
        waitDeadline,
        timeoutDeadline,
        attempt,
        tryPhase,
        error,
        retryStartedAt,
        attemptsElapsed,
        attemptStartedAt,
        fork,
        event);
  }

  public TaskExecutionFrame advance(int nextIndex, JsonNode nextCollection) {
    if (!iterating()) throw new IllegalStateException("task frame is not an iteration");
    return new TaskExecutionFrame(
        taskPath,
        rawInput,
        input,
        nextCollection,
        nextIndex,
        itemVariable,
        indexVariable,
        waitDeadline,
        timeoutDeadline,
        attempt,
        tryPhase,
        error,
        retryStartedAt,
        attemptsElapsed,
        attemptStartedAt,
        fork,
        event);
  }

  public TaskExecutionFrame withTimeout(Instant deadline) {
    return new TaskExecutionFrame(
        taskPath,
        rawInput,
        input,
        collection,
        iterationIndex,
        itemVariable,
        indexVariable,
        waitDeadline,
        Objects.requireNonNull(deadline, "deadline"),
        attempt,
        tryPhase,
        error,
        retryStartedAt,
        attemptsElapsed,
        attemptStartedAt,
        fork,
        event);
  }

  public static TaskExecutionFrame trying(
      String taskPath, JsonNode rawInput, JsonNode input, Instant startedAt) {
    return new TaskExecutionFrame(
        taskPath,
        rawInput,
        input,
        null,
        -1,
        null,
        null,
        null,
        null,
        1,
        TryPhase.BODY,
        null,
        Objects.requireNonNull(startedAt, "startedAt"),
        Duration.ZERO,
        startedAt,
        null,
        null);
  }

  public boolean trying() {
    return tryPhase != null;
  }

  public TaskExecutionFrame caught(JsonNode caughtError) {
    return new TaskExecutionFrame(
        taskPath,
        rawInput,
        input,
        null,
        -1,
        null,
        null,
        null,
        timeoutDeadline,
        attempt,
        TryPhase.CATCH,
        Objects.requireNonNull(caughtError, "caughtError"),
        retryStartedAt,
        attemptsElapsed,
        attemptStartedAt,
        null,
        null);
  }

  public TaskExecutionFrame retrying(
      JsonNode raisedError,
      int nextAttempt,
      Instant deadline,
      Instant startedAt,
      Instant failedAt) {
    Duration elapsed = attemptsElapsed.plus(Duration.between(attemptStartedAt, failedAt));
    return new TaskExecutionFrame(
        taskPath,
        rawInput,
        input,
        null,
        -1,
        null,
        null,
        Objects.requireNonNull(deadline, "deadline"),
        timeoutDeadline,
        nextAttempt,
        TryPhase.RETRY_DELAY,
        Objects.requireNonNull(raisedError, "raisedError"),
        Objects.requireNonNull(startedAt, "startedAt"),
        elapsed,
        attemptStartedAt,
        null,
        null);
  }

  public TaskExecutionFrame beginRetry(Instant startedAt) {
    if (tryPhase != TryPhase.RETRY_DELAY) {
      throw new IllegalStateException("Try frame is not awaiting retry");
    }
    return new TaskExecutionFrame(
        taskPath,
        rawInput,
        input,
        null,
        -1,
        null,
        null,
        null,
        timeoutDeadline,
        attempt,
        TryPhase.BODY,
        error,
        retryStartedAt,
        attemptsElapsed,
        Objects.requireNonNull(startedAt, "startedAt"),
        null,
        null);
  }

  public static TaskExecutionFrame forking(
      String taskPath, JsonNode rawInput, JsonNode input, ForkExecutionFrame fork) {
    return new TaskExecutionFrame(
        taskPath,
        rawInput,
        input,
        null,
        -1,
        null,
        null,
        null,
        null,
        0,
        null,
        null,
        null,
        null,
        null,
        Objects.requireNonNull(fork, "fork"),
        null);
  }

  public boolean forking() {
    return fork != null;
  }

  public TaskExecutionFrame withFork(ForkExecutionFrame next) {
    if (!forking()) throw new IllegalStateException("task frame is not a fork");
    return new TaskExecutionFrame(
        taskPath,
        rawInput,
        input,
        null,
        -1,
        null,
        null,
        null,
        timeoutDeadline,
        0,
        null,
        null,
        null,
        null,
        null,
        Objects.requireNonNull(next, "next"),
        null);
  }

  public static TaskExecutionFrame eventing(
      String taskPath, JsonNode rawInput, JsonNode input, EventExecutionFrame event) {
    return new TaskExecutionFrame(
        taskPath,
        rawInput,
        input,
        null,
        -1,
        null,
        null,
        null,
        null,
        0,
        null,
        null,
        null,
        null,
        null,
        null,
        Objects.requireNonNull(event, "event"));
  }

  public boolean eventing() {
    return event != null;
  }

  public TaskExecutionFrame withEvent(EventExecutionFrame next) {
    if (!eventing()) throw new IllegalStateException("task frame is not eventing");
    return eventing(taskPath, rawInput, input, next).withTimeoutOrNone(timeoutDeadline);
  }

  private TaskExecutionFrame withTimeoutOrNone(Instant deadline) {
    return deadline == null ? this : withTimeout(deadline);
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
