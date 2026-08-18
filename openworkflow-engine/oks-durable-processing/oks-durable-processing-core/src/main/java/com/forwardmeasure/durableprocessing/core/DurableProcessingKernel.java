package com.forwardmeasure.durableprocessing.core;

import com.forwardmeasure.durableprocessing.api.DurableAggregate;
import com.forwardmeasure.durableprocessing.api.DurableCommandMetadata;
import com.forwardmeasure.durableprocessing.api.DurableDecision;
import com.forwardmeasure.durableprocessing.api.DurableDisposition;
import com.forwardmeasure.durableprocessing.api.DurableProcess;
import com.forwardmeasure.durableprocessing.api.DurableProcessContext;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/** Pure revision, idempotency and continuation guard around a domain process. */
public final class DurableProcessingKernel<S, C, E, O> {
  private final DurableCommandMetadata<C> metadata;
  private final DurableProcess<S, C, E, O> process;

  public DurableProcessingKernel(
      DurableCommandMetadata<C> metadata, DurableProcess<S, C, E, O> process) {
    this.metadata = Objects.requireNonNull(metadata, "metadata");
    this.process = Objects.requireNonNull(process, "process");
  }

  public DurableDecision<S, C, E, O> decide(
      String recordKey,
      DurableAggregate<S> current,
      C command,
      String commandFingerprint,
      String priorFingerprint) {
    Objects.requireNonNull(command, "command");
    requireText(recordKey, "recordKey");
    requireSha256(commandFingerprint, "commandFingerprint");
    String aggregateKey = requireText(metadata.aggregateKey(command), "aggregateKey");
    String commandId = requireText(metadata.commandId(command), "commandId");
    Objects.requireNonNull(metadata.requestedAt(command), "requestedAt");
    if (!recordKey.equals(aggregateKey)) {
      throw new IllegalArgumentException("Kafka key does not match command aggregate key");
    }
    if (current != null && !aggregateKey.equals(current.aggregateKey())) {
      throw new IllegalArgumentException("Stored aggregate key does not match command");
    }

    boolean deduplicate = metadata.deduplicate(command);
    if (deduplicate && priorFingerprint != null) {
      requireSha256(priorFingerprint, "priorFingerprint");
      if (!priorFingerprint.equals(commandFingerprint)) {
        throw new CommandIdentityConflictException(commandId);
      }
      return ignored(DurableDisposition.DUPLICATE, current, null);
    }

    long currentRevision = current == null ? 0 : current.revision();
    OptionalLong expected =
        Objects.requireNonNull(metadata.expectedRevision(command), "expectedRevision");
    if (expected.isPresent()) {
      if (expected.getAsLong() < currentRevision) {
        return ignored(DurableDisposition.STALE, current, deduplicate ? commandFingerprint : null);
      }
      if (expected.getAsLong() > currentRevision) {
        throw new FutureRevisionException(expected.getAsLong(), currentRevision);
      }
    }

    long nextRevision = Math.addExact(currentRevision, 1);
    var context =
        new DurableProcessContext(
            aggregateKey, commandId, currentRevision, nextRevision, metadata.requestedAt(command));
    var transition =
        Objects.requireNonNull(
            process.decide(context, current == null ? null : current.state(), command),
            "process transition");

    DurableAggregate<S> aggregate;
    if (transition.stateRemoved()) {
      if (current == null) {
        throw new IllegalStateException("Cannot remove an aggregate that does not exist");
      }
      aggregate = null;
    } else if (transition.stateChanged()) {
      Instant requestedAt = metadata.requestedAt(command);
      Instant updatedAt =
          current == null || !requestedAt.isBefore(current.updatedAt())
              ? requestedAt
              : current.updatedAt();
      aggregate =
          new DurableAggregate<>(
              aggregateKey,
              nextRevision,
              transition.state(),
              current == null ? requestedAt : current.startedAt(),
              updatedAt);
      validateFollowUps(aggregateKey, nextRevision, transition.followUpCommands());
    } else {
      if (current == null) {
        throw new IllegalStateException("A new aggregate must change state");
      }
      aggregate = current;
    }

    return new DurableDecision<>(
        DurableDisposition.APPLIED,
        aggregate,
        transition.events(),
        transition.followUpCommands(),
        transition.outbox(),
        deduplicate ? commandFingerprint : null,
        transition.stateChanged(),
        transition.stateRemoved());
  }

  private void validateFollowUps(String aggregateKey, long revision, List<C> followUps) {
    for (C followUp : followUps) {
      Objects.requireNonNull(followUp, "followUp");
      if (!aggregateKey.equals(metadata.aggregateKey(followUp))) {
        throw new IllegalStateException("Follow-up command targets another aggregate");
      }
      OptionalLong expected = metadata.expectedRevision(followUp);
      if (expected.isEmpty() || expected.getAsLong() != revision) {
        throw new IllegalStateException(
            "Follow-up command must target committed revision " + revision);
      }
    }
  }

  private static <S, C, E, O> DurableDecision<S, C, E, O> ignored(
      DurableDisposition disposition, DurableAggregate<S> current, String receiptFingerprint) {
    return new DurableDecision<>(
        disposition, current, List.of(), List.of(), List.of(), receiptFingerprint, false, false);
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static void requireSha256(String value, String name) {
    Objects.requireNonNull(value, name);
    if (!value.matches("[0-9a-f]{64}")) {
      throw new IllegalArgumentException(name + " must be lowercase SHA-256");
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
