package com.forwardmeasure.durableprocessing.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.durableprocessing.api.DurableAggregate;
import com.forwardmeasure.durableprocessing.api.DurableCommandMetadata;
import com.forwardmeasure.durableprocessing.api.DurableDisposition;
import com.forwardmeasure.durableprocessing.api.DurableTransition;
import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class DurableProcessingKernelTest {
  private static final Instant NOW = Instant.parse("2026-07-28T20:00:00Z");
  private static final String FINGERPRINT = "1".repeat(64);
  private final DurableProcessingKernel<String, Command, String, String> kernel =
      new DurableProcessingKernel<>(
          metadata(),
          (context, state, command) ->
              DurableTransition.changed(
                  command.value(),
                  List.of("changed:" + command.value()),
                  command.followUp()
                      ? List.of(
                          new Command(
                              "continue",
                              command.key(),
                              context.nextRevision(),
                              "next",
                              false,
                              false))
                      : List.of()));

  @Test
  void ownsRevisionAndValidatesFollowUpGuard() {
    var decision =
        kernel.decide(
            "account-1",
            null,
            new Command("start", "account-1", null, "received", true, true),
            FINGERPRINT,
            null);

    assertEquals(DurableDisposition.APPLIED, decision.disposition());
    assertEquals(1, decision.aggregate().revision());
    assertEquals(1, decision.followUpCommands().size());
    assertEquals(1, decision.followUpCommands().getFirst().expectedRevision());
  }

  @Test
  void duplicateCommandDoesNotInvokeDomainProcess() {
    var current = aggregate(3);
    var decision =
        kernel.decide(
            "account-1",
            current,
            new Command("pause", "account-1", null, "ignored", true, false),
            FINGERPRINT,
            FINGERPRINT);

    assertEquals(DurableDisposition.DUPLICATE, decision.disposition());
    assertSame(current, decision.aggregate());
  }

  @Test
  void staleContinuationIsAHarmlessNoOp() {
    var current = aggregate(3);
    var decision =
        kernel.decide(
            "account-1",
            current,
            new Command("continue:2", "account-1", 2L, "ignored", false, false),
            FINGERPRINT,
            null);

    assertEquals(DurableDisposition.STALE, decision.disposition());
    assertSame(current, decision.aggregate());
    assertNull(decision.receiptFingerprint());
  }

  @Test
  void commandIdentityConflictFailsClosed() {
    assertThrows(
        CommandIdentityConflictException.class,
        () ->
            kernel.decide(
                "account-1",
                aggregate(1),
                new Command("start", "account-1", null, "different", true, false),
                "2".repeat(64),
                FINGERPRINT));
  }

  @Test
  void durableUpdatedAtCannotRegressOnLateClientTimestamp() {
    var lateTimestampKernel =
        new DurableProcessingKernel<String, Command, String, String>(
            new DurableCommandMetadata<>() {
              @Override
              public String aggregateKey(Command command) {
                return command.key();
              }

              @Override
              public String commandId(Command command) {
                return command.commandId();
              }

              @Override
              public Instant requestedAt(Command command) {
                return NOW.minusSeconds(60);
              }

              @Override
              public OptionalLong expectedRevision(Command command) {
                return OptionalLong.empty();
              }

              @Override
              public boolean deduplicate(Command command) {
                return false;
              }
            },
            (context, state, command) ->
                DurableTransition.changed("changed", List.of(), List.of()));

    var decision =
        lateTimestampKernel.decide(
            "account-1",
            aggregate(3),
            new Command("late", "account-1", null, "changed", false, false),
            FINGERPRINT,
            null);

    assertEquals(NOW, decision.aggregate().updatedAt());
  }

  @Test
  void removalIsAnAppliedAuditedTransitionWithoutRemainingState() {
    var removalKernel =
        new DurableProcessingKernel<String, Command, String, String>(
            metadata(),
            (context, state, command) ->
                DurableTransition.removed(
                    List.of("removed:" + state), List.of("cleanup:" + command.key())));
    Command purge = new Command("purge", "account-1", 3L, "ignored", true, false);

    var removed = removalKernel.decide("account-1", aggregate(3), purge, FINGERPRINT, null);

    assertEquals(DurableDisposition.APPLIED, removed.disposition());
    assertTrue(removed.stateRemoved());
    assertTrue(removed.stateChanged());
    assertNull(removed.aggregate());
    assertEquals(List.of("removed:current"), removed.events());
    assertEquals(List.of("cleanup:account-1"), removed.outbox());

    var duplicate = removalKernel.decide("account-1", null, purge, FINGERPRINT, FINGERPRINT);
    assertEquals(DurableDisposition.DUPLICATE, duplicate.disposition());
    assertNull(duplicate.aggregate());
  }

  @Test
  void cannotRemoveAnAggregateThatDoesNotExist() {
    var removalKernel =
        new DurableProcessingKernel<String, Command, String, String>(
            metadata(),
            (context, state, command) -> DurableTransition.removed(List.of(), List.of()));
    assertThrows(
        IllegalStateException.class,
        () ->
            removalKernel.decide(
                "account-1",
                null,
                new Command("purge", "account-1", null, "ignored", true, false),
                FINGERPRINT,
                null));
  }

  private static DurableAggregate<String> aggregate(long revision) {
    return new DurableAggregate<>("account-1", revision, "current", NOW, NOW);
  }

  private static DurableCommandMetadata<Command> metadata() {
    return new DurableCommandMetadata<>() {
      @Override
      public String aggregateKey(Command command) {
        return command.key();
      }

      @Override
      public String commandId(Command command) {
        return command.commandId();
      }

      @Override
      public Instant requestedAt(Command command) {
        return NOW;
      }

      @Override
      public OptionalLong expectedRevision(Command command) {
        return command.expectedRevision() == null
            ? OptionalLong.empty()
            : OptionalLong.of(command.expectedRevision());
      }

      @Override
      public boolean deduplicate(Command command) {
        return command.deduplicate();
      }
    };
  }

  private record Command(
      String commandId,
      String key,
      Long expectedRevision,
      String value,
      boolean deduplicate,
      boolean followUp) {}
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
