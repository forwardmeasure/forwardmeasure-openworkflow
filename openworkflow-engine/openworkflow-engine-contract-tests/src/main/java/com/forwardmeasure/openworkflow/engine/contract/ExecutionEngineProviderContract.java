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
package com.forwardmeasure.openworkflow.engine.contract;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.openworkflow.engine.api.CommandAcknowledgement;
import com.forwardmeasure.openworkflow.engine.api.EngineCommandException;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommandEnvelope;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Shared behavioral acceptance contract inherited by both real provider test suites. */
public abstract class ExecutionEngineProviderContract {
  protected abstract ExecutionEngineContractFixture createFixture();

  protected abstract ExecutionCommandEnvelope newStartCommand(EngineId engineId);

  @Test
  void providerIsLiveReadyAndSelfIdentifying() throws Exception {
    try (var fixture = createFixture()) {
      var provider = fixture.provider();
      var health = provider.health();
      assertEquals(provider.engineId(), health.engineId());
      assertTrue(health.live());
      assertTrue(health.ready());
    }
  }

  @Test
  void startIsDurableProjectedAndIdempotentByCommandId() throws Exception {
    try (var fixture = createFixture()) {
      var provider = fixture.provider();
      var start = newStartCommand(provider.engineId());

      CommandAcknowledgement first = await(provider.submit(start));
      CommandAcknowledgement duplicate = await(provider.submit(start));

      assertEquals(first, duplicate);
      assertEquals(ExecutionLifecycleState.RUNNING, first.state());
      assertEquals(provider.engineId(), first.engineId());
      assertEquals(
          ExecutionLifecycleState.RUNNING,
          fixture.awaitProjection(first.executionId(), ExecutionLifecycleState.RUNNING).state());
    }
  }

  @Test
  void providerRejectsAnExecutionPinnedToAnotherEngine() throws Exception {
    try (var fixture = createFixture()) {
      var provider = fixture.provider();
      var wrongEngine =
          provider.engineId().equals(EngineId.PEKKO) ? EngineId.KAFKA_STREAMS : EngineId.PEKKO;
      CompletionException failure =
          assertThrows(
              CompletionException.class,
              () -> provider.submit(newStartCommand(wrongEngine)).toCompletableFuture().join());
      var commandFailure = assertInstanceOf(EngineCommandException.class, failure.getCause());
      assertEquals(EngineCommandException.FailureKind.ENGINE_MISMATCH, commandFailure.kind());
    }
  }

  @Test
  void pauseResumeAndCancellationReachTheirDurableObservableStates() throws Exception {
    try (var fixture = createFixture()) {
      var provider = fixture.provider();
      var start = newStartCommand(provider.engineId());
      await(provider.submit(start));

      assertState(
          fixture,
          control(start, new ExecutionCommand.Pause(start.command().executionId())),
          ExecutionLifecycleState.PAUSED);
      assertState(
          fixture,
          control(start, new ExecutionCommand.Resume(start.command().executionId())),
          ExecutionLifecycleState.RUNNING);
      assertState(
          fixture,
          control(
              start, new ExecutionCommand.Cancel(start.command().executionId(), "contract test")),
          ExecutionLifecycleState.CANCELLED);
    }
  }

  private static void assertState(
      ExecutionEngineContractFixture fixture,
      ExecutionCommandEnvelope command,
      ExecutionLifecycleState expected)
      throws Exception {
    var acknowledgement = await(fixture.provider().submit(command));
    assertEquals(expected, acknowledgement.state());
    assertEquals(
        expected, fixture.awaitProjection(acknowledgement.executionId(), expected).state());
  }

  private static ExecutionCommandEnvelope control(
      ExecutionCommandEnvelope basis, ExecutionCommand command) {
    return new ExecutionCommandEnvelope(
        UUID.randomUUID(),
        basis.correlationId(),
        basis.context(),
        basis.selectedEngine(),
        switch (command) {
          case ExecutionCommand.Pause ignored -> 1;
          case ExecutionCommand.Resume ignored -> 2;
          case ExecutionCommand.Cancel ignored -> 3;
          case ExecutionCommand.Start ignored -> 0;
        },
        Instant.now(),
        command);
  }

  private static <T> T await(java.util.concurrent.CompletionStage<T> stage) throws Exception {
    return stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
  }
}
