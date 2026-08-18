package com.forwardmeasure.openworkflow.actor;

import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionStatus;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.javadsl.TimerScheduler;

/** Recoverable transport owner whose authoritative intent stays in WorkflowState. */
public final class ProtocolOperationCoordinatorEntity
    extends AbstractBehavior<ProtocolOperationCoordinatorCommand> {
  private static final Duration ASK_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration POLL_INTERVAL = Duration.ofMillis(250);

  private final ProtocolOperationCoordinatorSharding.Coordinates coordinates;
  private final WorkflowEndpoint workflows;
  private final ProtocolTransport transport;
  private final TimerScheduler<ProtocolOperationCoordinatorCommand> timers;
  private boolean queryInFlight;
  private CompletableFuture<org.apache.pekko.Done> active;
  private long generation;
  private boolean deadlineObservationInFlight;

  public static Behavior<ProtocolOperationCoordinatorCommand> create(
      ProtocolOperationCoordinatorSharding.Coordinates coordinates,
      WorkflowSharding workflows,
      ProtocolTransport transport) {
    return Behaviors.setup(
        context ->
            Behaviors.withTimers(
                timers ->
                    new ProtocolOperationCoordinatorEntity(
                        context,
                        coordinates,
                        (executionId, command, timeout) ->
                            workflows
                                .entityRef(executionId)
                                .<WorkflowReply>ask(command::apply, timeout),
                        transport,
                        timers)));
  }

  static Behavior<ProtocolOperationCoordinatorCommand> create(
      ProtocolOperationCoordinatorSharding.Coordinates coordinates,
      WorkflowEndpoint workflows,
      ProtocolTransport transport) {
    return Behaviors.setup(
        context ->
            Behaviors.withTimers(
                timers ->
                    new ProtocolOperationCoordinatorEntity(
                        context, coordinates, workflows, transport, timers)));
  }

  private ProtocolOperationCoordinatorEntity(
      ActorContext<ProtocolOperationCoordinatorCommand> context,
      ProtocolOperationCoordinatorSharding.Coordinates coordinates,
      WorkflowEndpoint workflows,
      ProtocolTransport transport,
      TimerScheduler<ProtocolOperationCoordinatorCommand> timers) {
    super(context);
    this.coordinates = Objects.requireNonNull(coordinates, "coordinates");
    this.workflows = Objects.requireNonNull(workflows, "workflows");
    this.transport = Objects.requireNonNull(transport, "transport");
    this.timers = Objects.requireNonNull(timers, "timers");
    schedule(Duration.ofMillis(10));
  }

  @Override
  public Receive<ProtocolOperationCoordinatorCommand> createReceive() {
    return newReceiveBuilder()
        .onMessage(ProtocolOperationCoordinatorCommand.Start.class, this::start)
        .onMessage(ProtocolOperationCoordinatorCommand.Poll.class, ignored -> poll())
        .onMessage(ProtocolOperationCoordinatorCommand.StateObserved.class, this::stateObserved)
        .onMessage(ProtocolOperationCoordinatorCommand.TransportEnded.class, this::transportEnded)
        .onMessage(
            ProtocolOperationCoordinatorCommand.DeadlineObserved.class, this::deadlineObserved)
        .onSignal(
            PostStop.class,
            ignored -> {
              stopTransport();
              return this;
            })
        .build();
  }

  private Behavior<ProtocolOperationCoordinatorCommand> start(
      ProtocolOperationCoordinatorCommand.Start command) {
    boolean accepted =
        coordinates.executionId().equals(command.executionId())
            && coordinates.operationId().equals(command.operationId());
    command
        .replyTo()
        .tell(
            new ProtocolOperationCoordinatorReply(
                coordinates.executionId(), coordinates.operationId(), accepted));
    if (accepted) getContext().getSelf().tell(new ProtocolOperationCoordinatorCommand.Poll());
    return this;
  }

  private Behavior<ProtocolOperationCoordinatorCommand> poll() {
    if (queryInFlight) return this;
    queryInFlight = true;
    workflows
        .ask(
            coordinates.executionId(),
            replyTo -> new WorkflowCommand.GetRuntimeState(coordinates.executionId(), replyTo),
            ASK_TIMEOUT)
        .whenComplete(
            (reply, failure) ->
                getContext()
                    .getSelf()
                    .tell(
                        new ProtocolOperationCoordinatorCommand.StateObserved(
                            reply instanceof WorkflowReply.RuntimeState runtime
                                ? runtime.state()
                                : null,
                            failure == null ? null : message(failure))));
    return this;
  }

  private Behavior<ProtocolOperationCoordinatorCommand> stateObserved(
      ProtocolOperationCoordinatorCommand.StateObserved observed) {
    queryInFlight = false;
    if (observed.failure() != null || observed.state() == null) {
      schedule(POLL_INTERVAL);
      return this;
    }
    WorkflowRuntimeState state = observed.state();
    if (state.status() == ExecutionStatus.PAUSED
        || state.status() == ExecutionStatus.PAUSING
        || state.status() == ExecutionStatus.CANCELLED
        || state.status() == ExecutionStatus.CANCELLING
        || state.status() == ExecutionStatus.COMPLETED
        || state.status() == ExecutionStatus.FAILED) {
      stopTransport();
      schedule(POLL_INTERVAL);
      return this;
    }
    ProtocolOperationDescriptor operation =
        findOperation(state.taskStack(), coordinates.operationId());
    if (operation == null) {
      stopTransport();
    } else if (operation.subscriptionDeadline() != null
        && !java.time.Instant.now().isBefore(operation.subscriptionDeadline())) {
      stopTransport();
      observeDeadline(operation);
    } else if (active == null) {
      launch(operation);
    }
    schedule(POLL_INTERVAL);
    return this;
  }

  private void launch(ProtocolOperationDescriptor operation) {
    long launchedGeneration = ++generation;
    active =
        transport
            .execute(
                coordinates.executionId(),
                operation,
                (observationId, value, failed, terminal, observedAt) ->
                    workflows
                        .ask(
                            coordinates.executionId(),
                            replyTo ->
                                new WorkflowCommand.ProtocolCallObserved(
                                    coordinates.executionId(),
                                    coordinates.operationId(),
                                    observationId,
                                    value,
                                    failed,
                                    terminal,
                                    observedAt,
                                    replyTo),
                            ASK_TIMEOUT)
                        .thenCompose(reply -> disposition(reply)))
            .toCompletableFuture();
    active.whenComplete(
        (done, failure) ->
            getContext()
                .getSelf()
                .tell(
                    new ProtocolOperationCoordinatorCommand.TransportEnded(
                        launchedGeneration, failure == null ? null : message(failure))));
  }

  private static java.util.concurrent.CompletionStage<ProtocolTransport.ObservationDisposition>
      disposition(WorkflowReply reply) {
    if (reply instanceof WorkflowReply.Accepted accepted) {
      boolean terminal =
          accepted.status() == ExecutionStatus.CANCELLED
              || accepted.status() == ExecutionStatus.COMPLETED
              || accepted.status() == ExecutionStatus.FAILED;
      return CompletableFuture.completedFuture(
          terminal
              ? ProtocolTransport.ObservationDisposition.STOP
              : ProtocolTransport.ObservationDisposition.CONTINUE);
    }
    WorkflowReply.Rejected rejected = (WorkflowReply.Rejected) reply;
    return CompletableFuture.failedFuture(
        new IllegalStateException(rejected.code() + ": " + rejected.message()));
  }

  private Behavior<ProtocolOperationCoordinatorCommand> transportEnded(
      ProtocolOperationCoordinatorCommand.TransportEnded ended) {
    if (ended.generation() == generation) active = null;
    schedule(POLL_INTERVAL);
    return this;
  }

  private void observeDeadline(ProtocolOperationDescriptor operation) {
    if (deadlineObservationInFlight) return;
    deadlineObservationInFlight = true;
    workflows
        .ask(
            coordinates.executionId(),
            replyTo ->
                new WorkflowCommand.ProtocolCallObserved(
                    coordinates.executionId(),
                    coordinates.operationId(),
                    "duration-" + operation.subscriptionDeadline(),
                    com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.nullNode(),
                    false,
                    true,
                    operation.subscriptionDeadline(),
                    replyTo),
            ASK_TIMEOUT)
        .whenComplete(
            (reply, failure) ->
                getContext()
                    .getSelf()
                    .tell(
                        new ProtocolOperationCoordinatorCommand.DeadlineObserved(
                            failure == null && reply instanceof WorkflowReply.Accepted
                                ? null
                                : failure == null
                                    ? "deadline observation rejected"
                                    : message(failure))));
  }

  private Behavior<ProtocolOperationCoordinatorCommand> deadlineObserved(
      ProtocolOperationCoordinatorCommand.DeadlineObserved observed) {
    deadlineObservationInFlight = false;
    schedule(POLL_INTERVAL);
    return this;
  }

  private void stopTransport() {
    if (active != null) {
      active.cancel(true);
      active = null;
      generation++;
    }
  }

  private void schedule(Duration delay) {
    timers.startSingleTimer("poll", new ProtocolOperationCoordinatorCommand.Poll(), delay);
  }

  private static ProtocolOperationDescriptor findOperation(
      List<TaskExecutionFrame> stack, String operationId) {
    for (int index = stack.size() - 1; index >= 0; index--) {
      TaskExecutionFrame frame = stack.get(index);
      if (frame.eventing()
          && frame.event().kind() == EventExecutionFrame.Kind.PROTOCOL_CALL
          && frame.event().operationId().equals(operationId)) {
        return frame.event().protocolOperation();
      }
      if (frame.forking()) {
        ProtocolOperationDescriptor nested = findOperation(frame.fork(), operationId);
        if (nested != null) return nested;
      }
    }
    return null;
  }

  private static ProtocolOperationDescriptor findOperation(
      ForkExecutionFrame fork, String operationId) {
    for (ForkBranchState branch : fork.branches()) {
      ProtocolOperationDescriptor operation = findOperation(branch.taskStack(), operationId);
      if (operation != null) return operation;
    }
    return null;
  }

  private static String message(Throwable failure) {
    Throwable root = failure;
    while (root.getCause() != null) root = root.getCause();
    return root.getMessage() == null ? root.getClass().getSimpleName() : root.getMessage();
  }

  @FunctionalInterface
  interface WorkflowEndpoint {
    java.util.concurrent.CompletionStage<WorkflowReply> ask(
        ExecutionId executionId,
        java.util.function.Function<
                org.apache.pekko.actor.typed.ActorRef<WorkflowReply>, WorkflowCommand>
            command,
        Duration timeout);
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
