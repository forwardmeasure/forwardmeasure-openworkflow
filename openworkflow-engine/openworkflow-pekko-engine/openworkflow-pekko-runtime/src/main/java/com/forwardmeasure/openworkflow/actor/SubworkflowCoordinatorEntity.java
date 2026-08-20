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
package com.forwardmeasure.openworkflow.actor;

import com.forwardmeasure.openworkflow.engine.api.ExecutionStatus;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.TimerScheduler;
import org.apache.pekko.persistence.typed.PersistenceId;
import org.apache.pekko.persistence.typed.RecoveryCompleted;
import org.apache.pekko.persistence.typed.javadsl.CommandHandler;
import org.apache.pekko.persistence.typed.javadsl.Effect;
import org.apache.pekko.persistence.typed.javadsl.EventHandler;
import org.apache.pekko.persistence.typed.javadsl.EventSourcedBehavior;
import org.apache.pekko.persistence.typed.javadsl.RetentionCriteria;
import org.apache.pekko.persistence.typed.javadsl.SignalHandler;

/** Durable at-least-once parent/child workflow coordinator. */
public final class SubworkflowCoordinatorEntity
    extends EventSourcedBehavior<
        SubworkflowCoordinatorCommand, SubworkflowCoordinatorEvent, SubworkflowCoordinatorState> {
  private static final Duration ASK_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration POLL_INTERVAL = Duration.ofMillis(250);

  private final com.forwardmeasure.openworkflow.engine.api.ExecutionId childExecutionId;
  private final WorkflowEndpoint workflows;
  private final ActorContext<SubworkflowCoordinatorCommand> context;
  private final TimerScheduler<SubworkflowCoordinatorCommand> timers;

  public static Behavior<SubworkflowCoordinatorCommand> create(
      com.forwardmeasure.openworkflow.engine.api.ExecutionId childExecutionId,
      WorkflowSharding workflows) {
    Objects.requireNonNull(childExecutionId, "childExecutionId");
    Objects.requireNonNull(workflows, "workflows");
    return Behaviors.setup(
        context ->
            Behaviors.withTimers(
                timers ->
                    new SubworkflowCoordinatorEntity(
                        childExecutionId,
                        (executionId, command, timeout) ->
                            workflows
                                .entityRef(executionId)
                                .<WorkflowReply>ask(command::apply, timeout),
                        context,
                        timers)));
  }

  static Behavior<SubworkflowCoordinatorCommand> create(
      com.forwardmeasure.openworkflow.engine.api.ExecutionId childExecutionId,
      WorkflowEndpoint workflows) {
    return Behaviors.setup(
        context ->
            Behaviors.withTimers(
                timers ->
                    new SubworkflowCoordinatorEntity(
                        childExecutionId, workflows, context, timers)));
  }

  private SubworkflowCoordinatorEntity(
      com.forwardmeasure.openworkflow.engine.api.ExecutionId childExecutionId,
      WorkflowEndpoint workflows,
      ActorContext<SubworkflowCoordinatorCommand> context,
      TimerScheduler<SubworkflowCoordinatorCommand> timers) {
    super(PersistenceId.ofUniqueId("workflow-subflow|" + childExecutionId.entityId()));
    this.childExecutionId = childExecutionId;
    this.workflows = workflows;
    this.context = context;
    this.timers = timers;
  }

  @Override
  public SubworkflowCoordinatorState emptyState() {
    return new SubworkflowCoordinatorState.Empty(childExecutionId);
  }

  @Override
  public CommandHandler<
          SubworkflowCoordinatorCommand, SubworkflowCoordinatorEvent, SubworkflowCoordinatorState>
      commandHandler() {
    return newCommandHandlerBuilder()
        .forAnyState()
        .onCommand(SubworkflowCoordinatorCommand.Launch.class, this::launch)
        .onCommand(SubworkflowCoordinatorCommand.Poll.class, this::poll)
        .onCommand(SubworkflowCoordinatorCommand.ParentObserved.class, this::parentObserved)
        .onCommand(SubworkflowCoordinatorCommand.ChildObserved.class, this::childObserved)
        .onCommand(
            SubworkflowCoordinatorCommand.ParentDeliveryObserved.class,
            this::parentDeliveryObserved)
        .build();
  }

  private Effect<SubworkflowCoordinatorEvent, SubworkflowCoordinatorState> launch(
      SubworkflowCoordinatorState state, SubworkflowCoordinatorCommand.Launch command) {
    if (!childExecutionId.equals(command.childExecutionId())) {
      command
          .replyTo()
          .tell(new SubworkflowCoordinatorReply(childExecutionId, state.revision(), false));
      return Effect().none();
    }
    if (!(state instanceof SubworkflowCoordinatorState.Empty)) {
      boolean same =
          launched(state).parentExecutionId().equals(command.parentExecutionId())
              && launched(state).operationId().equals(command.operationId());
      command
          .replyTo()
          .tell(new SubworkflowCoordinatorReply(childExecutionId, state.revision(), same));
      return Effect().none();
    }
    return Effect()
        .persist(
            new SubworkflowCoordinatorEvent.Launched(
                command.commandId(),
                command.parentExecutionId(),
                command.childExecutionId(),
                command.operationId(),
                command.actor(),
                command.childPlan(),
                command.childInput(),
                command.awaitParent(),
                command.requestedAt()))
        .thenRun(
            persisted -> {
              command
                  .replyTo()
                  .tell(
                      new SubworkflowCoordinatorReply(
                          childExecutionId, persisted.revision(), true));
              startChild((SubworkflowCoordinatorState.Active) persisted);
            });
  }

  private Effect<SubworkflowCoordinatorEvent, SubworkflowCoordinatorState> poll(
      SubworkflowCoordinatorState state, SubworkflowCoordinatorCommand.Poll ignored) {
    if (state instanceof SubworkflowCoordinatorState.Active active) {
      ask(
          active.parentExecutionId(),
          replyTo -> new WorkflowCommand.GetState(active.parentExecutionId(), replyTo),
          (reply, failure) ->
              new SubworkflowCoordinatorCommand.ParentObserved(
                  observation(reply), message(failure)));
    } else if (state instanceof SubworkflowCoordinatorState.Terminal terminal) {
      notifyParent(terminal);
    }
    return Effect().none();
  }

  private Effect<SubworkflowCoordinatorEvent, SubworkflowCoordinatorState> parentObserved(
      SubworkflowCoordinatorState state, SubworkflowCoordinatorCommand.ParentObserved observed) {
    if (!(state instanceof SubworkflowCoordinatorState.Active active)) {
      return Effect().none();
    }
    SubworkflowCoordinatorCommand.WorkflowObservation parent = observed.observation();
    if (observed.failure() != null || parent == null || !parent.snapshot()) {
      schedulePoll();
      return Effect().none();
    }
    if (parent.status() == ExecutionStatus.PAUSED || parent.status() == ExecutionStatus.PAUSING) {
      askControl(active, true);
      schedulePoll();
      return Effect().none();
    }
    if (parent.status() == ExecutionStatus.CANCELLED
        || parent.status() == ExecutionStatus.CANCELLING
        || parent.status() == ExecutionStatus.FAILED
        || parent.status() == ExecutionStatus.COMPLETED) {
      askControl(active, false);
      schedulePoll();
      return Effect().none();
    }
    ask(
        active.childExecutionId(),
        replyTo -> new WorkflowCommand.GetState(active.childExecutionId(), replyTo),
        (reply, failure) ->
            new SubworkflowCoordinatorCommand.ChildObserved(observation(reply), message(failure)));
    return Effect().none();
  }

  private Effect<SubworkflowCoordinatorEvent, SubworkflowCoordinatorState> childObserved(
      SubworkflowCoordinatorState state, SubworkflowCoordinatorCommand.ChildObserved observed) {
    if (!(state instanceof SubworkflowCoordinatorState.Active active)) {
      return Effect().none();
    }
    SubworkflowCoordinatorCommand.WorkflowObservation child = observed.observation();
    if (observed.failure() != null || child == null || !child.snapshot()) {
      if ((observed.cancellation() || observed.pause())
          && observed.failure() == null
          && child != null) {
        ask(
            active.childExecutionId(),
            replyTo -> new WorkflowCommand.GetState(active.childExecutionId(), replyTo),
            (reply, failure) ->
                new SubworkflowCoordinatorCommand.ChildObserved(
                    observation(reply), message(failure),
                    observed.cancellation(), observed.pause()));
        return Effect().none();
      }
      schedulePoll();
      return Effect().none();
    }
    if (observed.pause()
        && child.status() != ExecutionStatus.COMPLETED
        && child.status() != ExecutionStatus.CANCELLED
        && child.status() != ExecutionStatus.FAILED) {
      if (child.status() == ExecutionStatus.NEW) {
        startChild(active);
        schedulePoll();
      } else if (child.status() == ExecutionStatus.RUNNING) {
        askControl(active, true);
      } else {
        schedulePoll();
      }
      return Effect().none();
    }
    if (observed.cancellation()
        && child.status() != ExecutionStatus.COMPLETED
        && child.status() != ExecutionStatus.CANCELLED
        && child.status() != ExecutionStatus.FAILED) {
      if (child.status() == ExecutionStatus.NEW) {
        startChild(active);
        schedulePoll();
      } else {
        askControl(active, false);
      }
      return Effect().none();
    }
    return switch (child.status()) {
      case NEW -> {
        startChild(active);
        yield Effect().none();
      }
      case RUNNING -> {
        UUID commandId = commandId(active, "advance", child.revision());
        ask(
            active.childExecutionId(),
            replyTo ->
                new WorkflowCommand.RunNext(
                    commandId, active.childExecutionId(), active.actor(), Instant.now(), replyTo),
            (reply, failure) ->
                new SubworkflowCoordinatorCommand.ChildObserved(
                    observation(reply), message(failure)));
        yield Effect().none();
      }
      case PAUSED -> {
        UUID commandId = commandId(active, "resume", child.revision());
        ask(
            active.childExecutionId(),
            replyTo ->
                new WorkflowCommand.Resume(
                    commandId, active.childExecutionId(), active.actor(), Instant.now(), replyTo),
            (reply, failure) ->
                new SubworkflowCoordinatorCommand.ChildObserved(
                    observation(reply), message(failure)));
        yield Effect().none();
      }
      case COMPLETED, CANCELLED, FAILED -> {
        var terminal =
            new SubworkflowCoordinatorEvent.ChildTerminalObserved(
                child.status(), child.data(), null, Instant.now());
        if (active.awaitParent()) {
          yield Effect()
              .persist(terminal)
              .thenRun(persisted -> notifyParent((SubworkflowCoordinatorState.Terminal) persisted));
        }
        yield Effect()
            .persist(
                java.util.List.of(
                    terminal, new SubworkflowCoordinatorEvent.ParentNotified(Instant.now())));
      }
      default -> {
        schedulePoll();
        yield Effect().none();
      }
    };
  }

  private Effect<SubworkflowCoordinatorEvent, SubworkflowCoordinatorState> parentDeliveryObserved(
      SubworkflowCoordinatorState state,
      SubworkflowCoordinatorCommand.ParentDeliveryObserved observed) {
    if (!(state instanceof SubworkflowCoordinatorState.Terminal)) {
      return Effect().none();
    }
    if (observed.failure() == null && observed.accepted()) {
      return Effect().persist(new SubworkflowCoordinatorEvent.ParentNotified(Instant.now()));
    }
    schedulePoll();
    return Effect().none();
  }

  private void startChild(SubworkflowCoordinatorState.Active active) {
    ask(
        active.childExecutionId(),
        replyTo ->
            new WorkflowCommand.Start(
                commandId(active, "start", 0),
                active.childExecutionId(),
                active.actor(),
                active.childPlan(),
                active.childInput(),
                Instant.now(),
                replyTo),
        (reply, failure) ->
            new SubworkflowCoordinatorCommand.ChildObserved(observation(reply), message(failure)));
  }

  private void askControl(SubworkflowCoordinatorState.Active active, boolean pause) {
    UUID id = commandId(active, pause ? "pause" : "cancel", active.revision());
    ask(
        active.childExecutionId(),
        replyTo ->
            pause
                ? new WorkflowCommand.Pause(
                    id, active.childExecutionId(), active.actor(), Instant.now(), replyTo)
                : new WorkflowCommand.Cancel(
                    id, active.childExecutionId(), active.actor(), Instant.now(), replyTo),
        (reply, failure) ->
            new SubworkflowCoordinatorCommand.ChildObserved(
                observation(reply), message(failure), !pause, pause));
  }

  private void notifyParent(SubworkflowCoordinatorState.Terminal terminal) {
    SubworkflowCoordinatorState.Active active = terminal.launch();
    ask(
        active.parentExecutionId(),
        replyTo ->
            new WorkflowCommand.SubworkflowCompleted(
                commandId(active, "observe", terminal.revision()),
                active.parentExecutionId(),
                active.operationId(),
                active.childExecutionId(),
                terminal.status(),
                terminal.output(),
                terminal.failure(),
                Instant.now(),
                replyTo),
        (reply, failure) ->
            new SubworkflowCoordinatorCommand.ParentDeliveryObserved(
                reply instanceof WorkflowReply.Accepted, message(failure)));
  }

  private <T extends WorkflowReply> void ask(
      com.forwardmeasure.openworkflow.engine.api.ExecutionId executionId,
      java.util.function.Function<
              org.apache.pekko.actor.typed.ActorRef<WorkflowReply>, WorkflowCommand>
          command,
      org.apache.pekko.japi.function.Function2<
              WorkflowReply, Throwable, SubworkflowCoordinatorCommand>
          mapper) {
    context.pipeToSelf(workflows.ask(executionId, command, ASK_TIMEOUT), mapper);
  }

  private void schedulePoll() {
    timers.startSingleTimer("poll", new SubworkflowCoordinatorCommand.Poll(), POLL_INTERVAL);
  }

  private static String message(Throwable failure) {
    return failure == null
        ? null
        : failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
  }

  private static SubworkflowCoordinatorCommand.WorkflowObservation observation(
      WorkflowReply reply) {
    if (reply == null) return null;
    return switch (reply) {
      case WorkflowReply.StateSnapshot snapshot ->
          new SubworkflowCoordinatorCommand.WorkflowObservation(
              snapshot.executionId(),
              snapshot.revision(),
              snapshot.status(),
              snapshot.data(),
              true,
              true,
              null);
      case WorkflowReply.RuntimeState runtime ->
          new SubworkflowCoordinatorCommand.WorkflowObservation(
              runtime.state().executionId(),
              runtime.state().revision(),
              runtime.state().status(),
              runtime.state().data(),
              true,
              true,
              null);
      case WorkflowReply.Accepted accepted ->
          new SubworkflowCoordinatorCommand.WorkflowObservation(
              accepted.executionId(),
              accepted.revision(),
              accepted.status(),
              com.fasterxml.jackson.databind.node.NullNode.getInstance(),
              true,
              false,
              null);
      case WorkflowReply.Rejected rejected ->
          new SubworkflowCoordinatorCommand.WorkflowObservation(
              rejected.executionId(),
              rejected.revision(),
              rejected.status(),
              com.fasterxml.jackson.databind.node.NullNode.getInstance(),
              false,
              false,
              rejected.code());
    };
  }

  private static UUID commandId(
      SubworkflowCoordinatorState.Active active, String action, long revision) {
    return UUID.nameUUIDFromBytes(
        (active.childExecutionId().entityId() + "|subflow-coordinator|" + action + "|" + revision)
            .getBytes(StandardCharsets.UTF_8));
  }

  private static SubworkflowCoordinatorState.Active launched(SubworkflowCoordinatorState state) {
    return switch (state) {
      case SubworkflowCoordinatorState.Active active -> active;
      case SubworkflowCoordinatorState.Terminal terminal -> terminal.launch();
      case SubworkflowCoordinatorState.Delivered delivered -> delivered.terminal().launch();
      case SubworkflowCoordinatorState.Empty ignored ->
          throw new IllegalStateException("Coordinator has not launched");
    };
  }

  @Override
  public EventHandler<SubworkflowCoordinatorState, SubworkflowCoordinatorEvent> eventHandler() {
    return newEventHandlerBuilder()
        .forAnyState()
        .onAnyEvent(
            (state, event) ->
                switch (event) {
                  case SubworkflowCoordinatorEvent.Launched launched ->
                      new SubworkflowCoordinatorState.Active(
                          launched.commandId(),
                          launched.parentExecutionId(),
                          launched.childExecutionId(),
                          launched.operationId(),
                          launched.actor(),
                          launched.childPlan(),
                          launched.childInput(),
                          launched.awaitParent(),
                          state.revision() + 1);
                  case SubworkflowCoordinatorEvent.ChildTerminalObserved terminal ->
                      new SubworkflowCoordinatorState.Terminal(
                          (SubworkflowCoordinatorState.Active) state,
                          terminal.status(),
                          terminal.output(),
                          terminal.failure(),
                          state.revision() + 1);
                  case SubworkflowCoordinatorEvent.ParentNotified ignored ->
                      new SubworkflowCoordinatorState.Delivered(
                          (SubworkflowCoordinatorState.Terminal) state, state.revision() + 1);
                });
  }

  @Override
  public SignalHandler<SubworkflowCoordinatorState> signalHandler() {
    return newSignalHandlerBuilder()
        .onSignal(
            RecoveryCompleted.class,
            (state, ignored) -> {
              if (state instanceof SubworkflowCoordinatorState.Active
                  || state instanceof SubworkflowCoordinatorState.Terminal) {
                timers.startSingleTimer(
                    "poll", new SubworkflowCoordinatorCommand.Poll(), Duration.ZERO);
              }
            })
        .build();
  }

  @Override
  public RetentionCriteria retentionCriteria() {
    return RetentionCriteria.snapshotEvery(50, 3);
  }

  @FunctionalInterface
  interface WorkflowEndpoint {
    java.util.concurrent.CompletionStage<WorkflowReply> ask(
        com.forwardmeasure.openworkflow.engine.api.ExecutionId executionId,
        java.util.function.Function<
                org.apache.pekko.actor.typed.ActorRef<WorkflowReply>, WorkflowCommand>
            command,
        Duration timeout);
  }
}
