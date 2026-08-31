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

import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.openworkflow.definition.SchedulePlan;
import com.forwardmeasure.openworkflow.engine.api.EventConsumptionWindow;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.expression.RuntimeExpressionArguments;
import com.typesafe.config.Config;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.TimerScheduler;
import org.apache.pekko.persistence.typed.PersistenceId;
import org.apache.pekko.persistence.typed.RecoveryCompleted;
import org.apache.pekko.persistence.typed.javadsl.CommandHandlerWithReply;
import org.apache.pekko.persistence.typed.javadsl.EventHandler;
import org.apache.pekko.persistence.typed.javadsl.EventSourcedBehaviorWithEnforcedReplies;
import org.apache.pekko.persistence.typed.javadsl.ReplyEffect;
import org.apache.pekko.persistence.typed.javadsl.SignalHandler;

/** Durable Pekko coordinator for temporal Open Workflow schedule triggers. */
public final class WorkflowScheduleEntity
    extends EventSourcedBehaviorWithEnforcedReplies<ScheduleCommand, ScheduleEvent, ScheduleState> {
  /** See {@link WorkflowEntity#PROJECTION_TAG_COUNT}'s identical reasoning for reducing 32 -> 8. */
  public static final int PROJECTION_TAG_COUNT = 8;

  public static final String PROJECTION_TAG_PREFIX = "openworkflow-schedule-";
  private static final Duration MAX_TIMER_HORIZON = Duration.ofDays(30);
  private static final Duration DISPATCH_RETRY = Duration.ofSeconds(5);

  private final ScheduleId scheduleId;
  private final ActorRef<ScheduledExecutionRequest> dispatch;
  private final TimerScheduler<ScheduleCommand> timers;
  private final ActorSystem<?> system;
  private final Optional<PostgresConnectionSettings> postgresConnection;
  private final ScheduleTemporalPlanner planner = new ScheduleTemporalPlanner();
  private final CloudEventConsumptionEvaluator eventConsumption =
      new CloudEventConsumptionEvaluator();

  public static Behavior<ScheduleCommand> create(
      ScheduleId scheduleId, ActorRef<ScheduledExecutionRequest> dispatch) {
    return create(scheduleId, dispatch, Optional.empty());
  }

  public static Behavior<ScheduleCommand> create(
      ScheduleId scheduleId,
      ActorRef<ScheduledExecutionRequest> dispatch,
      Optional<PostgresConnectionSettings> postgresConnection) {
    Objects.requireNonNull(scheduleId, "scheduleId");
    Objects.requireNonNull(dispatch, "dispatch");
    Objects.requireNonNull(postgresConnection, "postgresConnection");
    return Behaviors.setup(
        context ->
            Behaviors.withTimers(
                timers ->
                    new WorkflowScheduleEntity(
                        scheduleId, dispatch, timers, context.getSystem(), postgresConnection)));
  }

  private WorkflowScheduleEntity(
      ScheduleId scheduleId,
      ActorRef<ScheduledExecutionRequest> dispatch,
      TimerScheduler<ScheduleCommand> timers,
      ActorSystem<?> system,
      Optional<PostgresConnectionSettings> postgresConnection) {
    super(PersistenceId.ofUniqueId("workflow-schedule|" + scheduleId.entityId()));
    this.scheduleId = scheduleId;
    this.dispatch = dispatch;
    this.timers = timers;
    this.system = Objects.requireNonNull(system, "system");
    this.postgresConnection = Objects.requireNonNull(postgresConnection, "postgresConnection");
  }

  /** See {@link WorkflowEntity}'s identical overrides for why this per-tenant routing is needed. */
  @Override
  public String journalPluginId() {
    return postgresConnection
        .map(connection -> TenantPersistencePlugins.journalPluginId(tenantSchema()))
        .orElseGet(super::journalPluginId);
  }

  @Override
  public Optional<Config> journalPluginConfig() {
    return postgresConnection.isEmpty()
        ? super.journalPluginConfig()
        : TenantPersistencePlugins.journalPluginConfig(
            system, tenantSchema(), postgresConnection.get());
  }

  @Override
  public String snapshotPluginId() {
    return postgresConnection
        .map(connection -> TenantPersistencePlugins.snapshotPluginId(tenantSchema()))
        .orElseGet(super::snapshotPluginId);
  }

  @Override
  public Optional<Config> snapshotPluginConfig() {
    return postgresConnection.isEmpty()
        ? super.snapshotPluginConfig()
        : TenantPersistencePlugins.snapshotPluginConfig(
            system, tenantSchema(), postgresConnection.get());
  }

  private TenantSchema tenantSchema() {
    return TenantSchema.forTenant(
        new com.forwardmeasure.jpa.tenancy.TenantId(scheduleId.tenantId().value()));
  }

  @Override
  public ScheduleState emptyState() {
    return new ScheduleState.Unregistered(scheduleId);
  }

  @Override
  public CommandHandlerWithReply<ScheduleCommand, ScheduleEvent, ScheduleState> commandHandler() {
    var builder = newCommandHandlerWithReplyBuilder();
    builder
        .forStateType(ScheduleState.Unregistered.class)
        .onCommand(ScheduleCommand.Register.class, this::register);
    builder
        .forStateType(ScheduleState.Active.class)
        .onCommand(ScheduleCommand.Register.class, this::alreadyRegistered)
        .onCommand(ScheduleCommand.Due.class, this::due)
        .onCommand(ScheduleCommand.ExecutionCompleted.class, this::completed)
        .onCommand(ScheduleCommand.EventReceived.class, this::eventReceived)
        .onCommand(ScheduleCommand.DispatchAcknowledged.class, this::acknowledge)
        .onCommand(ScheduleCommand.Recheck.class, this::recheck);
    builder
        .forAnyState()
        .onCommand(ScheduleCommand.Due.class, (state, ignored) -> Effect().noReply())
        .onCommand(ScheduleCommand.ExecutionCompleted.class, (state, ignored) -> Effect().noReply())
        .onCommand(ScheduleCommand.EventReceived.class, this::rejectInactiveEvent)
        .onCommand(
            ScheduleCommand.DispatchAcknowledged.class, (state, ignored) -> Effect().noReply())
        .onCommand(ScheduleCommand.Recheck.class, (state, ignored) -> Effect().noReply())
        .onCommand(ScheduleCommand.GetState.class, this::getState);
    return builder.build();
  }

  private ReplyEffect<ScheduleEvent, ScheduleState> register(
      ScheduleState.Unregistered state, ScheduleCommand.Register command) {
    if (!state.scheduleId().equals(command.scheduleId())) {
      return Effect()
          .reply(
              command.replyTo(),
              new ScheduleReply.Rejected(
                  command.commandId(),
                  state.scheduleId(),
                  state.revision(),
                  "wrong_schedule",
                  "Command was routed to another schedule"));
    }
    SchedulePlan schedule = command.plan().schedule();
    Instant nextEvery =
        schedule.every() == null
            ? null
            : nextDuration(command, schedule.every(), command.registeredAt(), "/schedule/every");
    Instant nextCron =
        schedule.cron() == null ? null : planner.nextCron(schedule.cron(), command.registeredAt());
    return Effect()
        .persist(
            new ScheduleEvent.Registered(
                command.commandId(),
                command.scheduleId(),
                command.actor(),
                command.plan(),
                command.input(),
                nextEvery,
                nextCron,
                command.registeredAt()))
        .thenRun(this::activate)
        .thenReply(
            command.replyTo(),
            persisted ->
                new ScheduleReply.Accepted(
                    command.commandId(), persisted.scheduleId(), persisted.revision()));
  }

  private Instant nextDuration(
      ScheduleCommand.Register command,
      com.forwardmeasure.openworkflow.definition.DurationPlan duration,
      Instant anchor,
      String path) {
    return planner.nextDuration(
        duration,
        command.input(),
        RuntimeExpressionArguments.empty(),
        command.plan().expressions().mode(),
        anchor,
        path);
  }

  private Instant nextDuration(
      ScheduleState.Active state,
      com.forwardmeasure.openworkflow.definition.DurationPlan duration,
      Instant anchor,
      String path) {
    return planner.nextDuration(
        duration,
        state.input(),
        RuntimeExpressionArguments.empty(),
        state.plan().expressions().mode(),
        anchor,
        path);
  }

  private ReplyEffect<ScheduleEvent, ScheduleState> alreadyRegistered(
      ScheduleState.Active state, ScheduleCommand.Register command) {
    return Effect()
        .reply(
            command.replyTo(),
            new ScheduleReply.Rejected(
                command.commandId(),
                state.scheduleId(),
                state.revision(),
                "already_registered",
                "Schedule is already registered"));
  }

  private ReplyEffect<ScheduleEvent, ScheduleState> due(
      ScheduleState.Active state, ScheduleCommand.Due command) {
    if (!state.scheduleId().equals(command.scheduleId()) || !matches(state, command))
      return Effect().noReply();
    Instant nextEvery = state.nextEvery();
    Instant nextCron = state.nextCron();
    Instant consumedAfter = null;
    if (command.trigger() == ScheduleTriggerKind.EVERY) {
      nextEvery =
          nextDuration(
              state, state.plan().schedule().every(), command.deadline(), "/schedule/every");
    } else if (command.trigger() == ScheduleTriggerKind.CRON) {
      nextCron = planner.nextCron(state.plan().schedule().cron(), command.deadline());
    } else {
      consumedAfter = command.deadline();
    }
    ScheduledExecutionRequest request = request(state, command.trigger(), command.deadline());
    return Effect()
        .persist(
            new ScheduleEvent.LaunchRequested(
                request, nextEvery, nextCron, consumedAfter, command.deadline()))
        .thenRun(this::activate)
        .thenNoReply();
  }

  private static boolean matches(ScheduleState.Active state, ScheduleCommand.Due command) {
    return switch (command.trigger()) {
      case EVERY -> command.deadline().equals(state.nextEvery());
      case CRON -> command.deadline().equals(state.nextCron());
      case AFTER -> state.afterDeadlines().contains(command.deadline());
      case EVENT -> false;
    };
  }

  private ReplyEffect<ScheduleEvent, ScheduleState> completed(
      ScheduleState.Active state, ScheduleCommand.ExecutionCompleted command) {
    if (!state.scheduleId().equals(command.scheduleId())
        || state.plan().schedule().after() == null
        || state.completedExecutions().contains(command.executionId())) {
      return Effect().noReply();
    }
    Instant deadline =
        nextDuration(
            state, state.plan().schedule().after(), command.completedAt(), "/schedule/after");
    return Effect()
        .persist(
            new ScheduleEvent.AfterScheduled(
                command.executionId(), deadline, command.completedAt()))
        .thenRun(this::activate)
        .thenNoReply();
  }

  private ReplyEffect<ScheduleEvent, ScheduleState> eventReceived(
      ScheduleState.Active state, ScheduleCommand.EventReceived command) {
    if (!state.scheduleId().equals(command.scheduleId()) || state.plan().schedule().on() == null) {
      return rejectEvent(
          command, state, "not_event_scheduled", "Schedule does not accept CloudEvents");
    }
    String eventKey = command.event().source() + "|" + command.event().id();
    if (state.seenEventKeys().contains(eventKey)) return acceptEvent(command, state);
    EventConsumptionWindow current = state.eventWindow();
    var offered =
        eventConsumption.offer(
            state.plan().schedule().on(),
            state.plan().schedule().readAs(),
            current,
            command.event(),
            state.input(),
            RuntimeExpressionArguments.empty(),
            state.plan().expressions().mode());
    if (offered == null) return acceptEvent(command, state);
    EventConsumptionWindow next = offered.window();
    ScheduledExecutionRequest request =
        offered.complete()
            ? request(
                state,
                ScheduleTriggerKind.EVENT,
                command.receivedAt(),
                CloudEventConsumptionEvaluator.read(
                    offered.window().accepted(), state.plan().schedule().readAs()),
                eventKey)
            : null;
    var effect =
        Effect()
            .persist(new ScheduleEvent.EventAccepted(eventKey, next, request, command.receivedAt()))
            .thenRun(this::activate);
    return command.replyTo() == null
        ? effect.thenNoReply()
        : effect.thenReply(
            command.replyTo(),
            persisted ->
                new ScheduleReply.Accepted(
                    command.commandId(), persisted.scheduleId(), persisted.revision()));
  }

  private ReplyEffect<ScheduleEvent, ScheduleState> rejectInactiveEvent(
      ScheduleState state, ScheduleCommand.EventReceived command) {
    return rejectEvent(command, state, "schedule_inactive", "Schedule is not registered");
  }

  private ReplyEffect<ScheduleEvent, ScheduleState> acceptEvent(
      ScheduleCommand.EventReceived command, ScheduleState state) {
    return command.replyTo() == null
        ? Effect().noReply()
        : Effect()
            .reply(
                command.replyTo(),
                new ScheduleReply.Accepted(
                    command.commandId(), state.scheduleId(), state.revision()));
  }

  private ReplyEffect<ScheduleEvent, ScheduleState> rejectEvent(
      ScheduleCommand.EventReceived command, ScheduleState state, String code, String message) {
    return command.replyTo() == null
        ? Effect().noReply()
        : Effect()
            .reply(
                command.replyTo(),
                new ScheduleReply.Rejected(
                    command.commandId(), state.scheduleId(), state.revision(), code, message));
  }

  private ReplyEffect<ScheduleEvent, ScheduleState> acknowledge(
      ScheduleState.Active state, ScheduleCommand.DispatchAcknowledged command) {
    if (!state.scheduleId().equals(command.scheduleId())
        || state.pending().stream()
            .noneMatch(request -> request.executionId().equals(command.executionId()))) {
      return Effect().noReply();
    }
    return Effect()
        .persist(
            new ScheduleEvent.DispatchAcknowledged(command.executionId(), command.acknowledgedAt()))
        .thenNoReply();
  }

  private ReplyEffect<ScheduleEvent, ScheduleState> recheck(
      ScheduleState.Active state, ScheduleCommand.Recheck command) {
    if (state.scheduleId().equals(command.scheduleId())) activate(state);
    return Effect().noReply();
  }

  private ReplyEffect<ScheduleEvent, ScheduleState> getState(
      ScheduleState state, ScheduleCommand.GetState command) {
    if (!state.scheduleId().equals(command.scheduleId())) {
      return Effect()
          .reply(
              command.replyTo(),
              new ScheduleReply.Rejected(
                  null,
                  state.scheduleId(),
                  state.revision(),
                  "wrong_schedule",
                  "Query was routed to another schedule"));
    }
    if (state instanceof ScheduleState.Unregistered) {
      return Effect()
          .reply(
              command.replyTo(),
              new ScheduleReply.Snapshot(
                  state.scheduleId(), 0, false, null, null, Set.of(), Set.of()));
    }
    ScheduleState.Active active = (ScheduleState.Active) state;
    return Effect()
        .reply(
            command.replyTo(),
            new ScheduleReply.Snapshot(
                active.scheduleId(),
                active.revision(),
                true,
                active.nextEvery(),
                active.nextCron(),
                active.afterDeadlines(),
                active.pending().stream()
                    .map(ScheduledExecutionRequest::executionId)
                    .collect(java.util.stream.Collectors.toSet())));
  }

  private ScheduledExecutionRequest request(
      ScheduleState.Active state, ScheduleTriggerKind trigger, Instant scheduledAt) {
    UUID value =
        UUID.nameUUIDFromBytes(
            (state.scheduleId().entityId() + "|" + trigger + "|" + scheduledAt)
                .getBytes(StandardCharsets.UTF_8));
    return new ScheduledExecutionRequest(
        state.scheduleId(),
        new ExecutionId(state.scheduleId().tenantId(), value),
        state.actor(),
        state.plan(),
        state.input(),
        trigger,
        scheduledAt);
  }

  private ScheduledExecutionRequest request(
      ScheduleState.Active state,
      ScheduleTriggerKind trigger,
      Instant scheduledAt,
      JsonNode input,
      String discriminator) {
    UUID value =
        UUID.nameUUIDFromBytes(
            (state.scheduleId().entityId() + "|" + trigger + "|" + discriminator)
                .getBytes(StandardCharsets.UTF_8));
    return new ScheduledExecutionRequest(
        state.scheduleId(),
        new ExecutionId(state.scheduleId().tenantId(), value),
        state.actor(),
        state.plan(),
        input,
        trigger,
        scheduledAt);
  }

  private void activate(ScheduleState state) {
    if (!(state instanceof ScheduleState.Active active)) return;
    schedule(active, ScheduleTriggerKind.EVERY, active.nextEvery());
    schedule(active, ScheduleTriggerKind.CRON, active.nextCron());
    active
        .afterDeadlines()
        .forEach(deadline -> schedule(active, ScheduleTriggerKind.AFTER, deadline));
    active.pending().forEach(dispatch::tell);
    if (!active.pending().isEmpty()) {
      timers.startSingleTimer(
          "dispatch-retry", new ScheduleCommand.Recheck(active.scheduleId()), DISPATCH_RETRY);
    }
  }

  private void schedule(ScheduleState.Active state, ScheduleTriggerKind trigger, Instant deadline) {
    if (deadline == null) return;
    Duration delay = Duration.between(Instant.now(), deadline);
    if (delay.isNegative()) delay = Duration.ZERO;
    String key = trigger + "|" + deadline;
    if (delay.compareTo(MAX_TIMER_HORIZON) > 0) {
      timers.startSingleTimer(
          key, new ScheduleCommand.Recheck(state.scheduleId()), MAX_TIMER_HORIZON);
      return;
    }
    timers.startSingleTimer(
        key, new ScheduleCommand.Due(state.scheduleId(), trigger, deadline), delay);
  }

  @Override
  public EventHandler<ScheduleState, ScheduleEvent> eventHandler() {
    var builder = newEventHandlerBuilder();
    builder
        .forStateType(ScheduleState.Unregistered.class)
        .onEvent(ScheduleEvent.Registered.class, this::registered);
    builder
        .forStateType(ScheduleState.Active.class)
        .onEvent(ScheduleEvent.AfterScheduled.class, this::afterScheduled)
        .onEvent(ScheduleEvent.EventAccepted.class, this::eventAccepted)
        .onEvent(ScheduleEvent.LaunchRequested.class, this::launchRequested)
        .onEvent(ScheduleEvent.DispatchAcknowledged.class, this::acknowledged);
    return builder.build();
  }

  private ScheduleState registered(
      ScheduleState.Unregistered state, ScheduleEvent.Registered event) {
    if (!state.scheduleId().equals(event.scheduleId())) {
      throw new IllegalStateException("Persisted registration has another schedule ID");
    }
    return new ScheduleState.Active(
        event.scheduleId(),
        event.actor(),
        event.plan(),
        event.input(),
        1,
        event.nextEvery(),
        event.nextCron(),
        Set.of(),
        Set.of(),
        List.of(),
        EventConsumptionWindow.empty(),
        Set.of());
  }

  @Override
  public Set<String> tagsFor(ScheduleEvent event) {
    return Set.of(projectionTagFor(scheduleId));
  }

  public static String projectionTagFor(ScheduleId scheduleId) {
    Objects.requireNonNull(scheduleId, "scheduleId");
    int partition = Math.floorMod(scheduleId.entityId().hashCode(), PROJECTION_TAG_COUNT);
    return PROJECTION_TAG_PREFIX + partition;
  }

  public static List<String> projectionTags() {
    return java.util.stream.IntStream.range(0, PROJECTION_TAG_COUNT)
        .mapToObj(index -> PROJECTION_TAG_PREFIX + index)
        .toList();
  }

  private ScheduleState afterScheduled(
      ScheduleState.Active state, ScheduleEvent.AfterScheduled event) {
    var after = new LinkedHashSet<>(state.afterDeadlines());
    after.add(event.deadline());
    var completed = new LinkedHashSet<>(state.completedExecutions());
    completed.add(event.completedExecutionId());
    return active(
        state,
        state.revision() + 1,
        state.nextEvery(),
        state.nextCron(),
        after,
        completed,
        state.pending());
  }

  private ScheduleState launchRequested(
      ScheduleState.Active state, ScheduleEvent.LaunchRequested event) {
    var after = new LinkedHashSet<>(state.afterDeadlines());
    if (event.consumedAfter() != null) after.remove(event.consumedAfter());
    var pending = new ArrayList<>(state.pending());
    if (pending.stream()
        .noneMatch(existing -> existing.executionId().equals(event.request().executionId())))
      pending.add(event.request());
    return active(
        state,
        state.revision() + 1,
        event.nextEvery(),
        event.nextCron(),
        after,
        state.completedExecutions(),
        pending);
  }

  private ScheduleState eventAccepted(
      ScheduleState.Active state, ScheduleEvent.EventAccepted event) {
    var seen = new LinkedHashSet<>(state.seenEventKeys());
    seen.add(event.eventKey());
    var pending = new ArrayList<>(state.pending());
    EventConsumptionWindow window = event.window();
    if (event.request() != null) {
      if (pending.stream()
          .noneMatch(existing -> existing.executionId().equals(event.request().executionId())))
        pending.add(event.request());
      window = EventConsumptionWindow.empty();
    }
    return new ScheduleState.Active(
        state.scheduleId(),
        state.actor(),
        state.plan(),
        state.input(),
        state.revision() + 1,
        state.nextEvery(),
        state.nextCron(),
        state.afterDeadlines(),
        state.completedExecutions(),
        pending,
        window,
        seen);
  }

  private ScheduleState acknowledged(
      ScheduleState.Active state, ScheduleEvent.DispatchAcknowledged event) {
    var pending = new ArrayList<>(state.pending());
    pending.removeIf(request -> request.executionId().equals(event.executionId()));
    return active(
        state,
        state.revision() + 1,
        state.nextEvery(),
        state.nextCron(),
        state.afterDeadlines(),
        state.completedExecutions(),
        pending);
  }

  private static ScheduleState.Active active(
      ScheduleState.Active state,
      long revision,
      Instant nextEvery,
      Instant nextCron,
      Set<Instant> after,
      Set<ExecutionId> completed,
      List<ScheduledExecutionRequest> pending) {
    return new ScheduleState.Active(
        state.scheduleId(),
        state.actor(),
        state.plan(),
        state.input(),
        revision,
        nextEvery,
        nextCron,
        after,
        completed,
        pending,
        state.eventWindow(),
        state.seenEventKeys());
  }

  @Override
  public SignalHandler<ScheduleState> signalHandler() {
    return newSignalHandlerBuilder().onSignal(RecoveryCompleted.instance(), this::activate).build();
  }
}
