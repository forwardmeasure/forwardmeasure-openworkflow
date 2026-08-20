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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.definition.CatchPlan;
import com.forwardmeasure.openworkflow.definition.DataSchemaValidationException;
import com.forwardmeasure.openworkflow.definition.DataSchemaValidator;
import com.forwardmeasure.openworkflow.definition.DurationPlan;
import com.forwardmeasure.openworkflow.definition.ErrorFilterPlan;
import com.forwardmeasure.openworkflow.definition.ErrorPlan;
import com.forwardmeasure.openworkflow.definition.EventConsumptionPlan;
import com.forwardmeasure.openworkflow.definition.EventFilterPlan;
import com.forwardmeasure.openworkflow.definition.EventReadMode;
import com.forwardmeasure.openworkflow.definition.EventTypeSelector;
import com.forwardmeasure.openworkflow.definition.ListenPlan;
import com.forwardmeasure.openworkflow.definition.PlanStep;
import com.forwardmeasure.openworkflow.definition.RetryPlan;
import com.forwardmeasure.openworkflow.definition.TaskDataFlow;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.AuthenticationExpressionContext;
import com.forwardmeasure.openworkflow.engine.api.DeadlineScope;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.engine.api.EventConsumptionWindow;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.FunctionOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.HttpOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationMaterializer;
import com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent;
import com.forwardmeasure.openworkflow.expression.ExpressionMode;
import com.forwardmeasure.openworkflow.expression.JqRuntimeExpressionEvaluator;
import com.forwardmeasure.openworkflow.expression.RuntimeExpressionArguments;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.TimerScheduler;
import org.apache.pekko.persistence.typed.PersistenceId;
import org.apache.pekko.persistence.typed.RecoveryCompleted;
import org.apache.pekko.persistence.typed.javadsl.CommandHandlerWithReply;
import org.apache.pekko.persistence.typed.javadsl.EventHandler;
import org.apache.pekko.persistence.typed.javadsl.EventSourcedBehaviorWithEnforcedReplies;
import org.apache.pekko.persistence.typed.javadsl.ReplyEffect;
import org.apache.pekko.persistence.typed.javadsl.RetentionCriteria;
import org.apache.pekko.persistence.typed.javadsl.SignalHandler;

/** One tenant-qualified workflow implemented as a Pekko persistent FSM. */
public final class WorkflowEntity
    extends EventSourcedBehaviorWithEnforcedReplies<WorkflowCommand, EngineEvent, WorkflowState> {

  public static final int PROJECTION_TAG_COUNT = 32;
  public static final String PROJECTION_TAG_PREFIX = "openworkflow-execution-";
  private static final Duration MAX_TIMER_HORIZON = Duration.ofDays(30);

  private final ExecutionId executionId;
  private final TimerScheduler<WorkflowCommand> timers;
  private final ActorRef<WorkflowCommand> self;
  private final ActorRef<WorkflowReply> continuationReplies;
  private final AtomicReference<ActorIdentity> executionActor;
  private final boolean automaticContinuation;
  private final JqRuntimeExpressionEvaluator expressions = new JqRuntimeExpressionEvaluator();
  private final CloudEventConsumptionEvaluator eventConsumption =
      new CloudEventConsumptionEvaluator();

  public static Behavior<WorkflowCommand> create(ExecutionId executionId) {
    return create(executionId, true);
  }

  static Behavior<WorkflowCommand> create(ExecutionId executionId, boolean automaticContinuation) {
    Objects.requireNonNull(executionId, "executionId");
    return Behaviors.setup(
        context -> {
          context.getLog().info("Activating workflow execution {}", executionId.entityId());
          var executionActor = new AtomicReference<ActorIdentity>();
          var replyRef = new AtomicReference<ActorRef<WorkflowReply>>();
          ActorRef<WorkflowReply> replies =
              context.messageAdapter(
                  WorkflowReply.class,
                  reply ->
                      continuationCommand(
                          executionId, executionActor.get(), replyRef.get(), reply));
          replyRef.set(replies);
          return Behaviors.withTimers(
              timers ->
                  new WorkflowEntity(
                      executionId,
                      timers,
                      context.getSelf(),
                      replies,
                      executionActor,
                      automaticContinuation));
        });
  }

  private WorkflowEntity(
      ExecutionId executionId,
      TimerScheduler<WorkflowCommand> timers,
      ActorRef<WorkflowCommand> self,
      ActorRef<WorkflowReply> continuationReplies,
      AtomicReference<ActorIdentity> executionActor,
      boolean automaticContinuation) {
    super(PersistenceId.ofUniqueId("workflow-execution|" + executionId.entityId()));
    this.executionId = executionId;
    this.timers = Objects.requireNonNull(timers, "timers");
    this.self = Objects.requireNonNull(self, "self");
    this.continuationReplies = Objects.requireNonNull(continuationReplies, "continuationReplies");
    this.executionActor = Objects.requireNonNull(executionActor, "executionActor");
    this.automaticContinuation = automaticContinuation;
  }

  private static WorkflowCommand continuationCommand(
      ExecutionId executionId,
      ActorIdentity actor,
      ActorRef<WorkflowReply> replies,
      WorkflowReply reply) {
    if (reply instanceof WorkflowReply.Accepted accepted
        && accepted.status() == com.forwardmeasure.openworkflow.engine.api.ExecutionStatus.RUNNING
        && actor != null
        && replies != null) {
      return new WorkflowCommand.RunNext(
          continuationCommandId(executionId, accepted.revision()),
          executionId,
          actor,
          Instant.now(),
          replies);
    }
    return new WorkflowCommand.RecheckTimers(executionId);
  }

  private static UUID continuationCommandId(ExecutionId executionId, long revision) {
    return UUID.nameUUIDFromBytes(
        (executionId.entityId() + "|durable-continuation|" + revision)
            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  private void continueIfRunning(WorkflowState state) {
    continueIfRunning(state, Instant.now());
  }

  private void continueIfRunning(WorkflowState state, Instant requestedAt) {
    if (!automaticContinuation) return;
    if (!(state instanceof WorkflowState.Running running)) return;
    ActorIdentity actor = executionActor.get();
    if (actor == null) {
      throw new IllegalStateException("Running workflow has no actor recovered from Started");
    }
    self.tell(
        new WorkflowCommand.RunNext(
            continuationCommandId(executionId, running.revision()),
            executionId,
            actor,
            requestedAt,
            continuationReplies));
  }

  @Override
  public WorkflowState emptyState() {
    return new WorkflowState.New(executionId);
  }

  @Override
  public CommandHandlerWithReply<WorkflowCommand, EngineEvent, WorkflowState> commandHandler() {
    var builder = newCommandHandlerWithReplyBuilder();

    builder
        .forStateType(WorkflowState.New.class)
        .onCommand(WorkflowCommand.Start.class, this::start)
        .onCommand(WorkflowCommand.RunNext.class, this::rejectNotRunning);
    builder
        .forStateType(WorkflowState.Running.class)
        .onCommand(WorkflowCommand.Start.class, this::rejectAlreadyStarted)
        .onCommand(WorkflowCommand.RunNext.class, this::runNext)
        .onCommand(WorkflowCommand.Pause.class, this::pauseRunning)
        .onCommand(WorkflowCommand.Resume.class, this::rejectNotPaused)
        .onCommand(WorkflowCommand.Cancel.class, this::cancelRunning)
        .onCommand(WorkflowCommand.TimerElapsed.class, this::completeForkWait)
        .onCommand(WorkflowCommand.RetryElapsed.class, this::beginForkRetry)
        .onCommand(WorkflowCommand.DeadlineElapsed.class, this::expireDeadline)
        .onCommand(WorkflowCommand.RecheckTimers.class, this::recheckTimers);
    builder
        .forStateType(WorkflowState.Running.class)
        .onCommand(WorkflowCommand.EffectAcknowledged.class, this::acknowledgeEffect)
        .onCommand(WorkflowCommand.HttpCallCompleted.class, this::completeHttpCall)
        .onCommand(WorkflowCommand.ProtocolCallObserved.class, this::observeProtocolCall)
        .onCommand(WorkflowCommand.SubworkflowCompleted.class, this::completeSubworkflow)
        .onCommand(WorkflowCommand.CloudEventReceived.class, this::receiveCloudEventAny);
    builder
        .forStateType(WorkflowState.Waiting.class)
        .onCommand(WorkflowCommand.Start.class, this::rejectAlreadyStarted)
        .onCommand(WorkflowCommand.RunNext.class, this::rejectNotRunning)
        .onCommand(WorkflowCommand.Pause.class, this::pauseWaiting)
        .onCommand(WorkflowCommand.Resume.class, this::rejectNotPaused)
        .onCommand(WorkflowCommand.Cancel.class, this::cancelWaiting)
        .onCommand(WorkflowCommand.TimerElapsed.class, this::completeWait)
        .onCommand(WorkflowCommand.RetryElapsed.class, this::beginRetry)
        .onCommand(WorkflowCommand.EffectAcknowledged.class, this::acknowledgeEffect)
        .onCommand(WorkflowCommand.HttpCallCompleted.class, this::completeHttpCall)
        .onCommand(WorkflowCommand.ProtocolCallObserved.class, this::observeProtocolCall)
        .onCommand(WorkflowCommand.SubworkflowCompleted.class, this::completeSubworkflow)
        .onCommand(WorkflowCommand.CloudEventReceived.class, this::receiveCloudEventAny)
        .onCommand(WorkflowCommand.DeadlineElapsed.class, this::expireDeadline)
        .onCommand(WorkflowCommand.RecheckTimers.class, this::recheckTimers);
    builder
        .forStateType(WorkflowState.Pausing.class)
        .onCommand(WorkflowCommand.Start.class, this::rejectAlreadyStarted)
        .onCommand(WorkflowCommand.RunNext.class, this::rejectTransitioning)
        .onCommand(WorkflowCommand.Pause.class, this::rejectTransitioning)
        .onCommand(WorkflowCommand.Resume.class, this::rejectTransitioning)
        .onCommand(WorkflowCommand.Cancel.class, this::cancelPausing);
    builder
        .forStateType(WorkflowState.Paused.class)
        .onCommand(WorkflowCommand.Start.class, this::rejectAlreadyStarted)
        .onCommand(WorkflowCommand.RunNext.class, this::rejectNotRunning)
        .onCommand(WorkflowCommand.Pause.class, this::pausedPause)
        .onCommand(WorkflowCommand.Resume.class, this::resume)
        .onCommand(WorkflowCommand.Cancel.class, this::cancelPaused);
    builder
        .forStateType(WorkflowState.Paused.class)
        .onCommand(
            WorkflowCommand.EffectAcknowledged.class, this::rejectPausedEffectAcknowledgement)
        .onCommand(WorkflowCommand.HttpCallCompleted.class, this::rejectPausedHttpCallCompletion)
        .onCommand(
            WorkflowCommand.ProtocolCallObserved.class, this::rejectPausedProtocolObservation)
        .onCommand(
            WorkflowCommand.SubworkflowCompleted.class, this::rejectPausedSubworkflowCompletion);
    builder
        .forStateType(WorkflowState.Cancelling.class)
        .onCommand(WorkflowCommand.Start.class, this::rejectAlreadyStarted)
        .onCommand(WorkflowCommand.RunNext.class, this::rejectTransitioning)
        .onCommand(WorkflowCommand.Pause.class, this::rejectTransitioning)
        .onCommand(WorkflowCommand.Resume.class, this::rejectTransitioning)
        .onCommand(WorkflowCommand.Cancel.class, this::rejectTransitioning);
    builder
        .forStateType(WorkflowState.Cancelled.class)
        .onCommand(WorkflowCommand.Start.class, this::rejectAlreadyStarted)
        .onCommand(WorkflowCommand.RunNext.class, this::rejectTerminal)
        .onCommand(WorkflowCommand.Pause.class, this::rejectTerminal)
        .onCommand(WorkflowCommand.Resume.class, this::rejectTerminal)
        .onCommand(WorkflowCommand.Cancel.class, this::cancelledCancel);
    builder
        .forStateType(WorkflowState.Cancelled.class)
        .onCommand(WorkflowCommand.EffectAcknowledged.class, this::acknowledgeAlreadyObserved)
        .onCommand(WorkflowCommand.HttpCallCompleted.class, this::acceptLateHttpCallCompletion)
        .onCommand(WorkflowCommand.ProtocolCallObserved.class, this::acceptLateProtocolObservation)
        .onCommand(
            WorkflowCommand.SubworkflowCompleted.class, this::acceptLateSubworkflowCompletion);
    builder
        .forStateType(WorkflowState.Completed.class)
        .onCommand(WorkflowCommand.Start.class, this::completedStart)
        .onCommand(WorkflowCommand.RunNext.class, this::rejectTerminal)
        .onCommand(WorkflowCommand.Pause.class, this::rejectTerminal)
        .onCommand(WorkflowCommand.Resume.class, this::rejectTerminal)
        .onCommand(WorkflowCommand.Cancel.class, this::rejectTerminal);
    builder
        .forStateType(WorkflowState.Completed.class)
        .onCommand(WorkflowCommand.EffectAcknowledged.class, this::acknowledgeAlreadyObserved)
        .onCommand(WorkflowCommand.HttpCallCompleted.class, this::acceptLateHttpCallCompletion)
        .onCommand(WorkflowCommand.ProtocolCallObserved.class, this::acceptLateProtocolObservation)
        .onCommand(
            WorkflowCommand.SubworkflowCompleted.class, this::acceptLateSubworkflowCompletion);
    builder
        .forStateType(WorkflowState.Failed.class)
        .onCommand(WorkflowCommand.Start.class, this::rejectAlreadyStarted)
        .onCommand(WorkflowCommand.RunNext.class, this::rejectTerminal)
        .onCommand(WorkflowCommand.Pause.class, this::rejectTerminal)
        .onCommand(WorkflowCommand.Resume.class, this::rejectTerminal)
        .onCommand(WorkflowCommand.Cancel.class, this::rejectTerminal);
    builder
        .forStateType(WorkflowState.Failed.class)
        .onCommand(WorkflowCommand.EffectAcknowledged.class, this::acknowledgeAlreadyObserved)
        .onCommand(WorkflowCommand.HttpCallCompleted.class, this::acceptLateHttpCallCompletion)
        .onCommand(WorkflowCommand.ProtocolCallObserved.class, this::acceptLateProtocolObservation)
        .onCommand(
            WorkflowCommand.SubworkflowCompleted.class, this::acceptLateSubworkflowCompletion);
    builder
        .forStateType(WorkflowState.New.class)
        .onCommand(WorkflowCommand.Pause.class, this::rejectNotRunning)
        .onCommand(WorkflowCommand.Resume.class, this::rejectNotPaused)
        .onCommand(WorkflowCommand.Cancel.class, this::rejectNotRunning);
    builder
        .forAnyState()
        .onCommand(WorkflowCommand.TimerElapsed.class, (state, ignored) -> Effect().noReply())
        .onCommand(WorkflowCommand.RetryElapsed.class, (state, ignored) -> Effect().noReply())
        .onCommand(WorkflowCommand.DeadlineElapsed.class, (state, ignored) -> Effect().noReply())
        .onCommand(WorkflowCommand.RecheckTimers.class, (state, ignored) -> Effect().noReply())
        .onCommand(WorkflowCommand.EffectAcknowledged.class, (state, ignored) -> Effect().noReply())
        .onCommand(WorkflowCommand.HttpCallCompleted.class, this::rejectHttpCallNotPending)
        .onCommand(WorkflowCommand.ProtocolCallObserved.class, this::rejectProtocolCallNotPending)
        .onCommand(WorkflowCommand.SubworkflowCompleted.class, this::rejectSubworkflowNotPending)
        .onCommand(WorkflowCommand.CloudEventReceived.class, this::ignoreCloudEvent)
        .onCommand(WorkflowCommand.GetState.class, this::getState);
    builder
        .forAnyState()
        .onCommand(
            WorkflowCommand.GetRuntimeState.class,
            (state, command) ->
                Effect()
                    .reply(
                        command.replyTo(),
                        new WorkflowReply.RuntimeState(WorkflowRuntimeState.from(state))));

    return builder.build();
  }

  private ReplyEffect<EngineEvent, WorkflowState> start(
      WorkflowState.New state, WorkflowCommand.Start command) {
    if (!state.executionId().equals(command.executionId())) {
      return reject(
          command,
          state,
          "wrong_execution",
          "Command was routed to another tenant-qualified execution");
    }
    try {
      MilestoneOneProgram.compile(command.plan());
    } catch (RuntimeException unsupported) {
      return reject(
          command,
          state,
          "unsupported_definition",
          unsupported.getMessage() == null
              ? "The definition is outside the accepted Milestone 1 slice"
              : unsupported.getMessage());
    }
    JsonNode initial;
    try {
      initial = initialData(command.plan(), command.input(), state.executionId());
    } catch (DataSchemaValidationException invalid) {
      return reject(command, state, "invalid_input", invalid.getMessage());
    } catch (RuntimeException invalid) {
      return reject(
          command,
          state,
          "invalid_input",
          invalid.getMessage() == null
              ? "Workflow input transformation failed"
              : invalid.getMessage());
    }

    EngineEvent.Started started =
        new EngineEvent.Started(
            command.commandId(),
            state.executionId(),
            command.actor(),
            command.plan(),
            command.input(),
            command.requestedAt());
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(started);
    if (command.plan().timeout() != null) {
      Instant deadline =
          resolveDuration(
              command.plan().timeout().after(),
              initial,
              workflowArguments(command.plan(), state.executionId(), command.input(), initial),
              command.plan().expressions().mode(),
              command.requestedAt(),
              "/timeout");
      events.add(
          new EngineEvent.DeadlineScheduled(
              command.commandId(), DeadlineScope.WORKFLOW, null, deadline, command.requestedAt()));
    }
    return Effect()
        .persist(events)
        .thenRun(this::scheduleDeadlines)
        .thenReply(
            command.replyTo(),
            persisted ->
                new WorkflowReply.Accepted(
                    command.commandId(),
                    persisted.executionId(),
                    persisted.revision(),
                    persisted.status()));
  }

  private ReplyEffect<EngineEvent, WorkflowState> runNext(
      WorkflowState.Running state, WorkflowCommand.RunNext command) {
    if (!state.executionId().equals(command.executionId())) {
      return Effect()
          .reply(
              command.replyTo(),
              new WorkflowReply.Rejected(
                  command.commandId(),
                  state.executionId(),
                  state.revision(),
                  state.status(),
                  "wrong_execution",
                  "Command was routed to another tenant-qualified execution"));
    }
    if (state.processedCommands().contains(command.commandId())) {
      return Effect().reply(command.replyTo(), accepted(command.commandId(), state));
    }
    try {
      MilestoneOneProgram program = MilestoneOneProgram.compile(state.plan());
      if (state.nextStep() > program.size()) {
        return Effect()
            .reply(
                command.replyTo(),
                new WorkflowReply.Rejected(
                    command.commandId(),
                    state.executionId(),
                    state.revision(),
                    state.status(),
                    "invalid_cursor",
                    "Execution cursor is beyond the compiled plan"));
      }
      if (state.nextStep() == program.size()) {
        JsonNode output = workflowOutput(state);
        return Effect()
            .persist(new EngineEvent.Completed(command.commandId(), output, command.requestedAt()))
            .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
      }
      if (activeFork(state) != null) {
        return advanceFork(state, command, program);
      }
      MilestoneOneProgram.Instruction instruction =
          resolveExtensionGates(program, state.nextStep(), state.taskStack());
      return switch (instruction) {
        case MilestoneOneProgram.EnterExtension enter -> enterExtension(state, command, enter);
        case MilestoneOneProgram.ExtensionGate ignored ->
            throw new IllegalStateException("Unresolved extension gate");
        case MilestoneOneProgram.ExitExtension exit -> exitExtension(state, command, exit);
        case MilestoneOneProgram.ExecuteSet execute -> executeSet(state, command, execute);
        case MilestoneOneProgram.ExecuteSwitch choose -> executeSwitch(state, command, choose);
        case MilestoneOneProgram.EnterDo enter -> enterDo(state, command, enter);
        case MilestoneOneProgram.ExitDo exit -> exitDo(state, command, exit);
        case MilestoneOneProgram.EnterFor enter -> enterFor(state, command, enter);
        case MilestoneOneProgram.ExitFor exit -> exitFor(state, command, exit);
        case MilestoneOneProgram.ExecuteWait wait -> executeWait(state, command, wait);
        case MilestoneOneProgram.ExecuteRaise raise -> executeRaise(state, command, raise);
        case MilestoneOneProgram.ExecuteEmit emit -> executeEmit(state, command, emit);
        case MilestoneOneProgram.ExecuteListen listen -> executeListen(state, command, listen);
        case MilestoneOneProgram.ExecuteSubworkflow subworkflow ->
            executeSubworkflow(state, command, subworkflow);
        case MilestoneOneProgram.ExecuteHttpCall call -> executeHttpCall(state, command, call);
        case MilestoneOneProgram.ExecuteProtocolCall call ->
            executeProtocolCall(state, command, call);
        case MilestoneOneProgram.EnterFunction function -> enterFunction(state, command, function);
        case MilestoneOneProgram.ExitFunction function -> exitFunction(state, command, function);
        case MilestoneOneProgram.ExitListen exit -> exitListen(state, command, exit);
        case MilestoneOneProgram.ExitProtocolCall exit -> exitProtocolCall(state, command, exit);
        case MilestoneOneProgram.EnterTry enter -> enterTry(state, command, enter);
        case MilestoneOneProgram.ExitTry exit -> exitTry(state, command, exit);
        case MilestoneOneProgram.EnterFork enter -> enterFork(state, command, enter);
        case MilestoneOneProgram.ExitFork ignored ->
            throw new IllegalStateException(
                "Fork exit can only be reached by its durable lane join");
      };
    } catch (RuntimeException failure) {
      return Effect()
          .persist(
              new EngineEvent.Failed(
                  command.commandId(),
                  failure.getMessage() == null
                      ? failure.getClass().getSimpleName()
                      : failure.getMessage(),
                  command.requestedAt()))
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
  }

  private MilestoneOneProgram.Instruction resolveExtensionGates(
      MilestoneOneProgram program, int cursor, List<TaskExecutionFrame> stack) {
    MilestoneOneProgram.Instruction instruction = program.instruction(cursor);
    while (instruction instanceof MilestoneOneProgram.ExtensionGate gate) {
      TaskExecutionFrame frame =
          stack.stream()
              .filter(candidate -> candidate.taskPath().equals(gate.step().path()))
              .reduce((first, second) -> second)
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Extension gate has no durable selection frame at "
                              + gate.step().path()));
      if (gate.application() >= frame.extensionDecisions().size()) {
        throw new IllegalStateException(
            "Extension selection does not match the compiled plan at " + gate.step().path());
      }
      cursor =
          frame.extensionDecisions().get(gate.application()) ? gate.next() : gate.skippedNext();
      instruction = program.instruction(cursor);
    }
    return instruction;
  }

  private List<Boolean> extensionDecisions(
      WorkflowState.Running state, PlanStep step, JsonNode rawInput, JsonNode input) {
    return step.extensionPlan().applications().stream()
        .map(
            application ->
                application.condition() == null
                    || expressions.evaluateCondition(
                        application.condition(),
                        input,
                        arguments(state, step, rawInput, input, null),
                        state.plan().expressions().mode()))
        .toList();
  }

  private ReplyEffect<EngineEvent, WorkflowState> enterExtension(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.EnterExtension instruction) {
    PlanStep step = instruction.step();
    JsonNode rawInput = state.data();
    boolean run = condition(state, step, rawInput);
    JsonNode input = run ? taskInput(state, step, rawInput) : rawInput;
    if (!run) {
      int after =
          ((MilestoneOneProgram.ExitExtension)
                  MilestoneOneProgram.compile(state.plan()).instruction(instruction.exit()))
              .next();
      TaskResult skipped = completeTask(state, step, rawInput, input, rawInput);
      return Effect()
          .persist(taskEvents(state, command, step, rawInput, input, after, skipped))
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.ExtensionEntered(
            command.commandId(),
            step.path(),
            rawInput,
            input,
            extensionDecisions(state, step, rawInput, input),
            instruction.next(),
            command.requestedAt()));
    appendTaskDeadline(state, command, step, rawInput, input, events);
    return Effect()
        .persist(events)
        .thenRun(this::scheduleDeadlines)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> exitExtension(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.ExitExtension instruction) {
    if (state.taskStack().isEmpty()) {
      throw new IllegalStateException(
          "Extension exit has no task frame at " + instruction.step().path());
    }
    TaskExecutionFrame frame = state.taskStack().getLast();
    if (!frame.taskPath().equals(instruction.step().path())) {
      throw new IllegalStateException(
          "Extension frame does not match " + instruction.step().path());
    }
    TaskResult result =
        completeTask(state, instruction.step(), frame.rawInput(), frame.input(), state.data());
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.TaskCompleted(
            command.commandId(),
            instruction.step().path(),
            instruction.next(),
            result.output(),
            result.context(),
            command.requestedAt()));
    appendCompletionIfTerminal(state, command, instruction.next(), result, events);
    return Effect()
        .persist(events)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> enterFork(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.EnterFork instruction) {
    PlanStep step = instruction.step();
    JsonNode rawInput = state.data();
    boolean execute = condition(state, step, rawInput);
    JsonNode input = execute ? taskInput(state, step, rawInput) : rawInput;
    int after =
        ((MilestoneOneProgram.ExitFork)
                MilestoneOneProgram.compile(state.plan()).instruction(instruction.exit()))
            .next();
    if (!execute) {
      TaskResult skipped = completeTask(state, step, rawInput, input, rawInput);
      return Effect()
          .persist(taskEvents(state, command, step, rawInput, input, after, skipped))
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    List<String> names =
        instruction.branches().stream().map(MilestoneOneProgram.BranchRange::name).toList();
    List<Integer> starts =
        instruction.branches().stream().map(MilestoneOneProgram.BranchRange::start).toList();
    List<Integer> ends =
        instruction.branches().stream().map(MilestoneOneProgram.BranchRange::end).toList();
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.ForkEntered(
            command.commandId(),
            step.path(),
            rawInput,
            input,
            names,
            starts,
            ends,
            step.forkPlan().compete(),
            state.nextStep(),
            command.requestedAt()));
    appendTaskDeadline(state, command, step, rawInput, input, events);
    return Effect()
        .persist(events)
        .thenRun(this::scheduleDeadlines)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> advanceFork(
      WorkflowState.Running state, WorkflowCommand.RunNext command, MilestoneOneProgram program) {
    TaskExecutionFrame frame = activeFork(state);
    ForkExecutionFrame fork = frame.fork();
    if (fork.complete()) {
      return completeRootFork(state, command, program, frame);
    }
    ForkSelection selection = selectForkLeaf(fork);
    MilestoneOneProgram.Instruction selectedInstruction =
        resolveExtensionGates(
            program, selection.branch().nextStep(), selection.branch().taskStack());
    if (selectedInstruction instanceof MilestoneOneProgram.ExecuteEmit emit) {
      return startForkEmit(state, command, frame, selection, emit);
    }
    if (selectedInstruction instanceof MilestoneOneProgram.ExecuteListen listen) {
      return startForkListen(state, command, frame, selection, listen);
    }
    if (selectedInstruction instanceof MilestoneOneProgram.ExecuteSubworkflow subworkflow) {
      return startForkSubworkflow(state, command, frame, selection, subworkflow);
    }
    if (selectedInstruction instanceof MilestoneOneProgram.ExecuteHttpCall call) {
      return startForkHttpCall(state, command, frame, selection, call);
    }
    if (selectedInstruction instanceof MilestoneOneProgram.ExecuteProtocolCall call) {
      return startForkProtocolCall(state, command, frame, selection, call);
    }
    if (selectedInstruction instanceof MilestoneOneProgram.ExitListen exitListen) {
      return advanceForkListenIteration(state, command, frame, selection, exitListen);
    }
    if (selectedInstruction instanceof MilestoneOneProgram.ExitProtocolCall exitProtocolCall) {
      return advanceForkProtocolCallIteration(state, command, frame, selection, exitProtocolCall);
    }
    if (selectedInstruction instanceof MilestoneOneProgram.EnterTry
        || selectedInstruction instanceof MilestoneOneProgram.ExitTry
        || selectedInstruction instanceof MilestoneOneProgram.ExecuteRaise) {
      return advanceForkFailure(state, command, program, frame, selection, selectedInstruction);
    }
    if (selection.completedNested() != null
        || selection.path().size() > 1
        || selectedInstruction instanceof MilestoneOneProgram.EnterFork) {
      return advanceNestedFork(state, command, program, frame, selection);
    }
    int branchIndex = nextRunnableBranch(fork);
    ForkBranchState branch = fork.branches().get(branchIndex);
    WorkflowState.Running laneState =
        new WorkflowState.Running(
            state.executionId(),
            state.plan(),
            branch.data(),
            branch.nextStep(),
            state.revision(),
            state.processedCommands(),
            branch.context(),
            state.rawWorkflowInput(),
            branch.taskStack(),
            state.workflowDeadline());
    MilestoneOneProgram.Instruction laneInstruction =
        resolveExtensionGates(program, branch.nextStep(), branch.taskStack());
    PlanStep laneStep = laneInstruction.step();
    if (laneInstruction instanceof MilestoneOneProgram.ExecuteWait wait) {
      return scheduleForkWait(state, command, frame, selection, wait);
    }
    JsonNode nextData;
    JsonNode nextContext = branch.context();
    int nextStep;
    String enteredTaskPath = null;
    JsonNode enteredRawInput = null;
    JsonNode enteredInput = null;
    String completedTaskPath = null;
    JsonNode completedContext = null;
    FunctionOperationDescriptor enteredFunction = null;
    JsonNode enteredCollection = null;
    int enteredIteration = -1;
    String enteredItemVariable = null;
    String enteredIndexVariable = null;
    Integer advancedIteration = null;
    List<Boolean> enteredExtensionDecisions = null;
    switch (laneInstruction) {
      case MilestoneOneProgram.EnterExtension enter -> {
        PlanStep step = enter.step();
        JsonNode rawInput = laneState.data();
        boolean run = condition(laneState, step, rawInput);
        JsonNode input = run ? taskInput(laneState, step, rawInput) : rawInput;
        if (run) {
          nextData = input;
          nextStep = enter.next();
          enteredTaskPath = step.path();
          enteredRawInput = rawInput;
          enteredInput = input;
          enteredExtensionDecisions = extensionDecisions(laneState, step, rawInput, input);
        } else {
          TaskResult result = completeTask(laneState, step, rawInput, input, rawInput);
          nextData = result.output();
          nextContext = result.context();
          nextStep = ((MilestoneOneProgram.ExitExtension) program.instruction(enter.exit())).next();
        }
      }
      case MilestoneOneProgram.ExtensionGate ignored ->
          throw new IllegalStateException("Unresolved extension gate");
      case MilestoneOneProgram.ExitExtension exit -> {
        if (branch.taskStack().isEmpty()
            || !branch.taskStack().getLast().taskPath().equals(exit.step().path())) {
          throw new IllegalStateException(
              "Fork lane extension stack does not match " + exit.step().path());
        }
        TaskExecutionFrame extension = branch.taskStack().getLast();
        TaskResult result =
            completeTask(
                laneState, exit.step(), extension.rawInput(), extension.input(), laneState.data());
        nextData = result.output();
        nextStep = exit.next();
        completedTaskPath = exit.step().path();
        completedContext = result.context();
        nextContext = result.context();
      }
      case MilestoneOneProgram.ExecuteSet execute -> {
        PlanStep step = execute.step();
        JsonNode rawInput = laneState.data();
        boolean run = condition(laneState, step, rawInput);
        JsonNode input = run ? taskInput(laneState, step, rawInput) : rawInput;
        JsonNode rawOutput =
            run
                ? expressions.evaluateTemplate(
                    step.configuration(),
                    input,
                    arguments(laneState, step, rawInput, input, null),
                    state.plan().expressions().mode())
                : rawInput;
        TaskResult result = completeTask(laneState, step, rawInput, input, rawOutput);
        nextData = result.output();
        nextContext = result.context();
        nextStep = execute.next();
      }
      case MilestoneOneProgram.ExecuteSwitch choose -> {
        PlanStep step = choose.step();
        JsonNode rawInput = laneState.data();
        boolean run = condition(laneState, step, rawInput);
        JsonNode input = run ? taskInput(laneState, step, rawInput) : rawInput;
        int selectedNext = choose.next();
        if (run) {
          Integer selected = null;
          for (MilestoneOneProgram.SwitchTarget candidate : choose.cases()) {
            if (expressions.evaluateCondition(
                candidate.condition(),
                input,
                arguments(laneState, step, rawInput, input, null),
                state.plan().expressions().mode())) {
              selected = candidate.next();
              break;
            }
          }
          if (selected == null) selected = choose.defaultNext();
          if (selected != null) selectedNext = selected;
        }
        TaskResult result = completeTask(laneState, step, rawInput, input, run ? input : rawInput);
        nextData = result.output();
        nextContext = result.context();
        nextStep = selectedNext;
      }
      case MilestoneOneProgram.EnterDo enter -> {
        PlanStep step = enter.step();
        JsonNode rawInput = laneState.data();
        boolean run = condition(laneState, step, rawInput);
        JsonNode input = run ? taskInput(laneState, step, rawInput) : rawInput;
        if (run) {
          nextData = input;
          nextStep = enter.next();
          enteredTaskPath = step.path();
          enteredRawInput = rawInput;
          enteredInput = input;
        } else {
          TaskResult result = completeTask(laneState, step, rawInput, input, rawInput);
          nextData = result.output();
          nextContext = result.context();
          nextStep = ((MilestoneOneProgram.ExitDo) program.instruction(enter.exit())).next();
        }
      }
      case MilestoneOneProgram.EnterFunction enter -> {
        PlanStep step = enter.step();
        JsonNode rawInput = laneState.data();
        boolean run = condition(laneState, step, rawInput);
        JsonNode input = run ? taskInput(laneState, step, rawInput) : rawInput;
        if (run) {
          JsonNode functionArguments = functionArguments(laneState, step, rawInput, input);
          nextData = functionArguments;
          nextStep = enter.next();
          enteredTaskPath = step.path();
          enteredRawInput = rawInput;
          enteredInput = input;
          enteredFunction =
              functionOperation(state, step, functionArguments, "fork:" + branchIndex);
        } else {
          TaskResult result = completeTask(laneState, step, rawInput, input, rawInput);
          nextData = result.output();
          nextContext = result.context();
          nextStep = ((MilestoneOneProgram.ExitFunction) program.instruction(enter.exit())).next();
        }
      }
      case MilestoneOneProgram.ExitFunction exit -> {
        if (branch.taskStack().isEmpty()
            || !branch.taskStack().getLast().taskPath().equals(exit.step().path())) {
          throw new IllegalStateException(
              "Fork lane function stack does not match " + exit.step().path());
        }
        TaskExecutionFrame nested = branch.taskStack().getLast();
        TaskResult result =
            completeTask(
                laneState, exit.step(), nested.rawInput(), nested.input(), laneState.data());
        nextData = result.output();
        nextStep = exit.next();
        completedTaskPath = exit.step().path();
        completedContext = result.context();
        nextContext = result.context();
      }
      case MilestoneOneProgram.ExitDo exit -> {
        if (branch.taskStack().isEmpty()
            || !branch.taskStack().getLast().taskPath().equals(exit.step().path())) {
          throw new IllegalStateException(
              "Fork lane task stack does not match " + exit.step().path());
        }
        TaskExecutionFrame nested = branch.taskStack().getLast();
        TaskResult result =
            completeTask(
                laneState, exit.step(), nested.rawInput(), nested.input(), laneState.data());
        nextData = result.output();
        nextStep = exit.next();
        completedTaskPath = exit.step().path();
        completedContext = result.context();
        nextContext = result.context();
      }
      case MilestoneOneProgram.EnterFor enter -> {
        PlanStep step = enter.step();
        JsonNode rawInput = laneState.data();
        boolean run = condition(laneState, step, rawInput);
        JsonNode input = run ? taskInput(laneState, step, rawInput) : rawInput;
        if (!run) {
          TaskResult result = completeTask(laneState, step, rawInput, input, rawInput);
          nextData = result.output();
          nextContext = result.context();
          nextStep = enter.after();
        } else {
          JsonNode configured = step.forPlan().collection();
          JsonNode collection =
              step.forPlan().expressionCollection()
                  ? expressions.evaluateTemplate(
                      configured,
                      input,
                      arguments(laneState, step, rawInput, input, null),
                      state.plan().expressions().mode())
                  : configured.deepCopy();
          if (!collection.isArray()) {
            throw new IllegalArgumentException(
                "for.in must evaluate to an array at " + step.path());
          }
          if (collection.isEmpty()
              || !continueIteration(laneState, step, rawInput, input, collection, 0)) {
            TaskResult result = completeTask(laneState, step, rawInput, input, input);
            nextData = result.output();
            nextContext = result.context();
            nextStep = enter.after();
          } else {
            nextData = input;
            nextStep = enter.next();
            enteredTaskPath = step.path();
            enteredRawInput = rawInput;
            enteredInput = input;
            enteredCollection = collection;
            enteredIteration = 0;
            enteredItemVariable = step.forPlan().itemVariable();
            enteredIndexVariable = step.forPlan().indexVariable();
          }
        }
      }
      case MilestoneOneProgram.ExitFor exit -> {
        if (branch.taskStack().isEmpty()
            || !branch.taskStack().getLast().iterating()
            || !branch.taskStack().getLast().taskPath().equals(exit.step().path())) {
          throw new IllegalStateException(
              "Fork lane iteration stack does not match " + exit.step().path());
        }
        TaskExecutionFrame iteration = branch.taskStack().getLast();
        int nextIndex = iteration.iterationIndex() + 1;
        if (nextIndex < iteration.collection().size()
            && continueIteration(
                laneState,
                exit.step(),
                iteration.rawInput(),
                laneState.data(),
                iteration.collection(),
                nextIndex)) {
          nextData = laneState.data();
          nextStep = exit.body();
          advancedIteration = nextIndex;
        } else {
          TaskResult result =
              completeTask(
                  laneState,
                  exit.step(),
                  iteration.rawInput(),
                  iteration.input(),
                  laneState.data());
          nextData = result.output();
          nextStep = exit.next();
          completedTaskPath = exit.step().path();
          completedContext = result.context();
          nextContext = result.context();
        }
      }
      default -> throw new IllegalStateException("Unsupported instruction in accepted fork lane");
    }
    boolean branchCompleted = nextStep == branch.endStep();
    Integer winner = fork.compete() && branchCompleted ? branchIndex : null;
    int nextBranch = nextRunnableBranchAfter(fork, branchIndex, branchCompleted);
    var events = new java.util.ArrayList<EngineEvent>();
    if (enteredExtensionDecisions != null) {
      events.add(
          new EngineEvent.ForkBranchExtensionEntered(
              command.commandId(),
              frame.taskPath(),
              branchIndex,
              enteredTaskPath,
              enteredRawInput,
              enteredInput,
              enteredExtensionDecisions,
              nextStep,
              nextBranch,
              command.requestedAt()));
    } else if (enteredCollection != null) {
      events.add(
          new EngineEvent.ForkBranchForEntered(
              command.commandId(),
              frame.taskPath(),
              branchIndex,
              enteredTaskPath,
              enteredRawInput,
              enteredInput,
              enteredCollection,
              enteredIteration,
              enteredItemVariable,
              enteredIndexVariable,
              nextStep,
              nextBranch,
              command.requestedAt()));
    } else if (advancedIteration != null) {
      events.add(
          new EngineEvent.ForkBranchForAdvanced(
              command.commandId(),
              frame.taskPath(),
              branchIndex,
              branch.taskStack().getLast().taskPath(),
              nextData,
              advancedIteration,
              nextStep,
              nextBranch,
              command.requestedAt()));
    } else if (enteredFunction != null) {
      events.add(
          new EngineEvent.ForkBranchFunctionEntered(
              command.commandId(),
              frame.taskPath(),
              branchIndex,
              enteredTaskPath,
              enteredRawInput,
              enteredInput,
              enteredFunction,
              nextStep,
              nextBranch,
              command.requestedAt()));
    } else if (enteredTaskPath != null) {
      events.add(
          new EngineEvent.ForkBranchTaskEntered(
              command.commandId(),
              frame.taskPath(),
              branchIndex,
              enteredTaskPath,
              enteredRawInput,
              enteredInput,
              nextStep,
              nextBranch,
              command.requestedAt()));
    } else if (completedTaskPath != null) {
      events.add(
          new EngineEvent.ForkBranchTaskCompleted(
              command.commandId(),
              frame.taskPath(),
              branchIndex,
              completedTaskPath,
              nextData,
              completedContext,
              nextStep,
              nextBranch,
              winner,
              command.requestedAt()));
    } else {
      events.add(
          new EngineEvent.ForkBranchAdvanced(
              command.commandId(),
              frame.taskPath(),
              branchIndex,
              nextData,
              nextStep,
              nextBranch,
              winner,
              command.requestedAt()));
    }
    if (enteredTaskPath != null) {
      appendTaskDeadline(laneState, command, laneStep, enteredRawInput, enteredInput, events);
    }
    if (!nextContext.equals(branch.context())) {
      events.add(
          new EngineEvent.ForkBranchContextUpdated(
              command.commandId(),
              frame.taskPath(),
              selection.path(),
              nextContext,
              command.requestedAt()));
    }
    var advancedBranches = new java.util.ArrayList<>(fork.branches());
    advancedBranches.set(
        branchIndex, branch.advance(nextData, nextContext, nextStep, branch.taskStack()));
    ForkExecutionFrame advancedFork =
        new ForkExecutionFrame(fork.compete(), advancedBranches, nextBranch, winner);
    boolean joined =
        winner != null || advancedBranches.stream().allMatch(ForkBranchState::completed);
    if (joined) {
      JsonNode rawOutput;
      if (winner != null) {
        rawOutput = nextData;
      } else {
        var outputs = JsonNodeFactory.instance.arrayNode();
        for (int index = 0; index < fork.branches().size(); index++) {
          outputs.add(index == branchIndex ? nextData : fork.branches().get(index).data());
        }
        rawOutput = outputs;
      }
      WorkflowState.Running joinState =
          new WorkflowState.Running(
              state.executionId(),
              state.plan(),
              state.data(),
              state.nextStep(),
              state.revision(),
              state.processedCommands(),
              mergedForkContext(state.context(), advancedFork),
              state.rawWorkflowInput(),
              state.taskStack(),
              state.workflowDeadline());
      TaskResult result =
          completeTask(
              joinState,
              program.instruction(state.nextStep()).step(),
              frame.rawInput(),
              frame.input(),
              rawOutput);
      int after =
          ((MilestoneOneProgram.ExitFork)
                  program.instruction(
                      ((MilestoneOneProgram.EnterFork) program.instruction(state.nextStep()))
                          .exit()))
              .next();
      events.add(
          new EngineEvent.TaskCompleted(
              command.commandId(),
              frame.taskPath(),
              after,
              result.output(),
              result.context(),
              command.requestedAt()));
      appendCompletionIfTerminal(state, command, after, result, events);
    } else if (!forkHasRunnable(advancedFork)) {
      events.add(
          new EngineEvent.ForkBranchesWaiting(
              command.commandId(), frame.taskPath(),
              earliestForkWaitOrNull(advancedFork), command.requestedAt()));
    }
    return Effect()
        .persist(events)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> startForkEmit(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      TaskExecutionFrame root,
      ForkSelection selection,
      MilestoneOneProgram.ExecuteEmit instruction) {
    ForkBranchState branch = selection.branch();
    WorkflowState.Running lane = forkLaneState(state, branch);
    PlanStep step = instruction.step();
    JsonNode rawInput = branch.data();
    if (!condition(lane, step, rawInput)) {
      return skipForkEffect(state, command, root, selection, step, rawInput, instruction.next());
    }
    JsonNode input = taskInput(lane, step, rawInput);
    JsonNode properties =
        expressions.evaluateTemplate(
            step.configuration(),
            input,
            arguments(lane, step, rawInput, input, null),
            state.plan().expressions().mode());
    if (!properties.isObject()) {
      throw new IllegalArgumentException(
          "emit.event.with must evaluate to an object at " + step.path());
    }
    String operationId =
        UUID.nameUUIDFromBytes(
                (state.executionId().entityId()
                        + "|fork-emit|"
                        + step.path()
                        + "|"
                        + selection.path()
                        + "|"
                        + state.revision())
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .toString();
    WorkflowCloudEvent event = cloudEvent(properties, operationId, command.requestedAt());
    ForkExecutionFrame hypothetical =
        updateForkTree(
            root.fork(),
            selection.path(),
            0,
            current -> {
              var stack = new java.util.ArrayList<>(current.taskStack());
              stack.add(
                  TaskExecutionFrame.eventing(
                      step.path(), rawInput, input, EventExecutionFrame.emit(operationId, event)));
              return current.advance(input, current.nextStep(), stack);
            });
    return Effect()
        .persist(
            new EngineEvent.ForkBranchEmitRequested(
                command.commandId(),
                root.taskPath(),
                selection.path(),
                step.path(),
                rawInput,
                input,
                operationId,
                event,
                !forkHasRunnable(hypothetical),
                command.requestedAt()))
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> startForkListen(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      TaskExecutionFrame root,
      ForkSelection selection,
      MilestoneOneProgram.ExecuteListen instruction) {
    ForkBranchState branch = selection.branch();
    WorkflowState.Running lane = forkLaneState(state, branch);
    PlanStep step = instruction.step();
    JsonNode rawInput = branch.data();
    if (!condition(lane, step, rawInput)) {
      return skipForkEffect(state, command, root, selection, step, rawInput, instruction.after());
    }
    JsonNode input = taskInput(lane, step, rawInput);
    String operationId =
        UUID.nameUUIDFromBytes(
                (state.executionId().entityId()
                        + "|fork-listen|"
                        + step.path()
                        + "|"
                        + selection.path()
                        + "|"
                        + state.revision())
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .toString();
    ForkExecutionFrame hypothetical =
        updateForkTree(
            root.fork(),
            selection.path(),
            0,
            current -> {
              var stack = new java.util.ArrayList<>(current.taskStack());
              stack.add(
                  TaskExecutionFrame.eventing(
                      step.path(), rawInput, input, EventExecutionFrame.listen(operationId)));
              return current.advance(input, current.nextStep(), stack);
            });
    return Effect()
        .persist(
            new EngineEvent.ForkBranchListenStarted(
                command.commandId(),
                root.taskPath(),
                selection.path(),
                step.path(),
                rawInput,
                input,
                operationId,
                EventTypeSelector.literalTypes(step.listenPlan().consumption()),
                !forkHasRunnable(hypothetical),
                command.requestedAt()))
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> startForkSubworkflow(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      TaskExecutionFrame root,
      ForkSelection selection,
      MilestoneOneProgram.ExecuteSubworkflow instruction) {
    ForkBranchState branch = selection.branch();
    WorkflowState.Running lane = forkLaneState(state, branch);
    PlanStep step = instruction.step();
    JsonNode rawInput = branch.data();
    if (!condition(lane, step, rawInput)) {
      return skipForkEffect(state, command, root, selection, step, rawInput, instruction.next());
    }
    JsonNode input = taskInput(lane, step, rawInput);
    var run = step.runPlan();
    JsonNode configuredInput = run.configuration().get("input");
    JsonNode childInput =
        configuredInput == null
            ? input
            : expressions.evaluateTemplate(
                configuredInput,
                input,
                arguments(lane, step, rawInput, input, null),
                state.plan().expressions().mode());
    UUID childUuid =
        UUID.nameUUIDFromBytes(
            (state.executionId().entityId()
                    + "|fork-subworkflow|"
                    + step.path()
                    + "|"
                    + selection.path()
                    + "|"
                    + state.revision()
                    + "|"
                    + run.subflow().canonical())
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    String operationId = childUuid.toString();
    ExecutionId childExecutionId = new ExecutionId(state.executionId().tenantId(), childUuid);
    TaskResult detached =
        run.await()
            ? null
            : completeTask(
                lane,
                step,
                rawInput,
                input,
                com.fasterxml.jackson.databind.node.NullNode.getInstance());
    ForkExecutionFrame hypothetical =
        updateForkTree(
            root.fork(),
            selection.path(),
            0,
            current -> {
              var stack = new java.util.ArrayList<>(current.taskStack());
              if (run.await()) {
                stack.add(
                    TaskExecutionFrame.eventing(
                        step.path(),
                        rawInput,
                        input,
                        EventExecutionFrame.subworkflow(operationId)));
                return current.advance(input, current.context(), current.nextStep(), stack);
              }
              return current.advance(
                  detached.output(), detached.context(), instruction.next(), stack);
            });
    return Effect()
        .persist(
            new EngineEvent.ForkBranchSubworkflowRequested(
                command.commandId(),
                root.taskPath(),
                selection.path(),
                step.path(),
                rawInput,
                input,
                instruction.next(),
                operationId,
                childExecutionId,
                command.actor(),
                run.subflow(),
                childInput,
                run.await(),
                detached == null ? null : detached.output(),
                detached == null ? null : detached.context(),
                !hypothetical.complete() && !forkHasRunnable(hypothetical),
                command.requestedAt()))
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private static WorkflowState.Running forkLaneState(WorkflowState state, ForkBranchState branch) {
    return new WorkflowState.Running(
        state.executionId(),
        activePlan(state),
        branch.data(),
        branch.nextStep(),
        state.revision(),
        state.processedCommands(),
        branch.context(),
        state.rawWorkflowInput(),
        branch.taskStack(),
        state.workflowDeadline());
  }

  private ReplyEffect<EngineEvent, WorkflowState> skipForkEffect(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      TaskExecutionFrame root,
      ForkSelection selection,
      PlanStep step,
      JsonNode rawInput,
      int nextStep) {
    WorkflowState.Running lane = forkLaneState(state, selection.branch());
    TaskResult result = completeTask(lane, step, rawInput, rawInput, rawInput);
    ForkExecutionFrame hypothetical =
        updateForkTree(
            root.fork(),
            selection.path(),
            0,
            current ->
                current.advance(result.output(), result.context(), nextStep, current.taskStack()));
    return Effect()
        .persist(
            new EngineEvent.ForkBranchEffectSkipped(
                command.commandId(),
                root.taskPath(),
                selection.path(),
                step.path(),
                result.output(),
                result.context(),
                nextStep,
                !hypothetical.complete() && !forkHasRunnable(hypothetical),
                command.requestedAt()))
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> completeRootFork(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram program,
      TaskExecutionFrame frame) {
    JsonNode rawOutput = forkOutput(frame.fork());
    MilestoneOneProgram.EnterFork enter =
        (MilestoneOneProgram.EnterFork) program.instruction(state.nextStep());
    WorkflowState.Running joinState =
        new WorkflowState.Running(
            state.executionId(),
            state.plan(),
            state.data(),
            state.nextStep(),
            state.revision(),
            state.processedCommands(),
            mergedForkContext(state.context(), frame.fork()),
            state.rawWorkflowInput(),
            state.taskStack(),
            state.workflowDeadline());
    TaskResult result =
        completeTask(joinState, enter.step(), frame.rawInput(), frame.input(), rawOutput);
    int after = ((MilestoneOneProgram.ExitFork) program.instruction(enter.exit())).next();
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.TaskCompleted(
            command.commandId(),
            frame.taskPath(),
            after,
            result.output(),
            result.context(),
            command.requestedAt()));
    appendCompletionIfTerminal(state, command, after, result, events);
    return Effect()
        .persist(events)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> advanceForkFailure(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram program,
      TaskExecutionFrame root,
      ForkSelection selection,
      MilestoneOneProgram.Instruction instruction) {
    ForkBranchState branch = selection.branch();
    WorkflowState.Running laneState =
        new WorkflowState.Running(
            state.executionId(),
            state.plan(),
            branch.data(),
            branch.nextStep(),
            state.revision(),
            state.processedCommands(),
            branch.context(),
            state.rawWorkflowInput(),
            branch.taskStack(),
            state.workflowDeadline());
    var events = new java.util.ArrayList<EngineEvent>();
    if (instruction instanceof MilestoneOneProgram.EnterTry enter) {
      PlanStep step = enter.step();
      JsonNode rawInput = branch.data();
      boolean run = condition(laneState, step, rawInput);
      JsonNode input = run ? taskInput(laneState, step, rawInput) : rawInput;
      if (!run) {
        TaskResult result = completeTask(laneState, step, rawInput, input, rawInput);
        int after = ((MilestoneOneProgram.ExitTry) program.instruction(enter.caughtExit())).next();
        events.add(
            new EngineEvent.ForkNestedBranchAdvanced(
                command.commandId(),
                root.taskPath(),
                selection.path(),
                result.output(),
                after,
                command.requestedAt()));
        if (!result.context().equals(branch.context())) {
          events.add(
              new EngineEvent.ForkBranchContextUpdated(
                  command.commandId(),
                  root.taskPath(),
                  selection.path(),
                  result.context(),
                  command.requestedAt()));
        }
      } else {
        events.add(
            new EngineEvent.ForkBranchTryEntered(
                command.commandId(),
                root.taskPath(),
                selection.path(),
                step.path(),
                rawInput,
                input,
                enter.next(),
                command.requestedAt()));
        appendTaskDeadline(laneState, command, step, rawInput, input, events);
      }
    } else if (instruction instanceof MilestoneOneProgram.ExitTry exit) {
      if (branch.taskStack().isEmpty()) {
        throw new IllegalStateException("Fork try stack is empty");
      }
      TaskExecutionFrame frame = branch.taskStack().getLast();
      TaskExecutionFrame.TryPhase expected =
          exit.caught() ? TaskExecutionFrame.TryPhase.CATCH : TaskExecutionFrame.TryPhase.BODY;
      if (!frame.taskPath().equals(exit.step().path()) || frame.tryPhase() != expected) {
        throw new IllegalStateException("Fork try stack does not match " + exit.step().path());
      }
      TaskResult result =
          completeTask(laneState, exit.step(), frame.rawInput(), frame.input(), branch.data());
      events.add(
          new EngineEvent.ForkBranchTryCompleted(
              command.commandId(),
              root.taskPath(),
              selection.path(),
              frame.taskPath(),
              result.output(),
              result.context(),
              exit.next(),
              command.requestedAt()));
    } else if (instruction instanceof MilestoneOneProgram.ExecuteRaise raise) {
      PlanStep step = raise.step();
      JsonNode rawInput = branch.data();
      if (!condition(laneState, step, rawInput)) {
        TaskResult result = completeTask(laneState, step, rawInput, rawInput, rawInput);
        events.add(
            new EngineEvent.ForkNestedBranchAdvanced(
                command.commandId(),
                root.taskPath(),
                selection.path(),
                result.output(),
                raise.next(),
                command.requestedAt()));
        if (!result.context().equals(branch.context())) {
          events.add(
              new EngineEvent.ForkBranchContextUpdated(
                  command.commandId(),
                  root.taskPath(),
                  selection.path(),
                  result.context(),
                  command.requestedAt()));
        }
      } else {
        JsonNode input = taskInput(laneState, step, rawInput);
        JsonNode error = materializeError(laneState, step, rawInput, input);
        events.add(
            new EngineEvent.ErrorRaised(
                command.commandId(), step.path(), error, command.requestedAt()));
        ErrorTarget target = matchingCatch(laneState, error);
        if (target == null) {
          events.add(
              new EngineEvent.Failed(
                  command.commandId(),
                  error
                      .path("detail")
                      .asText(error.path("title").asText("Unhandled workflow error")),
                  command.requestedAt()));
        } else {
          RetryDecision retry = retryDecision(laneState, target, error, command.requestedAt());
          if (retry == null) {
            events.add(
                new EngineEvent.ForkBranchErrorCaught(
                    command.commandId(),
                    root.taskPath(),
                    selection.path(),
                    target.frame().taskPath(),
                    error,
                    target.instruction().catchEntry(),
                    command.requestedAt()));
          } else {
            var stack = stackThroughTry(branch.taskStack(), target.frame().taskPath());
            stack.set(
                stack.size() - 1,
                target
                    .frame()
                    .retrying(
                        error,
                        target.frame().attempt() + 1,
                        retry.deadline(),
                        target.frame().retryStartedAt(),
                        command.requestedAt()));
            ForkExecutionFrame hypothetical =
                updateForkTree(
                    root.fork(),
                    selection.path(),
                    0,
                    current ->
                        current.advance(
                            target.frame().input(), target.instruction().next(), stack));
            events.add(
                new EngineEvent.ForkBranchRetryScheduled(
                    command.commandId(),
                    root.taskPath(),
                    selection.path(),
                    target.frame().taskPath(),
                    error,
                    target.frame().attempt() + 1,
                    target.instruction().next(),
                    retry.deadline(),
                    target.frame().retryStartedAt(),
                    !forkHasRunnable(hypothetical),
                    command.requestedAt()));
          }
        }
      }
    }
    boolean terminal = events.stream().anyMatch(EngineEvent.Failed.class::isInstance);
    boolean retryScheduled =
        events.stream().anyMatch(EngineEvent.ForkBranchRetryScheduled.class::isInstance);
    if (!terminal && !retryScheduled) {
      ForkExecutionFrame hypothetical = root.fork();
      for (EngineEvent event : events) {
        hypothetical = applyForkFailureEvent(hypothetical, event);
      }
      if (!hypothetical.complete() && !forkHasRunnable(hypothetical)) {
        events.add(
            new EngineEvent.ForkBranchesWaiting(
                command.commandId(), root.taskPath(),
                earliestForkWaitOrNull(hypothetical), command.requestedAt()));
      }
    }
    return Effect()
        .persist(events)
        .thenRun(this::scheduleDeadlines)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private static ForkExecutionFrame applyForkFailureEvent(
      ForkExecutionFrame fork, EngineEvent event) {
    if (event instanceof EngineEvent.ForkNestedBranchAdvanced) {
      return applyNestedForkEvent(fork, event);
    }
    List<Integer> path;
    java.util.function.UnaryOperator<ForkBranchState> update;
    if (event instanceof EngineEvent.ForkBranchTryEntered entered) {
      path = entered.branchPath();
      update =
          branch -> {
            var frames = new java.util.ArrayList<>(branch.taskStack());
            frames.add(
                TaskExecutionFrame.trying(
                    entered.taskPath(), entered.rawInput(), entered.input(), entered.occurredAt()));
            return branch.advance(entered.input(), entered.nextStep(), frames);
          };
    } else if (event instanceof EngineEvent.ForkBranchTryCompleted completed) {
      path = completed.branchPath();
      update =
          branch -> {
            var frames = stackThroughTry(branch.taskStack(), completed.taskPath());
            frames.removeLast();
            return branch.advance(
                completed.output(), completed.context(), completed.nextStep(), frames);
          };
    } else if (event instanceof EngineEvent.ForkBranchErrorCaught caught) {
      path = caught.branchPath();
      update =
          branch -> {
            var frames = stackThroughTry(branch.taskStack(), caught.tryTaskPath());
            frames.set(frames.size() - 1, frames.getLast().caught(caught.error()));
            return branch.advance(branch.data(), caught.nextStep(), frames);
          };
    } else if (event instanceof EngineEvent.ForkBranchContextUpdated context) {
      path = context.branchPath();
      update =
          branch ->
              branch.advance(
                  branch.data(), context.context(), branch.nextStep(), branch.taskStack());
    } else {
      return fork;
    }
    return updateForkTree(fork, path, 0, update);
  }

  private ReplyEffect<EngineEvent, WorkflowState> advanceNestedFork(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram program,
      TaskExecutionFrame root,
      ForkSelection selection) {
    ForkBranchState branch = selection.branch();
    WorkflowState.Running laneState =
        new WorkflowState.Running(
            state.executionId(),
            state.plan(),
            branch.data(),
            branch.nextStep(),
            state.revision(),
            state.processedCommands(),
            branch.context(),
            state.rawWorkflowInput(),
            branch.taskStack(),
            state.workflowDeadline());
    PlanStep laneStep =
        resolveExtensionGates(program, branch.nextStep(), branch.taskStack()).step();
    EngineEvent event;
    JsonNode nextContext = branch.context();
    if (selection.completedNested() != null) {
      TaskExecutionFrame nested = selection.completedNested();
      MilestoneOneProgram.EnterFork enter =
          (MilestoneOneProgram.EnterFork) program.instruction(branch.nextStep());
      WorkflowState.Running joinState =
          new WorkflowState.Running(
              laneState.executionId(),
              laneState.plan(),
              laneState.data(),
              laneState.nextStep(),
              laneState.revision(),
              laneState.processedCommands(),
              mergedForkContext(branch.context(), nested.fork()),
              laneState.rawWorkflowInput(),
              laneState.taskStack(),
              laneState.workflowDeadline());
      TaskResult result =
          completeTask(
              joinState,
              enter.step(),
              nested.rawInput(),
              nested.input(),
              forkOutput(nested.fork()));
      nextContext = result.context();
      int after = ((MilestoneOneProgram.ExitFork) program.instruction(enter.exit())).next();
      event =
          new EngineEvent.ForkNestedCompleted(
              command.commandId(),
              root.taskPath(),
              selection.path(),
              nested.taskPath(),
              result.output(),
              after,
              command.requestedAt());
    } else {
      MilestoneOneProgram.Instruction instruction =
          resolveExtensionGates(program, branch.nextStep(), branch.taskStack());
      if (instruction instanceof MilestoneOneProgram.ExecuteWait wait) {
        return scheduleForkWait(state, command, root, selection, wait);
      }
      if (instruction instanceof MilestoneOneProgram.EnterExtension enter) {
        PlanStep step = enter.step();
        JsonNode rawInput = laneState.data();
        boolean run = condition(laneState, step, rawInput);
        JsonNode input = run ? taskInput(laneState, step, rawInput) : rawInput;
        if (run) {
          event =
              new EngineEvent.ForkNestedExtensionEntered(
                  command.commandId(),
                  root.taskPath(),
                  selection.path(),
                  step.path(),
                  rawInput,
                  input,
                  extensionDecisions(laneState, step, rawInput, input),
                  enter.next(),
                  command.requestedAt());
        } else {
          TaskResult result = completeTask(laneState, step, rawInput, input, rawInput);
          nextContext = result.context();
          int after =
              ((MilestoneOneProgram.ExitExtension) program.instruction(enter.exit())).next();
          event =
              new EngineEvent.ForkNestedBranchAdvanced(
                  command.commandId(),
                  root.taskPath(),
                  selection.path(),
                  result.output(),
                  after,
                  command.requestedAt());
        }
      } else if (instruction instanceof MilestoneOneProgram.ExtensionGate) {
        throw new IllegalStateException("Unresolved extension gate");
      } else if (instruction instanceof MilestoneOneProgram.ExitExtension exit) {
        if (branch.taskStack().isEmpty()
            || !branch.taskStack().getLast().taskPath().equals(exit.step().path())) {
          throw new IllegalStateException(
              "Nested fork extension stack does not match " + exit.step().path());
        }
        TaskExecutionFrame extension = branch.taskStack().getLast();
        TaskResult result =
            completeTask(
                laneState, exit.step(), extension.rawInput(), extension.input(), laneState.data());
        nextContext = result.context();
        event =
            new EngineEvent.ForkNestedTaskCompleted(
                command.commandId(),
                root.taskPath(),
                selection.path(),
                exit.step().path(),
                result.output(),
                exit.next(),
                command.requestedAt());
      } else if (instruction instanceof MilestoneOneProgram.EnterFork enter) {
        PlanStep step = enter.step();
        JsonNode rawInput = laneState.data();
        boolean run = condition(laneState, step, rawInput);
        JsonNode input = run ? taskInput(laneState, step, rawInput) : rawInput;
        if (!run) {
          TaskResult result = completeTask(laneState, step, rawInput, input, rawInput);
          nextContext = result.context();
          int after = ((MilestoneOneProgram.ExitFork) program.instruction(enter.exit())).next();
          event =
              new EngineEvent.ForkNestedBranchAdvanced(
                  command.commandId(),
                  root.taskPath(),
                  selection.path(),
                  result.output(),
                  after,
                  command.requestedAt());
        } else {
          event =
              new EngineEvent.ForkNestedEntered(
                  command.commandId(),
                  root.taskPath(),
                  selection.path(),
                  step.path(),
                  rawInput,
                  input,
                  enter.branches().stream().map(MilestoneOneProgram.BranchRange::name).toList(),
                  enter.branches().stream().map(MilestoneOneProgram.BranchRange::start).toList(),
                  enter.branches().stream().map(MilestoneOneProgram.BranchRange::end).toList(),
                  step.forkPlan().compete(),
                  command.requestedAt());
        }
      } else if (instruction instanceof MilestoneOneProgram.EnterDo enter) {
        PlanStep step = enter.step();
        JsonNode rawInput = laneState.data();
        boolean run = condition(laneState, step, rawInput);
        JsonNode input = run ? taskInput(laneState, step, rawInput) : rawInput;
        if (run) {
          event =
              new EngineEvent.ForkNestedTaskEntered(
                  command.commandId(),
                  root.taskPath(),
                  selection.path(),
                  step.path(),
                  rawInput,
                  input,
                  enter.next(),
                  command.requestedAt());
        } else {
          TaskResult result = completeTask(laneState, step, rawInput, input, rawInput);
          int after = ((MilestoneOneProgram.ExitDo) program.instruction(enter.exit())).next();
          event =
              new EngineEvent.ForkNestedBranchAdvanced(
                  command.commandId(),
                  root.taskPath(),
                  selection.path(),
                  result.output(),
                  after,
                  command.requestedAt());
        }
      } else if (instruction instanceof MilestoneOneProgram.EnterFunction enter) {
        PlanStep step = enter.step();
        JsonNode rawInput = laneState.data();
        boolean run = condition(laneState, step, rawInput);
        JsonNode input = run ? taskInput(laneState, step, rawInput) : rawInput;
        if (run) {
          JsonNode functionArguments = functionArguments(laneState, step, rawInput, input);
          event =
              new EngineEvent.ForkNestedFunctionEntered(
                  command.commandId(),
                  root.taskPath(),
                  selection.path(),
                  step.path(),
                  rawInput,
                  input,
                  functionOperation(state, step, functionArguments, "fork:" + selection.path()),
                  enter.next(),
                  command.requestedAt());
        } else {
          TaskResult result = completeTask(laneState, step, rawInput, input, rawInput);
          int after = ((MilestoneOneProgram.ExitFunction) program.instruction(enter.exit())).next();
          event =
              new EngineEvent.ForkNestedBranchAdvanced(
                  command.commandId(),
                  root.taskPath(),
                  selection.path(),
                  result.output(),
                  after,
                  command.requestedAt());
        }
      } else if (instruction instanceof MilestoneOneProgram.ExitDo exit) {
        if (branch.taskStack().isEmpty()
            || !branch.taskStack().getLast().taskPath().equals(exit.step().path())) {
          throw new IllegalStateException(
              "Nested fork task stack does not match " + exit.step().path());
        }
        TaskExecutionFrame nestedTask = branch.taskStack().getLast();
        TaskResult result =
            completeTask(
                laneState,
                exit.step(),
                nestedTask.rawInput(),
                nestedTask.input(),
                laneState.data());
        nextContext = result.context();
        event =
            new EngineEvent.ForkNestedTaskCompleted(
                command.commandId(),
                root.taskPath(),
                selection.path(),
                exit.step().path(),
                result.output(),
                exit.next(),
                command.requestedAt());
      } else if (instruction instanceof MilestoneOneProgram.ExitFunction exit) {
        if (branch.taskStack().isEmpty()
            || !branch.taskStack().getLast().taskPath().equals(exit.step().path())) {
          throw new IllegalStateException(
              "Nested fork function stack does not match " + exit.step().path());
        }
        TaskExecutionFrame function = branch.taskStack().getLast();
        TaskResult result =
            completeTask(
                laneState, exit.step(), function.rawInput(), function.input(), laneState.data());
        nextContext = result.context();
        event =
            new EngineEvent.ForkNestedTaskCompleted(
                command.commandId(),
                root.taskPath(),
                selection.path(),
                exit.step().path(),
                result.output(),
                exit.next(),
                command.requestedAt());
      } else if (instruction instanceof MilestoneOneProgram.EnterFor enter) {
        PlanStep step = enter.step();
        JsonNode rawInput = laneState.data();
        boolean run = condition(laneState, step, rawInput);
        JsonNode input = run ? taskInput(laneState, step, rawInput) : rawInput;
        JsonNode collection = null;
        if (run) {
          JsonNode configured = step.forPlan().collection();
          collection =
              step.forPlan().expressionCollection()
                  ? expressions.evaluateTemplate(
                      configured,
                      input,
                      arguments(laneState, step, rawInput, input, null),
                      state.plan().expressions().mode())
                  : configured.deepCopy();
          if (!collection.isArray()) {
            throw new IllegalArgumentException(
                "for.in must evaluate to an array at " + step.path());
          }
        }
        if (!run
            || collection.isEmpty()
            || !continueIteration(laneState, step, rawInput, input, collection, 0)) {
          TaskResult result =
              completeTask(laneState, step, rawInput, input, run ? input : rawInput);
          nextContext = result.context();
          event =
              new EngineEvent.ForkNestedBranchAdvanced(
                  command.commandId(),
                  root.taskPath(),
                  selection.path(),
                  result.output(),
                  enter.after(),
                  command.requestedAt());
        } else {
          event =
              new EngineEvent.ForkNestedForEntered(
                  command.commandId(),
                  root.taskPath(),
                  selection.path(),
                  step.path(),
                  rawInput,
                  input,
                  collection,
                  0,
                  step.forPlan().itemVariable(),
                  step.forPlan().indexVariable(),
                  enter.next(),
                  command.requestedAt());
        }
      } else if (instruction instanceof MilestoneOneProgram.ExitFor exit) {
        if (branch.taskStack().isEmpty()
            || !branch.taskStack().getLast().iterating()
            || !branch.taskStack().getLast().taskPath().equals(exit.step().path())) {
          throw new IllegalStateException(
              "Nested fork iteration stack does not match " + exit.step().path());
        }
        TaskExecutionFrame iteration = branch.taskStack().getLast();
        int nextIndex = iteration.iterationIndex() + 1;
        if (nextIndex < iteration.collection().size()
            && continueIteration(
                laneState,
                exit.step(),
                iteration.rawInput(),
                laneState.data(),
                iteration.collection(),
                nextIndex)) {
          event =
              new EngineEvent.ForkNestedForAdvanced(
                  command.commandId(),
                  root.taskPath(),
                  selection.path(),
                  exit.step().path(),
                  laneState.data(),
                  nextIndex,
                  exit.body(),
                  command.requestedAt());
        } else {
          TaskResult result =
              completeTask(
                  laneState,
                  exit.step(),
                  iteration.rawInput(),
                  iteration.input(),
                  laneState.data());
          nextContext = result.context();
          event =
              new EngineEvent.ForkNestedTaskCompleted(
                  command.commandId(),
                  root.taskPath(),
                  selection.path(),
                  exit.step().path(),
                  result.output(),
                  exit.next(),
                  command.requestedAt());
        }
      } else {
        JsonNode nextData;
        int nextStep;
        if (instruction instanceof MilestoneOneProgram.ExecuteSet execute) {
          PlanStep step = execute.step();
          JsonNode rawInput = laneState.data();
          boolean run = condition(laneState, step, rawInput);
          JsonNode input = run ? taskInput(laneState, step, rawInput) : rawInput;
          JsonNode rawOutput =
              run
                  ? expressions.evaluateTemplate(
                      step.configuration(),
                      input,
                      arguments(laneState, step, rawInput, input, null),
                      state.plan().expressions().mode())
                  : rawInput;
          nextData = completeTask(laneState, step, rawInput, input, rawOutput).output();
          nextContext = completeTask(laneState, step, rawInput, input, rawOutput).context();
          nextStep = execute.next();
        } else if (instruction instanceof MilestoneOneProgram.ExecuteSwitch choose) {
          PlanStep step = choose.step();
          JsonNode rawInput = laneState.data();
          boolean run = condition(laneState, step, rawInput);
          JsonNode input = run ? taskInput(laneState, step, rawInput) : rawInput;
          int selectedNext = choose.next();
          if (run) {
            Integer selected = null;
            for (MilestoneOneProgram.SwitchTarget candidate : choose.cases()) {
              if (expressions.evaluateCondition(
                  candidate.condition(),
                  input,
                  arguments(laneState, step, rawInput, input, null),
                  state.plan().expressions().mode())) {
                selected = candidate.next();
                break;
              }
            }
            if (selected == null) selected = choose.defaultNext();
            if (selected != null) selectedNext = selected;
          }
          TaskResult result =
              completeTask(laneState, step, rawInput, input, run ? input : rawInput);
          nextData = result.output();
          nextContext = result.context();
          nextStep = selectedNext;
        } else {
          throw new IllegalStateException(
              "Nested fork lane task is not yet executable: " + instruction.step().kind());
        }
        event =
            new EngineEvent.ForkNestedBranchAdvanced(
                command.commandId(),
                root.taskPath(),
                selection.path(),
                nextData,
                nextStep,
                command.requestedAt());
      }
    }
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(event);
    if (event instanceof EngineEvent.ForkNestedEntered entered) {
      appendTaskDeadline(laneState, command, laneStep, entered.rawInput(), entered.input(), events);
    } else if (event instanceof EngineEvent.ForkNestedTaskEntered entered) {
      appendTaskDeadline(laneState, command, laneStep, entered.rawInput(), entered.input(), events);
    } else if (event instanceof EngineEvent.ForkNestedFunctionEntered entered) {
      appendTaskDeadline(laneState, command, laneStep, entered.rawInput(), entered.input(), events);
    } else if (event instanceof EngineEvent.ForkNestedForEntered entered) {
      appendTaskDeadline(laneState, command, laneStep, entered.rawInput(), entered.input(), events);
    }
    if (!nextContext.equals(branch.context())) {
      events.add(
          new EngineEvent.ForkBranchContextUpdated(
              command.commandId(),
              root.taskPath(),
              selection.path(),
              nextContext,
              command.requestedAt()));
    }
    ForkExecutionFrame hypothetical = applyNestedForkEvent(root.fork(), event);
    if (!hypothetical.complete() && !forkHasRunnable(hypothetical)) {
      events.add(
          new EngineEvent.ForkBranchesWaiting(
              command.commandId(), root.taskPath(),
              earliestForkWaitOrNull(hypothetical), command.requestedAt()));
    }
    return Effect()
        .persist(events)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private static ForkExecutionFrame applyNestedForkEvent(
      ForkExecutionFrame root, EngineEvent event) {
    List<Integer> path;
    java.util.function.UnaryOperator<ForkBranchState> update;
    if (event instanceof EngineEvent.ForkNestedBranchAdvanced advanced) {
      path = advanced.branchPath();
      update = branch -> branch.advance(advanced.data(), advanced.nextStep(), branch.taskStack());
    } else if (event instanceof EngineEvent.ForkNestedEntered entered) {
      path = entered.parentBranchPath();
      update =
          branch -> {
            var branches = new java.util.ArrayList<ForkBranchState>();
            for (int index = 0; index < entered.branchNames().size(); index++) {
              branches.add(
                  new ForkBranchState(
                      entered.branchNames().get(index),
                      entered.input(),
                      branch.context(),
                      entered.branchStarts().get(index),
                      entered.branchEnds().get(index),
                      List.of(),
                      false));
            }
            ForkExecutionFrame nested =
                new ForkExecutionFrame(entered.compete(), branches, 0, null);
            var stack = new java.util.ArrayList<>(branch.taskStack());
            stack.add(
                TaskExecutionFrame.forking(
                    entered.taskPath(), entered.rawInput(), entered.input(), nested));
            return branch.advance(branch.data(), branch.nextStep(), stack);
          };
    } else if (event instanceof EngineEvent.ForkNestedCompleted completed) {
      path = completed.parentBranchPath();
      update =
          branch -> {
            var stack = new java.util.ArrayList<>(branch.taskStack());
            stack.removeLast();
            return branch.advance(completed.output(), completed.nextStep(), stack);
          };
    } else if (event instanceof EngineEvent.ForkNestedTaskEntered entered) {
      path = entered.branchPath();
      update =
          branch -> {
            var stack = new java.util.ArrayList<>(branch.taskStack());
            stack.add(
                new TaskExecutionFrame(entered.taskPath(), entered.rawInput(), entered.input()));
            return branch.advance(entered.input(), entered.nextStep(), stack);
          };
    } else if (event instanceof EngineEvent.ForkNestedFunctionEntered entered) {
      path = entered.branchPath();
      update =
          branch -> {
            var stack = new java.util.ArrayList<>(branch.taskStack());
            stack.add(
                new TaskExecutionFrame(entered.taskPath(), entered.rawInput(), entered.input()));
            return branch.advance(entered.operation().arguments(), entered.nextStep(), stack);
          };
    } else if (event instanceof EngineEvent.ForkNestedTaskCompleted completed) {
      path = completed.branchPath();
      update =
          branch -> {
            var stack = new java.util.ArrayList<>(branch.taskStack());
            stack.removeLast();
            return branch.advance(completed.output(), completed.nextStep(), stack);
          };
    } else if (event instanceof EngineEvent.ForkNestedForEntered entered) {
      path = entered.branchPath();
      update =
          branch -> {
            var stack = new java.util.ArrayList<>(branch.taskStack());
            stack.add(
                new TaskExecutionFrame(
                    entered.taskPath(),
                    entered.rawInput(),
                    entered.input(),
                    entered.collection(),
                    entered.iterationIndex(),
                    entered.itemVariable(),
                    entered.indexVariable()));
            return branch.advance(entered.input(), entered.nextStep(), stack);
          };
    } else if (event instanceof EngineEvent.ForkNestedForAdvanced advanced) {
      path = advanced.branchPath();
      update =
          branch -> {
            var stack = new java.util.ArrayList<>(branch.taskStack());
            stack.set(stack.size() - 1, stack.getLast().advance(advanced.iterationIndex()));
            return branch.advance(advanced.data(), advanced.nextStep(), stack);
          };
    } else {
      throw new IllegalArgumentException("Event does not advance a nested fork");
    }
    return updateForkTree(root, path, 0, update);
  }

  private static JsonNode forkOutput(ForkExecutionFrame fork) {
    if (!fork.complete()) throw new IllegalStateException("Fork has not joined");
    if (fork.winner() != null) return fork.branches().get(fork.winner()).data();
    var output = JsonNodeFactory.instance.arrayNode();
    fork.branches().forEach(branch -> output.add(branch.data()));
    return output;
  }

  private static JsonNode mergedForkContext(JsonNode base, ForkExecutionFrame fork) {
    if (!fork.complete()) {
      throw new IllegalStateException("Fork context cannot merge before join");
    }
    if (fork.winner() != null) {
      return fork.branches().get(fork.winner()).context().deepCopy();
    }
    JsonNode merged = base.deepCopy();
    for (ForkBranchState branch : fork.branches()) {
      merged = applyContextChanges(base, branch.context(), merged);
    }
    return merged;
  }

  private static JsonNode applyContextChanges(
      JsonNode base, JsonNode branch, JsonNode accumulated) {
    if (base.equals(branch)) return accumulated;
    if (!base.isObject() || !branch.isObject() || !accumulated.isObject()) {
      return branch.deepCopy();
    }
    ObjectNode result = (ObjectNode) accumulated.deepCopy();
    var names = new java.util.LinkedHashSet<String>();
    base.fieldNames().forEachRemaining(names::add);
    branch.fieldNames().forEachRemaining(names::add);
    for (String name : names) {
      JsonNode before = base.get(name);
      JsonNode after = branch.get(name);
      if (after == null) {
        result.remove(name);
      } else if (before == null) {
        result.set(name, after.deepCopy());
      } else if (!before.equals(after)) {
        JsonNode current = result.get(name);
        result.set(name, applyContextChanges(before, after, current == null ? before : current));
      }
    }
    return result;
  }

  private static ForkSelection selectForkLeaf(ForkExecutionFrame root) {
    var path = new java.util.ArrayList<Integer>();
    ForkExecutionFrame fork = root;
    while (true) {
      int branchIndex = nextRunnableBranch(fork);
      path.add(branchIndex);
      ForkBranchState branch = fork.branches().get(branchIndex);
      if (!branch.taskStack().isEmpty() && branch.taskStack().getLast().forking()) {
        TaskExecutionFrame nested = branch.taskStack().getLast();
        if (nested.fork().complete()) {
          return new ForkSelection(List.copyOf(path), branch, nested);
        }
        fork = nested.fork();
      } else {
        return new ForkSelection(List.copyOf(path), branch, null);
      }
    }
  }

  private record ForkSelection(
      List<Integer> path, ForkBranchState branch, TaskExecutionFrame completedNested) {}

  private static TaskExecutionFrame activeFork(WorkflowState.Running state) {
    return activeFork(state.taskStack());
  }

  private static TaskExecutionFrame activeFork(List<TaskExecutionFrame> stack) {
    if (stack.isEmpty()) return null;
    TaskExecutionFrame frame = stack.getLast();
    return frame.forking() ? frame : null;
  }

  private static int nextRunnableBranch(ForkExecutionFrame fork) {
    for (int offset = 0; offset < fork.branches().size(); offset++) {
      int candidate = (fork.nextBranch() + offset) % fork.branches().size();
      if (branchRunnable(fork.branches().get(candidate))) return candidate;
    }
    throw new IllegalStateException("Completed fork has no runnable branch");
  }

  private static boolean branchRunnable(ForkBranchState branch) {
    if (branch.completed()) return false;
    if (branch.taskStack().isEmpty()) return true;
    TaskExecutionFrame frame = branch.taskStack().getLast();
    if (frame.waiting() || frame.eventing()) return false;
    return !frame.forking() || frame.fork().complete() || forkHasRunnable(frame.fork());
  }

  private static boolean forkHasRunnable(ForkExecutionFrame fork) {
    return fork.complete() || fork.branches().stream().anyMatch(WorkflowEntity::branchRunnable);
  }

  private static int nextRunnableBranchAfter(
      ForkExecutionFrame fork, int current, boolean currentCompleted) {
    for (int offset = 1; offset <= fork.branches().size(); offset++) {
      int candidate = (current + offset) % fork.branches().size();
      if (candidate == current) {
        if (!currentCompleted) return current;
      } else if (branchRunnable(fork.branches().get(candidate))) {
        return candidate;
      }
    }
    return current;
  }

  private ReplyEffect<EngineEvent, WorkflowState> executeSet(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.ExecuteSet instruction) {
    PlanStep step = instruction.step();
    JsonNode rawInput = state.data();
    boolean execute = condition(state, step, rawInput);
    JsonNode input = execute ? taskInput(state, step, rawInput) : rawInput;
    JsonNode rawOutput =
        execute
            ? expressions.evaluateTemplate(
                step.configuration(),
                input,
                arguments(state, step, rawInput, input, null),
                state.plan().expressions().mode())
            : rawInput;
    TaskResult result = completeTask(state, step, rawInput, input, rawOutput);
    return Effect()
        .persist(taskEvents(state, command, step, rawInput, input, instruction.next(), result))
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> executeSwitch(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.ExecuteSwitch instruction) {
    PlanStep step = instruction.step();
    JsonNode rawInput = state.data();
    boolean execute = condition(state, step, rawInput);
    JsonNode input = execute ? taskInput(state, step, rawInput) : rawInput;
    int next = instruction.next();
    if (execute) {
      Integer selected = null;
      for (MilestoneOneProgram.SwitchTarget candidate : instruction.cases()) {
        if (expressions.evaluateCondition(
            candidate.condition(),
            input,
            arguments(state, step, rawInput, input, null),
            state.plan().expressions().mode())) {
          selected = candidate.next();
          break;
        }
      }
      if (selected == null) selected = instruction.defaultNext();
      if (selected != null) next = selected;
    }
    TaskResult result = completeTask(state, step, rawInput, input, execute ? input : rawInput);
    return Effect()
        .persist(taskEvents(state, command, step, rawInput, input, next, result))
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> enterDo(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.EnterDo instruction) {
    PlanStep step = instruction.step();
    JsonNode rawInput = state.data();
    boolean execute = condition(state, step, rawInput);
    JsonNode input = execute ? taskInput(state, step, rawInput) : rawInput;
    if (!execute) {
      TaskResult skipped = completeTask(state, step, rawInput, input, rawInput);
      int after =
          ((MilestoneOneProgram.ExitDo)
                  MilestoneOneProgram.compile(state.plan()).instruction(instruction.exit()))
              .next();
      return Effect()
          .persist(taskEvents(state, command, step, rawInput, input, after, skipped))
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.TaskEntered(
            command.commandId(),
            step.path(),
            rawInput,
            input,
            instruction.next(),
            command.requestedAt()));
    appendTaskDeadline(state, command, step, rawInput, input, events);
    return Effect()
        .persist(events)
        .thenRun(this::scheduleDeadlines)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> enterFunction(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.EnterFunction instruction) {
    PlanStep step = instruction.step();
    JsonNode rawInput = state.data();
    boolean execute = condition(state, step, rawInput);
    JsonNode input = execute ? taskInput(state, step, rawInput) : rawInput;
    if (!execute) {
      TaskResult skipped = completeTask(state, step, rawInput, input, rawInput);
      int after =
          ((MilestoneOneProgram.ExitFunction)
                  MilestoneOneProgram.compile(state.plan()).instruction(instruction.exit()))
              .next();
      return Effect()
          .persist(taskEvents(state, command, step, rawInput, input, after, skipped))
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    JsonNode functionArguments = functionArguments(state, step, rawInput, input);
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.FunctionEntered(
            command.commandId(),
            step.path(),
            rawInput,
            input,
            functionOperation(state, step, functionArguments, "root"),
            instruction.next(),
            command.requestedAt()));
    appendTaskDeadline(state, command, step, rawInput, input, events);
    return Effect()
        .persist(events)
        .thenRun(this::scheduleDeadlines)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> exitFunction(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.ExitFunction instruction) {
    if (state.taskStack().isEmpty()
        || !state.taskStack().getLast().taskPath().equals(instruction.step().path())) {
      throw new IllegalStateException(
          "Function task stack does not match " + instruction.step().path());
    }
    TaskExecutionFrame frame = state.taskStack().getLast();
    TaskResult result =
        completeTask(state, instruction.step(), frame.rawInput(), frame.input(), state.data());
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.TaskCompleted(
            command.commandId(),
            instruction.step().path(),
            instruction.next(),
            result.output(),
            result.context(),
            command.requestedAt()));
    appendCompletionIfTerminal(state, command, instruction.next(), result, events);
    return Effect()
        .persist(events)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> exitDo(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.ExitDo instruction) {
    if (state.taskStack().isEmpty()) {
      throw new IllegalStateException("Nested task stack is empty at " + instruction.step().path());
    }
    TaskExecutionFrame frame = state.taskStack().getLast();
    if (!frame.taskPath().equals(instruction.step().path())) {
      throw new IllegalStateException(
          "Nested task stack does not match " + instruction.step().path());
    }
    TaskResult result =
        completeTask(state, instruction.step(), frame.rawInput(), frame.input(), state.data());
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.TaskCompleted(
            command.commandId(),
            instruction.step().path(),
            instruction.next(),
            result.output(),
            result.context(),
            command.requestedAt()));
    appendCompletionIfTerminal(state, command, instruction.next(), result, events);
    return Effect()
        .persist(events)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> enterFor(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.EnterFor instruction) {
    PlanStep step = instruction.step();
    JsonNode rawInput = state.data();
    boolean execute = condition(state, step, rawInput);
    JsonNode input = execute ? taskInput(state, step, rawInput) : rawInput;
    if (!execute) {
      TaskResult skipped = completeTask(state, step, rawInput, input, rawInput);
      return Effect()
          .persist(taskEvents(state, command, step, rawInput, input, instruction.after(), skipped))
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }

    JsonNode configured = step.forPlan().collection();
    JsonNode collection =
        step.forPlan().expressionCollection()
            ? expressions.evaluateTemplate(
                configured,
                input,
                arguments(state, step, rawInput, input, null),
                state.plan().expressions().mode())
            : configured.deepCopy();
    if (!collection.isArray()) {
      throw new IllegalArgumentException("for.in must evaluate to an array at " + step.path());
    }
    if (collection.isEmpty() || !continueIteration(state, step, rawInput, input, collection, 0)) {
      TaskResult empty = completeTask(state, step, rawInput, input, input);
      return Effect()
          .persist(taskEvents(state, command, step, rawInput, input, instruction.after(), empty))
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.ForEntered(
            command.commandId(),
            step.path(),
            rawInput,
            input,
            collection,
            0,
            step.forPlan().itemVariable(),
            step.forPlan().indexVariable(),
            instruction.next(),
            command.requestedAt()));
    appendTaskDeadline(state, command, step, rawInput, input, events);
    return Effect()
        .persist(events)
        .thenRun(this::scheduleDeadlines)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> exitFor(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.ExitFor instruction) {
    TaskExecutionFrame frame = activeIteration(state, instruction.step());
    int nextIndex = frame.iterationIndex() + 1;
    if (nextIndex < frame.collection().size()
        && continueIteration(
            state,
            instruction.step(),
            frame.rawInput(),
            state.data(),
            frame.collection(),
            nextIndex)) {
      return Effect()
          .persist(
              new EngineEvent.ForIterationAdvanced(
                  command.commandId(),
                  instruction.step().path(),
                  state.data(),
                  nextIndex,
                  instruction.body(),
                  command.requestedAt()))
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }

    TaskResult result =
        completeTask(state, instruction.step(), frame.rawInput(), frame.input(), state.data());
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.TaskCompleted(
            command.commandId(),
            instruction.step().path(),
            instruction.next(),
            result.output(),
            result.context(),
            command.requestedAt()));
    appendCompletionIfTerminal(state, command, instruction.next(), result, events);
    return Effect()
        .persist(events)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private boolean continueIteration(
      WorkflowState.Running state,
      PlanStep step,
      JsonNode rawInput,
      JsonNode evaluatedOn,
      JsonNode collection,
      int index) {
    if (step.forPlan().whileCondition() == null) return true;
    return expressions.evaluateCondition(
        step.forPlan().whileCondition(),
        evaluatedOn,
        arguments(
            state,
            step,
            rawInput,
            evaluatedOn,
            null,
            iterationVariables(state, step, collection, index)),
        state.plan().expressions().mode());
  }

  private TaskExecutionFrame activeIteration(WorkflowState.Running state, PlanStep step) {
    if (state.taskStack().isEmpty()) {
      throw new IllegalStateException("Iteration stack is empty at " + step.path());
    }
    TaskExecutionFrame frame = state.taskStack().getLast();
    if (!frame.taskPath().equals(step.path()) || !frame.iterating()) {
      throw new IllegalStateException("Iteration stack does not match " + step.path());
    }
    return frame;
  }

  private ReplyEffect<EngineEvent, WorkflowState> scheduleForkWait(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      TaskExecutionFrame root,
      ForkSelection selection,
      MilestoneOneProgram.ExecuteWait instruction) {
    ForkBranchState branch = selection.branch();
    WorkflowState.Running laneState =
        new WorkflowState.Running(
            state.executionId(),
            state.plan(),
            branch.data(),
            branch.nextStep(),
            state.revision(),
            state.processedCommands(),
            branch.context(),
            state.rawWorkflowInput(),
            branch.taskStack(),
            state.workflowDeadline());
    PlanStep step = instruction.step();
    JsonNode rawInput = laneState.data();
    boolean execute = condition(laneState, step, rawInput);
    JsonNode input = execute ? taskInput(laneState, step, rawInput) : rawInput;
    if (!execute) {
      TaskResult skipped = completeTask(laneState, step, rawInput, input, rawInput);
      return Effect()
          .persist(
              new EngineEvent.ForkNestedBranchAdvanced(
                  command.commandId(),
                  root.taskPath(),
                  selection.path(),
                  skipped.output(),
                  instruction.next(),
                  command.requestedAt()))
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    Instant deadline = resolveDeadline(laneState, step, rawInput, input, command.requestedAt());
    ForkExecutionFrame hypothetical =
        updateForkTree(
            root.fork(),
            selection.path(),
            0,
            current -> {
              var stack = new java.util.ArrayList<>(current.taskStack());
              stack.add(TaskExecutionFrame.waiting(step.path(), rawInput, input, deadline));
              return current.advance(input, current.nextStep(), stack);
            });
    boolean allBranchesWaiting = !forkHasRunnable(hypothetical);
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.ForkBranchWaitScheduled(
            command.commandId(),
            root.taskPath(),
            selection.path(),
            step.path(),
            rawInput,
            input,
            branch.nextStep(),
            deadline,
            allBranchesWaiting,
            command.requestedAt()));
    appendTaskDeadline(laneState, command, step, rawInput, input, events);
    return Effect()
        .persist(events)
        .thenRun(this::scheduleDeadlines)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> executeWait(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.ExecuteWait instruction) {
    PlanStep step = instruction.step();
    JsonNode rawInput = state.data();
    boolean execute = condition(state, step, rawInput);
    JsonNode input = execute ? taskInput(state, step, rawInput) : rawInput;
    if (!execute) {
      TaskResult skipped = completeTask(state, step, rawInput, input, rawInput);
      return Effect()
          .persist(taskEvents(state, command, step, rawInput, input, instruction.next(), skipped))
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    Instant deadline = resolveDeadline(state, step, rawInput, input, command.requestedAt());
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.WaitScheduled(
            command.commandId(),
            step.path(),
            rawInput,
            input,
            state.nextStep(),
            deadline,
            command.requestedAt()));
    appendTaskDeadline(state, command, step, rawInput, input, events);
    return Effect()
        .persist(events)
        .thenRun(this::scheduleDeadlines)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> executeEmit(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.ExecuteEmit instruction) {
    PlanStep step = instruction.step();
    JsonNode rawInput = state.data();
    boolean execute = condition(state, step, rawInput);
    JsonNode input = execute ? taskInput(state, step, rawInput) : rawInput;
    if (!execute) {
      TaskResult skipped = completeTask(state, step, rawInput, input, rawInput);
      return Effect()
          .persist(taskEvents(state, command, step, rawInput, input, instruction.next(), skipped))
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    JsonNode properties =
        expressions.evaluateTemplate(
            step.configuration(),
            input,
            arguments(state, step, rawInput, input, null),
            state.plan().expressions().mode());
    if (!properties.isObject()) {
      throw new IllegalArgumentException(
          "emit.event.with must evaluate to an object at " + step.path());
    }
    String operationId =
        UUID.nameUUIDFromBytes(
                (state.executionId().entityId() + "|emit|" + step.path() + "|" + state.revision())
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .toString();
    WorkflowCloudEvent event = cloudEvent(properties, operationId, command.requestedAt());
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.EmitRequested(
            command.commandId(),
            step.path(),
            rawInput,
            input,
            state.nextStep(),
            operationId,
            event,
            command.requestedAt()));
    appendTaskDeadline(state, command, step, rawInput, input, events);
    return Effect()
        .persist(events)
        .thenRun(this::scheduleDeadlines)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> executeHttpCall(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.ExecuteHttpCall instruction) {
    PlanStep step = instruction.step();
    JsonNode rawInput = state.data();
    boolean execute = condition(state, step, rawInput);
    JsonNode input = execute ? taskInput(state, step, rawInput) : rawInput;
    if (!execute) {
      TaskResult skipped = completeTask(state, step, rawInput, input, rawInput);
      return Effect()
          .persist(taskEvents(state, command, step, rawInput, input, instruction.next(), skipped))
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    String operationId =
        UUID.nameUUIDFromBytes(
                (state.executionId().entityId()
                        + "|http-call|"
                        + step.path()
                        + "|"
                        + state.revision())
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .toString();
    RuntimeExpressionArguments expressionArguments = arguments(state, step, rawInput, input, null);
    JsonNode evaluated =
        expressions.evaluateTemplate(
            step.callPlan().arguments(),
            input,
            expressionArguments,
            state.plan().expressions().mode());
    HttpOperationDescriptor operation =
        HttpOperationMaterializer.materialize(
                state.plan(),
                step,
                evaluated,
                input,
                operationId,
                authenticationContext(expressionArguments))
            .requestedBy(command.actor());
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.HttpCallRequested(
            command.commandId(),
            step.path(),
            rawInput,
            input,
            instruction.next(),
            operation,
            command.requestedAt()));
    appendTaskDeadline(state, command, step, rawInput, input, events);
    return Effect()
        .persist(events)
        .thenRun(this::scheduleDeadlines)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> startForkHttpCall(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      TaskExecutionFrame root,
      ForkSelection selection,
      MilestoneOneProgram.ExecuteHttpCall instruction) {
    ForkBranchState branch = selection.branch();
    WorkflowState.Running lane = forkLaneState(state, branch);
    PlanStep step = instruction.step();
    JsonNode rawInput = branch.data();
    if (!condition(lane, step, rawInput)) {
      return skipForkEffect(state, command, root, selection, step, rawInput, instruction.next());
    }
    JsonNode input = taskInput(lane, step, rawInput);
    String operationId =
        UUID.nameUUIDFromBytes(
                (state.executionId().entityId()
                        + "|fork-http-call|"
                        + step.path()
                        + "|"
                        + selection.path()
                        + "|"
                        + state.revision())
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .toString();
    RuntimeExpressionArguments expressionArguments = arguments(lane, step, rawInput, input, null);
    JsonNode evaluated =
        expressions.evaluateTemplate(
            step.callPlan().arguments(),
            input,
            expressionArguments,
            state.plan().expressions().mode());
    HttpOperationDescriptor operation =
        HttpOperationMaterializer.materialize(
                state.plan(),
                step,
                evaluated,
                input,
                operationId,
                authenticationContext(expressionArguments))
            .requestedBy(command.actor());
    ForkExecutionFrame hypothetical =
        updateForkTree(
            root.fork(),
            selection.path(),
            0,
            current -> {
              var stack = new java.util.ArrayList<>(current.taskStack());
              stack.add(
                  TaskExecutionFrame.eventing(
                      step.path(), rawInput, input, EventExecutionFrame.httpCall(operationId)));
              return current.advance(input, current.nextStep(), stack);
            });
    return Effect()
        .persist(
            new EngineEvent.ForkBranchHttpCallRequested(
                command.commandId(),
                root.taskPath(),
                selection.path(),
                step.path(),
                rawInput,
                input,
                operation,
                !forkHasRunnable(hypothetical),
                command.requestedAt()))
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> executeProtocolCall(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.ExecuteProtocolCall instruction) {
    PlanStep step = instruction.step();
    JsonNode rawInput = state.data();
    boolean execute = condition(state, step, rawInput);
    JsonNode input = execute ? taskInput(state, step, rawInput) : rawInput;
    if (!execute) {
      TaskResult skipped = completeTask(state, step, rawInput, input, rawInput);
      return Effect()
          .persist(taskEvents(state, command, step, rawInput, input, instruction.next(), skipped))
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    String operationId =
        UUID.nameUUIDFromBytes(
                (state.executionId().entityId()
                        + "|protocol-call|"
                        + step.path()
                        + "|"
                        + state.revision())
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .toString();
    RuntimeExpressionArguments expressionArguments = arguments(state, step, rawInput, input, null);
    JsonNode evaluated = evaluateProtocolArguments(state, step, input, expressionArguments);
    ProtocolOperationDescriptor operation =
        ProtocolOperationMaterializer.materialize(
                state.plan(),
                step,
                evaluated,
                operationId,
                authenticationContext(expressionArguments),
                protocolDeadline(state, step, rawInput, input, command.requestedAt()))
            .requestedBy(command.actor());
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.ProtocolCallRequested(
            command.commandId(),
            step.path(),
            rawInput,
            input,
            instruction.next(),
            operation,
            command.requestedAt()));
    appendTaskDeadline(state, command, step, rawInput, input, events);
    return Effect()
        .persist(events)
        .thenRun(this::scheduleDeadlines)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> startForkProtocolCall(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      TaskExecutionFrame root,
      ForkSelection selection,
      MilestoneOneProgram.ExecuteProtocolCall instruction) {
    ForkBranchState branch = selection.branch();
    WorkflowState.Running lane = forkLaneState(state, branch);
    PlanStep step = instruction.step();
    JsonNode rawInput = branch.data();
    if (!condition(lane, step, rawInput)) {
      return skipForkEffect(state, command, root, selection, step, rawInput, instruction.next());
    }
    JsonNode input = taskInput(lane, step, rawInput);
    String operationId =
        UUID.nameUUIDFromBytes(
                (state.executionId().entityId()
                        + "|fork-protocol-call|"
                        + step.path()
                        + "|"
                        + selection.path()
                        + "|"
                        + state.revision())
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .toString();
    RuntimeExpressionArguments expressionArguments = arguments(lane, step, rawInput, input, null);
    JsonNode evaluated = evaluateProtocolArguments(lane, step, input, expressionArguments);
    ProtocolOperationDescriptor operation =
        ProtocolOperationMaterializer.materialize(
                state.plan(),
                step,
                evaluated,
                operationId,
                authenticationContext(expressionArguments),
                protocolDeadline(lane, step, rawInput, input, command.requestedAt()))
            .requestedBy(command.actor());
    ForkExecutionFrame hypothetical =
        updateForkTree(
            root.fork(),
            selection.path(),
            0,
            current -> {
              var stack = new java.util.ArrayList<>(current.taskStack());
              stack.add(
                  TaskExecutionFrame.eventing(
                      step.path(), rawInput, input, EventExecutionFrame.protocolCall(operationId)));
              return current.advance(input, current.nextStep(), stack);
            });
    return Effect()
        .persist(
            new EngineEvent.ForkBranchProtocolCallRequested(
                command.commandId(),
                root.taskPath(),
                selection.path(),
                step.path(),
                rawInput,
                input,
                operation,
                !forkHasRunnable(hypothetical),
                command.requestedAt()))
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> executeSubworkflow(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.ExecuteSubworkflow instruction) {
    PlanStep step = instruction.step();
    JsonNode rawInput = state.data();
    boolean execute = condition(state, step, rawInput);
    JsonNode input = execute ? taskInput(state, step, rawInput) : rawInput;
    if (!execute) {
      TaskResult skipped = completeTask(state, step, rawInput, input, rawInput);
      return Effect()
          .persist(taskEvents(state, command, step, rawInput, input, instruction.next(), skipped))
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    var run = step.runPlan();
    JsonNode configuredInput = run.configuration().get("input");
    JsonNode childInput =
        configuredInput == null
            ? input
            : expressions.evaluateTemplate(
                configuredInput,
                input,
                arguments(state, step, rawInput, input, null),
                state.plan().expressions().mode());
    UUID childUuid =
        UUID.nameUUIDFromBytes(
            (state.executionId().entityId()
                    + "|subworkflow|"
                    + step.path()
                    + "|"
                    + state.revision()
                    + "|"
                    + run.subflow().canonical())
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    String operationId = childUuid.toString();
    ExecutionId childExecutionId = new ExecutionId(state.executionId().tenantId(), childUuid);
    TaskResult detached =
        run.await()
            ? null
            : completeTask(
                state,
                step,
                rawInput,
                input,
                com.fasterxml.jackson.databind.node.NullNode.getInstance());
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.SubworkflowRequested(
            command.commandId(),
            step.path(),
            rawInput,
            input,
            instruction.next(),
            operationId,
            childExecutionId,
            command.actor(),
            run.subflow(),
            childInput,
            run.await(),
            detached == null ? null : detached.output(),
            detached == null ? null : detached.context(),
            command.requestedAt()));
    appendTaskDeadline(state, command, step, rawInput, input, events);
    if (!run.await() && instruction.next() == MilestoneOneProgram.compile(state.plan()).size()) {
      events.add(
          new EngineEvent.Completed(
              command.commandId(),
              workflowOutput(state, detached.output(), detached.context()),
              command.requestedAt()));
    }
    return Effect()
        .persist(events)
        .thenRun(this::scheduleDeadlines)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> completeSubworkflow(
      WorkflowState state, WorkflowCommand.SubworkflowCompleted command) {
    if (!state.executionId().equals(command.executionId())) {
      return rejectSubworkflowNotPending(state, command);
    }
    TaskExecutionFrame root = activeFork(state.taskStack());
    if (root != null) {
      ForkEventSelection selection =
          findForkEvent(
              root.fork(),
              EventExecutionFrame.Kind.SUBWORKFLOW,
              command.operationId(),
              new java.util.ArrayList<>());
      if (selection != null) {
        return completeForkSubworkflow(state, command, root, selection);
      }
    }
    if (state.taskStack().isEmpty()) {
      return rejectSubworkflowNotPending(state, command);
    }
    TaskExecutionFrame frame = state.taskStack().getLast();
    if (!frame.eventing()
        || frame.event().kind() != EventExecutionFrame.Kind.SUBWORKFLOW
        || !frame.event().operationId().equals(command.operationId())
        || !command.childExecutionId().value().toString().equals(command.operationId())) {
      return rejectSubworkflowNotPending(state, command);
    }
    if (command.childStatus()
        != com.forwardmeasure.openworkflow.engine.api.ExecutionStatus.COMPLETED) {
      return failSubworkflow(state, command, frame.taskPath());
    }
    MilestoneOneProgram program = MilestoneOneProgram.compile(activePlan(state));
    if (!(program.instruction(activeNextStep(state))
        instanceof MilestoneOneProgram.ExecuteSubworkflow instruction)) {
      throw new IllegalStateException("Durable subworkflow cursor is not run.workflow");
    }
    WorkflowState.Running running =
        state instanceof WorkflowState.Running current
            ? current
            : running((WorkflowState.Waiting) state);
    TaskResult result =
        completeTask(
            running, instruction.step(), frame.rawInput(), frame.input(), command.output());
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.SubworkflowCompleted(
            command.commandId(),
            frame.taskPath(),
            command.operationId(),
            command.childExecutionId(),
            command.childStatus(),
            result.output(),
            result.context(),
            instruction.next(),
            command.observedAt()));
    if (instruction.next() == program.size()) {
      events.add(
          new EngineEvent.Completed(
              command.commandId(),
              workflowOutput(running, result.output(), result.context()),
              command.observedAt()));
    }
    var effect = Effect().persist(events).thenRun(this::continueIfRunning);
    return command.replyTo() == null
        ? effect.thenNoReply()
        : effect.thenReply(
            command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> completeHttpCall(
      WorkflowState state, WorkflowCommand.HttpCallCompleted command) {
    if (!state.executionId().equals(command.executionId())) {
      return rejectHttpCallNotPending(state, command);
    }
    TaskExecutionFrame root = activeFork(state.taskStack());
    if (root != null) {
      ForkEventSelection selection =
          findForkEvent(
              root.fork(),
              EventExecutionFrame.Kind.HTTP_CALL,
              command.operationId(),
              new java.util.ArrayList<>());
      if (selection != null) {
        return command.error() == null
            ? completeForkHttpCall(state, command, root, selection)
            : failForkHttpCall(state, command, root, selection);
      }
    }
    if (!(state instanceof WorkflowState.Waiting) || state.taskStack().isEmpty()) {
      return rejectHttpCallNotPending(state, command);
    }
    TaskExecutionFrame frame = state.taskStack().getLast();
    if (!frame.eventing()
        || frame.event().kind() != EventExecutionFrame.Kind.HTTP_CALL
        || !frame.event().operationId().equals(command.operationId())) {
      return rejectHttpCallNotPending(state, command);
    }
    if (command.error() != null) {
      return failHttpCall(state, command, frame.taskPath());
    }
    MilestoneOneProgram program = MilestoneOneProgram.compile(activePlan(state));
    if (!(program.instruction(activeNextStep(state))
        instanceof MilestoneOneProgram.ExecuteHttpCall instruction)) {
      throw new IllegalStateException("Durable HTTP call cursor is not a call");
    }
    WorkflowState.Running running = running((WorkflowState.Waiting) state);
    TaskResult result =
        completeTask(
            running, instruction.step(), frame.rawInput(), frame.input(), command.output());
    UUID commandId = httpResultCommandId(state.executionId(), command.operationId());
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.HttpCallCompleted(
            commandId,
            frame.taskPath(),
            command.operationId(),
            result.output(),
            result.context(),
            instruction.next(),
            command.observedAt()));
    if (instruction.next() == program.size()) {
      events.add(
          new EngineEvent.Completed(
              commandId,
              workflowOutput(running, result.output(), result.context()),
              command.observedAt()));
    }
    var effect = Effect().persist(events).thenRun(this::continueIfRunning);
    return command.replyTo() == null
        ? effect.thenNoReply()
        : effect.thenReply(command.replyTo(), persisted -> accepted(commandId, persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> completeForkHttpCall(
      WorkflowState state,
      WorkflowCommand.HttpCallCompleted command,
      TaskExecutionFrame root,
      ForkEventSelection selection) {
    MilestoneOneProgram program = MilestoneOneProgram.compile(activePlan(state));
    if (!(program.instruction(selection.branch().nextStep())
        instanceof MilestoneOneProgram.ExecuteHttpCall instruction)) {
      throw new IllegalStateException("Durable fork HTTP cursor is not a call");
    }
    WorkflowState.Running lane = forkLaneState(state, selection.branch());
    TaskResult result =
        completeTask(
            lane,
            instruction.step(),
            selection.frame().rawInput(),
            selection.frame().input(),
            command.output());
    ForkExecutionFrame hypothetical =
        updateForkTree(
            root.fork(),
            selection.path(),
            0,
            branch -> {
              var stack = new java.util.ArrayList<>(branch.taskStack());
              stack.removeLast();
              return branch.advance(result.output(), result.context(), instruction.next(), stack);
            });
    UUID commandId = httpResultCommandId(state.executionId(), command.operationId());
    var effect =
        Effect()
            .persist(
                new EngineEvent.ForkBranchHttpCallCompleted(
                    commandId,
                    root.taskPath(),
                    selection.path(),
                    instruction.step().path(),
                    command.operationId(),
                    result.output(),
                    result.context(),
                    instruction.next(),
                    !hypothetical.complete() && !forkHasRunnable(hypothetical),
                    command.observedAt()))
            .thenRun(this::continueIfRunning);
    return command.replyTo() == null
        ? effect.thenNoReply()
        : effect.thenReply(command.replyTo(), persisted -> accepted(commandId, persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> failHttpCall(
      WorkflowState state, WorkflowCommand.HttpCallCompleted command, String taskPath) {
    JsonNode error = httpError(command, taskPath);
    WorkflowState.Running runningState = running((WorkflowState.Waiting) state);
    var events = new java.util.ArrayList<EngineEvent>();
    UUID commandId = httpResultCommandId(state.executionId(), command.operationId());
    events.add(new EngineEvent.ErrorRaised(commandId, taskPath, error, command.observedAt()));
    ErrorTarget target = matchingCatch(runningState, error);
    if (target == null) {
      events.add(
          new EngineEvent.Failed(
              commandId,
              error.path("detail").asText("HTTP operation failed"),
              command.observedAt()));
    } else {
      RetryDecision retry = retryDecision(runningState, target, error, command.observedAt());
      if (retry == null)
        events.add(
            new EngineEvent.ErrorCaught(
                commandId,
                target.frame().taskPath(),
                error,
                target.instruction().catchEntry(),
                command.observedAt()));
      else
        events.add(
            new EngineEvent.RetryScheduled(
                commandId,
                target.frame().taskPath(),
                error,
                target.frame().attempt() + 1,
                target.instruction().next(),
                retry.deadline(),
                target.frame().retryStartedAt(),
                command.observedAt()));
    }
    var effect =
        Effect().persist(events).thenRun(this::scheduleDeadlines).thenRun(this::continueIfRunning);
    return command.replyTo() == null
        ? effect.thenNoReply()
        : effect.thenReply(command.replyTo(), persisted -> accepted(commandId, persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> failForkHttpCall(
      WorkflowState state,
      WorkflowCommand.HttpCallCompleted command,
      TaskExecutionFrame root,
      ForkEventSelection selection) {
    JsonNode error = httpError(command, selection.frame().taskPath());
    WorkflowState.Running lane = forkLaneState(state, selection.branch());
    var events = new java.util.ArrayList<EngineEvent>();
    UUID commandId = httpResultCommandId(state.executionId(), command.operationId());
    events.add(
        new EngineEvent.ErrorRaised(
            commandId, selection.frame().taskPath(), error, command.observedAt()));
    ErrorTarget target = matchingCatch(lane, error);
    if (target == null) {
      events.add(
          new EngineEvent.Failed(
              commandId,
              error.path("detail").asText("HTTP operation failed"),
              command.observedAt()));
    } else {
      RetryDecision retry = retryDecision(lane, target, error, command.observedAt());
      if (retry == null)
        events.add(
            new EngineEvent.ForkBranchErrorCaught(
                commandId,
                root.taskPath(),
                selection.path(),
                target.frame().taskPath(),
                error,
                target.instruction().catchEntry(),
                command.observedAt()));
      else {
        var stack = stackThroughTry(selection.branch().taskStack(), target.frame().taskPath());
        stack.set(
            stack.size() - 1,
            target
                .frame()
                .retrying(
                    error,
                    target.frame().attempt() + 1,
                    retry.deadline(),
                    target.frame().retryStartedAt(),
                    command.observedAt()));
        ForkExecutionFrame hypothetical =
            updateForkTree(
                root.fork(),
                selection.path(),
                0,
                branch ->
                    branch.advance(target.frame().input(), target.instruction().next(), stack));
        events.add(
            new EngineEvent.ForkBranchRetryScheduled(
                commandId,
                root.taskPath(),
                selection.path(),
                target.frame().taskPath(),
                error,
                target.frame().attempt() + 1,
                target.instruction().next(),
                retry.deadline(),
                target.frame().retryStartedAt(),
                !forkHasRunnable(hypothetical),
                command.observedAt()));
      }
    }
    var effect =
        Effect().persist(events).thenRun(this::scheduleDeadlines).thenRun(this::continueIfRunning);
    return command.replyTo() == null
        ? effect.thenNoReply()
        : effect.thenReply(command.replyTo(), persisted -> accepted(commandId, persisted));
  }

  private static JsonNode httpError(WorkflowCommand.HttpCallCompleted command, String taskPath) {
    if (!command.error().isObject()) {
      throw new IllegalArgumentException("HTTP adapter error must be an RFC 9457 object");
    }
    ObjectNode error = (ObjectNode) command.error().deepCopy();
    error.put("type", "https://serverlessworkflow.io/dsl/errors/types/communication");
    if (!error.hasNonNull("status")) error.put("status", 500);
    if (!error.hasNonNull("title")) error.put("title", "HTTP operation failed");
    if (!error.hasNonNull("detail")) error.put("detail", "HTTP operation failed");
    error.put("instance", taskPath);
    return error;
  }

  private static UUID httpResultCommandId(ExecutionId executionId, String operationId) {
    return UUID.nameUUIDFromBytes(
        (executionId.entityId() + "|http-result|" + operationId)
            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  private ReplyEffect<EngineEvent, WorkflowState> rejectHttpCallNotPending(
      WorkflowState state, WorkflowCommand.HttpCallCompleted command) {
    if (command.replyTo() == null) return Effect().noReply();
    return Effect()
        .reply(
            command.replyTo(),
            new WorkflowReply.Rejected(
                null,
                state.executionId(),
                state.revision(),
                state.status(),
                "http_call_not_pending",
                "The HTTP operation is not pending"));
  }

  private ReplyEffect<EngineEvent, WorkflowState> rejectPausedHttpCallCompletion(
      WorkflowState.Paused state, WorkflowCommand.HttpCallCompleted command) {
    if (command.replyTo() == null) return Effect().noReply();
    return Effect()
        .reply(
            command.replyTo(),
            new WorkflowReply.Rejected(
                null,
                state.executionId(),
                state.revision(),
                state.status(),
                "execution_paused",
                "HTTP result observation will be retried after resume"));
  }

  private ReplyEffect<EngineEvent, WorkflowState> acceptLateHttpCallCompletion(
      WorkflowState state, WorkflowCommand.HttpCallCompleted command) {
    if (command.replyTo() == null) return Effect().noReply();
    return Effect()
        .reply(
            command.replyTo(),
            new WorkflowReply.Accepted(
                httpResultCommandId(state.executionId(), command.operationId()),
                state.executionId(),
                state.revision(),
                state.status()));
  }

  private ReplyEffect<EngineEvent, WorkflowState> observeProtocolCall(
      WorkflowState state, WorkflowCommand.ProtocolCallObserved command) {
    if (!state.executionId().equals(command.executionId())) {
      return rejectProtocolCallNotPending(state, command);
    }
    UUID observationCommandId = protocolObservationCommandId(command);
    if (state.processedCommands().contains(observationCommandId)) {
      return command.replyTo() == null
          ? Effect().noReply()
          : Effect().reply(command.replyTo(), accepted(observationCommandId, state));
    }
    TaskExecutionFrame root = activeFork(state.taskStack());
    if (root != null) {
      ForkEventSelection selection =
          findForkEvent(
              root.fork(),
              EventExecutionFrame.Kind.PROTOCOL_CALL,
              command.operationId(),
              new java.util.ArrayList<>());
      if (selection != null) {
        return observeForkProtocolCall(state, command, root, selection);
      }
    }
    if (!(state instanceof WorkflowState.Waiting waiting) || state.taskStack().isEmpty()) {
      return rejectProtocolCallNotPending(state, command);
    }
    TaskExecutionFrame frame = state.taskStack().getLast();
    if (!frame.eventing()
        || frame.event().kind() != EventExecutionFrame.Kind.PROTOCOL_CALL
        || !frame.event().operationId().equals(command.operationId())) {
      return rejectProtocolCallNotPending(state, command);
    }
    if (command.failed()) {
      return failProtocolCall(state, command, frame.taskPath());
    }
    MilestoneOneProgram program = MilestoneOneProgram.compile(activePlan(state));
    if (!(program.instruction(activeNextStep(state))
        instanceof MilestoneOneProgram.ExecuteProtocolCall instruction)) {
      throw new IllegalStateException("Durable protocol cursor is not an AsyncAPI/gRPC call");
    }
    WorkflowState.Running running = running(waiting);
    ProtocolObservationDecision decision =
        protocolObservationDecision(running, instruction, frame, command);
    UUID commandId = protocolObservationCommandId(command);
    var events = new java.util.ArrayList<EngineEvent>();
    if (decision.acceptedItem() != null) {
      events.add(
          new EngineEvent.ProtocolCallItemAccepted(
              commandId,
              frame.taskPath(),
              command.operationId(),
              decision.acceptedItem(),
              command.observedAt()));
    }
    if (decision.terminal()) {
      JsonNode adapterOutput = protocolOutput(running, instruction, frame, decision.acceptedItem());
      var subscription = protocolSubscription(instruction.step());
      if (subscription != null
          && subscription.foreach()
          && adapterOutput.isArray()
          && !adapterOutput.isEmpty()) {
        events.add(
            new EngineEvent.ProtocolCallIterationStarted(
                commandId,
                frame.taskPath(),
                command.operationId(),
                frame.rawInput(),
                frame.input(),
                adapterOutput,
                subscription.itemVariable(),
                subscription.indexVariable(),
                instruction.next(),
                command.observedAt()));
      } else {
        TaskResult result =
            completeTask(
                running, instruction.step(), frame.rawInput(), frame.input(), adapterOutput);
        events.add(
            new EngineEvent.ProtocolCallCompleted(
                commandId,
                frame.taskPath(),
                command.operationId(),
                result.output(),
                result.context(),
                instruction.after(),
                command.observedAt()));
        if (instruction.after() == program.size()) {
          events.add(
              new EngineEvent.Completed(
                  commandId,
                  workflowOutput(running, result.output(), result.context()),
                  command.observedAt()));
        }
      }
    }
    if (events.isEmpty())
      return command.replyTo() == null
          ? Effect().noReply()
          : Effect().reply(command.replyTo(), accepted(commandId, state));
    var effect = Effect().persist(events).thenRun(this::continueIfRunning);
    return command.replyTo() == null
        ? effect.thenNoReply()
        : effect.thenReply(command.replyTo(), persisted -> accepted(commandId, persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> observeForkProtocolCall(
      WorkflowState state,
      WorkflowCommand.ProtocolCallObserved command,
      TaskExecutionFrame root,
      ForkEventSelection selection) {
    if (command.failed()) {
      return failForkProtocolCall(state, command, root, selection);
    }
    MilestoneOneProgram program = MilestoneOneProgram.compile(activePlan(state));
    if (!(program.instruction(selection.branch().nextStep())
        instanceof MilestoneOneProgram.ExecuteProtocolCall instruction)) {
      throw new IllegalStateException("Durable fork protocol cursor is not an AsyncAPI/gRPC call");
    }
    UUID commandId = protocolObservationCommandId(command);
    var events = new java.util.ArrayList<EngineEvent>();
    WorkflowState.Running lane = forkLaneState(state, selection.branch());
    ProtocolObservationDecision decision =
        protocolObservationDecision(lane, instruction, selection.frame(), command);
    if (decision.acceptedItem() != null) {
      events.add(
          new EngineEvent.ForkBranchProtocolCallItemAccepted(
              commandId,
              root.taskPath(),
              selection.path(),
              selection.frame().taskPath(),
              command.operationId(),
              decision.acceptedItem(),
              !forkHasRunnable(root.fork()),
              command.observedAt()));
    }
    if (decision.terminal()) {
      JsonNode adapterOutput =
          protocolOutput(lane, instruction, selection.frame(), decision.acceptedItem());
      var subscription = protocolSubscription(instruction.step());
      if (subscription != null
          && subscription.foreach()
          && adapterOutput.isArray()
          && !adapterOutput.isEmpty()) {
        ForkExecutionFrame hypothetical =
            updateForkTree(
                root.fork(),
                selection.path(),
                0,
                branch -> {
                  var stack = new java.util.ArrayList<>(branch.taskStack());
                  stack.removeLast();
                  stack.add(
                      new TaskExecutionFrame(
                          selection.frame().taskPath(),
                          selection.frame().rawInput(),
                          selection.frame().input(),
                          adapterOutput,
                          0,
                          subscription.itemVariable(),
                          subscription.indexVariable()));
                  return branch.advance(selection.frame().input(), instruction.next(), stack);
                });
        events.add(
            new EngineEvent.ForkBranchProtocolCallIterationStarted(
                commandId,
                root.taskPath(),
                selection.path(),
                selection.frame().taskPath(),
                command.operationId(),
                selection.frame().rawInput(),
                selection.frame().input(),
                adapterOutput,
                subscription.itemVariable(),
                subscription.indexVariable(),
                instruction.next(),
                !hypothetical.complete() && !forkHasRunnable(hypothetical),
                command.observedAt()));
      } else {
        TaskResult result =
            completeTask(
                lane,
                instruction.step(),
                selection.frame().rawInput(),
                selection.frame().input(),
                adapterOutput);
        ForkExecutionFrame hypothetical =
            updateForkTree(
                root.fork(),
                selection.path(),
                0,
                branch -> {
                  var stack = new java.util.ArrayList<>(branch.taskStack());
                  stack.removeLast();
                  return branch.advance(
                      result.output(), result.context(), instruction.after(), stack);
                });
        events.add(
            new EngineEvent.ForkBranchProtocolCallCompleted(
                commandId,
                root.taskPath(),
                selection.path(),
                selection.frame().taskPath(),
                command.operationId(),
                result.output(),
                result.context(),
                instruction.after(),
                !hypothetical.complete() && !forkHasRunnable(hypothetical),
                command.observedAt()));
      }
    }
    if (events.isEmpty())
      return command.replyTo() == null
          ? Effect().noReply()
          : Effect().reply(command.replyTo(), accepted(commandId, state));
    var effect = Effect().persist(events).thenRun(this::continueIfRunning);
    return command.replyTo() == null
        ? effect.thenNoReply()
        : effect.thenReply(command.replyTo(), persisted -> accepted(commandId, persisted));
  }

  private ProtocolObservationDecision protocolObservationDecision(
      WorkflowState.Running running,
      MilestoneOneProgram.ExecuteProtocolCall instruction,
      TaskExecutionFrame frame,
      WorkflowCommand.ProtocolCallObserved command) {
    JsonNode item = command.item();
    var subscription = protocolSubscription(instruction.step());
    if (subscription == null || item == null) {
      return new ProtocolObservationDecision(item, command.terminal());
    }
    RuntimeExpressionArguments expressionArguments =
        arguments(running, instruction.step(), frame.rawInput(), frame.input(), item);
    if (subscription.filter() != null
        && !expressions.evaluateCondition(
            subscription.filter(),
            item,
            expressionArguments,
            running.plan().expressions().mode())) {
      return new ProtocolObservationDecision(null, command.terminal());
    }
    int acceptedCount = frame.event().protocolItems().size() + 1;
    boolean consumptionComplete =
        switch (subscription.consumption().mode()) {
          case AMOUNT -> acceptedCount >= subscription.consumption().amount();
          case WHILE ->
              !expressions.evaluateCondition(
                  subscription.consumption().condition(),
                  item,
                  expressionArguments,
                  running.plan().expressions().mode());
          case UNTIL ->
              expressions.evaluateCondition(
                  subscription.consumption().condition(),
                  item,
                  expressionArguments,
                  running.plan().expressions().mode());
        };
    // The terminating item belongs to UNTIL and AMOUNT. A false WHILE item
    // is the boundary and is not part of the consumed sequence.
    JsonNode acceptedItem =
        subscription.consumption().mode()
                    == com.forwardmeasure.openworkflow.definition.AsyncApiSubscriptionPlan
                        .Consumption.Mode.WHILE
                && consumptionComplete
            ? null
            : item;
    return new ProtocolObservationDecision(acceptedItem, command.terminal() || consumptionComplete);
  }

  private record ProtocolObservationDecision(JsonNode acceptedItem, boolean terminal) {}

  private JsonNode protocolOutput(
      WorkflowState.Running running,
      MilestoneOneProgram.ExecuteProtocolCall instruction,
      TaskExecutionFrame frame,
      JsonNode terminalItem) {
    var items = new java.util.ArrayList<>(frame.event().protocolItems());
    if (terminalItem != null) items.add(terminalItem);
    RuntimeExpressionArguments expressionArguments =
        arguments(running, instruction.step(), frame.rawInput(), frame.input(), null);
    JsonNode evaluated =
        evaluateProtocolArguments(running, instruction.step(), frame.input(), expressionArguments);
    ProtocolOperationDescriptor descriptor =
        ProtocolOperationMaterializer.materialize(
            running.plan(),
            instruction.step(),
            evaluated,
            frame.event().operationId(),
            authenticationContext(expressionArguments));
    boolean streaming =
        switch (descriptor.mode()) {
          case SUBSCRIBE, GRPC_SERVER_STREAM, GRPC_CLIENT_STREAM, GRPC_BIDI_STREAM, RPC_STREAM ->
              true;
          default -> false;
        };
    if (!streaming) {
      return items.isEmpty() ? JsonNodeFactory.instance.nullNode() : items.getLast();
    }
    var output = JsonNodeFactory.instance.arrayNode();
    items.forEach(output::add);
    return output;
  }

  private ReplyEffect<EngineEvent, WorkflowState> failProtocolCall(
      WorkflowState state, WorkflowCommand.ProtocolCallObserved command, String taskPath) {
    JsonNode error = protocolError(command);
    WorkflowState.Running runningState = running((WorkflowState.Waiting) state);
    var events = new java.util.ArrayList<EngineEvent>();
    UUID commandId = protocolObservationCommandId(command);
    events.add(new EngineEvent.ErrorRaised(commandId, taskPath, error, command.observedAt()));
    ErrorTarget target = matchingCatch(runningState, error);
    if (target == null) {
      events.add(
          new EngineEvent.Failed(
              commandId,
              error.path("detail").asText("Protocol operation failed"),
              command.observedAt()));
    } else {
      RetryDecision retry = retryDecision(runningState, target, error, command.observedAt());
      if (retry == null)
        events.add(
            new EngineEvent.ErrorCaught(
                commandId,
                target.frame().taskPath(),
                error,
                target.instruction().catchEntry(),
                command.observedAt()));
      else
        events.add(
            new EngineEvent.RetryScheduled(
                commandId,
                target.frame().taskPath(),
                error,
                target.frame().attempt() + 1,
                target.instruction().next(),
                retry.deadline(),
                target.frame().retryStartedAt(),
                command.observedAt()));
    }
    var effect =
        Effect().persist(events).thenRun(this::scheduleDeadlines).thenRun(this::continueIfRunning);
    return command.replyTo() == null
        ? effect.thenNoReply()
        : effect.thenReply(command.replyTo(), persisted -> accepted(commandId, persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> failForkProtocolCall(
      WorkflowState state,
      WorkflowCommand.ProtocolCallObserved command,
      TaskExecutionFrame root,
      ForkEventSelection selection) {
    JsonNode error = protocolError(command);
    WorkflowState.Running lane = forkLaneState(state, selection.branch());
    var events = new java.util.ArrayList<EngineEvent>();
    UUID commandId = protocolObservationCommandId(command);
    events.add(
        new EngineEvent.ErrorRaised(
            commandId, selection.frame().taskPath(), error, command.observedAt()));
    ErrorTarget target = matchingCatch(lane, error);
    if (target == null) {
      events.add(
          new EngineEvent.Failed(
              commandId,
              error.path("detail").asText("Protocol operation failed"),
              command.observedAt()));
    } else {
      RetryDecision retry = retryDecision(lane, target, error, command.observedAt());
      if (retry == null)
        events.add(
            new EngineEvent.ForkBranchErrorCaught(
                commandId,
                root.taskPath(),
                selection.path(),
                target.frame().taskPath(),
                error,
                target.instruction().catchEntry(),
                command.observedAt()));
      else {
        var stack = stackThroughTry(selection.branch().taskStack(), target.frame().taskPath());
        stack.set(
            stack.size() - 1,
            target
                .frame()
                .retrying(
                    error,
                    target.frame().attempt() + 1,
                    retry.deadline(),
                    target.frame().retryStartedAt(),
                    command.observedAt()));
        ForkExecutionFrame hypothetical =
            updateForkTree(
                root.fork(),
                selection.path(),
                0,
                branch ->
                    branch.advance(target.frame().input(), target.instruction().next(), stack));
        events.add(
            new EngineEvent.ForkBranchRetryScheduled(
                commandId,
                root.taskPath(),
                selection.path(),
                target.frame().taskPath(),
                error,
                target.frame().attempt() + 1,
                target.instruction().next(),
                retry.deadline(),
                target.frame().retryStartedAt(),
                !forkHasRunnable(hypothetical),
                command.observedAt()));
      }
    }
    var effect =
        Effect().persist(events).thenRun(this::scheduleDeadlines).thenRun(this::continueIfRunning);
    return command.replyTo() == null
        ? effect.thenNoReply()
        : effect.thenReply(command.replyTo(), persisted -> accepted(commandId, persisted));
  }

  private static JsonNode protocolError(WorkflowCommand.ProtocolCallObserved command) {
    if (!command.error().isObject())
      throw new IllegalArgumentException("Protocol adapter error must be an RFC 9457 object");
    ObjectNode error = (ObjectNode) command.error().deepCopy();
    if (!error.hasNonNull("type")) error.put("type", "urn:openworkflow:protocol:error");
    if (!error.hasNonNull("status")) error.put("status", 500);
    if (!error.hasNonNull("title")) error.put("title", "Protocol operation failed");
    if (!error.hasNonNull("detail")) error.put("detail", "Protocol operation failed");
    error.putIfAbsent(
        "instance",
        JsonNodeFactory.instance.textNode("urn:openworkflow:operation:" + command.operationId()));
    return error;
  }

  private static UUID protocolObservationCommandId(WorkflowCommand.ProtocolCallObserved command) {
    return UUID.nameUUIDFromBytes(
        (command.executionId().entityId()
                + "|protocol-observation|"
                + command.operationId()
                + "|"
                + command.observationId())
            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  private ReplyEffect<EngineEvent, WorkflowState> rejectProtocolCallNotPending(
      WorkflowState state, WorkflowCommand.ProtocolCallObserved command) {
    if (command.replyTo() == null) return Effect().noReply();
    return Effect()
        .reply(
            command.replyTo(),
            new WorkflowReply.Rejected(
                null,
                state.executionId(),
                state.revision(),
                state.status(),
                "protocol_call_not_pending",
                "The AsyncAPI/gRPC operation is not pending"));
  }

  private ReplyEffect<EngineEvent, WorkflowState> rejectPausedProtocolObservation(
      WorkflowState.Paused state, WorkflowCommand.ProtocolCallObserved command) {
    if (command.replyTo() == null) return Effect().noReply();
    return Effect()
        .reply(
            command.replyTo(),
            new WorkflowReply.Rejected(
                null,
                state.executionId(),
                state.revision(),
                state.status(),
                "execution_paused",
                "Protocol observation will be retried after resume"));
  }

  private ReplyEffect<EngineEvent, WorkflowState> acceptLateProtocolObservation(
      WorkflowState state, WorkflowCommand.ProtocolCallObserved command) {
    if (command.replyTo() == null) return Effect().noReply();
    UUID commandId =
        UUID.nameUUIDFromBytes(
            (state.executionId().entityId()
                    + "|protocol-observation|"
                    + command.operationId()
                    + "|"
                    + command.observedAt())
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    return Effect()
        .reply(
            command.replyTo(),
            new WorkflowReply.Accepted(
                commandId, state.executionId(), state.revision(), state.status()));
  }

  private ReplyEffect<EngineEvent, WorkflowState> failSubworkflow(
      WorkflowState state, WorkflowCommand.SubworkflowCompleted command, String taskPath) {
    String message =
        command.failure() == null || command.failure().isBlank()
            ? "Child workflow "
                + command.childExecutionId().value()
                + " ended "
                + command.childStatus()
            : command.failure();
    ObjectNode error = subworkflowError(command, message);
    WorkflowState.Running runningState =
        state instanceof WorkflowState.Running running
            ? running
            : running((WorkflowState.Waiting) state);
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.ErrorRaised(command.commandId(), taskPath, error, command.observedAt()));
    ErrorTarget target = matchingCatch(runningState, error);
    if (target == null) {
      events.add(new EngineEvent.Failed(command.commandId(), message, command.observedAt()));
    } else {
      RetryDecision retry = retryDecision(runningState, target, error, command.observedAt());
      if (retry == null) {
        events.add(
            new EngineEvent.ErrorCaught(
                command.commandId(),
                target.frame().taskPath(),
                error,
                target.instruction().catchEntry(),
                command.observedAt()));
      } else {
        events.add(
            new EngineEvent.RetryScheduled(
                command.commandId(),
                target.frame().taskPath(),
                error,
                target.frame().attempt() + 1,
                target.instruction().next(),
                retry.deadline(),
                target.frame().retryStartedAt(),
                command.observedAt()));
      }
    }
    var effect =
        Effect().persist(events).thenRun(this::scheduleDeadlines).thenRun(this::continueIfRunning);
    return command.replyTo() == null
        ? effect.thenNoReply()
        : effect.thenReply(
            command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> completeForkSubworkflow(
      WorkflowState state,
      WorkflowCommand.SubworkflowCompleted command,
      TaskExecutionFrame root,
      ForkEventSelection selection) {
    if (!command.childExecutionId().value().toString().equals(command.operationId())) {
      return rejectSubworkflowNotPending(state, command);
    }
    if (command.childStatus()
        != com.forwardmeasure.openworkflow.engine.api.ExecutionStatus.COMPLETED) {
      return failForkSubworkflow(state, command, root, selection);
    }
    MilestoneOneProgram program = MilestoneOneProgram.compile(activePlan(state));
    if (!(program.instruction(selection.branch().nextStep())
        instanceof MilestoneOneProgram.ExecuteSubworkflow instruction)) {
      throw new IllegalStateException("Durable fork subworkflow cursor is not run.workflow");
    }
    WorkflowState.Running lane = forkLaneState(state, selection.branch());
    TaskResult result =
        completeTask(
            lane,
            instruction.step(),
            selection.frame().rawInput(),
            selection.frame().input(),
            command.output());
    ForkExecutionFrame hypothetical =
        updateForkTree(
            root.fork(),
            selection.path(),
            0,
            branch -> {
              var stack = new java.util.ArrayList<>(branch.taskStack());
              if (stack.isEmpty()
                  || !stack.getLast().eventing()
                  || stack.getLast().event().kind() != EventExecutionFrame.Kind.SUBWORKFLOW
                  || !stack.getLast().event().operationId().equals(command.operationId())) {
                throw new IllegalStateException(
                    "Fork child completion has no matching durable frame");
              }
              stack.removeLast();
              return branch.advance(result.output(), result.context(), instruction.next(), stack);
            });
    var effect =
        Effect()
            .persist(
                new EngineEvent.ForkBranchSubworkflowCompleted(
                    command.commandId(),
                    root.taskPath(),
                    selection.path(),
                    instruction.step().path(),
                    command.operationId(),
                    command.childExecutionId(),
                    command.childStatus(),
                    result.output(),
                    result.context(),
                    instruction.next(),
                    !hypothetical.complete() && !forkHasRunnable(hypothetical),
                    command.observedAt()))
            .thenRun(this::continueIfRunning);
    return command.replyTo() == null
        ? effect.thenNoReply()
        : effect.thenReply(
            command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> failForkSubworkflow(
      WorkflowState state,
      WorkflowCommand.SubworkflowCompleted command,
      TaskExecutionFrame root,
      ForkEventSelection selection) {
    String message =
        command.failure() == null || command.failure().isBlank()
            ? "Child workflow "
                + command.childExecutionId().value()
                + " ended "
                + command.childStatus()
            : command.failure();
    ObjectNode error = subworkflowError(command, message);
    WorkflowState.Running lane = forkLaneState(state, selection.branch());
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.ErrorRaised(
            command.commandId(), selection.frame().taskPath(), error, command.observedAt()));
    ErrorTarget target = matchingCatch(lane, error);
    if (target == null) {
      events.add(new EngineEvent.Failed(command.commandId(), message, command.observedAt()));
    } else {
      RetryDecision retry = retryDecision(lane, target, error, command.observedAt());
      if (retry == null) {
        events.add(
            new EngineEvent.ForkBranchErrorCaught(
                command.commandId(),
                root.taskPath(),
                selection.path(),
                target.frame().taskPath(),
                error,
                target.instruction().catchEntry(),
                command.observedAt()));
      } else {
        var stack = stackThroughTry(selection.branch().taskStack(), target.frame().taskPath());
        stack.set(
            stack.size() - 1,
            target
                .frame()
                .retrying(
                    error,
                    target.frame().attempt() + 1,
                    retry.deadline(),
                    target.frame().retryStartedAt(),
                    command.observedAt()));
        ForkExecutionFrame hypothetical =
            updateForkTree(
                root.fork(),
                selection.path(),
                0,
                branch ->
                    branch.advance(target.frame().input(), target.instruction().next(), stack));
        events.add(
            new EngineEvent.ForkBranchRetryScheduled(
                command.commandId(),
                root.taskPath(),
                selection.path(),
                target.frame().taskPath(),
                error,
                target.frame().attempt() + 1,
                target.instruction().next(),
                retry.deadline(),
                target.frame().retryStartedAt(),
                !forkHasRunnable(hypothetical),
                command.observedAt()));
      }
    }
    var effect =
        Effect().persist(events).thenRun(this::scheduleDeadlines).thenRun(this::continueIfRunning);
    return command.replyTo() == null
        ? effect.thenNoReply()
        : effect.thenReply(
            command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private static ObjectNode subworkflowError(
      WorkflowCommand.SubworkflowCompleted command, String message) {
    return JsonNodeFactory.instance
        .objectNode()
        .put(
            "type",
            "urn:openworkflow:subworkflow:"
                + command.childStatus().name().toLowerCase(java.util.Locale.ROOT))
        .put("status", 500)
        .put("title", "Child workflow " + command.childStatus())
        .put("detail", message)
        .put("instance", command.childExecutionId().value().toString());
  }

  private ReplyEffect<EngineEvent, WorkflowState> rejectPausedSubworkflowCompletion(
      WorkflowState.Paused state, WorkflowCommand.SubworkflowCompleted command) {
    if (command.replyTo() == null) return Effect().noReply();
    return Effect()
        .reply(
            command.replyTo(),
            new WorkflowReply.Rejected(
                command.commandId(),
                state.executionId(),
                state.revision(),
                state.status(),
                "execution_paused",
                "Child completion will be retried after the parent resumes"));
  }

  private ReplyEffect<EngineEvent, WorkflowState> acceptLateSubworkflowCompletion(
      WorkflowState state, WorkflowCommand.SubworkflowCompleted command) {
    if (command.replyTo() == null) return Effect().noReply();
    return Effect().reply(command.replyTo(), accepted(command.commandId(), state));
  }

  private ReplyEffect<EngineEvent, WorkflowState> rejectSubworkflowNotPending(
      WorkflowState state, WorkflowCommand.SubworkflowCompleted command) {
    if (command.replyTo() == null) return Effect().noReply();
    return Effect()
        .reply(
            command.replyTo(),
            new WorkflowReply.Rejected(
                command.commandId(),
                state.executionId(),
                state.revision(),
                state.status(),
                "subworkflow_not_pending",
                "The child workflow is not pending in this execution state"));
  }

  private static WorkflowCloudEvent cloudEvent(
      JsonNode properties, String defaultId, Instant defaultTime) {
    String id = properties.path("id").asText(defaultId);
    URI source = URI.create(properties.required("source").textValue());
    String type = properties.required("type").textValue();
    String subject = properties.path("subject").asText(null);
    Instant time =
        properties.hasNonNull("time")
            ? Instant.parse(properties.required("time").textValue())
            : defaultTime;
    String contentType = properties.path("datacontenttype").asText("application/json");
    JsonNode data = properties.path("data");
    var extensions = new LinkedHashMap<String, JsonNode>();
    if (properties.hasNonNull("dataschema")) {
      extensions.put("dataschema", properties.required("dataschema"));
    }
    properties
        .properties()
        .iterator()
        .forEachRemaining(
            entry -> {
              if (!Set.of(
                      "specversion",
                      "id",
                      "source",
                      "type",
                      "subject",
                      "time",
                      "datacontenttype",
                      "dataschema",
                      "data")
                  .contains(entry.getKey())) {
                extensions.put(entry.getKey(), entry.getValue());
              }
            });
    return new WorkflowCloudEvent(
        properties.path("specversion").asText("1.0"),
        id,
        source,
        type,
        subject,
        time,
        contentType,
        data,
        extensions);
  }

  private ReplyEffect<EngineEvent, WorkflowState> acknowledgeEmit(
      WorkflowState.Waiting state, WorkflowCommand.EffectAcknowledged command) {
    if (!state.executionId().equals(command.executionId()) || state.taskStack().isEmpty())
      return rejectEffectAck(state, command);
    TaskExecutionFrame frame = state.taskStack().getLast();
    if (!frame.eventing()
        || frame.event().kind() != EventExecutionFrame.Kind.EMIT
        || !frame.event().operationId().equals(command.operationId())) {
      return rejectEffectAck(state, command);
    }
    MilestoneOneProgram program = MilestoneOneProgram.compile(state.plan());
    if (!(program.instruction(state.nextStep())
        instanceof MilestoneOneProgram.ExecuteEmit instruction)) {
      throw new IllegalStateException("Durable emit cursor is not emit");
    }
    WorkflowState.Running running = running(state);
    JsonNode emitted = CloudEventConsumptionEvaluator.envelope(frame.event().emitted());
    TaskResult result =
        completeTask(running, instruction.step(), frame.rawInput(), frame.input(), emitted);
    UUID commandId =
        UUID.nameUUIDFromBytes(
            (state.executionId().entityId() + "|emit-ack|" + command.operationId())
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.EmitAcknowledged(
            commandId,
            frame.taskPath(),
            command.operationId(),
            result.output(),
            result.context(),
            instruction.next(),
            command.acknowledgedAt()));
    if (instruction.next() == program.size()) {
      events.add(
          new EngineEvent.Completed(
              commandId,
              workflowOutput(running, result.output(), result.context()),
              command.acknowledgedAt()));
    }
    var effect = Effect().persist(events).thenRun(this::continueIfRunning);
    return command.replyTo() == null
        ? effect.thenNoReply()
        : effect.thenReply(
            command.replyTo(),
            persisted ->
                new WorkflowReply.Accepted(
                    commandId, persisted.executionId(), persisted.revision(), persisted.status()));
  }

  private ReplyEffect<EngineEvent, WorkflowState> acknowledgeEffect(
      WorkflowState state, WorkflowCommand.EffectAcknowledged command) {
    if (state instanceof WorkflowState.Waiting waiting && !state.taskStack().isEmpty()) {
      TaskExecutionFrame global = state.taskStack().getLast();
      if (global.eventing() && global.event().kind() == EventExecutionFrame.Kind.EMIT) {
        return acknowledgeEmit(waiting, command);
      }
    }
    if (!state.executionId().equals(command.executionId())) {
      return rejectEffectAck(state, command);
    }
    TaskExecutionFrame root = activeFork(state.taskStack());
    if (root == null) return acknowledgeAlreadyObserved(state, command);
    ForkEventSelection selection =
        findForkEvent(
            root.fork(),
            EventExecutionFrame.Kind.EMIT,
            command.operationId(),
            new java.util.ArrayList<>());
    if (selection == null) return acknowledgeAlreadyObserved(state, command);
    MilestoneOneProgram program = MilestoneOneProgram.compile(activePlan(state));
    if (!(program.instruction(selection.branch().nextStep())
        instanceof MilestoneOneProgram.ExecuteEmit instruction)) {
      throw new IllegalStateException("Durable fork emit cursor is not emit");
    }
    WorkflowState.Running lane = forkLaneState(state, selection.branch());
    JsonNode emitted = CloudEventConsumptionEvaluator.envelope(selection.frame().event().emitted());
    TaskResult result =
        completeTask(
            lane,
            instruction.step(),
            selection.frame().rawInput(),
            selection.frame().input(),
            emitted);
    ForkExecutionFrame hypothetical =
        updateForkTree(
            root.fork(),
            selection.path(),
            0,
            branch -> {
              var stack = new java.util.ArrayList<>(branch.taskStack());
              stack.removeLast();
              return branch.advance(result.output(), result.context(), instruction.next(), stack);
            });
    UUID commandId =
        UUID.nameUUIDFromBytes(
            (state.executionId().entityId() + "|fork-emit-ack|" + command.operationId())
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    var effect =
        Effect()
            .persist(
                new EngineEvent.ForkBranchEmitAcknowledged(
                    commandId,
                    root.taskPath(),
                    selection.path(),
                    instruction.step().path(),
                    command.operationId(),
                    result.output(),
                    result.context(),
                    instruction.next(),
                    !hypothetical.complete() && !forkHasRunnable(hypothetical),
                    command.acknowledgedAt()))
            .thenRun(this::continueIfRunning);
    return command.replyTo() == null
        ? effect.thenNoReply()
        : effect.thenReply(command.replyTo(), persisted -> accepted(commandId, persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> rejectEffectAck(
      WorkflowState state, WorkflowCommand.EffectAcknowledged command) {
    if (command.replyTo() == null) return Effect().noReply();
    return Effect()
        .reply(
            command.replyTo(),
            new WorkflowReply.Rejected(
                null,
                state.executionId(),
                state.revision(),
                state.status(),
                "effect_not_pending",
                "The effect is not pending in this execution state"));
  }

  private ReplyEffect<EngineEvent, WorkflowState> rejectPausedEffectAcknowledgement(
      WorkflowState.Paused state, WorkflowCommand.EffectAcknowledged command) {
    if (command.replyTo() == null) return Effect().noReply();
    return Effect()
        .reply(
            command.replyTo(),
            new WorkflowReply.Rejected(
                null,
                state.executionId(),
                state.revision(),
                state.status(),
                "execution_paused",
                "Effect observation will be retried after resume"));
  }

  private ReplyEffect<EngineEvent, WorkflowState> acknowledgeAlreadyObserved(
      WorkflowState state, WorkflowCommand.EffectAcknowledged command) {
    if (command.replyTo() == null) return Effect().noReply();
    return Effect()
        .reply(
            command.replyTo(),
            new WorkflowReply.Accepted(
                UUID.nameUUIDFromBytes(
                    (state.executionId().entityId() + "|effect-observed|" + command.operationId())
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                state.executionId(),
                state.revision(),
                state.status()));
  }

  private ReplyEffect<EngineEvent, WorkflowState> executeListen(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.ExecuteListen instruction) {
    PlanStep step = instruction.step();
    JsonNode rawInput = state.data();
    boolean execute = condition(state, step, rawInput);
    JsonNode input = execute ? taskInput(state, step, rawInput) : rawInput;
    if (!execute) {
      TaskResult skipped = completeTask(state, step, rawInput, input, rawInput);
      return Effect()
          .persist(taskEvents(state, command, step, rawInput, input, instruction.next(), skipped))
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    String operationId =
        UUID.nameUUIDFromBytes(
                (state.executionId().entityId() + "|listen|" + step.path() + "|" + state.revision())
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .toString();
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.ListenStarted(
            command.commandId(),
            step.path(),
            rawInput,
            input,
            state.nextStep(),
            operationId,
            EventTypeSelector.literalTypes(step.listenPlan().consumption()),
            command.requestedAt()));
    appendTaskDeadline(state, command, step, rawInput, input, events);
    return Effect()
        .persist(events)
        .thenRun(this::scheduleDeadlines)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> receiveCloudEvent(
      WorkflowState.Waiting state, WorkflowCommand.CloudEventReceived command) {
    if (!state.executionId().equals(command.executionId()) || state.taskStack().isEmpty()) {
      return rejectCloudEvent(
          command, state, "not_listening", "Execution is not waiting for a CloudEvent");
    }
    TaskExecutionFrame frame = state.taskStack().getLast();
    if (!frame.eventing() || frame.event().kind() != EventExecutionFrame.Kind.LISTEN) {
      return rejectCloudEvent(
          command, state, "not_listening", "Execution is not waiting for a listen task");
    }
    MilestoneOneProgram program = MilestoneOneProgram.compile(state.plan());
    if (!(program.instruction(state.nextStep())
        instanceof MilestoneOneProgram.ExecuteListen instruction)) {
      throw new IllegalStateException("Durable listen cursor is not listen");
    }
    WorkflowState.Running running = running(state);
    ListenPlan listen = instruction.step().listenPlan();
    var offered =
        eventConsumption.offer(
            listen.consumption(),
            listen.readAs(),
            new EventConsumptionWindow(
                frame.event().accepted(), frame.event().correlations(),
                frame.event().matchedFilters(), frame.event().untilWindow()),
            command.event(),
            frame.input(),
            arguments(running, instruction.step(), frame.rawInput(), frame.input(), null),
            state.plan().expressions().mode());
    if (offered == null) return acceptCloudEvent(command, state);
    if (offered.terminatingEvent()) {
      JsonNode rawOutput =
          CloudEventConsumptionEvaluator.read(offered.window().accepted(), listen.readAs());
      if (listen.foreach()) {
        UUID commandId = listenEventCommandId(state.executionId(), frame, command.event());
        return beginListenIteration(state, command, instruction, frame, rawOutput, commandId);
      }
      TaskResult result =
          completeTask(running, instruction.step(), frame.rawInput(), frame.input(), rawOutput);
      UUID commandId = listenEventCommandId(state.executionId(), frame, command.event());
      var events = new java.util.ArrayList<EngineEvent>();
      events.add(
          new EngineEvent.ListenEventAccepted(
              commandId,
              frame.taskPath(),
              frame.event().operationId(),
              command.event(),
              frame.event().accepted(),
              frame.event().correlations(),
              frame.event().matchedFilters(),
              true,
              result.output(),
              result.context(),
              instruction.after(),
              command.receivedAt()));
      if (instruction.after() == program.size()) {
        events.add(
            new EngineEvent.Completed(
                commandId,
                workflowOutput(running, result.output(), result.context()),
                command.receivedAt()));
      }
      var effect = Effect().persist(events).thenRun(this::continueIfRunning);
      return command.replyTo() == null
          ? effect.thenNoReply()
          : effect.thenReply(
              command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    if (offered.untilProgress()) {
      UUID commandId = listenEventCommandId(state.executionId(), frame, command.event());
      var effect =
          Effect()
              .persist(
                  new EngineEvent.ListenUntilAdvanced(
                      commandId,
                      frame.taskPath(),
                      frame.event().operationId(),
                      command.event(),
                      offered.window().untilWindow(),
                      command.receivedAt()));
      return command.replyTo() == null
          ? effect.thenNoReply()
          : effect.thenReply(
              command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    EventExecutionFrame accepted =
        new EventExecutionFrame(
            EventExecutionFrame.Kind.LISTEN,
            frame.event().operationId(),
            null,
            offered.window().accepted(),
            offered.window().correlations(),
            offered.window().matchedFilters(),
            offered.window().untilWindow());
    boolean complete = offered.complete();
    JsonNode output = null;
    JsonNode context = null;
    if (complete && !listen.foreach()) {
      TaskResult result =
          completeTask(
              running,
              instruction.step(),
              frame.rawInput(),
              frame.input(),
              CloudEventConsumptionEvaluator.read(accepted.accepted(), listen.readAs()));
      output = result.output();
      context = result.context();
    }
    UUID commandId = listenEventCommandId(state.executionId(), frame, command.event());
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.ListenEventAccepted(
            commandId,
            frame.taskPath(),
            frame.event().operationId(),
            command.event(),
            accepted.accepted(),
            accepted.correlations(),
            accepted.matchedFilters(),
            complete && !listen.foreach(),
            output,
            context,
            complete && !listen.foreach() ? instruction.after() : state.nextStep(),
            command.receivedAt()));
    if (complete && listen.foreach()) {
      events.add(
          new EngineEvent.ListenIterationStarted(
              commandId,
              frame.taskPath(),
              frame.rawInput(),
              frame.input(),
              CloudEventConsumptionEvaluator.read(accepted.accepted(), listen.readAs()),
              listen.itemVariable(),
              listen.indexVariable(),
              instruction.next(),
              command.receivedAt()));
    }
    if (complete && !listen.foreach() && instruction.after() == program.size()) {
      events.add(
          new EngineEvent.Completed(
              commandId, workflowOutput(running, output, context), command.receivedAt()));
    }
    var effect = Effect().persist(events).thenRun(this::continueIfRunning);
    return command.replyTo() == null
        ? effect.thenNoReply()
        : effect.thenReply(
            command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> receiveCloudEventAny(
      WorkflowState state, WorkflowCommand.CloudEventReceived command) {
    if (!state.executionId().equals(command.executionId())) {
      return rejectCloudEvent(
          command,
          state,
          "wrong_execution",
          "CloudEvent was routed to another tenant-qualified execution");
    }
    if (state instanceof WorkflowState.Waiting waiting && !state.taskStack().isEmpty()) {
      TaskExecutionFrame global = state.taskStack().getLast();
      if (global.eventing() && global.event().kind() == EventExecutionFrame.Kind.LISTEN) {
        return receiveCloudEvent(waiting, command);
      }
    }
    TaskExecutionFrame root = activeFork(state.taskStack());
    if (root == null) return ignoreCloudEvent(state, command);
    MilestoneOneProgram program = MilestoneOneProgram.compile(activePlan(state));
    var selections = new java.util.ArrayList<ForkEventSelection>();
    collectForkEvents(
        root.fork(), EventExecutionFrame.Kind.LISTEN, new java.util.ArrayList<>(), selections);
    if (selections.isEmpty()) return ignoreCloudEvent(state, command);

    var updates = new java.util.ArrayList<EngineEvent.ForkListenUpdate>();
    ForkExecutionFrame hypothetical = root.fork();
    for (ForkEventSelection selection : selections) {
      if (!(program.instruction(selection.branch().nextStep())
          instanceof MilestoneOneProgram.ExecuteListen instruction)) {
        throw new IllegalStateException("Durable fork listen cursor is not listen");
      }
      WorkflowState.Running lane = forkLaneState(state, selection.branch());
      TaskExecutionFrame frame = selection.frame();
      ListenPlan listen = instruction.step().listenPlan();
      var offered =
          eventConsumption.offer(
              listen.consumption(),
              listen.readAs(),
              new EventConsumptionWindow(
                  frame.event().accepted(), frame.event().correlations(),
                  frame.event().matchedFilters(), frame.event().untilWindow()),
              command.event(),
              frame.input(),
              arguments(lane, instruction.step(), frame.rawInput(), frame.input(), null),
              activePlan(state).expressions().mode());
      if (offered == null) continue;
      EngineEvent.ForkListenDisposition disposition = EngineEvent.ForkListenDisposition.PARTIAL;
      JsonNode output = null;
      JsonNode context = null;
      JsonNode collection = null;
      int nextStep = selection.branch().nextStep();
      if (offered.complete() || offered.terminatingEvent()) {
        JsonNode read =
            CloudEventConsumptionEvaluator.read(offered.window().accepted(), listen.readAs());
        if (listen.foreach()) {
          disposition = EngineEvent.ForkListenDisposition.ITERATE;
          collection = read;
          nextStep = instruction.next();
        } else {
          disposition = EngineEvent.ForkListenDisposition.COMPLETE;
          TaskResult result =
              completeTask(lane, instruction.step(), frame.rawInput(), frame.input(), read);
          output = result.output();
          context = result.context();
          nextStep = instruction.after();
        }
      }
      var update =
          new EngineEvent.ForkListenUpdate(
              selection.path(),
              frame.taskPath(),
              frame.event().operationId(),
              offered.window().accepted(),
              offered.window().correlations(),
              offered.window().matchedFilters(),
              offered.window().untilWindow(),
              disposition,
              output,
              context,
              nextStep,
              collection,
              disposition == EngineEvent.ForkListenDisposition.ITERATE
                  ? listen.itemVariable()
                  : null,
              disposition == EngineEvent.ForkListenDisposition.ITERATE
                  ? listen.indexVariable()
                  : null);
      updates.add(update);
      hypothetical = applyForkListenUpdate(hypothetical, update);
    }
    if (updates.isEmpty()) return acceptCloudEvent(command, state);
    UUID commandId =
        UUID.nameUUIDFromBytes(
            (state.executionId().entityId()
                    + "|fork-listen-event|"
                    + root.taskPath()
                    + "|"
                    + command.event().source()
                    + "|"
                    + command.event().id())
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    var effect =
        Effect()
            .persist(
                new EngineEvent.ForkBranchListenAccepted(
                    commandId,
                    root.taskPath(),
                    command.event(),
                    updates,
                    hasActiveForkListeners(hypothetical),
                    !hypothetical.complete() && !forkHasRunnable(hypothetical),
                    command.receivedAt()))
            .thenRun(this::continueIfRunning);
    return command.replyTo() == null
        ? effect.thenNoReply()
        : effect.thenReply(
            command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> beginListenIteration(
      WorkflowState.Waiting state,
      WorkflowCommand.CloudEventReceived command,
      MilestoneOneProgram.ExecuteListen instruction,
      TaskExecutionFrame frame,
      JsonNode collection,
      UUID commandId) {
    ListenPlan listen = instruction.step().listenPlan();
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.ListenEventAccepted(
            commandId,
            frame.taskPath(),
            frame.event().operationId(),
            command.event(),
            frame.event().accepted(),
            frame.event().correlations(),
            frame.event().matchedFilters(),
            false,
            null,
            null,
            state.nextStep(),
            command.receivedAt()));
    events.add(
        new EngineEvent.ListenIterationStarted(
            commandId,
            frame.taskPath(),
            frame.rawInput(),
            frame.input(),
            collection,
            listen.itemVariable(),
            listen.indexVariable(),
            instruction.next(),
            command.receivedAt()));
    var effect = Effect().persist(events).thenRun(this::continueIfRunning);
    return command.replyTo() == null
        ? effect.thenNoReply()
        : effect.thenReply(
            command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> ignoreCloudEvent(
      WorkflowState state, WorkflowCommand.CloudEventReceived command) {
    if (command.replyTo() == null) return Effect().noReply();
    if (!state.executionId().equals(command.executionId())) {
      return rejectCloudEvent(
          command,
          state,
          "wrong_execution",
          "CloudEvent was routed to another tenant-qualified execution");
    }
    if (state.status() == com.forwardmeasure.openworkflow.engine.api.ExecutionStatus.CANCELLED
        || state.status() == com.forwardmeasure.openworkflow.engine.api.ExecutionStatus.COMPLETED
        || state.status() == com.forwardmeasure.openworkflow.engine.api.ExecutionStatus.FAILED) {
      return acceptCloudEvent(command, state);
    }
    String code =
        state.status() == com.forwardmeasure.openworkflow.engine.api.ExecutionStatus.PAUSED
            ? "execution_paused"
            : "not_listening";
    String message =
        state.status() == com.forwardmeasure.openworkflow.engine.api.ExecutionStatus.PAUSED
            ? "Execution is paused; delivery may be retried after resume"
            : "Execution is not currently waiting for a CloudEvent";
    return rejectCloudEvent(command, state, code, message);
  }

  private ReplyEffect<EngineEvent, WorkflowState> acceptCloudEvent(
      WorkflowCommand.CloudEventReceived command, WorkflowState state) {
    return command.replyTo() == null
        ? Effect().noReply()
        : Effect().reply(command.replyTo(), accepted(command.commandId(), state));
  }

  private ReplyEffect<EngineEvent, WorkflowState> rejectCloudEvent(
      WorkflowCommand.CloudEventReceived command,
      WorkflowState state,
      String code,
      String message) {
    return command.replyTo() == null
        ? Effect().noReply()
        : Effect()
            .reply(
                command.replyTo(),
                new WorkflowReply.Rejected(
                    command.commandId(),
                    state.executionId(),
                    state.revision(),
                    state.status(),
                    code,
                    message));
  }

  private ReplyEffect<EngineEvent, WorkflowState> exitListen(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.ExitListen instruction) {
    TaskExecutionFrame frame = activeIteration(state, instruction.step());
    TaskResult item =
        completeDataFlow(
            state,
            instruction.step(),
            instruction.step().listenPlan().iteratorDataFlow(),
            frame.rawInput(),
            frame.input(),
            state.data());
    var collection =
        ((com.fasterxml.jackson.databind.node.ArrayNode) frame.collection().deepCopy());
    collection.set(frame.iterationIndex(), item.output());
    int nextIndex = frame.iterationIndex() + 1;
    if (nextIndex < collection.size()) {
      return Effect()
          .persist(
              new EngineEvent.ListenIterationAdvanced(
                  command.commandId(),
                  instruction.step().path(),
                  collection,
                  nextIndex,
                  frame.input(),
                  item.context(),
                  instruction.body(),
                  command.requestedAt()))
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    WorkflowState.Running withIteratorContext =
        new WorkflowState.Running(
            state.executionId(),
            state.plan(),
            state.data(),
            state.nextStep(),
            state.revision(),
            state.processedCommands(),
            item.context(),
            state.rawWorkflowInput(),
            state.taskStack(),
            state.workflowDeadline());
    TaskResult result =
        completeTask(
            withIteratorContext, instruction.step(), frame.rawInput(), frame.input(), collection);
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.TaskCompleted(
            command.commandId(),
            instruction.step().path(),
            instruction.next(),
            result.output(),
            result.context(),
            command.requestedAt()));
    appendCompletionIfTerminal(state, command, instruction.next(), result, events);
    return Effect()
        .persist(events)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> exitProtocolCall(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.ExitProtocolCall instruction) {
    TaskExecutionFrame frame = activeIteration(state, instruction.step());
    TaskResult item =
        completeDataFlow(
            state,
            instruction.step(),
            protocolSubscription(instruction.step()).iteratorDataFlow(),
            frame.rawInput(),
            frame.input(),
            state.data());
    var collection = (com.fasterxml.jackson.databind.node.ArrayNode) frame.collection().deepCopy();
    collection.set(frame.iterationIndex(), item.output());
    int nextIndex = frame.iterationIndex() + 1;
    if (nextIndex < collection.size()) {
      return Effect()
          .persist(
              new EngineEvent.ProtocolCallIterationAdvanced(
                  command.commandId(),
                  instruction.step().path(),
                  collection,
                  nextIndex,
                  frame.input(),
                  item.context(),
                  instruction.body(),
                  false,
                  command.requestedAt()))
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    WorkflowState.Running withIteratorContext =
        new WorkflowState.Running(
            state.executionId(),
            state.plan(),
            state.data(),
            state.nextStep(),
            state.revision(),
            state.processedCommands(),
            item.context(),
            state.rawWorkflowInput(),
            state.taskStack(),
            state.workflowDeadline());
    TaskResult result =
        completeTask(
            withIteratorContext, instruction.step(), frame.rawInput(), frame.input(), collection);
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.ProtocolCallIterationAdvanced(
            command.commandId(),
            instruction.step().path(),
            collection,
            frame.iterationIndex(),
            result.output(),
            result.context(),
            instruction.next(),
            true,
            command.requestedAt()));
    appendCompletionIfTerminal(state, command, instruction.next(), result, events);
    return Effect()
        .persist(events)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> advanceForkListenIteration(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      TaskExecutionFrame root,
      ForkSelection selection,
      MilestoneOneProgram.ExitListen instruction) {
    ForkBranchState branch = selection.branch();
    if (branch.taskStack().isEmpty()
        || !branch.taskStack().getLast().iterating()
        || !branch.taskStack().getLast().taskPath().equals(instruction.step().path())) {
      throw new IllegalStateException(
          "Fork lane listen iteration stack does not match " + instruction.step().path());
    }
    TaskExecutionFrame frame = branch.taskStack().getLast();
    WorkflowState.Running lane = forkLaneState(state, branch);
    TaskResult item =
        completeDataFlow(
            lane,
            instruction.step(),
            instruction.step().listenPlan().iteratorDataFlow(),
            frame.rawInput(),
            frame.input(),
            branch.data());
    var collection = (com.fasterxml.jackson.databind.node.ArrayNode) frame.collection().deepCopy();
    collection.set(frame.iterationIndex(), item.output());
    int nextIndex = frame.iterationIndex() + 1;
    boolean completed = nextIndex >= collection.size();
    JsonNode data;
    JsonNode context;
    int nextStep;
    ForkExecutionFrame hypothetical;
    if (!completed) {
      data = frame.input();
      context = item.context();
      nextStep = instruction.body();
      hypothetical =
          updateForkTree(
              root.fork(),
              selection.path(),
              0,
              current -> {
                var stack = new java.util.ArrayList<>(current.taskStack());
                stack.set(stack.size() - 1, frame.advance(nextIndex, collection));
                return current.advance(data, context, nextStep, stack);
              });
    } else {
      WorkflowState.Running withContext =
          new WorkflowState.Running(
              state.executionId(),
              state.plan(),
              branch.data(),
              branch.nextStep(),
              state.revision(),
              state.processedCommands(),
              item.context(),
              state.rawWorkflowInput(),
              branch.taskStack(),
              state.workflowDeadline());
      TaskResult result =
          completeTask(
              withContext, instruction.step(), frame.rawInput(), frame.input(), collection);
      data = result.output();
      context = result.context();
      nextStep = instruction.next();
      hypothetical =
          updateForkTree(
              root.fork(),
              selection.path(),
              0,
              current -> {
                var stack = new java.util.ArrayList<>(current.taskStack());
                stack.removeLast();
                return current.advance(data, context, nextStep, stack);
              });
    }
    return Effect()
        .persist(
            new EngineEvent.ForkBranchListenIterationAdvanced(
                command.commandId(),
                root.taskPath(),
                selection.path(),
                instruction.step().path(),
                collection,
                completed ? frame.iterationIndex() : nextIndex,
                data,
                context,
                nextStep,
                completed,
                hasActiveForkListeners(hypothetical),
                !hypothetical.complete() && !forkHasRunnable(hypothetical),
                command.requestedAt()))
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> advanceForkProtocolCallIteration(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      TaskExecutionFrame root,
      ForkSelection selection,
      MilestoneOneProgram.ExitProtocolCall instruction) {
    ForkBranchState branch = selection.branch();
    if (branch.taskStack().isEmpty()
        || !branch.taskStack().getLast().iterating()
        || !branch.taskStack().getLast().taskPath().equals(instruction.step().path())) {
      throw new IllegalStateException(
          "Fork lane protocol iteration stack does not match " + instruction.step().path());
    }
    TaskExecutionFrame frame = branch.taskStack().getLast();
    WorkflowState.Running lane = forkLaneState(state, branch);
    TaskResult item =
        completeDataFlow(
            lane,
            instruction.step(),
            protocolSubscription(instruction.step()).iteratorDataFlow(),
            frame.rawInput(),
            frame.input(),
            branch.data());
    var collection = (com.fasterxml.jackson.databind.node.ArrayNode) frame.collection().deepCopy();
    collection.set(frame.iterationIndex(), item.output());
    int nextIndex = frame.iterationIndex() + 1;
    boolean completed = nextIndex >= collection.size();
    JsonNode data;
    JsonNode context;
    int nextStep;
    ForkExecutionFrame hypothetical;
    if (!completed) {
      data = frame.input();
      context = item.context();
      nextStep = instruction.body();
      hypothetical =
          updateForkTree(
              root.fork(),
              selection.path(),
              0,
              current -> {
                var stack = new java.util.ArrayList<>(current.taskStack());
                stack.set(stack.size() - 1, frame.advance(nextIndex, collection));
                return current.advance(data, context, nextStep, stack);
              });
    } else {
      WorkflowState.Running withContext =
          new WorkflowState.Running(
              state.executionId(),
              state.plan(),
              branch.data(),
              branch.nextStep(),
              state.revision(),
              state.processedCommands(),
              item.context(),
              state.rawWorkflowInput(),
              branch.taskStack(),
              state.workflowDeadline());
      TaskResult result =
          completeTask(
              withContext, instruction.step(), frame.rawInput(), frame.input(), collection);
      data = result.output();
      context = result.context();
      nextStep = instruction.next();
      hypothetical =
          updateForkTree(
              root.fork(),
              selection.path(),
              0,
              current -> {
                var stack = new java.util.ArrayList<>(current.taskStack());
                stack.removeLast();
                return current.advance(data, context, nextStep, stack);
              });
    }
    return Effect()
        .persist(
            new EngineEvent.ForkBranchProtocolCallIterationAdvanced(
                command.commandId(),
                root.taskPath(),
                selection.path(),
                instruction.step().path(),
                collection,
                completed ? frame.iterationIndex() : nextIndex,
                data,
                context,
                nextStep,
                completed,
                !hypothetical.complete() && !forkHasRunnable(hypothetical),
                command.requestedAt()))
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private static UUID listenEventCommandId(
      ExecutionId executionId, TaskExecutionFrame frame, WorkflowCloudEvent event) {
    return UUID.nameUUIDFromBytes(
        (executionId.entityId()
                + "|listen-event|"
                + frame.event().operationId()
                + "|"
                + event.source()
                + "|"
                + event.id())
            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  private Match matchEvent(
      WorkflowState.Running state,
      PlanStep step,
      TaskExecutionFrame frame,
      EventConsumptionPlan consumption,
      WorkflowCloudEvent event) {
    ObjectNode envelope = eventEnvelope(event);
    if (consumption.mode() == EventConsumptionPlan.Mode.ANY && consumption.filters().isEmpty()) {
      return new Match(-1, frame.event().correlations());
    }
    for (int index = 0; index < consumption.filters().size(); index++) {
      if (consumption.mode() == EventConsumptionPlan.Mode.ALL
          && frame.event().matchedFilters().contains(index)) continue;
      EventFilterPlan filter = consumption.filters().get(index);
      if (!matchesEventProperties(filter.properties(), envelope, state, step, frame)) continue;
      var correlations = new LinkedHashMap<>(frame.event().correlations());
      boolean matches = true;
      for (var correlation : filter.correlations()) {
        JsonNode actual =
            expressions.evaluateExpression(
                explicitExpression(correlation.fromExpression()),
                event.data(),
                arguments(state, step, frame.rawInput(), frame.input(), null),
                state.plan().expressions().mode());
        JsonNode expected = correlations.get(correlation.name());
        if (expected == null && correlation.expected() != null) {
          expected =
              isRuntimeExpression(correlation.expected())
                  ? expressions.evaluateExpression(
                      correlation.expected(),
                      frame.input(),
                      arguments(state, step, frame.rawInput(), frame.input(), null),
                      state.plan().expressions().mode())
                  : JsonNodeFactory.instance.textNode(correlation.expected());
        }
        if (expected != null && !expected.equals(actual)) {
          matches = false;
          break;
        }
        correlations.putIfAbsent(correlation.name(), actual);
      }
      if (matches) return new Match(index, correlations);
    }
    return null;
  }

  private boolean matchesEventProperties(
      JsonNode expected,
      JsonNode actual,
      WorkflowState.Running state,
      PlanStep step,
      TaskExecutionFrame frame) {
    var fields = expected.properties().iterator();
    while (fields.hasNext()) {
      var field = fields.next();
      JsonNode value = actual.get(field.getKey());
      if (value == null) return false;
      JsonNode pattern = field.getValue();
      if (pattern.isTextual() && pattern.textValue().trim().startsWith("${")) {
        if (!expressions.evaluateCondition(
            pattern.textValue(),
            value,
            arguments(state, step, frame.rawInput(), frame.input(), null),
            state.plan().expressions().mode())) return false;
      } else if (pattern.isObject() && value.isObject()) {
        if (!matchesEventProperties(pattern, value, state, step, frame)) return false;
      } else if (!pattern.equals(value)) return false;
    }
    return true;
  }

  private static String explicitExpression(String expression) {
    String trimmed = expression.trim();
    return trimmed.startsWith("${") ? expression : "${ " + expression + " }";
  }

  private static boolean isRuntimeExpression(String value) {
    String trimmed = value.trim();
    return trimmed.startsWith("${") && trimmed.endsWith("}");
  }

  private static ObjectNode eventEnvelope(WorkflowCloudEvent event) {
    ObjectNode value = JsonNodeFactory.instance.objectNode();
    value.put("specversion", event.specVersion());
    value.put("id", event.id());
    value.put("source", event.source().toString());
    value.put("type", event.type());
    if (event.subject() != null) value.put("subject", event.subject());
    if (event.time() != null) value.put("time", event.time().toString());
    if (event.dataContentType() != null) {
      value.put("datacontenttype", event.dataContentType());
    }
    event.extensions().forEach(value::set);
    value.set("data", event.data());
    return value;
  }

  private static JsonNode readEvents(List<WorkflowCloudEvent> events, EventReadMode mode) {
    var output = JsonNodeFactory.instance.arrayNode();
    for (WorkflowCloudEvent event : events) {
      output.add(mode == EventReadMode.ENVELOPE ? eventEnvelope(event) : event.data());
    }
    return output;
  }

  private record Match(int filterIndex, Map<String, JsonNode> correlations) {}

  private ReplyEffect<EngineEvent, WorkflowState> enterTry(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.EnterTry instruction) {
    PlanStep step = instruction.step();
    JsonNode rawInput = state.data();
    boolean execute = condition(state, step, rawInput);
    JsonNode input = execute ? taskInput(state, step, rawInput) : rawInput;
    if (!execute) {
      TaskResult skipped = completeTask(state, step, rawInput, input, rawInput);
      int after =
          ((MilestoneOneProgram.ExitTry)
                  MilestoneOneProgram.compile(state.plan()).instruction(instruction.caughtExit()))
              .next();
      return Effect()
          .persist(taskEvents(state, command, step, rawInput, input, after, skipped))
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.TryEntered(
            command.commandId(),
            step.path(),
            rawInput,
            input,
            instruction.next(),
            command.requestedAt()));
    appendTaskDeadline(state, command, step, rawInput, input, events);
    return Effect()
        .persist(events)
        .thenRun(this::scheduleDeadlines)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> exitTry(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.ExitTry instruction) {
    if (state.taskStack().isEmpty()) {
      throw new IllegalStateException("Try stack is empty at " + instruction.step().path());
    }
    TaskExecutionFrame frame = state.taskStack().getLast();
    TaskExecutionFrame.TryPhase expected =
        instruction.caught() ? TaskExecutionFrame.TryPhase.CATCH : TaskExecutionFrame.TryPhase.BODY;
    if (!frame.taskPath().equals(instruction.step().path()) || frame.tryPhase() != expected) {
      throw new IllegalStateException("Try stack does not match " + instruction.step().path());
    }
    TaskResult result =
        completeTask(state, instruction.step(), frame.rawInput(), frame.input(), state.data());
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.TaskCompleted(
            command.commandId(),
            instruction.step().path(),
            instruction.next(),
            result.output(),
            result.context(),
            command.requestedAt()));
    appendCompletionIfTerminal(state, command, instruction.next(), result, events);
    return Effect()
        .persist(events)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> executeRaise(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      MilestoneOneProgram.ExecuteRaise instruction) {
    PlanStep step = instruction.step();
    JsonNode rawInput = state.data();
    if (!condition(state, step, rawInput)) {
      TaskResult skipped = completeTask(state, step, rawInput, rawInput, rawInput);
      return Effect()
          .persist(
              taskEvents(state, command, step, rawInput, rawInput, instruction.next(), skipped))
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    JsonNode input = taskInput(state, step, rawInput);
    JsonNode error = materializeError(state, step, rawInput, input);
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.ErrorRaised(
            command.commandId(), step.path(), error, command.requestedAt()));
    ErrorTarget target = matchingCatch(state, error);
    if (target == null) {
      events.add(
          new EngineEvent.Failed(
              command.commandId(),
              error
                  .path("detail")
                  .asText(
                      error
                          .path("title")
                          .asText("Unhandled workflow error " + error.path("type").asText())),
              command.requestedAt()));
      return Effect()
          .persist(events)
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    RetryDecision retry = retryDecision(state, target, error, command.requestedAt());
    if (retry != null) {
      events.add(
          new EngineEvent.RetryScheduled(
              command.commandId(),
              target.frame().taskPath(),
              error,
              target.frame().attempt() + 1,
              target.instruction().next(),
              retry.deadline(),
              target.frame().retryStartedAt(),
              command.requestedAt()));
      return Effect()
          .persist(events)
          .thenRun(this::scheduleDeadlines)
          .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
    }
    events.add(
        new EngineEvent.ErrorCaught(
            command.commandId(),
            target.frame().taskPath(),
            error,
            target.instruction().catchEntry(),
            command.requestedAt()));
    return Effect()
        .persist(events)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private JsonNode materializeError(
      WorkflowState.Running state, PlanStep step, JsonNode rawInput, JsonNode input) {
    ErrorPlan plan = step.raisePlan().error();
    ObjectNode error = JsonNodeFactory.instance.objectNode();
    RuntimeExpressionArguments args = arguments(state, step, rawInput, input, null);
    error.set(
        "type",
        expressions.evaluateTemplate(plan.type(), input, args, state.plan().expressions().mode()));
    error.put("status", plan.status());
    addDynamicErrorMember(
        error, "instance", plan.instance(), input, args, state.plan().expressions().mode());
    if (!error.hasNonNull("instance")) error.put("instance", step.path());
    addDynamicErrorMember(
        error, "title", plan.title(), input, args, state.plan().expressions().mode());
    addDynamicErrorMember(
        error, "detail", plan.detail(), input, args, state.plan().expressions().mode());
    return error;
  }

  private void addDynamicErrorMember(
      ObjectNode error,
      String name,
      JsonNode template,
      JsonNode input,
      RuntimeExpressionArguments arguments,
      ExpressionMode mode) {
    if (template != null) {
      error.set(name, expressions.evaluateTemplate(template, input, arguments, mode));
    }
  }

  private ErrorTarget matchingCatch(WorkflowState.Running state, JsonNode error) {
    MilestoneOneProgram program = MilestoneOneProgram.compile(state.plan());
    for (int index = state.taskStack().size() - 1; index >= 0; index--) {
      TaskExecutionFrame frame = state.taskStack().get(index);
      if (!frame.trying() || frame.tryPhase() != TaskExecutionFrame.TryPhase.BODY) {
        continue;
      }
      MilestoneOneProgram.EnterTry instruction = program.tryScope(frame.taskPath());
      CatchPlan clause = instruction.step().tryPlan().catchPlan();
      Map<String, JsonNode> variables = new LinkedHashMap<>(iterationVariables(state));
      variables.put(clause.as(), error);
      boolean matches =
          matches(clause.errors(), error)
              && (clause.when() == null
                  || expressions.evaluateCondition(
                      clause.when(),
                      state.data(),
                      arguments(
                          state,
                          instruction.step(),
                          frame.rawInput(),
                          frame.input(),
                          null,
                          variables),
                      state.plan().expressions().mode()))
              && (clause.exceptWhen() == null
                  || !expressions.evaluateCondition(
                      clause.exceptWhen(),
                      state.data(),
                      arguments(
                          state,
                          instruction.step(),
                          frame.rawInput(),
                          frame.input(),
                          null,
                          variables),
                      state.plan().expressions().mode()));
      if (matches) return new ErrorTarget(frame, instruction, clause, variables);
    }
    return null;
  }

  private static boolean matches(ErrorFilterPlan filter, JsonNode error) {
    if (filter == null) return true;
    return matches(filter.type(), error.path("type").asText(null))
        && (filter.status() == null || filter.status() == error.path("status").asInt())
        && matches(filter.instance(), error.path("instance").asText(null))
        && matches(filter.title(), error.path("title").asText(null))
        && matches(filter.detail(), error.path("detail").asText(null));
  }

  private static boolean matches(String expected, String actual) {
    return expected == null || "*".equals(expected) || Objects.equals(expected, actual);
  }

  private RetryDecision retryDecision(
      WorkflowState.Running state, ErrorTarget target, JsonNode error, Instant failedAt) {
    RetryPlan retry = target.clause().retry();
    if (retry == null) return null;
    if (retry.attemptCount() != null && target.frame().attempt() >= retry.attemptCount())
      return null;
    RuntimeExpressionArguments args =
        arguments(
            state,
            target.instruction().step(),
            target.frame().rawInput(),
            target.frame().input(),
            null,
            target.variables());
    if (retry.when() != null
        && !expressions.evaluateCondition(
            retry.when(), error, args, state.plan().expressions().mode())) return null;
    if (retry.exceptWhen() != null
        && expressions.evaluateCondition(
            retry.exceptWhen(), error, args, state.plan().expressions().mode())) return null;
    if (retry.attemptDuration() != null) {
      Duration maximumAttempts =
          Duration.between(
              failedAt,
              resolveDuration(
                  retry.attemptDuration(),
                  error,
                  args,
                  state.plan().expressions().mode(),
                  failedAt,
                  target.frame().taskPath() + "/catch/retry/limit/attempt/duration"));
      Duration consumedAttempts =
          target
              .frame()
              .attemptsElapsed()
              .plus(Duration.between(target.frame().attemptStartedAt(), failedAt));
      if (consumedAttempts.compareTo(maximumAttempts) >= 0) return null;
    }
    Duration base =
        retry.delay() == null
            ? Duration.ZERO
            : Duration.between(
                failedAt,
                resolveDuration(
                    retry.delay(),
                    error,
                    args,
                    state.plan().expressions().mode(),
                    failedAt,
                    target.frame().taskPath() + "/catch/retry/delay"));
    long factor =
        switch (retry.backoff()) {
          case CONSTANT -> 1L;
          case LINEAR -> target.frame().attempt();
          case EXPONENTIAL -> 1L << Math.min(30, target.frame().attempt() - 1);
        };
    Duration delay;
    try {
      delay = base.multipliedBy(factor);
    } catch (ArithmeticException overflow) {
      delay = Duration.ofDays(365_000);
    }
    if (retry.jitterFrom() != null) {
      Duration from =
          Duration.between(
              failedAt,
              resolveDuration(
                  retry.jitterFrom(),
                  error,
                  args,
                  state.plan().expressions().mode(),
                  failedAt,
                  target.frame().taskPath() + "/catch/retry/jitter/from"));
      Duration to =
          Duration.between(
              failedAt,
              resolveDuration(
                  retry.jitterTo(),
                  error,
                  args,
                  state.plan().expressions().mode(),
                  failedAt,
                  target.frame().taskPath() + "/catch/retry/jitter/to"));
      if (to.compareTo(from) < 0) {
        throw new IllegalArgumentException("Retry jitter upper bound precedes lower bound");
      }
      long range = Math.subtractExact(to.toMillis(), from.toMillis());
      long hash =
          Integer.toUnsignedLong(
              Objects.hash(
                  state.executionId(), target.frame().taskPath(), target.frame().attempt() + 1));
      delay = delay.plusMillis(from.toMillis() + (range == 0 ? 0 : hash % (range + 1)));
    }
    Instant deadline = failedAt.plus(delay);
    if (retry.totalDuration() != null) {
      Instant limit =
          resolveDuration(
              retry.totalDuration(),
              error,
              args,
              state.plan().expressions().mode(),
              target.frame().retryStartedAt(),
              target.frame().taskPath() + "/catch/retry/limit/duration");
      if (deadline.isAfter(limit)) return null;
    }
    return new RetryDecision(deadline);
  }

  private record ErrorTarget(
      TaskExecutionFrame frame,
      MilestoneOneProgram.EnterTry instruction,
      CatchPlan clause,
      Map<String, JsonNode> variables) {}

  private record RetryDecision(Instant deadline) {}

  private void appendTaskDeadline(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      PlanStep step,
      JsonNode rawInput,
      JsonNode input,
      List<EngineEvent> events) {
    if (step.timeout() == null) return;
    Instant deadline =
        resolveDuration(
            step.timeout().after(),
            input,
            arguments(state, step, rawInput, input, null),
            state.plan().expressions().mode(),
            command.requestedAt(),
            step.path() + "/timeout");
    events.add(
        new EngineEvent.DeadlineScheduled(
            command.commandId(), DeadlineScope.TASK, step.path(), deadline, command.requestedAt()));
  }

  private ReplyEffect<EngineEvent, WorkflowState> completeWait(
      WorkflowState.Waiting state, WorkflowCommand.TimerElapsed command) {
    if (activeFork(state.taskStack()) != null) {
      return completeForkWait(state, command);
    }
    if (!state.executionId().equals(command.executionId()) || state.taskStack().isEmpty())
      return Effect().noReply();
    TaskExecutionFrame frame = state.taskStack().getLast();
    if (!frame.waiting()
        || frame.trying()
        || !frame.taskPath().equals(command.taskPath())
        || !frame.waitDeadline().equals(command.deadline())) {
      return Effect().noReply();
    }
    MilestoneOneProgram program = MilestoneOneProgram.compile(state.plan());
    if (!(program.instruction(state.nextStep())
        instanceof MilestoneOneProgram.ExecuteWait instruction)) {
      throw new IllegalStateException("Durable wait cursor is not a wait task");
    }
    WorkflowState.Running running = running(state);
    TaskResult result =
        completeTask(running, instruction.step(), frame.rawInput(), frame.input(), state.data());
    UUID commandId = waitCommandId(state.executionId(), frame);
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.TaskCompleted(
            commandId,
            instruction.step().path(),
            instruction.next(),
            result.output(),
            result.context(),
            command.deadline()));
    if (instruction.next() == program.size()) {
      events.add(
          new EngineEvent.Completed(
              commandId,
              workflowOutput(running, result.output(), result.context()),
              command.deadline()));
    }
    return Effect()
        .persist(events)
        .thenRun(persisted -> continueIfRunning(persisted, command.deadline()))
        .thenNoReply();
  }

  private ReplyEffect<EngineEvent, WorkflowState> completeForkWait(
      WorkflowState.Running state, WorkflowCommand.TimerElapsed command) {
    return completeForkWait(
        state,
        state.plan(),
        state.context(),
        state.rawWorkflowInput(),
        state.taskStack(),
        state.workflowDeadline(),
        command);
  }

  private ReplyEffect<EngineEvent, WorkflowState> completeForkWait(
      WorkflowState.Waiting state, WorkflowCommand.TimerElapsed command) {
    return completeForkWait(
        state,
        state.plan(),
        state.context(),
        state.rawWorkflowInput(),
        state.taskStack(),
        state.workflowDeadline(),
        command);
  }

  private ReplyEffect<EngineEvent, WorkflowState> completeForkWait(
      WorkflowState state,
      com.forwardmeasure.openworkflow.definition.WorkflowPlan plan,
      JsonNode context,
      JsonNode rawWorkflowInput,
      List<TaskExecutionFrame> taskStack,
      Instant workflowDeadline,
      WorkflowCommand.TimerElapsed command) {
    if (!state.executionId().equals(command.executionId())) return Effect().noReply();
    TaskExecutionFrame root = activeFork(taskStack);
    if (root == null) return Effect().noReply();
    ForkWaitSelection selection = findForkWait(root.fork(), command.taskPath(), command.deadline());
    if (selection == null) return Effect().noReply();
    ForkBranchState branch = selection.branch();
    TaskExecutionFrame wait = selection.frame();
    MilestoneOneProgram program = MilestoneOneProgram.compile(plan);
    if (!(program.instruction(branch.nextStep())
            instanceof MilestoneOneProgram.ExecuteWait instruction)
        || !instruction.step().path().equals(wait.taskPath())) {
      throw new IllegalStateException("Durable fork wait cursor is not its wait task");
    }
    WorkflowState.Running laneState =
        new WorkflowState.Running(
            state.executionId(),
            plan,
            branch.data(),
            branch.nextStep(),
            state.revision(),
            state.processedCommands(),
            branch.context(),
            rawWorkflowInput,
            branch.taskStack(),
            workflowDeadline);
    TaskResult result =
        completeTask(laneState, instruction.step(), wait.rawInput(), wait.input(), branch.data());
    ForkExecutionFrame hypothetical =
        updateForkTree(
            root.fork(),
            selection.path(),
            0,
            current -> {
              var stack = new java.util.ArrayList<>(current.taskStack());
              stack.removeLast();
              return current.advance(result.output(), instruction.next(), stack);
            });
    boolean allBranchesWaiting = !forkHasRunnable(hypothetical);
    UUID commandId = waitCommandId(state.executionId(), wait);
    var events = new java.util.ArrayList<EngineEvent>();
    if (!result.context().equals(branch.context())) {
      events.add(
          new EngineEvent.ForkBranchContextUpdated(
              commandId, root.taskPath(), selection.path(), result.context(), command.deadline()));
    }
    events.add(
        new EngineEvent.ForkBranchWaitCompleted(
            commandId,
            root.taskPath(),
            selection.path(),
            wait.taskPath(),
            result.output(),
            instruction.next(),
            allBranchesWaiting,
            command.deadline()));
    return Effect()
        .persist(events)
        .thenRun(persisted -> continueIfRunning(persisted, command.deadline()))
        .thenNoReply();
  }

  private static ForkWaitSelection findForkWait(
      ForkExecutionFrame fork, String taskPath, Instant deadline) {
    return findForkWait(fork, taskPath, deadline, new java.util.ArrayList<>());
  }

  private static ForkWaitSelection findForkWait(
      ForkExecutionFrame fork,
      String taskPath,
      Instant deadline,
      java.util.ArrayList<Integer> path) {
    for (int index = 0; index < fork.branches().size(); index++) {
      ForkBranchState branch = fork.branches().get(index);
      if (branch.taskStack().isEmpty()) continue;
      path.add(index);
      TaskExecutionFrame frame = branch.taskStack().getLast();
      if (frame.waiting()
          && !frame.trying()
          && frame.taskPath().equals(taskPath)
          && frame.waitDeadline().equals(deadline)) {
        ForkWaitSelection found = new ForkWaitSelection(List.copyOf(path), branch, frame);
        path.removeLast();
        return found;
      }
      if (frame.forking()) {
        ForkWaitSelection found = findForkWait(frame.fork(), taskPath, deadline, path);
        if (found != null) {
          path.removeLast();
          return found;
        }
      }
      path.removeLast();
    }
    return null;
  }

  private record ForkWaitSelection(
      List<Integer> path, ForkBranchState branch, TaskExecutionFrame frame) {}

  private void scheduleWait(WorkflowState.Waiting state) {
    if (state.taskStack().isEmpty() || !state.taskStack().getLast().waiting()) return;
    TaskExecutionFrame frame = state.taskStack().getLast();
    if (frame.tryPhase() == TaskExecutionFrame.TryPhase.RETRY_DELAY) {
      scheduleRetry(state, frame);
      return;
    }
    Duration delay = Duration.between(Instant.now(), frame.waitDeadline());
    if (delay.isNegative()) delay = Duration.ZERO;
    if (delay.compareTo(MAX_TIMER_HORIZON) > 0) {
      timers.startSingleTimer(
          waitTimerKey(frame),
          new WorkflowCommand.RecheckTimers(state.executionId()),
          MAX_TIMER_HORIZON);
      return;
    }
    timers.startSingleTimer(
        waitTimerKey(frame),
        new WorkflowCommand.TimerElapsed(
            state.executionId(), frame.taskPath(), frame.waitDeadline()),
        delay);
  }

  private void scheduleRetry(WorkflowState.Waiting state, TaskExecutionFrame frame) {
    Duration delay = Duration.between(Instant.now(), frame.waitDeadline());
    if (delay.isNegative()) delay = Duration.ZERO;
    String key = "retry|" + frame.taskPath() + "|" + frame.waitDeadline();
    if (delay.compareTo(MAX_TIMER_HORIZON) > 0) {
      timers.startSingleTimer(
          key, new WorkflowCommand.RecheckTimers(state.executionId()), MAX_TIMER_HORIZON);
      return;
    }
    timers.startSingleTimer(
        key,
        new WorkflowCommand.RetryElapsed(
            state.executionId(), frame.taskPath(), frame.waitDeadline()),
        delay);
  }

  private ReplyEffect<EngineEvent, WorkflowState> beginRetry(
      WorkflowState.Waiting state, WorkflowCommand.RetryElapsed command) {
    if (activeFork(state.taskStack()) != null) {
      return beginForkRetry(state, command);
    }
    if (!state.executionId().equals(command.executionId()) || state.taskStack().isEmpty())
      return Effect().noReply();
    TaskExecutionFrame frame = state.taskStack().getLast();
    if (frame.tryPhase() != TaskExecutionFrame.TryPhase.RETRY_DELAY
        || !frame.taskPath().equals(command.tryTaskPath())
        || !frame.waitDeadline().equals(command.deadline())) {
      return Effect().noReply();
    }
    MilestoneOneProgram.EnterTry scope =
        MilestoneOneProgram.compile(state.plan()).tryScope(frame.taskPath());
    return Effect()
        .persist(
            new EngineEvent.RetryStarted(
                retryCommandId(state.executionId(), frame),
                frame.taskPath(),
                frame.attempt(),
                scope.next(),
                command.deadline()))
        .thenRun(this::continueIfRunning)
        .thenNoReply();
  }

  private ReplyEffect<EngineEvent, WorkflowState> beginForkRetry(
      WorkflowState.Running state, WorkflowCommand.RetryElapsed command) {
    return beginForkRetry((WorkflowState) state, state.plan(), state.taskStack(), command);
  }

  private ReplyEffect<EngineEvent, WorkflowState> beginForkRetry(
      WorkflowState.Waiting state, WorkflowCommand.RetryElapsed command) {
    return beginForkRetry((WorkflowState) state, state.plan(), state.taskStack(), command);
  }

  private ReplyEffect<EngineEvent, WorkflowState> beginForkRetry(
      WorkflowState state,
      com.forwardmeasure.openworkflow.definition.WorkflowPlan plan,
      List<TaskExecutionFrame> taskStack,
      WorkflowCommand.RetryElapsed command) {
    if (!state.executionId().equals(command.executionId())) return Effect().noReply();
    TaskExecutionFrame root = activeFork(taskStack);
    if (root == null) return Effect().noReply();
    ForkWaitSelection selection =
        findForkRetry(
            root.fork(), command.tryTaskPath(), command.deadline(), new java.util.ArrayList<>());
    if (selection == null) return Effect().noReply();
    TaskExecutionFrame frame = selection.frame();
    MilestoneOneProgram.EnterTry scope =
        MilestoneOneProgram.compile(plan).tryScope(frame.taskPath());
    return Effect()
        .persist(
            new EngineEvent.ForkBranchRetryStarted(
                retryCommandId(state.executionId(), frame),
                root.taskPath(),
                selection.path(),
                frame.taskPath(),
                frame.attempt(),
                scope.next(),
                false,
                command.deadline()))
        .thenRun(this::continueIfRunning)
        .thenNoReply();
  }

  private static ForkWaitSelection findForkRetry(
      ForkExecutionFrame fork,
      String taskPath,
      Instant deadline,
      java.util.ArrayList<Integer> path) {
    for (int index = 0; index < fork.branches().size(); index++) {
      ForkBranchState branch = fork.branches().get(index);
      if (branch.taskStack().isEmpty()) continue;
      path.add(index);
      TaskExecutionFrame frame = branch.taskStack().getLast();
      if (frame.tryPhase() == TaskExecutionFrame.TryPhase.RETRY_DELAY
          && frame.taskPath().equals(taskPath)
          && frame.waitDeadline().equals(deadline)) {
        ForkWaitSelection found = new ForkWaitSelection(List.copyOf(path), branch, frame);
        path.removeLast();
        return found;
      }
      if (frame.forking()) {
        ForkWaitSelection found = findForkRetry(frame.fork(), taskPath, deadline, path);
        if (found != null) {
          path.removeLast();
          return found;
        }
      }
      path.removeLast();
    }
    return null;
  }

  private static UUID retryCommandId(ExecutionId executionId, TaskExecutionFrame frame) {
    return UUID.nameUUIDFromBytes(
        (executionId.entityId()
                + "|retry|"
                + frame.taskPath()
                + "|"
                + frame.attempt()
                + "|"
                + frame.waitDeadline())
            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  private void scheduleDeadlines(WorkflowState state) {
    if (state instanceof WorkflowState.Waiting waiting && activeFork(state.taskStack()) == null)
      scheduleWait(waiting);
    TaskExecutionFrame rootFork = activeFork(state.taskStack());
    if (rootFork != null) {
      scheduleForkWaits(state.executionId(), rootFork.fork());
    }
    if (state.workflowDeadline() != null) {
      scheduleDeadline(state.executionId(), DeadlineScope.WORKFLOW, null, state.workflowDeadline());
    }
    for (TaskExecutionFrame frame : state.taskStack()) {
      scheduleFrameDeadlines(state.executionId(), frame);
    }
  }

  private void scheduleFrameDeadlines(ExecutionId executionId, TaskExecutionFrame frame) {
    if (frame.timeoutDeadline() != null) {
      scheduleDeadline(executionId, DeadlineScope.TASK, frame.taskPath(), frame.timeoutDeadline());
    }
    if (frame.forking()) {
      for (ForkBranchState branch : frame.fork().branches()) {
        for (TaskExecutionFrame nested : branch.taskStack()) {
          scheduleFrameDeadlines(executionId, nested);
        }
      }
    }
  }

  private void scheduleForkWaits(ExecutionId executionId, ForkExecutionFrame fork) {
    for (ForkBranchState branch : fork.branches()) {
      if (branch.taskStack().isEmpty()) continue;
      TaskExecutionFrame frame = branch.taskStack().getLast();
      if (frame.tryPhase() == TaskExecutionFrame.TryPhase.RETRY_DELAY) {
        scheduleForkRetry(executionId, frame);
      } else if (frame.waiting() && !frame.trying()) {
        scheduleForkWait(executionId, frame);
      } else if (frame.forking()) {
        scheduleForkWaits(executionId, frame.fork());
      }
    }
  }

  private void scheduleForkRetry(ExecutionId executionId, TaskExecutionFrame frame) {
    Duration delay = Duration.between(Instant.now(), frame.waitDeadline());
    if (delay.isNegative()) delay = Duration.ZERO;
    String key = "retry|" + frame.taskPath() + "|" + frame.waitDeadline();
    if (delay.compareTo(MAX_TIMER_HORIZON) > 0) {
      timers.startSingleTimer(
          key, new WorkflowCommand.RecheckTimers(executionId), MAX_TIMER_HORIZON);
      return;
    }
    timers.startSingleTimer(
        key,
        new WorkflowCommand.RetryElapsed(executionId, frame.taskPath(), frame.waitDeadline()),
        delay);
  }

  private void scheduleForkWait(ExecutionId executionId, TaskExecutionFrame frame) {
    Duration delay = Duration.between(Instant.now(), frame.waitDeadline());
    if (delay.isNegative()) delay = Duration.ZERO;
    if (delay.compareTo(MAX_TIMER_HORIZON) > 0) {
      timers.startSingleTimer(
          waitTimerKey(frame), new WorkflowCommand.RecheckTimers(executionId), MAX_TIMER_HORIZON);
      return;
    }
    timers.startSingleTimer(
        waitTimerKey(frame),
        new WorkflowCommand.TimerElapsed(executionId, frame.taskPath(), frame.waitDeadline()),
        delay);
  }

  private void scheduleDeadline(
      ExecutionId executionId, DeadlineScope scope, String taskPath, Instant deadline) {
    Duration delay = Duration.between(Instant.now(), deadline);
    if (delay.isNegative()) delay = Duration.ZERO;
    String key = deadlineTimerKey(scope, taskPath, deadline);
    if (delay.compareTo(MAX_TIMER_HORIZON) > 0) {
      timers.startSingleTimer(
          key, new WorkflowCommand.RecheckTimers(executionId), MAX_TIMER_HORIZON);
      return;
    }
    timers.startSingleTimer(
        key, new WorkflowCommand.DeadlineElapsed(executionId, scope, taskPath, deadline), delay);
  }

  private ReplyEffect<EngineEvent, WorkflowState> recheckTimers(
      WorkflowState state, WorkflowCommand.RecheckTimers command) {
    if (!state.executionId().equals(command.executionId())) return Effect().noReply();
    scheduleDeadlines(state);
    return Effect().noReply();
  }

  private ReplyEffect<EngineEvent, WorkflowState> expireDeadline(
      WorkflowState state, WorkflowCommand.DeadlineElapsed command) {
    if (!state.executionId().equals(command.executionId()) || !deadlineMatches(state, command))
      return Effect().noReply();
    String message =
        command.scope() == DeadlineScope.WORKFLOW
            ? "Workflow timeout elapsed at " + command.deadline()
            : "Task timeout elapsed at " + command.taskPath() + " at " + command.deadline();
    return Effect()
        .persist(new EngineEvent.Failed(deadlineCommandId(command), message, command.deadline()))
        .thenNoReply();
  }

  private static boolean deadlineMatches(
      WorkflowState state, WorkflowCommand.DeadlineElapsed command) {
    if (command.scope() == DeadlineScope.WORKFLOW) {
      return command.deadline().equals(state.workflowDeadline());
    }
    return state.taskStack().stream()
        .anyMatch(frame -> deadlineMatches(frame, command.taskPath(), command.deadline()));
  }

  private static boolean deadlineMatches(
      TaskExecutionFrame frame, String taskPath, Instant deadline) {
    if (frame.taskPath().equals(taskPath) && deadline.equals(frame.timeoutDeadline())) return true;
    if (!frame.forking()) return false;
    return frame.fork().branches().stream()
        .flatMap(branch -> branch.taskStack().stream())
        .anyMatch(nested -> deadlineMatches(nested, taskPath, deadline));
  }

  private static UUID deadlineCommandId(WorkflowCommand.DeadlineElapsed command) {
    return UUID.nameUUIDFromBytes(
        (command.executionId().entityId()
                + "|deadline|"
                + command.scope()
                + "|"
                + command.taskPath()
                + "|"
                + command.deadline())
            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  private static String deadlineTimerKey(DeadlineScope scope, String taskPath, Instant deadline) {
    return "deadline|" + scope + "|" + taskPath + "|" + deadline;
  }

  private static String waitTimerKey(TaskExecutionFrame frame) {
    return frame.taskPath() + "|" + frame.waitDeadline();
  }

  private static UUID waitCommandId(ExecutionId executionId, TaskExecutionFrame frame) {
    return UUID.nameUUIDFromBytes(
        (executionId.entityId() + "|" + waitTimerKey(frame))
            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  private static WorkflowState.Running running(WorkflowState.Waiting state) {
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision(),
        state.processedCommands(),
        state.context(),
        state.rawWorkflowInput(),
        state.taskStack(),
        state.workflowDeadline());
  }

  private Instant resolveDeadline(
      WorkflowState.Running state,
      PlanStep step,
      JsonNode rawInput,
      JsonNode input,
      Instant anchor) {
    return resolveDuration(
        step.waitPlan().duration(),
        input,
        arguments(state, step, rawInput, input, null),
        state.plan().expressions().mode(),
        anchor,
        step.path() + "/wait");
  }

  private Instant protocolDeadline(
      WorkflowState.Running state,
      PlanStep step,
      JsonNode rawInput,
      JsonNode input,
      Instant anchor) {
    var subscription = protocolSubscription(step);
    if (subscription == null || subscription.consumption().duration() == null) return null;
    return resolveDuration(
        subscription.consumption().duration(),
        input,
        arguments(state, step, rawInput, input, null),
        state.plan().expressions().mode(),
        anchor,
        step.path() + "/call/asyncapi/subscription/consume/for");
  }

  private JsonNode evaluateProtocolArguments(
      WorkflowState.Running state,
      PlanStep step,
      JsonNode input,
      RuntimeExpressionArguments arguments) {
    JsonNode template =
        step.callPlan() == null ? step.runPlan().configuration() : step.callPlan().arguments();
    if (protocolSubscription(step) != null
        && protocolSubscription(step).foreach()
        && template.isObject()) {
      ObjectNode copy = (ObjectNode) template.deepCopy();
      JsonNode foreach = copy.path("subscription").path("foreach");
      if (foreach instanceof ObjectNode object) object.remove("do");
      template = copy;
    }
    return expressions.evaluateTemplate(
        template, input, arguments, state.plan().expressions().mode());
  }

  private static com.forwardmeasure.openworkflow.definition.AsyncApiSubscriptionPlan
      protocolSubscription(PlanStep step) {
    return step.callPlan() == null ? null : step.callPlan().asyncApiSubscription();
  }

  private Instant resolveDuration(
      DurationPlan duration,
      JsonNode evaluatedOn,
      RuntimeExpressionArguments arguments,
      ExpressionMode expressionMode,
      Instant anchor,
      String path) {
    return OpenWorkflowDurationResolver.resolve(
        duration, evaluatedOn, arguments, expressionMode, anchor, path, expressions);
  }

  private boolean condition(WorkflowState.Running state, PlanStep step, JsonNode rawInput) {
    return step.dataFlow().condition() == null
        || expressions.evaluateCondition(
            step.dataFlow().condition(),
            rawInput,
            arguments(state, step, rawInput, null, null),
            state.plan().expressions().mode());
  }

  private JsonNode taskInput(WorkflowState.Running state, PlanStep step, JsonNode rawInput) {
    TaskDataFlow flow = step.dataFlow();
    new DataSchemaValidator(state.plan().resources()).validate(flow.inputSchema(), rawInput);
    return flow.inputFrom() == null
        ? rawInput.deepCopy()
        : expressions.evaluateTemplate(
            flow.inputFrom(),
            rawInput,
            arguments(state, step, rawInput, null, null),
            state.plan().expressions().mode());
  }

  private JsonNode functionArguments(
      WorkflowState.Running state, PlanStep step, JsonNode rawInput, JsonNode input) {
    return expressions.evaluateTemplate(
        step.callPlan().arguments(),
        input,
        arguments(state, step, rawInput, input, null),
        state.plan().expressions().mode());
  }

  private static FunctionOperationDescriptor functionOperation(
      WorkflowState.Running state, PlanStep step, JsonNode arguments, String coordinate) {
    String operationId =
        UUID.nameUUIDFromBytes(
                (state.executionId().entityId()
                        + "|function|"
                        + step.path()
                        + "|"
                        + coordinate
                        + "|"
                        + state.revision())
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .toString();
    return new FunctionOperationDescriptor(
        operationId, step.callPlan().functionName(), step.callPlan().resource(), arguments);
  }

  private TaskResult completeTask(
      WorkflowState.Running state,
      PlanStep step,
      JsonNode rawInput,
      JsonNode input,
      JsonNode rawOutput) {
    TaskDataFlow flow = step.dataFlow();
    JsonNode output =
        flow.outputAs() == null
            ? rawOutput.deepCopy()
            : expressions.evaluateTemplate(
                flow.outputAs(),
                rawOutput,
                arguments(state, step, rawInput, input, null),
                state.plan().expressions().mode());
    DataSchemaValidator schemas = new DataSchemaValidator(state.plan().resources());
    schemas.validate(flow.outputSchema(), output);
    JsonNode context =
        flow.exportAs() == null
            ? state.context().deepCopy()
            : expressions.evaluateTemplate(
                flow.exportAs(),
                output,
                arguments(state, step, rawInput, input, output),
                state.plan().expressions().mode());
    schemas.validate(flow.exportSchema(), context);
    return new TaskResult(output, context);
  }

  private TaskResult completeDataFlow(
      WorkflowState.Running state,
      PlanStep step,
      TaskDataFlow flow,
      JsonNode rawInput,
      JsonNode input,
      JsonNode rawOutput) {
    JsonNode output =
        flow.outputAs() == null
            ? rawOutput.deepCopy()
            : expressions.evaluateTemplate(
                flow.outputAs(),
                rawOutput,
                arguments(state, step, rawInput, input, null),
                state.plan().expressions().mode());
    DataSchemaValidator schemas = new DataSchemaValidator(state.plan().resources());
    schemas.validate(flow.outputSchema(), output);
    JsonNode context =
        flow.exportAs() == null
            ? state.context().deepCopy()
            : expressions.evaluateTemplate(
                flow.exportAs(),
                output,
                arguments(state, step, rawInput, input, output),
                state.plan().expressions().mode());
    schemas.validate(flow.exportSchema(), context);
    return new TaskResult(output, context);
  }

  private JsonNode workflowOutput(WorkflowState.Running state) {
    return workflowOutput(state, state.data(), state.context());
  }

  private JsonNode workflowOutput(
      WorkflowState.Running state, JsonNode rawOutput, JsonNode context) {
    JsonNode output =
        state.plan().dataFlow().outputAs() == null
            ? rawOutput.deepCopy()
            : expressions.evaluateTemplate(
                state.plan().dataFlow().outputAs(),
                rawOutput,
                workflowArguments(
                    state.plan(), state.executionId(), state.rawWorkflowInput(), context),
                state.plan().expressions().mode());
    new DataSchemaValidator(state.plan().resources())
        .validate(state.plan().dataFlow().outputSchema(), output);
    return output;
  }

  private JsonNode initialData(WorkflowPlan plan, JsonNode rawInput, ExecutionId executionId) {
    DataSchemaValidator schemas = new DataSchemaValidator(plan.resources());
    schemas.validate(plan.dataFlow().inputSchema(), rawInput);
    return plan.dataFlow().inputFrom() == null
        ? rawInput.deepCopy()
        : expressions.evaluateTemplate(
            plan.dataFlow().inputFrom(),
            rawInput,
            workflowArguments(plan, executionId, rawInput, null),
            plan.expressions().mode());
  }

  private RuntimeExpressionArguments arguments(
      WorkflowState.Running state,
      PlanStep step,
      JsonNode rawInput,
      JsonNode input,
      JsonNode output) {
    return arguments(state, step, rawInput, input, output, iterationVariables(state));
  }

  private RuntimeExpressionArguments arguments(
      WorkflowState.Running state,
      PlanStep step,
      JsonNode rawInput,
      JsonNode input,
      JsonNode output,
      Map<String, JsonNode> variables) {
    ObjectNode task =
        JsonNodeFactory.instance
            .objectNode()
            .put("name", step.name())
            .put("reference", step.path())
            .set("definition", step.definition());
    task.set("input", rawInput);
    if (output != null) task.set("output", output);
    RuntimeExpressionArguments base =
        workflowArguments(
            state.plan(), state.executionId(), state.rawWorkflowInput(), state.context());
    return new RuntimeExpressionArguments(
        state.context(),
        input,
        output,
        null,
        null,
        task,
        base.workflow(),
        base.runtime(),
        variables);
  }

  private static AuthenticationExpressionContext authenticationContext(
      RuntimeExpressionArguments arguments) {
    return new AuthenticationExpressionContext(
        arguments.context(),
        arguments.input(),
        arguments.output(),
        arguments.authorization(),
        arguments.task(),
        arguments.workflow(),
        arguments.runtime(),
        arguments.variables());
  }

  private Map<String, JsonNode> iterationVariables(WorkflowState.Running state) {
    Map<String, JsonNode> variables = new LinkedHashMap<>();
    for (TaskExecutionFrame frame : state.taskStack()) {
      if (frame.iterating()) {
        variables.put(frame.itemVariable(), frame.collection().get(frame.iterationIndex()));
        variables.put(
            frame.indexVariable(), JsonNodeFactory.instance.numberNode(frame.iterationIndex()));
      }
      if (frame.tryPhase() == TaskExecutionFrame.TryPhase.CATCH && frame.error() != null) {
        CatchPlan clause =
            MilestoneOneProgram.compile(state.plan())
                .tryScope(frame.taskPath())
                .step()
                .tryPlan()
                .catchPlan();
        variables.put(clause.as(), frame.error());
      }
    }
    return variables;
  }

  private Map<String, JsonNode> iterationVariables(
      WorkflowState.Running state, PlanStep step, JsonNode collection, int index) {
    Map<String, JsonNode> variables = new LinkedHashMap<>(iterationVariables(state));
    variables.put(step.forPlan().itemVariable(), collection.get(index));
    variables.put(step.forPlan().indexVariable(), JsonNodeFactory.instance.numberNode(index));
    return variables;
  }

  private RuntimeExpressionArguments workflowArguments(
      WorkflowPlan plan, ExecutionId executionId, JsonNode rawInput, JsonNode context) {
    ObjectNode workflow =
        JsonNodeFactory.instance
            .objectNode()
            .put("id", executionId.value().toString())
            .set("definition", plan.definition());
    workflow.set("input", rawInput);
    ObjectNode runtime =
        JsonNodeFactory.instance
            .objectNode()
            .put("name", "openworkflow-actor-engine")
            .put("version", "1.0.0");
    return new RuntimeExpressionArguments(context, null, null, null, null, null, workflow, runtime);
  }

  private record TaskResult(JsonNode output, JsonNode context) {}

  private List<EngineEvent> taskEvents(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      PlanStep step,
      JsonNode rawInput,
      JsonNode input,
      int next,
      TaskResult result) {
    var events = new java.util.ArrayList<EngineEvent>();
    events.add(
        new EngineEvent.TaskEntered(
            command.commandId(),
            step.path(),
            rawInput,
            input,
            state.nextStep(),
            command.requestedAt()));
    events.add(
        new EngineEvent.TaskCompleted(
            command.commandId(),
            step.path(),
            next,
            result.output(),
            result.context(),
            command.requestedAt()));
    appendCompletionIfTerminal(state, command, next, result, events);
    return events;
  }

  private void appendCompletionIfTerminal(
      WorkflowState.Running state,
      WorkflowCommand.RunNext command,
      int next,
      TaskResult result,
      List<EngineEvent> events) {
    if (next == MilestoneOneProgram.compile(state.plan()).size()) {
      events.add(
          new EngineEvent.Completed(
              command.commandId(),
              workflowOutput(state, result.output(), result.context()),
              command.requestedAt()));
    }
  }

  private ReplyEffect<EngineEvent, WorkflowState> completedStart(
      WorkflowState.Completed state, WorkflowCommand.Start command) {
    if (state.executionId().equals(command.executionId())
        && state.processedCommands().contains(command.commandId())) {
      return Effect()
          .reply(
              command.replyTo(),
              new WorkflowReply.Accepted(
                  command.commandId(), state.executionId(), state.revision(), state.status()));
    }
    return rejectAlreadyStarted(state, command);
  }

  private ReplyEffect<EngineEvent, WorkflowState> rejectAlreadyStarted(
      WorkflowState state, WorkflowCommand.Start command) {
    if (sameReceipt(state, command.executionId(), command.commandId())) {
      return Effect().reply(command.replyTo(), accepted(command.commandId(), state));
    }
    return reject(
        command, state, "already_started", "An execution can start only from the new state");
  }

  private ReplyEffect<EngineEvent, WorkflowState> pauseRunning(
      WorkflowState.Running state, WorkflowCommand.Pause command) {
    return pause(state, state.plan(), state.data(), state.nextStep(), command);
  }

  private ReplyEffect<EngineEvent, WorkflowState> pauseWaiting(
      WorkflowState.Waiting state, WorkflowCommand.Pause command) {
    return pause(state, state.plan(), state.data(), state.nextStep(), command);
  }

  private ReplyEffect<EngineEvent, WorkflowState> pause(
      WorkflowState state,
      com.forwardmeasure.openworkflow.definition.WorkflowPlan plan,
      JsonNode data,
      int nextStep,
      WorkflowCommand.Pause command) {
    if (!state.executionId().equals(command.executionId())) {
      return reject(
          command,
          state,
          "wrong_execution",
          "Command was routed to another tenant-qualified execution");
    }
    return Effect()
        .persist(
            List.of(
                new EngineEvent.PauseRequested(
                    command.commandId(), command.actor(), command.requestedAt()),
                new EngineEvent.Paused(
                    command.commandId(), activeTaskPaths(state), command.requestedAt())))
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> pausedPause(
      WorkflowState.Paused state, WorkflowCommand.Pause command) {
    if (sameReceipt(state, command.executionId(), command.commandId())) {
      return Effect().reply(command.replyTo(), accepted(command.commandId(), state));
    }
    return reject(command, state, "already_paused", "Execution is already paused");
  }

  private ReplyEffect<EngineEvent, WorkflowState> resume(
      WorkflowState.Paused state, WorkflowCommand.Resume command) {
    if (!state.executionId().equals(command.executionId())) {
      return reject(
          command,
          state,
          "wrong_execution",
          "Command was routed to another tenant-qualified execution");
    }
    return Effect()
        .persist(
            new EngineEvent.Resumed(
                command.commandId(),
                command.actor(),
                activeTaskPaths(state),
                command.requestedAt()))
        .thenRun(this::scheduleDeadlines)
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> cancelRunning(
      WorkflowState.Running state, WorkflowCommand.Cancel command) {
    return cancel(state, command);
  }

  private ReplyEffect<EngineEvent, WorkflowState> cancelWaiting(
      WorkflowState.Waiting state, WorkflowCommand.Cancel command) {
    return cancel(state, command);
  }

  private ReplyEffect<EngineEvent, WorkflowState> cancelPausing(
      WorkflowState.Pausing state, WorkflowCommand.Cancel command) {
    return cancel(state, command);
  }

  private ReplyEffect<EngineEvent, WorkflowState> cancelPaused(
      WorkflowState.Paused state, WorkflowCommand.Cancel command) {
    return cancel(state, command);
  }

  private ReplyEffect<EngineEvent, WorkflowState> cancel(
      WorkflowState state, WorkflowCommand.Cancel command) {
    if (!state.executionId().equals(command.executionId())) {
      return reject(
          command,
          state,
          "wrong_execution",
          "Command was routed to another tenant-qualified execution");
    }
    return Effect()
        .persist(
            List.of(
                new EngineEvent.CancellationRequested(
                    command.commandId(), command.actor(), command.requestedAt()),
                new EngineEvent.Cancelled(
                    command.commandId(), activeTaskPaths(state), command.requestedAt())))
        .thenReply(command.replyTo(), persisted -> accepted(command.commandId(), persisted));
  }

  private ReplyEffect<EngineEvent, WorkflowState> cancelledCancel(
      WorkflowState.Cancelled state, WorkflowCommand.Cancel command) {
    if (sameReceipt(state, command.executionId(), command.commandId())) {
      return Effect().reply(command.replyTo(), accepted(command.commandId(), state));
    }
    return reject(command, state, "already_cancelled", "Execution is already cancelled");
  }

  private static List<String> activeTaskPaths(WorkflowState state) {
    var paths = new java.util.ArrayList<String>();
    collectActiveTaskPaths(state.taskStack(), paths);
    return List.copyOf(paths);
  }

  private static void collectActiveTaskPaths(List<TaskExecutionFrame> stack, List<String> paths) {
    for (TaskExecutionFrame frame : stack) {
      paths.add(frame.taskPath());
      if (frame.forking()) {
        frame.fork().branches().stream()
            .filter(branch -> !branch.completed())
            .forEach(branch -> collectActiveTaskPaths(branch.taskStack(), paths));
      }
    }
  }

  private ReplyEffect<EngineEvent, WorkflowState> rejectNotRunning(
      WorkflowState state, WorkflowCommand command) {
    return rejectControl(command, state, "not_running", "Execution is not running");
  }

  private ReplyEffect<EngineEvent, WorkflowState> rejectNotPaused(
      WorkflowState state, WorkflowCommand command) {
    if (state instanceof WorkflowState.Running) {
      continueIfRunning(state);
    }
    return rejectControl(command, state, "not_paused", "Execution is not paused");
  }

  private ReplyEffect<EngineEvent, WorkflowState> rejectTransitioning(
      WorkflowState state, WorkflowCommand command) {
    return rejectControl(
        command,
        state,
        "transition_in_progress",
        "Another durable control transition is in progress");
  }

  private ReplyEffect<EngineEvent, WorkflowState> rejectTerminal(
      WorkflowState state, WorkflowCommand command) {
    return rejectControl(
        command, state, "terminal_execution", "Terminal executions cannot be controlled");
  }

  private ReplyEffect<EngineEvent, WorkflowState> rejectControl(
      WorkflowCommand command, WorkflowState state, String code, String message) {
    UUID commandId = controlCommandId(command);
    return Effect()
        .reply(
            command.replyTo(),
            new WorkflowReply.Rejected(
                commandId, state.executionId(), state.revision(), state.status(), code, message));
  }

  private static UUID controlCommandId(WorkflowCommand command) {
    return switch (command) {
      case WorkflowCommand.RunNext runNext -> runNext.commandId();
      case WorkflowCommand.Pause pause -> pause.commandId();
      case WorkflowCommand.Resume resume -> resume.commandId();
      case WorkflowCommand.Cancel cancel -> cancel.commandId();
      default -> throw new IllegalArgumentException("Not a control command");
    };
  }

  private static boolean sameReceipt(
      WorkflowState state, ExecutionId commandExecution, UUID commandId) {
    return state.executionId().equals(commandExecution)
        && state.processedCommands().contains(commandId);
  }

  private static WorkflowReply.Accepted accepted(UUID commandId, WorkflowState state) {
    return new WorkflowReply.Accepted(
        commandId, state.executionId(), state.revision(), state.status());
  }

  private ReplyEffect<EngineEvent, WorkflowState> reject(
      WorkflowCommand.Pause command, WorkflowState state, String code, String message) {
    return Effect()
        .reply(
            command.replyTo(),
            new WorkflowReply.Rejected(
                command.commandId(),
                state.executionId(),
                state.revision(),
                state.status(),
                code,
                message));
  }

  private ReplyEffect<EngineEvent, WorkflowState> reject(
      WorkflowCommand.Resume command, WorkflowState state, String code, String message) {
    return Effect()
        .reply(
            command.replyTo(),
            new WorkflowReply.Rejected(
                command.commandId(),
                state.executionId(),
                state.revision(),
                state.status(),
                code,
                message));
  }

  private ReplyEffect<EngineEvent, WorkflowState> reject(
      WorkflowCommand.Cancel command, WorkflowState state, String code, String message) {
    return Effect()
        .reply(
            command.replyTo(),
            new WorkflowReply.Rejected(
                command.commandId(),
                state.executionId(),
                state.revision(),
                state.status(),
                code,
                message));
  }

  private ReplyEffect<EngineEvent, WorkflowState> getState(
      WorkflowState state, WorkflowCommand.GetState command) {
    if (!state.executionId().equals(command.executionId())) {
      return Effect()
          .reply(
              command.replyTo(),
              new WorkflowReply.Rejected(
                  null,
                  state.executionId(),
                  state.revision(),
                  state.status(),
                  "wrong_execution",
                  "Query was routed to another tenant-qualified execution"));
    }
    return Effect()
        .reply(
            command.replyTo(),
            new WorkflowReply.StateSnapshot(
                state.executionId(), state.revision(), state.status(), state.data()));
  }

  private ReplyEffect<EngineEvent, WorkflowState> reject(
      WorkflowCommand.Start command, WorkflowState state, String code, String message) {
    return Effect()
        .reply(
            command.replyTo(),
            new WorkflowReply.Rejected(
                command.commandId(),
                state.executionId(),
                state.revision(),
                state.status(),
                code,
                message));
  }

  @Override
  public EventHandler<WorkflowState, EngineEvent> eventHandler() {
    var builder = newEventHandlerBuilder();

    builder
        .forStateType(WorkflowState.New.class)
        .onEvent(EngineEvent.Started.class, this::onStarted);
    builder
        .forStateType(WorkflowState.Running.class)
        .onEvent(EngineEvent.TaskEntered.class, this::onTaskEntered)
        .onEvent(EngineEvent.ExtensionEntered.class, this::onExtensionEntered)
        .onEvent(EngineEvent.FunctionEntered.class, this::onFunctionEntered)
        .onEvent(EngineEvent.ForEntered.class, this::onForEntered)
        .onEvent(EngineEvent.ForIterationAdvanced.class, this::onForIterationAdvanced)
        .onEvent(EngineEvent.WaitScheduled.class, this::onWaitScheduled)
        .onEvent(EngineEvent.TryEntered.class, this::onTryEntered)
        .onEvent(EngineEvent.ForkEntered.class, this::onForkEntered)
        .onEvent(EngineEvent.ForkBranchAdvanced.class, this::onForkBranchAdvanced)
        .onEvent(EngineEvent.ForkBranchTaskEntered.class, this::onForkBranchTaskEntered)
        .onEvent(EngineEvent.ForkBranchExtensionEntered.class, this::onForkBranchExtensionEntered)
        .onEvent(EngineEvent.ForkBranchFunctionEntered.class, this::onForkBranchFunctionEntered)
        .onEvent(EngineEvent.ForkBranchTaskCompleted.class, this::onForkBranchTaskCompleted)
        .onEvent(EngineEvent.ForkBranchForEntered.class, this::onForkBranchForEntered)
        .onEvent(EngineEvent.ForkBranchForAdvanced.class, this::onForkBranchForAdvanced)
        .onEvent(EngineEvent.ForkNestedEntered.class, this::onForkNestedEntered)
        .onEvent(EngineEvent.ForkNestedBranchAdvanced.class, this::onForkNestedBranchAdvanced)
        .onEvent(EngineEvent.ForkNestedCompleted.class, this::onForkNestedCompleted)
        .onEvent(EngineEvent.ForkNestedTaskEntered.class, this::onForkNestedTaskEntered)
        .onEvent(EngineEvent.ForkNestedExtensionEntered.class, this::onForkNestedExtensionEntered)
        .onEvent(EngineEvent.ForkNestedFunctionEntered.class, this::onForkNestedFunctionEntered)
        .onEvent(EngineEvent.ForkNestedTaskCompleted.class, this::onForkNestedTaskCompleted)
        .onEvent(EngineEvent.ForkNestedForEntered.class, this::onForkNestedForEntered)
        .onEvent(EngineEvent.ForkNestedForAdvanced.class, this::onForkNestedForAdvanced)
        .onEvent(EngineEvent.ForkBranchWaitScheduled.class, this::onForkBranchWaitScheduled)
        .onEvent(EngineEvent.ForkBranchWaitCompleted.class, this::onForkBranchWaitCompleted)
        .onEvent(EngineEvent.ForkBranchContextUpdated.class, this::onForkBranchContextUpdated)
        .onEvent(EngineEvent.ForkBranchTryEntered.class, this::onForkBranchTryEntered)
        .onEvent(EngineEvent.ForkBranchTryCompleted.class, this::onForkBranchTryCompleted)
        .onEvent(EngineEvent.ForkBranchErrorCaught.class, this::onForkBranchErrorCaught)
        .onEvent(EngineEvent.ForkBranchRetryScheduled.class, this::onForkBranchRetryScheduled)
        .onEvent(EngineEvent.ForkBranchRetryStarted.class, this::onForkBranchRetryStarted)
        .onEvent(EngineEvent.ForkBranchEmitRequested.class, this::onForkBranchEmitRequested)
        .onEvent(EngineEvent.ForkBranchEmitAcknowledged.class, this::onForkBranchEmitAcknowledged)
        .onEvent(EngineEvent.ForkBranchHttpCallRequested.class, this::onForkBranchHttpCallRequested)
        .onEvent(EngineEvent.ForkBranchHttpCallCompleted.class, this::onForkBranchHttpCallCompleted)
        .onEvent(
            EngineEvent.ForkBranchProtocolCallRequested.class,
            this::onForkBranchProtocolCallRequested)
        .onEvent(
            EngineEvent.ForkBranchProtocolCallItemAccepted.class,
            this::onForkBranchProtocolCallItemAccepted)
        .onEvent(
            EngineEvent.ForkBranchProtocolCallCompleted.class,
            this::onForkBranchProtocolCallCompleted)
        .onEvent(
            EngineEvent.ForkBranchProtocolCallIterationStarted.class,
            this::onForkBranchProtocolCallIterationStarted)
        .onEvent(
            EngineEvent.ForkBranchProtocolCallIterationAdvanced.class,
            this::onForkBranchProtocolCallIterationAdvanced)
        .onEvent(EngineEvent.ForkBranchListenStarted.class, this::onForkBranchListenStarted)
        .onEvent(EngineEvent.ForkBranchListenAccepted.class, this::onForkBranchListenAccepted)
        .onEvent(
            EngineEvent.ForkBranchListenIterationAdvanced.class,
            this::onForkBranchListenIterationAdvanced)
        .onEvent(EngineEvent.ForkBranchEffectSkipped.class, this::onForkBranchEffectSkipped)
        .onEvent(
            EngineEvent.ForkBranchSubworkflowRequested.class,
            this::onForkBranchSubworkflowRequested)
        .onEvent(
            EngineEvent.ForkBranchSubworkflowCompleted.class,
            this::onForkBranchSubworkflowCompleted)
        .onEvent(EngineEvent.SubworkflowRequested.class, this::onSubworkflowRequested)
        .onEvent(EngineEvent.EmitRequested.class, this::onEmitRequested)
        .onEvent(EngineEvent.HttpCallRequested.class, this::onHttpCallRequested)
        .onEvent(EngineEvent.ProtocolCallRequested.class, this::onProtocolCallRequested)
        .onEvent(EngineEvent.ListenStarted.class, this::onListenStarted)
        .onEvent(EngineEvent.ListenIterationAdvanced.class, this::onListenIterationAdvanced)
        .onEvent(
            EngineEvent.ProtocolCallIterationAdvanced.class, this::onProtocolCallIterationAdvanced)
        .onEvent(EngineEvent.ForkBranchesWaiting.class, this::onForkBranchesWaiting)
        .onEvent(EngineEvent.ErrorRaised.class, this::onErrorRaised)
        .onEvent(EngineEvent.ErrorCaught.class, this::onErrorCaught)
        .onEvent(EngineEvent.RetryScheduled.class, this::onRetryScheduled)
        .onEvent(EngineEvent.DeadlineScheduled.class, this::onDeadlineScheduled)
        .onEvent(EngineEvent.TaskCompleted.class, this::onTaskCompleted)
        .onEvent(EngineEvent.PauseRequested.class, this::onPauseRequested)
        .onEvent(EngineEvent.CancellationRequested.class, this::onCancellationRequested)
        .onEvent(EngineEvent.Completed.class, this::onCompleted)
        .onEvent(EngineEvent.Failed.class, this::onFailed);
    builder
        .forStateType(WorkflowState.Waiting.class)
        .onEvent(EngineEvent.DeadlineScheduled.class, this::onDeadlineScheduled)
        .onEvent(EngineEvent.ForkBranchWaitCompleted.class, this::onForkBranchWaitCompleted)
        .onEvent(EngineEvent.ForkBranchContextUpdated.class, this::onForkBranchContextUpdated)
        .onEvent(EngineEvent.ForkBranchErrorCaught.class, this::onForkBranchErrorCaught)
        .onEvent(EngineEvent.ForkBranchRetryScheduled.class, this::onForkBranchRetryScheduled)
        .onEvent(EngineEvent.ForkBranchRetryStarted.class, this::onForkBranchRetryStarted)
        .onEvent(EngineEvent.ForkBranchEmitAcknowledged.class, this::onForkBranchEmitAcknowledged)
        .onEvent(EngineEvent.ForkBranchHttpCallCompleted.class, this::onForkBranchHttpCallCompleted)
        .onEvent(
            EngineEvent.ForkBranchProtocolCallItemAccepted.class,
            this::onForkBranchProtocolCallItemAccepted)
        .onEvent(
            EngineEvent.ForkBranchProtocolCallCompleted.class,
            this::onForkBranchProtocolCallCompleted)
        .onEvent(
            EngineEvent.ForkBranchProtocolCallIterationStarted.class,
            this::onForkBranchProtocolCallIterationStarted)
        .onEvent(
            EngineEvent.ForkBranchProtocolCallIterationAdvanced.class,
            this::onForkBranchProtocolCallIterationAdvanced)
        .onEvent(EngineEvent.ForkBranchListenAccepted.class, this::onForkBranchListenAccepted)
        .onEvent(
            EngineEvent.ForkBranchListenIterationAdvanced.class,
            this::onForkBranchListenIterationAdvanced)
        .onEvent(
            EngineEvent.ForkBranchSubworkflowCompleted.class,
            this::onForkBranchSubworkflowCompleted)
        .onEvent(EngineEvent.EmitAcknowledged.class, this::onEmitAcknowledged)
        .onEvent(EngineEvent.HttpCallCompleted.class, this::onHttpCallCompleted)
        .onEvent(EngineEvent.ProtocolCallItemAccepted.class, this::onProtocolCallItemAccepted)
        .onEvent(EngineEvent.ProtocolCallCompleted.class, this::onProtocolCallCompleted)
        .onEvent(
            EngineEvent.ProtocolCallIterationStarted.class, this::onProtocolCallIterationStarted)
        .onEvent(EngineEvent.SubworkflowCompleted.class, this::onSubworkflowCompleted)
        .onEvent(EngineEvent.ListenEventAccepted.class, this::onListenEventAccepted)
        .onEvent(EngineEvent.ListenUntilAdvanced.class, this::onListenUntilAdvanced)
        .onEvent(EngineEvent.ListenIterationStarted.class, this::onListenIterationStarted)
        .onEvent(EngineEvent.ErrorRaised.class, this::onErrorRaised)
        .onEvent(EngineEvent.ErrorCaught.class, this::onErrorCaught)
        .onEvent(EngineEvent.RetryScheduled.class, this::onRetryScheduled)
        .onEvent(EngineEvent.TaskCompleted.class, this::onWaitCompleted)
        .onEvent(EngineEvent.RetryStarted.class, this::onRetryStarted)
        .onEvent(EngineEvent.PauseRequested.class, this::onPauseRequested)
        .onEvent(EngineEvent.CancellationRequested.class, this::onCancellationRequested)
        .onEvent(EngineEvent.Failed.class, this::onFailed);
    builder
        .forStateType(WorkflowState.Pausing.class)
        .onEvent(EngineEvent.Paused.class, this::onPaused)
        .onEvent(EngineEvent.CancellationRequested.class, this::onCancellationRequested);
    builder
        .forStateType(WorkflowState.Paused.class)
        .onEvent(EngineEvent.Resumed.class, this::onResumed)
        .onEvent(EngineEvent.CancellationRequested.class, this::onCancellationRequested);
    builder
        .forStateType(WorkflowState.Cancelling.class)
        .onEvent(EngineEvent.Cancelled.class, this::onCancelled);

    return builder.build();
  }

  private WorkflowState onStarted(WorkflowState.New state, EngineEvent.Started event) {
    if (!state.executionId().equals(event.executionId())) {
      throw new IllegalStateException(
          "Persisted start event has another execution ID: expected "
              + state.executionId()
              + " but was "
              + event.executionId());
    }
    JsonNode input = initialData(event.plan(), event.input(), event.executionId());
    executionActor.set(event.actor());
    return new WorkflowState.Running(
        event.executionId(),
        event.plan(),
        input,
        0,
        1,
        receipts(Set.of(), event.commandId()),
        input,
        event.input(),
        List.of(),
        null);
  }

  private WorkflowState onTaskEntered(WorkflowState.Running state, EngineEvent.TaskEntered event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    stack.add(new TaskExecutionFrame(event.taskPath(), event.rawInput(), event.input()));
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        event.input(),
        event.nextStep(),
        state.revision() + 1,
        state.processedCommands(),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onExtensionEntered(
      WorkflowState.Running state, EngineEvent.ExtensionEntered event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    stack.add(
        TaskExecutionFrame.extending(
            event.taskPath(), event.rawInput(), event.input(), event.decisions()));
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        event.input(),
        event.nextStep(),
        state.revision() + 1,
        state.processedCommands(),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onFunctionEntered(
      WorkflowState.Running state, EngineEvent.FunctionEntered event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    stack.add(new TaskExecutionFrame(event.taskPath(), event.rawInput(), event.input()));
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        event.operation().arguments(),
        event.nextStep(),
        state.revision() + 1,
        state.processedCommands(),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onTaskCompleted(
      WorkflowState.Running state, EngineEvent.TaskCompleted event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    if (!stack.isEmpty() && stack.getLast().taskPath().equals(event.taskPath())) {
      stack.removeLast();
    }
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        event.output(),
        event.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        event.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onForEntered(WorkflowState.Running state, EngineEvent.ForEntered event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    stack.add(
        new TaskExecutionFrame(
            event.taskPath(),
            event.rawInput(),
            event.input(),
            event.collection(),
            event.iterationIndex(),
            event.itemVariable(),
            event.indexVariable()));
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        event.input(),
        event.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onForIterationAdvanced(
      WorkflowState.Running state, EngineEvent.ForIterationAdvanced event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    if (stack.isEmpty()
        || !stack.getLast().taskPath().equals(event.taskPath())
        || !stack.getLast().iterating()) {
      throw new IllegalStateException(
          "Persisted iteration advance does not match the active task stack");
    }
    stack.set(stack.size() - 1, stack.getLast().advance(event.iterationIndex()));
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        event.data(),
        event.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onWaitScheduled(
      WorkflowState.Running state, EngineEvent.WaitScheduled event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    stack.add(
        TaskExecutionFrame.waiting(
            event.taskPath(), event.rawInput(), event.input(), event.deadline()));
    return new WorkflowState.Waiting(
        state.executionId(),
        state.plan(),
        event.input(),
        event.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        "wait:" + event.taskPath(),
        event.deadline(),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onSubworkflowRequested(
      WorkflowState.Running state, EngineEvent.SubworkflowRequested event) {
    long revision = state.revision() + 1;
    Set<UUID> receipts = receipts(state.processedCommands(), event.commandId());
    if (!event.await()) {
      return new WorkflowState.Running(
          state.executionId(),
          state.plan(),
          event.detachedOutput(),
          event.nextStep(),
          revision,
          receipts,
          event.detachedContext(),
          state.rawWorkflowInput(),
          state.taskStack(),
          state.workflowDeadline());
    }
    var stack = new java.util.ArrayList<>(state.taskStack());
    stack.add(
        TaskExecutionFrame.eventing(
            event.taskPath(),
            event.rawInput(),
            event.input(),
            EventExecutionFrame.subworkflow(event.operationId())));
    return new WorkflowState.Waiting(
        state.executionId(),
        state.plan(),
        event.input(),
        state.nextStep(),
        revision,
        receipts,
        "subworkflow:" + event.operationId(),
        null,
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onSubworkflowCompleted(
      WorkflowState.Waiting state, EngineEvent.SubworkflowCompleted event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    if (stack.isEmpty()
        || !stack.getLast().eventing()
        || stack.getLast().event().kind() != EventExecutionFrame.Kind.SUBWORKFLOW
        || !stack.getLast().event().operationId().equals(event.operationId())) {
      throw new IllegalStateException("Persisted subworkflow completion has no matching frame");
    }
    stack.removeLast();
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        event.output(),
        event.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        event.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onEmitRequested(
      WorkflowState.Running state, EngineEvent.EmitRequested event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    stack.add(
        TaskExecutionFrame.eventing(
            event.taskPath(),
            event.rawInput(),
            event.input(),
            EventExecutionFrame.emit(event.operationId(), event.event())));
    return new WorkflowState.Waiting(
        state.executionId(),
        state.plan(),
        event.input(),
        event.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        "emit:" + event.operationId(),
        null,
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onHttpCallRequested(
      WorkflowState.Running state, EngineEvent.HttpCallRequested event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    stack.add(
        TaskExecutionFrame.eventing(
            event.taskPath(),
            event.rawInput(),
            event.input(),
            EventExecutionFrame.httpCall(event.operation().operationId())));
    return new WorkflowState.Waiting(
        state.executionId(),
        state.plan(),
        event.input(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        "http-call:" + event.operation().operationId(),
        null,
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onProtocolCallRequested(
      WorkflowState.Running state, EngineEvent.ProtocolCallRequested event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    stack.add(
        TaskExecutionFrame.eventing(
            event.taskPath(),
            event.rawInput(),
            event.input(),
            EventExecutionFrame.protocolCall(event.operation())));
    return new WorkflowState.Waiting(
        state.executionId(),
        state.plan(),
        event.input(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        "protocol-call:" + event.operation().operationId(),
        null,
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onProtocolCallItemAccepted(
      WorkflowState.Waiting state, EngineEvent.ProtocolCallItemAccepted event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    if (stack.isEmpty()
        || !stack.getLast().eventing()
        || stack.getLast().event().kind() != EventExecutionFrame.Kind.PROTOCOL_CALL
        || !stack.getLast().taskPath().equals(event.taskPath())
        || !stack.getLast().event().operationId().equals(event.operationId())) {
      throw new IllegalStateException("Persisted protocol item does not match its task frame");
    }
    TaskExecutionFrame current = stack.removeLast();
    stack.add(
        TaskExecutionFrame.eventing(
            current.taskPath(),
            current.rawInput(),
            current.input(),
            current.event().acceptProtocolItem(event.item())));
    return new WorkflowState.Waiting(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.reason(),
        state.deadline(),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onProtocolCallCompleted(
      WorkflowState.Waiting state, EngineEvent.ProtocolCallCompleted event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    if (stack.isEmpty()
        || !stack.getLast().eventing()
        || stack.getLast().event().kind() != EventExecutionFrame.Kind.PROTOCOL_CALL
        || !stack.getLast().taskPath().equals(event.taskPath())
        || !stack.getLast().event().operationId().equals(event.operationId())) {
      throw new IllegalStateException(
          "Persisted protocol completion does not match its task frame");
    }
    stack.removeLast();
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        event.output(),
        event.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        event.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onProtocolCallIterationStarted(
      WorkflowState.Waiting state, EngineEvent.ProtocolCallIterationStarted event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    if (stack.isEmpty()
        || !stack.getLast().eventing()
        || stack.getLast().event().kind() != EventExecutionFrame.Kind.PROTOCOL_CALL
        || !stack.getLast().taskPath().equals(event.taskPath())
        || !stack.getLast().event().operationId().equals(event.operationId())) {
      throw new IllegalStateException("Persisted protocol iteration does not match its task frame");
    }
    stack.removeLast();
    stack.add(
        new TaskExecutionFrame(
            event.taskPath(),
            event.rawInput(),
            event.input(),
            event.collection(),
            0,
            event.itemVariable(),
            event.indexVariable()));
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        event.input(),
        event.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onProtocolCallIterationAdvanced(
      WorkflowState.Running state, EngineEvent.ProtocolCallIterationAdvanced event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    if (stack.isEmpty()
        || !stack.getLast().iterating()
        || !stack.getLast().taskPath().equals(event.taskPath())) {
      throw new IllegalStateException(
          "Persisted protocol iteration advance does not match its frame");
    }
    if (event.completed()) stack.removeLast();
    else
      stack.set(
          stack.size() - 1, stack.getLast().advance(event.iterationIndex(), event.collection()));
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        event.data(),
        event.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        event.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onHttpCallCompleted(
      WorkflowState.Waiting state, EngineEvent.HttpCallCompleted event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    if (stack.isEmpty()
        || !stack.getLast().eventing()
        || stack.getLast().event().kind() != EventExecutionFrame.Kind.HTTP_CALL
        || !stack.getLast().taskPath().equals(event.taskPath())
        || !stack.getLast().event().operationId().equals(event.operationId())) {
      throw new IllegalStateException("Persisted HTTP result does not match its task frame");
    }
    stack.removeLast();
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        event.output(),
        event.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        event.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onEmitAcknowledged(
      WorkflowState.Waiting state, EngineEvent.EmitAcknowledged event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    if (stack.isEmpty()
        || !stack.getLast().eventing()
        || stack.getLast().event().kind() != EventExecutionFrame.Kind.EMIT
        || !stack.getLast().taskPath().equals(event.taskPath())
        || !stack.getLast().event().operationId().equals(event.operationId())) {
      throw new IllegalStateException(
          "Persisted emit acknowledgement does not match its task frame");
    }
    stack.removeLast();
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        event.output(),
        event.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        event.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onListenStarted(
      WorkflowState.Running state, EngineEvent.ListenStarted event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    stack.add(
        TaskExecutionFrame.eventing(
            event.taskPath(),
            event.rawInput(),
            event.input(),
            EventExecutionFrame.listen(event.operationId())));
    return new WorkflowState.Waiting(
        state.executionId(),
        state.plan(),
        event.input(),
        event.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        "listen:" + event.operationId(),
        null,
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onListenEventAccepted(
      WorkflowState.Waiting state, EngineEvent.ListenEventAccepted event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    if (stack.isEmpty()
        || !stack.getLast().eventing()
        || stack.getLast().event().kind() != EventExecutionFrame.Kind.LISTEN
        || !stack.getLast().taskPath().equals(event.taskPath())
        || !stack.getLast().event().operationId().equals(event.operationId())) {
      throw new IllegalStateException("Persisted listen event does not match its task frame");
    }
    if (event.completed()) {
      stack.removeLast();
      return new WorkflowState.Running(
          state.executionId(),
          state.plan(),
          event.output(),
          event.nextStep(),
          state.revision() + 1,
          receipts(state.processedCommands(), event.commandId()),
          event.context(),
          state.rawWorkflowInput(),
          stack,
          state.workflowDeadline());
    }
    EventExecutionFrame current = stack.getLast().event();
    EventExecutionFrame next =
        new EventExecutionFrame(
            EventExecutionFrame.Kind.LISTEN,
            current.operationId(),
            null,
            event.accepted(),
            event.correlations(),
            event.matchedFilters());
    stack.set(stack.size() - 1, stack.getLast().withEvent(next));
    return new WorkflowState.Waiting(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.reason(),
        state.deadline(),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onListenIterationStarted(
      WorkflowState.Waiting state, EngineEvent.ListenIterationStarted event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    if (stack.isEmpty()
        || !stack.getLast().eventing()
        || !stack.getLast().taskPath().equals(event.taskPath())) {
      throw new IllegalStateException("Persisted listen iteration does not match its task frame");
    }
    stack.removeLast();
    stack.add(
        new TaskExecutionFrame(
            event.taskPath(),
            event.rawInput(),
            event.input(),
            event.collection(),
            0,
            event.itemVariable(),
            event.indexVariable()));
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        event.input(),
        event.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onListenUntilAdvanced(
      WorkflowState.Waiting state, EngineEvent.ListenUntilAdvanced event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    if (stack.isEmpty()
        || !stack.getLast().eventing()
        || !stack.getLast().taskPath().equals(event.taskPath())
        || !stack.getLast().event().operationId().equals(event.operationId())) {
      throw new IllegalStateException(
          "Persisted listen until progress does not match its task frame");
    }
    stack.set(
        stack.size() - 1,
        stack.getLast().withEvent(stack.getLast().event().withUntil(event.untilWindow())));
    return new WorkflowState.Waiting(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.reason(),
        state.deadline(),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onListenIterationAdvanced(
      WorkflowState.Running state, EngineEvent.ListenIterationAdvanced event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    if (stack.isEmpty()
        || !stack.getLast().iterating()
        || !stack.getLast().taskPath().equals(event.taskPath())) {
      throw new IllegalStateException(
          "Persisted listen iteration advance does not match its task frame");
    }
    stack.set(
        stack.size() - 1, stack.getLast().advance(event.iterationIndex(), event.collection()));
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        event.data(),
        event.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        event.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onTryEntered(WorkflowState.Running state, EngineEvent.TryEntered event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    stack.add(
        TaskExecutionFrame.trying(
            event.taskPath(), event.rawInput(), event.input(), event.occurredAt()));
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        event.input(),
        event.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onForkEntered(WorkflowState.Running state, EngineEvent.ForkEntered event) {
    if (event.branchNames().size() != event.branchStarts().size()
        || event.branchNames().size() != event.branchEnds().size()) {
      throw new IllegalStateException("Persisted fork branch metadata is inconsistent");
    }
    var branches = new java.util.ArrayList<ForkBranchState>();
    for (int index = 0; index < event.branchNames().size(); index++) {
      branches.add(
          new ForkBranchState(
              event.branchNames().get(index),
              event.input(),
              state.context(),
              event.branchStarts().get(index),
              event.branchEnds().get(index),
              List.of(),
              false));
    }
    var stack = new java.util.ArrayList<>(state.taskStack());
    stack.add(
        TaskExecutionFrame.forking(
            event.taskPath(),
            event.rawInput(),
            event.input(),
            new ForkExecutionFrame(event.compete(), branches, 0, null)));
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        event.input(),
        event.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onForkBranchAdvanced(
      WorkflowState.Running state, EngineEvent.ForkBranchAdvanced event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    if (stack.isEmpty()
        || !stack.getLast().forking()
        || !stack.getLast().taskPath().equals(event.forkTaskPath())) {
      throw new IllegalStateException(
          "Persisted fork advance does not match the active task stack");
    }
    TaskExecutionFrame task = stack.getLast();
    ForkExecutionFrame fork = task.fork();
    if (event.branchIndex() < 0 || event.branchIndex() >= fork.branches().size()) {
      throw new IllegalStateException("Persisted fork branch index is outside the fork");
    }
    var branches = new java.util.ArrayList<>(fork.branches());
    ForkBranchState branch = branches.get(event.branchIndex());
    branches.set(
        event.branchIndex(), branch.advance(event.data(), event.nextStep(), branch.taskStack()));
    ForkExecutionFrame advanced =
        new ForkExecutionFrame(fork.compete(), branches, event.nextBranch(), event.winner());
    stack.set(stack.size() - 1, task.withFork(advanced));
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onForkBranchTaskEntered(
      WorkflowState.Running state, EngineEvent.ForkBranchTaskEntered event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    TaskExecutionFrame task = matchingForkTask(stack, event.forkTaskPath());
    ForkExecutionFrame fork = task.fork();
    ForkBranchState branch = fork.branches().get(event.branchIndex());
    var laneStack = new java.util.ArrayList<>(branch.taskStack());
    laneStack.add(new TaskExecutionFrame(event.taskPath(), event.rawInput(), event.input()));
    var branches = new java.util.ArrayList<>(fork.branches());
    branches.set(event.branchIndex(), branch.advance(event.input(), event.nextStep(), laneStack));
    stack.set(
        stack.size() - 1,
        task.withFork(new ForkExecutionFrame(fork.compete(), branches, event.nextBranch(), null)));
    return runningWithForkEvent(state, event, stack);
  }

  private WorkflowState onForkBranchExtensionEntered(
      WorkflowState.Running state, EngineEvent.ForkBranchExtensionEntered event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    TaskExecutionFrame task = matchingForkTask(stack, event.forkTaskPath());
    ForkExecutionFrame fork = task.fork();
    ForkBranchState branch = fork.branches().get(event.branchIndex());
    var laneStack = new java.util.ArrayList<>(branch.taskStack());
    laneStack.add(
        TaskExecutionFrame.extending(
            event.taskPath(), event.rawInput(), event.input(), event.decisions()));
    var branches = new java.util.ArrayList<>(fork.branches());
    branches.set(event.branchIndex(), branch.advance(event.input(), event.nextStep(), laneStack));
    stack.set(
        stack.size() - 1,
        task.withFork(new ForkExecutionFrame(fork.compete(), branches, event.nextBranch(), null)));
    return runningWithForkEvent(state, event, stack);
  }

  private WorkflowState onForkBranchFunctionEntered(
      WorkflowState.Running state, EngineEvent.ForkBranchFunctionEntered event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    TaskExecutionFrame task = matchingForkTask(stack, event.forkTaskPath());
    ForkExecutionFrame fork = task.fork();
    ForkBranchState branch = fork.branches().get(event.branchIndex());
    var laneStack = new java.util.ArrayList<>(branch.taskStack());
    laneStack.add(new TaskExecutionFrame(event.taskPath(), event.rawInput(), event.input()));
    var branches = new java.util.ArrayList<>(fork.branches());
    branches.set(
        event.branchIndex(),
        branch.advance(event.operation().arguments(), event.nextStep(), laneStack));
    stack.set(
        stack.size() - 1,
        task.withFork(new ForkExecutionFrame(fork.compete(), branches, event.nextBranch(), null)));
    return runningWithForkEvent(state, event, stack);
  }

  private WorkflowState onForkBranchTaskCompleted(
      WorkflowState.Running state, EngineEvent.ForkBranchTaskCompleted event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    TaskExecutionFrame task = matchingForkTask(stack, event.forkTaskPath());
    ForkExecutionFrame fork = task.fork();
    ForkBranchState branch = fork.branches().get(event.branchIndex());
    var laneStack = new java.util.ArrayList<>(branch.taskStack());
    if (laneStack.isEmpty() || !laneStack.getLast().taskPath().equals(event.taskPath())) {
      throw new IllegalStateException(
          "Persisted fork task completion does not match its lane stack");
    }
    laneStack.removeLast();
    var branches = new java.util.ArrayList<>(fork.branches());
    branches.set(event.branchIndex(), branch.advance(event.output(), event.nextStep(), laneStack));
    stack.set(
        stack.size() - 1,
        task.withFork(
            new ForkExecutionFrame(fork.compete(), branches, event.nextBranch(), event.winner())));
    return runningWithForkEvent(state, event, stack);
  }

  private WorkflowState onForkBranchForEntered(
      WorkflowState.Running state, EngineEvent.ForkBranchForEntered event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    TaskExecutionFrame task = matchingForkTask(stack, event.forkTaskPath());
    ForkExecutionFrame fork = task.fork();
    ForkBranchState branch = fork.branches().get(event.branchIndex());
    var laneStack = new java.util.ArrayList<>(branch.taskStack());
    laneStack.add(
        new TaskExecutionFrame(
            event.taskPath(),
            event.rawInput(),
            event.input(),
            event.collection(),
            event.iterationIndex(),
            event.itemVariable(),
            event.indexVariable()));
    var branches = new java.util.ArrayList<>(fork.branches());
    branches.set(event.branchIndex(), branch.advance(event.input(), event.nextStep(), laneStack));
    stack.set(
        stack.size() - 1,
        task.withFork(new ForkExecutionFrame(fork.compete(), branches, event.nextBranch(), null)));
    return runningWithForkEvent(state, event, stack);
  }

  private WorkflowState onForkBranchForAdvanced(
      WorkflowState.Running state, EngineEvent.ForkBranchForAdvanced event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    TaskExecutionFrame task = matchingForkTask(stack, event.forkTaskPath());
    ForkExecutionFrame fork = task.fork();
    ForkBranchState branch = fork.branches().get(event.branchIndex());
    var laneStack = new java.util.ArrayList<>(branch.taskStack());
    if (laneStack.isEmpty()
        || !laneStack.getLast().iterating()
        || !laneStack.getLast().taskPath().equals(event.taskPath())) {
      throw new IllegalStateException("Persisted fork iteration does not match its lane stack");
    }
    laneStack.set(laneStack.size() - 1, laneStack.getLast().advance(event.iterationIndex()));
    var branches = new java.util.ArrayList<>(fork.branches());
    branches.set(event.branchIndex(), branch.advance(event.data(), event.nextStep(), laneStack));
    stack.set(
        stack.size() - 1,
        task.withFork(new ForkExecutionFrame(fork.compete(), branches, event.nextBranch(), null)));
    return runningWithForkEvent(state, event, stack);
  }

  private WorkflowState onForkNestedEntered(
      WorkflowState.Running state, EngineEvent.ForkNestedEntered event) {
    return updateNestedForkState(
        state,
        event.rootForkTaskPath(),
        event.parentBranchPath(),
        event,
        branch -> {
          var nestedBranches = new java.util.ArrayList<ForkBranchState>();
          for (int index = 0; index < event.branchNames().size(); index++) {
            nestedBranches.add(
                new ForkBranchState(
                    event.branchNames().get(index),
                    event.input(),
                    branch.context(),
                    event.branchStarts().get(index),
                    event.branchEnds().get(index),
                    List.of(),
                    false));
          }
          ForkExecutionFrame nested =
              new ForkExecutionFrame(event.compete(), nestedBranches, 0, null);
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          laneStack.add(
              TaskExecutionFrame.forking(
                  event.taskPath(), event.rawInput(), event.input(), nested));
          return branch.advance(branch.data(), branch.nextStep(), laneStack);
        });
  }

  private WorkflowState onForkNestedBranchAdvanced(
      WorkflowState.Running state, EngineEvent.ForkNestedBranchAdvanced event) {
    return updateNestedForkState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        branch -> branch.advance(event.data(), event.nextStep(), branch.taskStack()));
  }

  private WorkflowState onForkNestedCompleted(
      WorkflowState.Running state, EngineEvent.ForkNestedCompleted event) {
    return updateNestedForkState(
        state,
        event.rootForkTaskPath(),
        event.parentBranchPath(),
        event,
        branch -> {
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          if (laneStack.isEmpty()
              || !laneStack.getLast().forking()
              || !laneStack.getLast().fork().complete()
              || !laneStack.getLast().taskPath().equals(event.taskPath())) {
            throw new IllegalStateException(
                "Persisted nested fork completion does not match its lane");
          }
          laneStack.removeLast();
          return branch.advance(event.output(), event.nextStep(), laneStack);
        });
  }

  private WorkflowState onForkNestedTaskEntered(
      WorkflowState.Running state, EngineEvent.ForkNestedTaskEntered event) {
    return updateNestedForkState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        branch -> {
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          laneStack.add(new TaskExecutionFrame(event.taskPath(), event.rawInput(), event.input()));
          return branch.advance(event.input(), event.nextStep(), laneStack);
        });
  }

  private WorkflowState onForkNestedExtensionEntered(
      WorkflowState.Running state, EngineEvent.ForkNestedExtensionEntered event) {
    return updateNestedForkState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        branch -> {
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          laneStack.add(
              TaskExecutionFrame.extending(
                  event.taskPath(), event.rawInput(), event.input(), event.decisions()));
          return branch.advance(event.input(), event.nextStep(), laneStack);
        });
  }

  private WorkflowState onForkNestedFunctionEntered(
      WorkflowState.Running state, EngineEvent.ForkNestedFunctionEntered event) {
    return updateNestedForkState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        branch -> {
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          laneStack.add(new TaskExecutionFrame(event.taskPath(), event.rawInput(), event.input()));
          return branch.advance(event.operation().arguments(), event.nextStep(), laneStack);
        });
  }

  private WorkflowState onForkNestedTaskCompleted(
      WorkflowState.Running state, EngineEvent.ForkNestedTaskCompleted event) {
    return updateNestedForkState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        branch -> {
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          if (laneStack.isEmpty() || !laneStack.getLast().taskPath().equals(event.taskPath())) {
            throw new IllegalStateException(
                "Persisted nested task completion does not match its lane");
          }
          laneStack.removeLast();
          return branch.advance(event.output(), event.nextStep(), laneStack);
        });
  }

  private WorkflowState onForkNestedForEntered(
      WorkflowState.Running state, EngineEvent.ForkNestedForEntered event) {
    return updateNestedForkState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        branch -> {
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          laneStack.add(
              new TaskExecutionFrame(
                  event.taskPath(),
                  event.rawInput(),
                  event.input(),
                  event.collection(),
                  event.iterationIndex(),
                  event.itemVariable(),
                  event.indexVariable()));
          return branch.advance(event.input(), event.nextStep(), laneStack);
        });
  }

  private WorkflowState onForkNestedForAdvanced(
      WorkflowState.Running state, EngineEvent.ForkNestedForAdvanced event) {
    return updateNestedForkState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        branch -> {
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          if (laneStack.isEmpty()
              || !laneStack.getLast().iterating()
              || !laneStack.getLast().taskPath().equals(event.taskPath())) {
            throw new IllegalStateException("Persisted nested iteration does not match its lane");
          }
          laneStack.set(laneStack.size() - 1, laneStack.getLast().advance(event.iterationIndex()));
          return branch.advance(event.data(), event.nextStep(), laneStack);
        });
  }

  private WorkflowState onForkBranchWaitScheduled(
      WorkflowState.Running state, EngineEvent.ForkBranchWaitScheduled event) {
    var stack =
        updateForkStack(
            state.taskStack(),
            event.rootForkTaskPath(),
            event.branchPath(),
            branch -> {
              var laneStack = new java.util.ArrayList<>(branch.taskStack());
              laneStack.add(
                  TaskExecutionFrame.waiting(
                      event.taskPath(), event.rawInput(), event.input(), event.deadline()));
              return branch.advance(event.input(), event.nextStep(), laneStack);
            });
    TaskExecutionFrame root = activeFork(stack);
    boolean actuallyWaiting = root != null && !forkHasRunnable(root.fork());
    if (actuallyWaiting != event.allBranchesWaiting()) {
      throw new IllegalStateException(
          "Persisted fork wait availability does not match its fork tree");
    }
    long revision = state.revision() + 1;
    Set<UUID> receipts = receipts(state.processedCommands(), event.commandId());
    if (actuallyWaiting) {
      return new WorkflowState.Waiting(
          state.executionId(),
          state.plan(),
          state.data(),
          state.nextStep(),
          revision,
          receipts,
          "fork-wait:" + event.rootForkTaskPath(),
          earliestForkWait(root.fork()),
          state.context(),
          state.rawWorkflowInput(),
          stack,
          state.workflowDeadline());
    }
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        revision,
        receipts,
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onForkBranchEmitRequested(
      WorkflowState.Running state, EngineEvent.ForkBranchEmitRequested event) {
    return updateForkEffectState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        event.allBranchesBlocked(),
        branch -> {
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          laneStack.add(
              TaskExecutionFrame.eventing(
                  event.taskPath(),
                  event.rawInput(),
                  event.input(),
                  EventExecutionFrame.emit(event.operationId(), event.event())));
          return branch.advance(event.input(), branch.nextStep(), laneStack);
        });
  }

  private WorkflowState onForkBranchHttpCallRequested(
      WorkflowState.Running state, EngineEvent.ForkBranchHttpCallRequested event) {
    return updateForkEffectState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        event.allBranchesBlocked(),
        branch -> {
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          laneStack.add(
              TaskExecutionFrame.eventing(
                  event.taskPath(),
                  event.rawInput(),
                  event.input(),
                  EventExecutionFrame.httpCall(event.operation().operationId())));
          return branch.advance(event.input(), branch.nextStep(), laneStack);
        });
  }

  private WorkflowState onForkBranchProtocolCallRequested(
      WorkflowState.Running state, EngineEvent.ForkBranchProtocolCallRequested event) {
    return updateForkEffectState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        event.allBranchesBlocked(),
        branch -> {
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          laneStack.add(
              TaskExecutionFrame.eventing(
                  event.taskPath(),
                  event.rawInput(),
                  event.input(),
                  EventExecutionFrame.protocolCall(event.operation())));
          return branch.advance(event.input(), branch.nextStep(), laneStack);
        });
  }

  private WorkflowState onForkBranchListenStarted(
      WorkflowState.Running state, EngineEvent.ForkBranchListenStarted event) {
    return updateForkEffectState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        event.allBranchesBlocked(),
        branch -> {
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          laneStack.add(
              TaskExecutionFrame.eventing(
                  event.taskPath(),
                  event.rawInput(),
                  event.input(),
                  EventExecutionFrame.listen(event.operationId())));
          return branch.advance(event.input(), branch.nextStep(), laneStack);
        });
  }

  private WorkflowState onForkBranchEmitAcknowledged(
      WorkflowState state, EngineEvent.ForkBranchEmitAcknowledged event) {
    return updateForkEffectState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        event.allBranchesBlocked(),
        branch -> {
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          if (laneStack.isEmpty()
              || !laneStack.getLast().eventing()
              || laneStack.getLast().event().kind() != EventExecutionFrame.Kind.EMIT
              || !laneStack.getLast().event().operationId().equals(event.operationId())) {
            throw new IllegalStateException(
                "Persisted fork emit acknowledgement has no matching frame");
          }
          laneStack.removeLast();
          return branch.advance(event.output(), event.context(), event.nextStep(), laneStack);
        });
  }

  private WorkflowState onForkBranchHttpCallCompleted(
      WorkflowState state, EngineEvent.ForkBranchHttpCallCompleted event) {
    return updateForkEffectState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        event.allBranchesBlocked(),
        branch -> {
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          if (laneStack.isEmpty()
              || !laneStack.getLast().eventing()
              || laneStack.getLast().event().kind() != EventExecutionFrame.Kind.HTTP_CALL
              || !laneStack.getLast().event().operationId().equals(event.operationId())) {
            throw new IllegalStateException("Persisted fork HTTP result has no matching frame");
          }
          laneStack.removeLast();
          return branch.advance(event.output(), event.context(), event.nextStep(), laneStack);
        });
  }

  private WorkflowState onForkBranchProtocolCallItemAccepted(
      WorkflowState state, EngineEvent.ForkBranchProtocolCallItemAccepted event) {
    return updateForkEffectState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        event.allBranchesBlocked(),
        branch -> {
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          if (laneStack.isEmpty()
              || !laneStack.getLast().eventing()
              || laneStack.getLast().event().kind() != EventExecutionFrame.Kind.PROTOCOL_CALL
              || !laneStack.getLast().event().operationId().equals(event.operationId())) {
            throw new IllegalStateException("Persisted fork protocol item has no matching frame");
          }
          TaskExecutionFrame current = laneStack.removeLast();
          laneStack.add(
              TaskExecutionFrame.eventing(
                  current.taskPath(),
                  current.rawInput(),
                  current.input(),
                  current.event().acceptProtocolItem(event.item())));
          return branch.advance(branch.data(), branch.nextStep(), laneStack);
        });
  }

  private WorkflowState onForkBranchProtocolCallCompleted(
      WorkflowState state, EngineEvent.ForkBranchProtocolCallCompleted event) {
    return updateForkEffectState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        event.allBranchesBlocked(),
        branch -> {
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          if (laneStack.isEmpty()
              || !laneStack.getLast().eventing()
              || laneStack.getLast().event().kind() != EventExecutionFrame.Kind.PROTOCOL_CALL
              || !laneStack.getLast().event().operationId().equals(event.operationId())) {
            throw new IllegalStateException(
                "Persisted fork protocol completion has no matching frame");
          }
          laneStack.removeLast();
          return branch.advance(event.output(), event.context(), event.nextStep(), laneStack);
        });
  }

  private WorkflowState onForkBranchProtocolCallIterationStarted(
      WorkflowState state, EngineEvent.ForkBranchProtocolCallIterationStarted event) {
    return updateForkEffectState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        event.allBranchesBlocked(),
        branch -> {
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          if (laneStack.isEmpty()
              || !laneStack.getLast().eventing()
              || laneStack.getLast().event().kind() != EventExecutionFrame.Kind.PROTOCOL_CALL
              || !laneStack.getLast().event().operationId().equals(event.operationId())) {
            throw new IllegalStateException(
                "Persisted fork protocol iteration has no matching frame");
          }
          laneStack.removeLast();
          laneStack.add(
              new TaskExecutionFrame(
                  event.taskPath(),
                  event.rawInput(),
                  event.input(),
                  event.collection(),
                  0,
                  event.itemVariable(),
                  event.indexVariable()));
          return branch.advance(event.input(), event.nextStep(), laneStack);
        });
  }

  private WorkflowState onForkBranchProtocolCallIterationAdvanced(
      WorkflowState state, EngineEvent.ForkBranchProtocolCallIterationAdvanced event) {
    return updateForkEffectState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        event.allBranchesBlocked(),
        branch -> {
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          if (laneStack.isEmpty()
              || !laneStack.getLast().iterating()
              || !laneStack.getLast().taskPath().equals(event.taskPath())) {
            throw new IllegalStateException(
                "Persisted fork protocol iteration advance has no frame");
          }
          if (event.completed()) laneStack.removeLast();
          else
            laneStack.set(
                laneStack.size() - 1,
                laneStack.getLast().advance(event.iterationIndex(), event.collection()));
          return branch.advance(event.data(), event.context(), event.nextStep(), laneStack);
        });
  }

  private WorkflowState onForkBranchListenAccepted(
      WorkflowState state, EngineEvent.ForkBranchListenAccepted event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    TaskExecutionFrame root = matchingForkTask(stack, event.rootForkTaskPath());
    ForkExecutionFrame next = root.fork();
    for (EngineEvent.ForkListenUpdate update : event.updates()) {
      next = applyForkListenUpdate(next, update);
    }
    if (hasActiveForkListeners(next) != event.hasActiveListeners()) {
      throw new IllegalStateException(
          "Persisted fork listener availability does not match its tree");
    }
    return replaceForkEffectState(state, event, stack, root, next, event.allBranchesBlocked());
  }

  private WorkflowState onForkBranchListenIterationAdvanced(
      WorkflowState state, EngineEvent.ForkBranchListenIterationAdvanced event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    TaskExecutionFrame root = matchingForkTask(stack, event.rootForkTaskPath());
    ForkExecutionFrame next =
        updateForkTree(
            root.fork(),
            event.branchPath(),
            0,
            branch -> {
              var laneStack = new java.util.ArrayList<>(branch.taskStack());
              if (laneStack.isEmpty()
                  || !laneStack.getLast().iterating()
                  || !laneStack.getLast().taskPath().equals(event.taskPath())) {
                throw new IllegalStateException(
                    "Persisted fork listen iteration has no matching frame");
              }
              if (event.completed()) laneStack.removeLast();
              else
                laneStack.set(
                    laneStack.size() - 1,
                    laneStack.getLast().advance(event.iterationIndex(), event.collection()));
              return branch.advance(event.data(), event.context(), event.nextStep(), laneStack);
            });
    if (hasActiveForkListeners(next) != event.hasActiveListeners()) {
      throw new IllegalStateException(
          "Persisted fork listener availability does not match its tree");
    }
    return replaceForkEffectState(state, event, stack, root, next, event.allBranchesBlocked());
  }

  private WorkflowState onForkBranchEffectSkipped(
      WorkflowState.Running state, EngineEvent.ForkBranchEffectSkipped event) {
    return updateForkEffectState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        event.allBranchesBlocked(),
        branch ->
            branch.advance(event.output(), event.context(), event.nextStep(), branch.taskStack()));
  }

  private WorkflowState onForkBranchSubworkflowRequested(
      WorkflowState.Running state, EngineEvent.ForkBranchSubworkflowRequested event) {
    return updateForkEffectState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        event.allBranchesBlocked(),
        branch -> {
          if (!event.await()) {
            return branch.advance(
                event.detachedOutput(), event.detachedContext(),
                event.nextStep(), branch.taskStack());
          }
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          laneStack.add(
              TaskExecutionFrame.eventing(
                  event.taskPath(),
                  event.rawInput(),
                  event.input(),
                  EventExecutionFrame.subworkflow(event.operationId())));
          return branch.advance(event.input(), branch.context(), branch.nextStep(), laneStack);
        });
  }

  private WorkflowState onForkBranchSubworkflowCompleted(
      WorkflowState state, EngineEvent.ForkBranchSubworkflowCompleted event) {
    return updateForkEffectState(
        state,
        event.rootForkTaskPath(),
        event.branchPath(),
        event,
        event.allBranchesBlocked(),
        branch -> {
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          if (laneStack.isEmpty()
              || !laneStack.getLast().eventing()
              || laneStack.getLast().event().kind() != EventExecutionFrame.Kind.SUBWORKFLOW
              || !laneStack.getLast().event().operationId().equals(event.operationId())) {
            throw new IllegalStateException(
                "Persisted fork child completion has no matching frame");
          }
          laneStack.removeLast();
          return branch.advance(event.output(), event.context(), event.nextStep(), laneStack);
        });
  }

  private WorkflowState updateForkEffectState(
      WorkflowState state,
      String rootTaskPath,
      List<Integer> branchPath,
      EngineEvent event,
      boolean persistedBlocked,
      java.util.function.UnaryOperator<ForkBranchState> update) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    TaskExecutionFrame root = matchingForkTask(stack, rootTaskPath);
    ForkExecutionFrame next = updateForkTree(root.fork(), branchPath, 0, update);
    return replaceForkEffectState(state, event, stack, root, next, persistedBlocked);
  }

  private WorkflowState replaceForkEffectState(
      WorkflowState state,
      EngineEvent event,
      java.util.ArrayList<TaskExecutionFrame> stack,
      TaskExecutionFrame root,
      ForkExecutionFrame next,
      boolean persistedBlocked) {
    stack.set(stack.size() - 1, root.withFork(next));
    boolean actuallyBlocked = !next.complete() && !forkHasRunnable(next);
    if (actuallyBlocked != persistedBlocked) {
      throw new IllegalStateException("Persisted fork effect availability does not match its tree");
    }
    long revision = state.revision() + 1;
    Set<UUID> receipts = receipts(state.processedCommands(), event.commandId());
    if (actuallyBlocked) {
      return new WorkflowState.Waiting(
          state.executionId(),
          activePlan(state),
          state.data(),
          activeNextStep(state),
          revision,
          receipts,
          "fork-blocked:" + root.taskPath(),
          earliestForkWaitOrNull(next),
          state.context(),
          state.rawWorkflowInput(),
          stack,
          state.workflowDeadline());
    }
    return new WorkflowState.Running(
        state.executionId(),
        activePlan(state),
        state.data(),
        activeNextStep(state),
        revision,
        receipts,
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onForkBranchWaitCompleted(
      WorkflowState.Running state, EngineEvent.ForkBranchWaitCompleted event) {
    List<TaskExecutionFrame> stack = completeForkWaitStack(state.taskStack(), event);
    TaskExecutionFrame root = activeFork(stack);
    boolean actuallyWaiting = root != null && !forkHasRunnable(root.fork());
    if (actuallyWaiting != event.allBranchesWaiting()) {
      throw new IllegalStateException(
          "Persisted fork wait completion availability is inconsistent");
    }
    if (actuallyWaiting) {
      return new WorkflowState.Waiting(
          state.executionId(),
          state.plan(),
          state.data(),
          state.nextStep(),
          state.revision() + 1,
          receipts(state.processedCommands(), event.commandId()),
          "fork-wait:" + event.rootForkTaskPath(),
          earliestForkWaitOrNull(root.fork()),
          state.context(),
          state.rawWorkflowInput(),
          stack,
          state.workflowDeadline());
    }
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onForkBranchesWaiting(
      WorkflowState.Running state, EngineEvent.ForkBranchesWaiting event) {
    TaskExecutionFrame root = activeFork(state.taskStack());
    if (root == null
        || !root.taskPath().equals(event.rootForkTaskPath())
        || forkHasRunnable(root.fork())
        || !java.util.Objects.equals(earliestForkWaitOrNull(root.fork()), event.deadline())) {
      throw new IllegalStateException("Persisted fork blocked fact does not match its fork tree");
    }
    return new WorkflowState.Waiting(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        (event.deadline() == null ? "fork-blocked:" : "fork-wait:") + event.rootForkTaskPath(),
        event.deadline(),
        state.context(),
        state.rawWorkflowInput(),
        state.taskStack(),
        state.workflowDeadline());
  }

  private WorkflowState onForkBranchContextUpdated(
      WorkflowState.Running state, EngineEvent.ForkBranchContextUpdated event) {
    var stack =
        updateForkStack(
            state.taskStack(),
            event.rootForkTaskPath(),
            event.branchPath(),
            branch ->
                branch.advance(
                    branch.data(), event.context(), branch.nextStep(), branch.taskStack()));
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onForkBranchContextUpdated(
      WorkflowState.Waiting state, EngineEvent.ForkBranchContextUpdated event) {
    var stack =
        updateForkStack(
            state.taskStack(),
            event.rootForkTaskPath(),
            event.branchPath(),
            branch ->
                branch.advance(
                    branch.data(), event.context(), branch.nextStep(), branch.taskStack()));
    return new WorkflowState.Waiting(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.reason(),
        state.deadline(),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onForkBranchTryEntered(
      WorkflowState.Running state, EngineEvent.ForkBranchTryEntered event) {
    var stack =
        updateForkStack(
            state.taskStack(),
            event.rootForkTaskPath(),
            event.branchPath(),
            branch -> {
              var frames = new java.util.ArrayList<>(branch.taskStack());
              frames.add(
                  TaskExecutionFrame.trying(
                      event.taskPath(), event.rawInput(), event.input(), event.occurredAt()));
              return branch.advance(event.input(), event.nextStep(), frames);
            });
    return runningWithForkEvent(state, event, stack);
  }

  private WorkflowState onForkBranchTryCompleted(
      WorkflowState.Running state, EngineEvent.ForkBranchTryCompleted event) {
    var stack =
        updateForkStack(
            state.taskStack(),
            event.rootForkTaskPath(),
            event.branchPath(),
            branch -> {
              var frames = stackThroughTry(branch.taskStack(), event.taskPath());
              frames.removeLast();
              return branch.advance(event.output(), event.context(), event.nextStep(), frames);
            });
    return runningWithForkEvent(state, event, stack);
  }

  private WorkflowState onForkBranchErrorCaught(
      WorkflowState.Running state, EngineEvent.ForkBranchErrorCaught event) {
    var stack =
        updateForkStack(
            state.taskStack(),
            event.rootForkTaskPath(),
            event.branchPath(),
            branch -> {
              var frames = stackThroughTry(branch.taskStack(), event.tryTaskPath());
              frames.set(frames.size() - 1, frames.getLast().caught(event.error()));
              return branch.advance(branch.data(), event.nextStep(), frames);
            });
    return runningWithForkEvent(state, event, stack);
  }

  private WorkflowState onForkBranchErrorCaught(
      WorkflowState.Waiting state, EngineEvent.ForkBranchErrorCaught event) {
    return onForkBranchErrorCaught(running(state), event);
  }

  private WorkflowState onForkBranchRetryScheduled(
      WorkflowState.Running state, EngineEvent.ForkBranchRetryScheduled event) {
    var stack =
        updateForkStack(
            state.taskStack(),
            event.rootForkTaskPath(),
            event.branchPath(),
            branch -> {
              var frames = stackThroughTry(branch.taskStack(), event.tryTaskPath());
              TaskExecutionFrame frame = frames.getLast();
              frames.set(
                  frames.size() - 1,
                  frame.retrying(
                      event.error(),
                      event.nextAttempt(),
                      event.deadline(),
                      event.retryStartedAt(),
                      event.occurredAt()));
              return branch.advance(frame.input(), event.retryStep(), frames);
            });
    TaskExecutionFrame root = activeFork(stack);
    boolean blocked = root != null && !forkHasRunnable(root.fork());
    if (blocked != event.allBranchesWaiting()) {
      throw new IllegalStateException("Persisted branch retry availability differs");
    }
    if (blocked) {
      return new WorkflowState.Waiting(
          state.executionId(),
          state.plan(),
          state.data(),
          state.nextStep(),
          state.revision() + 1,
          receipts(state.processedCommands(), event.commandId()),
          "fork-retry:" + event.tryTaskPath(),
          earliestForkWait(root.fork()),
          state.context(),
          state.rawWorkflowInput(),
          stack,
          state.workflowDeadline());
    }
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onForkBranchRetryScheduled(
      WorkflowState.Waiting state, EngineEvent.ForkBranchRetryScheduled event) {
    return onForkBranchRetryScheduled(running(state), event);
  }

  private WorkflowState onForkBranchRetryStarted(
      WorkflowState.Running state, EngineEvent.ForkBranchRetryStarted event) {
    return branchRetryStarted(state, event);
  }

  private WorkflowState onForkBranchRetryStarted(
      WorkflowState.Waiting state, EngineEvent.ForkBranchRetryStarted event) {
    var stack =
        updateForkStack(
            state.taskStack(),
            event.rootForkTaskPath(),
            event.branchPath(),
            branch -> {
              var frames = stackThroughTry(branch.taskStack(), event.tryTaskPath());
              frames.set(frames.size() - 1, frames.getLast().beginRetry(event.occurredAt()));
              return branch.advance(frames.getLast().input(), event.nextStep(), frames);
            });
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState branchRetryStarted(
      WorkflowState.Running state, EngineEvent.ForkBranchRetryStarted event) {
    var stack =
        updateForkStack(
            state.taskStack(),
            event.rootForkTaskPath(),
            event.branchPath(),
            branch -> {
              var frames = stackThroughTry(branch.taskStack(), event.tryTaskPath());
              frames.set(frames.size() - 1, frames.getLast().beginRetry(event.occurredAt()));
              return branch.advance(frames.getLast().input(), event.nextStep(), frames);
            });
    return runningWithForkEvent(state, event, stack);
  }

  private WorkflowState onForkBranchWaitCompleted(
      WorkflowState.Waiting state, EngineEvent.ForkBranchWaitCompleted event) {
    List<TaskExecutionFrame> stack = completeForkWaitStack(state.taskStack(), event);
    TaskExecutionFrame root = activeFork(stack);
    boolean actuallyWaiting = root != null && !forkHasRunnable(root.fork());
    if (actuallyWaiting != event.allBranchesWaiting()) {
      throw new IllegalStateException(
          "Persisted fork wait completion availability is inconsistent");
    }
    if (actuallyWaiting) {
      return new WorkflowState.Waiting(
          state.executionId(),
          state.plan(),
          state.data(),
          state.nextStep(),
          state.revision() + 1,
          receipts(state.processedCommands(), event.commandId()),
          "fork-wait:" + event.rootForkTaskPath(),
          earliestForkWaitOrNull(root.fork()),
          state.context(),
          state.rawWorkflowInput(),
          stack,
          state.workflowDeadline());
    }
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private static List<TaskExecutionFrame> completeForkWaitStack(
      List<TaskExecutionFrame> existing, EngineEvent.ForkBranchWaitCompleted event) {
    return updateForkStack(
        existing,
        event.rootForkTaskPath(),
        event.branchPath(),
        branch -> {
          var laneStack = new java.util.ArrayList<>(branch.taskStack());
          if (laneStack.isEmpty()
              || !laneStack.getLast().waiting()
              || !laneStack.getLast().taskPath().equals(event.taskPath())) {
            throw new IllegalStateException(
                "Persisted fork wait completion does not match its lane");
          }
          laneStack.removeLast();
          return branch.advance(event.output(), event.nextStep(), laneStack);
        });
  }

  private static java.util.ArrayList<TaskExecutionFrame> updateForkStack(
      List<TaskExecutionFrame> existing,
      String rootTaskPath,
      List<Integer> branchPath,
      java.util.function.UnaryOperator<ForkBranchState> leafUpdate) {
    var stack = new java.util.ArrayList<>(existing);
    TaskExecutionFrame root = matchingForkTask(stack, rootTaskPath);
    stack.set(
        stack.size() - 1, root.withFork(updateForkTree(root.fork(), branchPath, 0, leafUpdate)));
    return stack;
  }

  private static Instant earliestForkWait(ForkExecutionFrame fork) {
    Instant earliest = earliestForkWaitOrNull(fork);
    if (earliest == null) {
      throw new IllegalStateException("Blocked fork has no durable wait deadline");
    }
    return earliest;
  }

  private static Instant earliestForkWaitOrNull(ForkExecutionFrame fork) {
    Instant earliest = null;
    for (ForkBranchState branch : fork.branches()) {
      if (branch.taskStack().isEmpty()) continue;
      TaskExecutionFrame frame = branch.taskStack().getLast();
      Instant candidate = null;
      if (frame.waiting()) candidate = frame.waitDeadline();
      else if (frame.forking()) candidate = earliestForkWaitOrNull(frame.fork());
      if (candidate != null && (earliest == null || candidate.isBefore(earliest))) {
        earliest = candidate;
      }
    }
    return earliest;
  }

  private WorkflowState updateNestedForkState(
      WorkflowState.Running state,
      String rootTaskPath,
      List<Integer> branchPath,
      EngineEvent event,
      java.util.function.UnaryOperator<ForkBranchState> leafUpdate) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    TaskExecutionFrame root = matchingForkTask(stack, rootTaskPath);
    ForkExecutionFrame updated = updateForkTree(root.fork(), branchPath, 0, leafUpdate);
    stack.set(stack.size() - 1, root.withFork(updated));
    return runningWithForkEvent(state, event, stack);
  }

  private static ForkExecutionFrame updateForkTree(
      ForkExecutionFrame fork,
      List<Integer> branchPath,
      int depth,
      java.util.function.UnaryOperator<ForkBranchState> leafUpdate) {
    int branchIndex = branchPath.get(depth);
    if (branchIndex < 0 || branchIndex >= fork.branches().size()) {
      throw new IllegalStateException("Persisted nested fork path is outside the tree");
    }
    ForkBranchState branch = fork.branches().get(branchIndex);
    ForkBranchState updatedBranch;
    if (depth == branchPath.size() - 1) {
      updatedBranch = leafUpdate.apply(branch);
    } else {
      var laneStack = new java.util.ArrayList<>(branch.taskStack());
      if (laneStack.isEmpty() || !laneStack.getLast().forking()) {
        throw new IllegalStateException("Persisted nested fork path has no fork frame");
      }
      TaskExecutionFrame nested = laneStack.getLast();
      ForkExecutionFrame updatedNested =
          updateForkTree(nested.fork(), branchPath, depth + 1, leafUpdate);
      laneStack.set(laneStack.size() - 1, nested.withFork(updatedNested));
      updatedBranch =
          new ForkBranchState(
              branch.name(),
              branch.data(),
              branch.context(),
              branch.nextStep(),
              branch.endStep(),
              laneStack,
              branch.completed());
    }
    var branches = new java.util.ArrayList<>(fork.branches());
    branches.set(branchIndex, updatedBranch);
    Integer winner = fork.winner();
    if (winner == null && fork.compete() && updatedBranch.completed()) {
      winner = branchIndex;
    }
    int nextBranch =
        winner != null
            ? branchIndex
            : nextRunnableBranchAfter(fork, branchIndex, updatedBranch.completed());
    return new ForkExecutionFrame(fork.compete(), branches, nextBranch, winner);
  }

  private record ForkEventSelection(
      List<Integer> path, ForkBranchState branch, TaskExecutionFrame frame) {}

  private static ForkEventSelection findForkEvent(
      ForkExecutionFrame fork,
      EventExecutionFrame.Kind kind,
      String operationId,
      java.util.ArrayList<Integer> path) {
    for (int index = 0; index < fork.branches().size(); index++) {
      ForkBranchState branch = fork.branches().get(index);
      if (branch.taskStack().isEmpty()) continue;
      path.add(index);
      TaskExecutionFrame frame = branch.taskStack().getLast();
      if (frame.eventing()
          && frame.event().kind() == kind
          && (operationId == null || frame.event().operationId().equals(operationId))) {
        ForkEventSelection found = new ForkEventSelection(List.copyOf(path), branch, frame);
        path.removeLast();
        return found;
      }
      if (frame.forking()) {
        ForkEventSelection found = findForkEvent(frame.fork(), kind, operationId, path);
        if (found != null) {
          path.removeLast();
          return found;
        }
      }
      path.removeLast();
    }
    return null;
  }

  private static void collectForkEvents(
      ForkExecutionFrame fork,
      EventExecutionFrame.Kind kind,
      java.util.ArrayList<Integer> path,
      java.util.List<ForkEventSelection> selections) {
    for (int index = 0; index < fork.branches().size(); index++) {
      ForkBranchState branch = fork.branches().get(index);
      if (branch.taskStack().isEmpty()) continue;
      path.add(index);
      TaskExecutionFrame frame = branch.taskStack().getLast();
      if (frame.eventing() && frame.event().kind() == kind) {
        selections.add(new ForkEventSelection(List.copyOf(path), branch, frame));
      } else if (frame.forking()) {
        collectForkEvents(frame.fork(), kind, path, selections);
      }
      path.removeLast();
    }
  }

  private static ForkExecutionFrame applyForkListenUpdate(
      ForkExecutionFrame fork, EngineEvent.ForkListenUpdate update) {
    return updateForkTree(
        fork,
        update.branchPath(),
        0,
        branch -> {
          var stack = new java.util.ArrayList<>(branch.taskStack());
          if (stack.isEmpty()
              || !stack.getLast().eventing()
              || stack.getLast().event().kind() != EventExecutionFrame.Kind.LISTEN
              || !stack.getLast().event().operationId().equals(update.operationId())) {
            throw new IllegalStateException("Persisted fork listen update has no matching frame");
          }
          TaskExecutionFrame frame = stack.getLast();
          return switch (update.disposition()) {
            case PARTIAL -> {
              stack.set(
                  stack.size() - 1,
                  frame.withEvent(
                      new EventExecutionFrame(
                          EventExecutionFrame.Kind.LISTEN,
                          update.operationId(),
                          null,
                          update.accepted(),
                          update.correlations(),
                          update.matchedFilters(),
                          update.untilWindow())));
              yield branch.advance(branch.data(), branch.context(), update.nextStep(), stack);
            }
            case COMPLETE -> {
              stack.removeLast();
              yield branch.advance(update.output(), update.context(), update.nextStep(), stack);
            }
            case ITERATE -> {
              stack.removeLast();
              stack.add(
                  new TaskExecutionFrame(
                      update.taskPath(),
                      frame.rawInput(),
                      frame.input(),
                      update.collection(),
                      0,
                      update.itemVariable(),
                      update.indexVariable()));
              yield branch.advance(frame.input(), branch.context(), update.nextStep(), stack);
            }
          };
        });
  }

  private static boolean hasActiveForkListeners(ForkExecutionFrame fork) {
    for (ForkBranchState branch : fork.branches()) {
      if (branch.taskStack().isEmpty()) continue;
      TaskExecutionFrame frame = branch.taskStack().getLast();
      if (frame.eventing() && frame.event().kind() == EventExecutionFrame.Kind.LISTEN) {
        return true;
      }
      if (frame.forking() && hasActiveForkListeners(frame.fork())) return true;
    }
    return false;
  }

  private static com.forwardmeasure.openworkflow.definition.WorkflowPlan activePlan(
      WorkflowState state) {
    if (state instanceof WorkflowState.Running running) return running.plan();
    if (state instanceof WorkflowState.Waiting waiting) return waiting.plan();
    throw new IllegalStateException("Workflow state has no active plan");
  }

  private static int activeNextStep(WorkflowState state) {
    if (state instanceof WorkflowState.Running running) return running.nextStep();
    if (state instanceof WorkflowState.Waiting waiting) return waiting.nextStep();
    throw new IllegalStateException("Workflow state has no active cursor");
  }

  private static TaskExecutionFrame matchingForkTask(
      java.util.ArrayList<TaskExecutionFrame> stack, String taskPath) {
    if (stack.isEmpty()
        || !stack.getLast().forking()
        || !stack.getLast().taskPath().equals(taskPath)) {
      throw new IllegalStateException(
          "Persisted fork task event does not match the active task stack");
    }
    return stack.getLast();
  }

  private static WorkflowState.Running runningWithForkEvent(
      WorkflowState.Running state,
      EngineEvent event,
      java.util.ArrayList<TaskExecutionFrame> stack) {
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onErrorRaised(WorkflowState.Running state, EngineEvent.ErrorRaised event) {
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        state.taskStack(),
        state.workflowDeadline());
  }

  private WorkflowState onErrorRaised(WorkflowState.Waiting state, EngineEvent.ErrorRaised event) {
    return new WorkflowState.Waiting(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.reason(),
        state.deadline(),
        state.context(),
        state.rawWorkflowInput(),
        state.taskStack(),
        state.workflowDeadline());
  }

  private WorkflowState onErrorCaught(WorkflowState.Running state, EngineEvent.ErrorCaught event) {
    var stack = stackThroughTry(state.taskStack(), event.tryTaskPath());
    stack.set(stack.size() - 1, stack.getLast().caught(event.error()));
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        state.data(),
        event.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onErrorCaught(WorkflowState.Waiting state, EngineEvent.ErrorCaught event) {
    return onErrorCaught(running(state), event);
  }

  private WorkflowState onRetryScheduled(
      WorkflowState.Running state, EngineEvent.RetryScheduled event) {
    var stack = stackThroughTry(state.taskStack(), event.tryTaskPath());
    TaskExecutionFrame frame = stack.getLast();
    stack.set(
        stack.size() - 1,
        frame.retrying(
            event.error(),
            event.nextAttempt(),
            event.deadline(),
            event.retryStartedAt(),
            event.occurredAt()));
    return new WorkflowState.Waiting(
        state.executionId(),
        state.plan(),
        frame.input(),
        event.retryStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        "retry:" + event.tryTaskPath(),
        event.deadline(),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onRetryScheduled(
      WorkflowState.Waiting state, EngineEvent.RetryScheduled event) {
    return onRetryScheduled(running(state), event);
  }

  private WorkflowState onRetryStarted(
      WorkflowState.Waiting state, EngineEvent.RetryStarted event) {
    var stack = stackThroughTry(state.taskStack(), event.tryTaskPath());
    TaskExecutionFrame frame = stack.getLast();
    if (frame.tryPhase() != TaskExecutionFrame.TryPhase.RETRY_DELAY
        || frame.attempt() != event.attempt()) {
      throw new IllegalStateException("Persisted retry does not match try frame");
    }
    stack.set(stack.size() - 1, frame.beginRetry(event.occurredAt()));
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        frame.input(),
        event.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private static java.util.ArrayList<TaskExecutionFrame> stackThroughTry(
      List<TaskExecutionFrame> existing, String taskPath) {
    var stack = new java.util.ArrayList<>(existing);
    int index = -1;
    for (int candidate = stack.size() - 1; candidate >= 0; candidate--) {
      TaskExecutionFrame frame = stack.get(candidate);
      if (frame.trying() && frame.taskPath().equals(taskPath)) {
        index = candidate;
        break;
      }
    }
    if (index < 0) {
      throw new IllegalStateException("Persisted error does not match an active try scope");
    }
    while (stack.size() > index + 1) stack.removeLast();
    return stack;
  }

  private WorkflowState onWaitCompleted(
      WorkflowState.Waiting state, EngineEvent.TaskCompleted event) {
    var stack = new java.util.ArrayList<>(state.taskStack());
    if (stack.isEmpty()
        || !stack.getLast().waiting()
        || !stack.getLast().taskPath().equals(event.taskPath())) {
      throw new IllegalStateException("Completed wait does not match the active task stack");
    }
    stack.removeLast();
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        event.output(),
        event.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        event.context(),
        state.rawWorkflowInput(),
        stack,
        state.workflowDeadline());
  }

  private WorkflowState onDeadlineScheduled(
      WorkflowState.Running state, EngineEvent.DeadlineScheduled event) {
    List<TaskExecutionFrame> stack = deadlineStack(state.taskStack(), event);
    Instant workflowDeadline =
        event.scope() == DeadlineScope.WORKFLOW ? event.deadline() : state.workflowDeadline();
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        workflowDeadline);
  }

  private WorkflowState onDeadlineScheduled(
      WorkflowState.Waiting state, EngineEvent.DeadlineScheduled event) {
    List<TaskExecutionFrame> stack = deadlineStack(state.taskStack(), event);
    Instant workflowDeadline =
        event.scope() == DeadlineScope.WORKFLOW ? event.deadline() : state.workflowDeadline();
    return new WorkflowState.Waiting(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.reason(),
        state.deadline(),
        state.context(),
        state.rawWorkflowInput(),
        stack,
        workflowDeadline);
  }

  private static List<TaskExecutionFrame> deadlineStack(
      List<TaskExecutionFrame> existing, EngineEvent.DeadlineScheduled event) {
    if (event.scope() == DeadlineScope.WORKFLOW) return existing;
    var stack = new java.util.ArrayList<>(existing);
    boolean found = false;
    for (int index = stack.size() - 1; index >= 0 && !found; index--) {
      TaskExecutionFrame frame = stack.get(index);
      TaskExecutionFrame updated = withTaskDeadline(frame, event.taskPath(), event.deadline());
      if (updated != frame) {
        stack.set(index, updated);
        found = true;
      }
    }
    if (!found) {
      throw new IllegalStateException(
          "Persisted task deadline does not match the active task stack");
    }
    return stack;
  }

  private static TaskExecutionFrame withTaskDeadline(
      TaskExecutionFrame frame, String taskPath, Instant deadline) {
    if (frame.taskPath().equals(taskPath)) return frame.withTimeout(deadline);
    if (!frame.forking()) return frame;
    var branches = new java.util.ArrayList<>(frame.fork().branches());
    for (int branchIndex = 0; branchIndex < branches.size(); branchIndex++) {
      ForkBranchState branch = branches.get(branchIndex);
      var laneStack = new java.util.ArrayList<>(branch.taskStack());
      for (int index = laneStack.size() - 1; index >= 0; index--) {
        TaskExecutionFrame nested = laneStack.get(index);
        TaskExecutionFrame updated = withTaskDeadline(nested, taskPath, deadline);
        if (updated != nested) {
          laneStack.set(index, updated);
          branches.set(
              branchIndex,
              new ForkBranchState(
                  branch.name(),
                  branch.data(),
                  branch.context(),
                  branch.nextStep(),
                  branch.endStep(),
                  laneStack,
                  branch.completed()));
          return frame.withFork(
              new ForkExecutionFrame(
                  frame.fork().compete(),
                  branches,
                  frame.fork().nextBranch(),
                  frame.fork().winner()));
        }
      }
    }
    return frame;
  }

  private WorkflowState onCompleted(WorkflowState.Running state, EngineEvent.Completed event) {
    return new WorkflowState.Completed(
        state.executionId(),
        event.output(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()));
  }

  private WorkflowState onFailed(WorkflowState.Running state, EngineEvent.Failed event) {
    return new WorkflowState.Failed(
        state.executionId(),
        state.data(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        event.message());
  }

  private WorkflowState onFailed(WorkflowState.Waiting state, EngineEvent.Failed event) {
    return new WorkflowState.Failed(
        state.executionId(),
        state.data(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        event.message());
  }

  private WorkflowState onPauseRequested(
      WorkflowState.Running state, EngineEvent.PauseRequested event) {
    return new WorkflowState.Pausing(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        state.taskStack(),
        state.workflowDeadline());
  }

  private WorkflowState onPauseRequested(
      WorkflowState.Waiting state, EngineEvent.PauseRequested event) {
    return new WorkflowState.Pausing(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        state.taskStack(),
        state.workflowDeadline());
  }

  private WorkflowState onPaused(WorkflowState.Pausing state, EngineEvent.Paused event) {
    return new WorkflowState.Paused(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        state.taskStack(),
        state.workflowDeadline());
  }

  private WorkflowState onResumed(WorkflowState.Paused state, EngineEvent.Resumed event) {
    TaskExecutionFrame rootFork = activeFork(state.taskStack());
    if (rootFork != null && !forkHasRunnable(rootFork.fork())) {
      return new WorkflowState.Waiting(
          state.executionId(),
          state.plan(),
          state.data(),
          state.nextStep(),
          state.revision() + 1,
          receipts(state.processedCommands(), event.commandId()),
          "fork-blocked:" + rootFork.taskPath(),
          earliestForkWaitOrNull(rootFork.fork()),
          state.context(),
          state.rawWorkflowInput(),
          state.taskStack(),
          state.workflowDeadline());
    }
    if (!state.taskStack().isEmpty() && state.taskStack().getLast().eventing()) {
      TaskExecutionFrame frame = state.taskStack().getLast();
      return new WorkflowState.Waiting(
          state.executionId(),
          state.plan(),
          state.data(),
          state.nextStep(),
          state.revision() + 1,
          receipts(state.processedCommands(), event.commandId()),
          frame.event().kind().name().toLowerCase(java.util.Locale.ROOT)
              + ":"
              + frame.event().operationId(),
          null,
          state.context(),
          state.rawWorkflowInput(),
          state.taskStack(),
          state.workflowDeadline());
    }
    if (!state.taskStack().isEmpty() && state.taskStack().getLast().waiting()) {
      TaskExecutionFrame frame = state.taskStack().getLast();
      return new WorkflowState.Waiting(
          state.executionId(),
          state.plan(),
          state.data(),
          state.nextStep(),
          state.revision() + 1,
          receipts(state.processedCommands(), event.commandId()),
          (frame.tryPhase() == TaskExecutionFrame.TryPhase.RETRY_DELAY ? "retry:" : "wait:")
              + frame.taskPath(),
          frame.waitDeadline(),
          state.context(),
          state.rawWorkflowInput(),
          state.taskStack(),
          state.workflowDeadline());
    }
    return new WorkflowState.Running(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()),
        state.context(),
        state.rawWorkflowInput(),
        state.taskStack(),
        state.workflowDeadline());
  }

  private WorkflowState onCancellationRequested(
      WorkflowState.Running state, EngineEvent.CancellationRequested event) {
    return cancelling(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision(),
        state.processedCommands(),
        event.commandId(),
        state.context(),
        state.rawWorkflowInput(),
        state.taskStack(),
        state.workflowDeadline());
  }

  private WorkflowState onCancellationRequested(
      WorkflowState.Waiting state, EngineEvent.CancellationRequested event) {
    return cancelling(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision(),
        state.processedCommands(),
        event.commandId(),
        state.context(),
        state.rawWorkflowInput(),
        state.taskStack(),
        state.workflowDeadline());
  }

  private WorkflowState onCancellationRequested(
      WorkflowState.Pausing state, EngineEvent.CancellationRequested event) {
    return cancelling(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision(),
        state.processedCommands(),
        event.commandId(),
        state.context(),
        state.rawWorkflowInput(),
        state.taskStack(),
        state.workflowDeadline());
  }

  private WorkflowState onCancellationRequested(
      WorkflowState.Paused state, EngineEvent.CancellationRequested event) {
    return cancelling(
        state.executionId(),
        state.plan(),
        state.data(),
        state.nextStep(),
        state.revision(),
        state.processedCommands(),
        event.commandId(),
        state.context(),
        state.rawWorkflowInput(),
        state.taskStack(),
        state.workflowDeadline());
  }

  private static WorkflowState.Cancelling cancelling(
      ExecutionId executionId,
      com.forwardmeasure.openworkflow.definition.WorkflowPlan plan,
      JsonNode data,
      int nextStep,
      long revision,
      Set<UUID> processed,
      UUID commandId,
      JsonNode context,
      JsonNode rawWorkflowInput,
      List<TaskExecutionFrame> taskStack,
      Instant workflowDeadline) {
    return new WorkflowState.Cancelling(
        executionId,
        plan,
        data,
        nextStep,
        revision + 1,
        receipts(processed, commandId),
        context,
        rawWorkflowInput,
        taskStack,
        workflowDeadline);
  }

  private WorkflowState onCancelled(WorkflowState.Cancelling state, EngineEvent.Cancelled event) {
    return new WorkflowState.Cancelled(
        state.executionId(),
        state.data(),
        state.revision() + 1,
        receipts(state.processedCommands(), event.commandId()));
  }

  private static Set<UUID> receipts(Set<UUID> existing, UUID commandId) {
    var result = new LinkedHashSet<>(existing);
    result.add(commandId);
    return result;
  }

  @Override
  public RetentionCriteria retentionCriteria() {
    return RetentionCriteria.snapshotEvery(100, 3);
  }

  @Override
  public SignalHandler<WorkflowState> signalHandler() {
    return newSignalHandlerBuilder()
        .onSignal(RecoveryCompleted.instance(), this::scheduleDeadlines)
        .build();
  }

  @Override
  public Set<String> tagsFor(EngineEvent event) {
    return Set.of(projectionTagFor(executionId));
  }

  public static String projectionTagFor(ExecutionId executionId) {
    Objects.requireNonNull(executionId, "executionId");
    int partition = Math.floorMod(executionId.entityId().hashCode(), PROJECTION_TAG_COUNT);
    return PROJECTION_TAG_PREFIX + partition;
  }

  public static List<String> projectionTags() {
    return java.util.stream.IntStream.range(0, PROJECTION_TAG_COUNT)
        .mapToObj(index -> PROJECTION_TAG_PREFIX + index)
        .toList();
  }
}
