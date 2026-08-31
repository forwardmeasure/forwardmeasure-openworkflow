package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.durableprocessing.api.DurableProcess;
import com.forwardmeasure.durableprocessing.api.DurableProcessContext;
import com.forwardmeasure.durableprocessing.api.DurableTransition;
import com.forwardmeasure.openworkflow.definition.AsyncApiSubscriptionPlan;
import com.forwardmeasure.openworkflow.definition.AuthenticationPlan;
import com.forwardmeasure.openworkflow.definition.CallPlan;
import com.forwardmeasure.openworkflow.definition.DataSchemaValidationException;
import com.forwardmeasure.openworkflow.definition.DataSchemaValidator;
import com.forwardmeasure.openworkflow.definition.DurationPlan;
import com.forwardmeasure.openworkflow.definition.ErrorFilterPlan;
import com.forwardmeasure.openworkflow.definition.ErrorPlan;
import com.forwardmeasure.openworkflow.definition.EventConsumptionPlan;
import com.forwardmeasure.openworkflow.definition.EventFilterPlan;
import com.forwardmeasure.openworkflow.definition.ForPlan;
import com.forwardmeasure.openworkflow.definition.Iso8601Duration;
import com.forwardmeasure.openworkflow.definition.ListenPlan;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.PlanStep;
import com.forwardmeasure.openworkflow.definition.PlanStepKind;
import com.forwardmeasure.openworkflow.definition.RetryPlan;
import com.forwardmeasure.openworkflow.definition.RunPlan;
import com.forwardmeasure.openworkflow.definition.SwitchCasePlan;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import com.forwardmeasure.openworkflow.engine.api.AuthenticationExpressionContext;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationMaterializer;
import com.forwardmeasure.openworkflow.expression.JqRuntimeExpressionEvaluator;
import com.forwardmeasure.openworkflow.expression.RuntimeExpressionArguments;
import com.forwardmeasure.openworkflow.expression.RuntimeExpressionException;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.Actors;
import com.forwardmeasure.openworkflow.workflow.runtime.api.AdvanceExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.BusinessCorrelationId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ControlExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReferenceJson;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionControlAction;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionEventType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionFailure;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionHistoryEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.FireTimerCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ForkPosition;
import com.forwardmeasure.openworkflow.workflow.runtime.api.HumanTaskObservation;
import com.forwardmeasure.openworkflow.workflow.runtime.api.HumanTaskObservationStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.IterationPosition;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ObserveAsyncApiSubscriptionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ObserveHumanTaskCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ObserveOperationCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ObserveWorkflowComputationCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ObserveWorkflowComputationFailureCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservation;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservationStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.PurgeExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ReapplyExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ReceiveAsyncApiMessageCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ReceiveEventCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.RuntimeDataLimitException;
import com.forwardmeasure.openworkflow.workflow.runtime.api.StartExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.SwitchCaseEvaluation;
import com.forwardmeasure.openworkflow.workflow.runtime.api.SwitchDecision;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionBundle;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffectType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowError;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Pure OpenWorkflow state machine layered on the generic durable-processing contract.
 *
 * <p>One advance command crosses at most one durable task boundary. The engine never performs I/O,
 * blocks, assigns aggregate revisions or deduplicates commands. Those responsibilities belong to
 * the durability engine.
 */
public final class WorkflowExecutionEngine
    implements DurableProcess<
        ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect> {
  private static final String RUNTIME_ERROR_TYPE =
      "https://open-workflow-specification.org/spec/1.0.0/errors/runtime";
  private static final String RUNTIME_VERSION =
      java.util.Optional.ofNullable(
              WorkflowExecutionEngine.class.getPackage().getImplementationVersion())
          .orElse("development");
  static final int MAX_DURABLE_STATE_BYTES = 512 * 1024;
  private static final String OPERATION_KIND_RUN_WORKFLOW = "run-workflow";
  private static final ObjectMapper DURABLE_STATE_JSON =
      new ObjectMapper().findAndRegisterModules();
  private final WorkflowDefinitionResolver definitions;
  private final ActorId runtimeActorId;
  private final String runtimeComponent;
  private final Duration cancellationGracePeriod;
  private final WorkflowRuntimeDataAccess dataAccess;
  private final boolean deferredComputationEnabled;
  private final PreparedWorkflowTransitionCodec transitionCodec;
  private final JqRuntimeExpressionEvaluator expressions = new JqRuntimeExpressionEvaluator();
  private final Map<String, DataSchemaValidator> schemaValidators = new HashMap<>();

  public WorkflowExecutionEngine(
      WorkflowDefinitionResolver definitions, ActorId runtimeActorId, String runtimeComponent) {
    this(
        definitions,
        runtimeActorId,
        runtimeComponent,
        Duration.ofSeconds(30),
        WorkflowRuntimeDataAccess.inlineOnly(),
        true);
  }

  public WorkflowExecutionEngine(
      WorkflowDefinitionResolver definitions,
      ActorId runtimeActorId,
      String runtimeComponent,
      Duration cancellationGracePeriod) {
    this(
        definitions,
        runtimeActorId,
        runtimeComponent,
        cancellationGracePeriod,
        WorkflowRuntimeDataAccess.inlineOnly(),
        true);
  }

  public WorkflowExecutionEngine(
      WorkflowDefinitionResolver definitions,
      ActorId runtimeActorId,
      String runtimeComponent,
      Duration cancellationGracePeriod,
      WorkflowRuntimeDataAccess dataAccess) {
    this(definitions, runtimeActorId, runtimeComponent, cancellationGracePeriod, dataAccess, true);
  }

  public WorkflowExecutionEngine(
      WorkflowDefinitionResolver definitions,
      ActorId runtimeActorId,
      String runtimeComponent,
      Duration cancellationGracePeriod,
      WorkflowRuntimeDataAccess dataAccess,
      boolean deferredComputationEnabled) {
    this.definitions = Objects.requireNonNull(definitions, "definitions");
    this.runtimeActorId = Objects.requireNonNull(runtimeActorId, "runtimeActorId");
    this.runtimeComponent = Objects.requireNonNull(runtimeComponent, "runtimeComponent");
    if (runtimeComponent.isBlank()) {
      throw new IllegalArgumentException("runtimeComponent must not be blank");
    }
    this.cancellationGracePeriod =
        Objects.requireNonNull(cancellationGracePeriod, "cancellationGracePeriod");
    if (cancellationGracePeriod.isNegative() || cancellationGracePeriod.isZero()) {
      throw new IllegalArgumentException("cancellationGracePeriod must be positive");
    }
    this.dataAccess = Objects.requireNonNull(dataAccess, "dataAccess");
    this.deferredComputationEnabled = deferredComputationEnabled;
    this.transitionCodec =
        new PreparedWorkflowTransitionCodec(new ObjectMapper().findAndRegisterModules());
  }

  @Override
  public DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      decide(DurableProcessContext context, ExecutionSnapshot current, ExecutionCommand command) {
    return enforceDurableStateLimit(current, command, decideUnbounded(context, current, command));
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      decideUnbounded(
          DurableProcessContext context, ExecutionSnapshot current, ExecutionCommand command) {
    Objects.requireNonNull(context, "context");
    Objects.requireNonNull(command, "command");
    current = attachAdmittedPlan(current);
    if (current != null && current.phase() == ExecutionPhase.COMPUTING) {
      return switch (command) {
        case ObserveWorkflowComputationCommand observation ->
            completeComputation(context, current, observation);
        case ObserveWorkflowComputationFailureCommand failure ->
            failComputation(context, current, failure);
        case ControlExecutionCommand control
            when control.action() == ExecutionControlAction.CANCEL ->
            cancelDuringComputation(current, control);
        default -> queueDuringComputation(context, current, command);
      };
    }
    if (command instanceof ObserveWorkflowComputationCommand
        || command instanceof ObserveWorkflowComputationFailureCommand) {
      if (current != null) {
        /*
         * A worker result can race with cancellation or with a
         * previously committed result. Once the cutpoint is gone it
         * is stale by construction and must not poison the command
         * partition.
         */
        return DurableTransition.unchanged(current);
      }
      throw new IllegalArgumentException("Execution has no pending workflow computation");
    }
    try {
      var transition =
          switch (command) {
            case StartExecutionCommand start -> start(context, current, start);
            case AdvanceExecutionCommand advance ->
                advance(context, requireCurrent(current), advance);
            case ControlExecutionCommand control ->
                control(context, requireCurrent(current), control);
            case ReceiveEventCommand received ->
                receiveEvent(context, requireCurrent(current), received);
            case ReceiveAsyncApiMessageCommand received ->
                receiveAsyncApiMessage(context, requireCurrent(current), received);
            case ObserveAsyncApiSubscriptionCommand observed ->
                observeAsyncApiSubscription(context, requireCurrent(current), observed);
            case FireTimerCommand fired -> fireTimer(context, requireCurrent(current), fired);
            case ObserveOperationCommand observed ->
                observeOperation(context, requireCurrent(current), observed);
            case ObserveHumanTaskCommand observed ->
                observeHumanTask(context, requireCurrent(current), observed);
            case ReapplyExecutionCommand reapply ->
                reapplyQueued(context, requireCurrent(current), reapply);
            case PurgeExecutionCommand purge -> purge(requireCurrent(current), purge);
            case ObserveWorkflowComputationCommand ignored ->
                throw new IllegalStateException("Workflow computation was not intercepted");
            case ObserveWorkflowComputationFailureCommand ignored ->
                throw new IllegalStateException(
                    "Workflow computation failure was not " + "intercepted");
          };
      return transition.stateRemoved()
          ? transition
          : reconcileTimeouts(current, command, transition);
    } catch (DataSchemaValidationException failure) {
      ExecutionSnapshot snapshot = requireCurrent(current);
      ExecutionFailure structured = failure(failure);
      return reconcileTimeouts(
          current,
          command,
          routeTechnicalFailure(
              context,
              snapshot,
              command,
              structured,
              owningStep(snapshot.plan().steps(), failure.schema().definitionPath())));
    } catch (RuntimeExpressionException failure) {
      ExecutionSnapshot snapshot = requireCurrent(current);
      PlanStep step = nextOrActiveStep(snapshot);
      String path = step == null ? "/output/as" : step.path();
      return reconcileTimeouts(
          current,
          command,
          routeTechnicalFailure(
              context, snapshot, command, expressionFailure(path, snapshot.data(), failure), step));
    } catch (RuntimeDataLimitException | WorkflowDataMaterializationRequiredException cutpoint) {
      return deferredComputationEnabled
          ? deferComputation(context, current, command)
          : failUnavailableComputation(context, current, command, cutpoint);
    }
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      enforceDurableStateLimit(
          ExecutionSnapshot previous,
          ExecutionCommand command,
          DurableTransition<
                  ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
              transition) {
    if (!transition.stateChanged() || transition.stateRemoved()) {
      return transition;
    }
    final int encodedBytes;
    try {
      encodedBytes = DURABLE_STATE_JSON.writeValueAsBytes(transition.state()).length;
    } catch (com.fasterxml.jackson.core.JsonProcessingException failure) {
      throw new IllegalStateException("Workflow execution state is not serializable", failure);
    }
    if (encodedBytes <= MAX_DURABLE_STATE_BYTES) {
      return transition;
    }

    ExecutionSnapshot attempted = transition.state();
    long sequence = previous == null ? 0 : previous.nextSequence();
    String message =
        "Durable workflow state requires "
            + encodedBytes
            + " bytes; maximum is "
            + MAX_DURABLE_STATE_BYTES;
    var failure =
        new ExecutionFailure(
            RUNTIME_ERROR_TYPE,
            message,
            attempted.cursor().complete()
                ? "/"
                : Objects.requireNonNullElse(attempted.cursor().current().taskPath(), "/"),
            attempted.data(),
            List.of(),
            413,
            null,
            "Workflow state is too large",
            message);
    var compact =
        new ExecutionSnapshot(
            attempted.key(),
            attempted.definition(),
            attempted.plan(),
            attempted.startedBy(),
            attempted.startedAt(),
            ExecutionPhase.FAILED,
            new ExecutionCursor(List.of()),
            attempted.initialInput(),
            attempted.context(),
            attempted.data(),
            sequence + 1,
            failure,
            null,
            null,
            List.of(),
            null,
            List.of(),
            null,
            null);
    var event =
        failedEvent(
            attempted.key(),
            attempted.definition().definitionSha256(),
            sequence,
            null,
            attempted.data(),
            failure,
            List.of(),
            List.of(),
            command);
    return DurableTransition.changed(compact, List.of(event), List.of());
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      failUnavailableComputation(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ExecutionCommand command,
          RuntimeException cutpoint) {
    String message =
        "Workflow data requires off-thread computation, "
            + "but that capability is disabled: "
            + Objects.requireNonNullElse(
                cutpoint.getMessage(), cutpoint.getClass().getSimpleName());
    if (current == null) {
      if (!(command instanceof StartExecutionCommand start)) {
        throw new IllegalStateException("Only a start command may fail without execution state");
      }
      WorkflowPlan plan = requireDefinition(start).plan();
      return failStart(start, plan, unavailableComputationFailure(message, "/", start.input()));
    }
    PlanStep step = nextOrActiveStep(current);
    return reconcileTimeouts(
        current,
        command,
        routeTechnicalFailure(
            context,
            current,
            command,
            unavailableComputationFailure(
                message, step == null ? "/" : step.path(), current.data()),
            step));
  }

  private static ExecutionFailure unavailableComputationFailure(
      String message, String definitionPath, DataReference rejectedData) {
    return new ExecutionFailure(
        RUNTIME_ERROR_TYPE,
        message,
        definitionPath,
        rejectedData,
        List.of(),
        503,
        null,
        "Workflow computation unavailable",
        message);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      deferComputation(
          DurableProcessContext context, ExecutionSnapshot current, ExecutionCommand command) {
    long basisRevision = context.nextRevision();
    String computationId = computationId(command.key(), command.commandId(), basisRevision);
    PendingWorkflowComputation pending =
        new PendingWorkflowComputation(
            computationId,
            basisRevision,
            current == null,
            current == null ? null : current.phase(),
            command,
            List.of());
    ExecutionSnapshot computing =
        current == null ? deferredStart(command, pending) : computing(current, pending);
    return DurableTransition.changed(
        computing, List.of(), List.of(), List.of(computationEffect(computing, pending, command)));
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      queueDuringComputation(
          DurableProcessContext ignored, ExecutionSnapshot current, ExecutionCommand command) {
    PendingWorkflowComputation existing =
        Objects.requireNonNull(current.pendingComputation(), "pendingComputation");
    PendingWorkflowComputation queued = existing.enqueue(command);
    ExecutionSnapshot computing = computing(current, queued);
    return DurableTransition.changed(computing, List.of(), List.of());
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      cancelDuringComputation(ExecutionSnapshot current, ControlExecutionCommand command) {
    PendingWorkflowComputation pending =
        Objects.requireNonNull(current.pendingComputation(), "pendingComputation");
    ExecutionPhase basePhase =
        pending.startsExecution() ? ExecutionPhase.RUNNING : pending.basePhase();
    /*
     * Cancellation takes precedence over the off-thread computation.
     * Removing the cutpoint atomically makes any late result stale. The
     * computation worker may finish its current pure reducer decision,
     * but it can no longer mutate this execution.
     */
    return cancel(restoreComputationBase(current, basePhase), command);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      completeComputation(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ObserveWorkflowComputationCommand command) {
    PendingWorkflowComputation pending =
        Objects.requireNonNull(current.pendingComputation(), "pendingComputation");
    if (!pending.computationId().equals(command.computationId())
        || pending.basisRevision() != command.basisRevision()) {
      return DurableTransition.unchanged(current);
    }
    PreparedWorkflowTransition prepared =
        transitionCodec.decode(command.transition(), command.transitionSha256());
    validatePreparedTransition(current, pending, prepared);
    List<ExecutionCommand> released =
        new ArrayList<>(pending.queuedCommands().size() + prepared.followUpCommands().size());
    released.addAll(flattenQueued(pending.queuedCommands()));
    for (ExecutionCommand followUp : prepared.followUpCommands()) {
      released.add(rebaseContinuation(followUp, context.nextRevision()));
    }
    List<ExecutionCommand> followUps = reapplyBatch(released, context.nextRevision());
    return DurableTransition.changed(
        prepared.state(), prepared.events(), followUps, prepared.outbox());
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      failComputation(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ObserveWorkflowComputationFailureCommand command) {
    PendingWorkflowComputation pending =
        Objects.requireNonNull(current.pendingComputation(), "pendingComputation");
    if (!pending.computationId().equals(command.computationId())
        || pending.basisRevision() != command.basisRevision()) {
      return DurableTransition.unchanged(current);
    }
    String path =
        current.cursor().complete()
            ? "/"
            : Objects.requireNonNullElse(current.cursor().current().taskPath(), "/");
    ExecutionFailure failure =
        new ExecutionFailure(
            command.errorType(),
            command.message(),
            path,
            current.data(),
            List.of(),
            500,
            pending.computationId(),
            "Workflow computation failed",
            command.message());
    DurableTransition<ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
        failed;
    if (pending.startsExecution()) {
      failed = failStart((StartExecutionCommand) pending.command(), current.plan(), failure);
    } else {
      ExecutionSnapshot base = restoreComputationBase(current, pending.basePhase());
      failed = routeTechnicalFailure(context, base, command, failure, nextOrActiveStep(base));
    }
    List<ExecutionCommand> released =
        new ArrayList<>(pending.queuedCommands().size() + failed.followUpCommands().size());
    released.addAll(flattenQueued(pending.queuedCommands()));
    for (ExecutionCommand followUp : failed.followUpCommands()) {
      released.add(rebaseContinuation(followUp, context.nextRevision()));
    }
    List<ExecutionCommand> followUps = reapplyBatch(released, context.nextRevision());
    return DurableTransition.changed(failed.state(), failed.events(), followUps, failed.outbox());
  }

  private static ExecutionSnapshot restoreComputationBase(
      ExecutionSnapshot computing, ExecutionPhase basePhase) {
    return new ExecutionSnapshot(
        computing.key(),
        computing.definition(),
        computing.plan(),
        computing.startedBy(),
        computing.startedAt(),
        basePhase,
        computing.cursor(),
        computing.initialInput(),
        computing.context(),
        computing.data(),
        computing.nextSequence(),
        computing.failure(),
        computing.laneRootTaskPath(),
        computing.activeFork(),
        computing.forkPositions(),
        computing.pendingInteraction(),
        computing.activeTimeouts(),
        computing.cancellation(),
        null);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      reapplyQueued(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ReapplyExecutionCommand replay) {
    List<ExecutionCommand> remaining = new ArrayList<>(replay.remainingCommands());
    ExecutionCommand next = replay.command();
    while (true) {
      DurableTransition<ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
          applied = decideUnbounded(context, current, next);
      if (applied.stateRemoved() || remaining.isEmpty()) {
        return applied;
      }
      if (!applied.stateChanged()) {
        next = remaining.removeFirst();
        continue;
      }

      List<ExecutionCommand> future =
          new ArrayList<>(remaining.size() + applied.followUpCommands().size());
      future.addAll(remaining);
      future.addAll(flattenQueued(applied.followUpCommands()));
      ExecutionSnapshot state = applied.state();
      if (state.phase() == ExecutionPhase.COMPUTING) {
        PendingWorkflowComputation pending =
            Objects.requireNonNull(state.pendingComputation(), "pendingComputation");
        for (ExecutionCommand command : future) {
          pending = pending.enqueue(command);
        }
        return DurableTransition.changed(
            computing(state, pending), applied.events(), List.of(), applied.outbox());
      }
      return DurableTransition.changed(
          state, applied.events(), reapplyBatch(future, context.nextRevision()), applied.outbox());
    }
  }

  private static List<ExecutionCommand> flattenQueued(List<ExecutionCommand> commands) {
    List<ExecutionCommand> flattened = new ArrayList<>();
    for (ExecutionCommand command : commands) {
      if (command instanceof ReapplyExecutionCommand replay) {
        flattened.add(replay.command());
        flattened.addAll(replay.remainingCommands());
      } else {
        flattened.add(command);
      }
    }
    return flattened;
  }

  private static List<ExecutionCommand> reapplyBatch(
      List<ExecutionCommand> commands, long expectedRevision) {
    if (commands.isEmpty()) return List.of();
    return List.of(
        new ReapplyExecutionCommand(
            commands.getFirst(), expectedRevision, commands.subList(1, commands.size())));
  }

  private ExecutionSnapshot deferredStart(
      ExecutionCommand deferred, PendingWorkflowComputation pending) {
    if (!(deferred instanceof StartExecutionCommand command)) {
      throw new IllegalStateException("Only a start command may create a deferred execution");
    }
    WorkflowDefinitionBundle bundle = requireDefinition(command);
    WorkflowPlan plan = bundle.plan();
    return new ExecutionSnapshot(
        command.key(),
        command.definition(),
        plan,
        command.actor(),
        command.requestedAt(),
        ExecutionPhase.COMPUTING,
        ExecutionCursor.start(command.input()),
        command.input(),
        command.input(),
        command.input(),
        0,
        null,
        null,
        null,
        List.of(),
        null,
        List.of(),
        null,
        pending);
  }

  private static ExecutionSnapshot computing(
      ExecutionSnapshot current, PendingWorkflowComputation pending) {
    return new ExecutionSnapshot(
        current.key(),
        current.definition(),
        current.plan(),
        current.startedBy(),
        current.startedAt(),
        ExecutionPhase.COMPUTING,
        current.cursor(),
        current.initialInput(),
        current.context(),
        current.data(),
        current.nextSequence(),
        current.failure(),
        current.laneRootTaskPath(),
        current.activeFork(),
        current.forkPositions(),
        current.pendingInteraction(),
        current.activeTimeouts(),
        current.cancellation(),
        pending);
  }

  private WorkflowEffect computationEffect(
      ExecutionSnapshot computing, PendingWorkflowComputation pending, ExecutionCommand trigger) {
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("computationId", pending.computationId());
    descriptor.put("basisRevision", pending.basisRevision());
    descriptor.put("executionKey", computing.key().canonical());
    descriptor.put("definitionSha256", computing.definition().definitionSha256());
    descriptor.put("deferredCommandId", pending.command().commandId());
    descriptor.put("queuedCommandCount", pending.queuedCommands().size());
    String taskPath =
        computing.cursor().complete()
            ? "/"
            : Objects.requireNonNullElse(computing.cursor().current().taskPath(), "/");
    return new WorkflowEffect(
        pending.computationId(),
        computing.key(),
        WorkflowEffectType.COMPUTE_WORKFLOW_TRANSITION,
        taskPath,
        WorkflowRuntimeDataAccess.inlineOnly().reference(descriptor),
        trigger.actor(),
        trigger.requestedAt());
  }

  private static String computationId(
      com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey key,
      String commandId,
      long basisRevision) {
    String identity = key.canonical() + "|" + commandId + "|" + basisRevision;
    try {
      return "computation-"
          + HexFormat.of()
              .formatHex(
                  MessageDigest.getInstance("SHA-256")
                      .digest(identity.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("JVM does not provide SHA-256", impossible);
    }
  }

  private void validatePreparedTransition(
      ExecutionSnapshot computing,
      PendingWorkflowComputation pending,
      PreparedWorkflowTransition prepared) {
    ExecutionSnapshot state = prepared.state();
    if (!state.key().equals(computing.key())) {
      throw new SecurityException("Computed transition targets another execution");
    }
    if (!state.definition().equals(computing.definition())) {
      throw new SecurityException("Computed transition changes the admitted definition");
    }
    if (state.phase() == ExecutionPhase.COMPUTING || state.pendingComputation() != null) {
      throw new SecurityException("Computed transition cannot retain its cutpoint");
    }
    if (!state.startedBy().equals(computing.startedBy())
        || !state.startedAt().equals(computing.startedAt())) {
      throw new SecurityException("Computed transition changes execution ownership");
    }
    if (!pending.startsExecution() && !state.initialInput().equals(computing.initialInput())) {
      throw new SecurityException("Computed transition changes initial input");
    }
    long expectedSequence = computing.nextSequence();
    for (ExecutionHistoryEvent event : prepared.events()) {
      if (!event.key().equals(computing.key())
          || !event.definitionSha256().equals(computing.definition().definitionSha256())
          || event.sequence() != expectedSequence) {
        throw new SecurityException("Computed transition history is not contiguous");
      }
      expectedSequence++;
    }
    if (state.nextSequence() != expectedSequence) {
      throw new SecurityException("Computed transition sequence does not match history");
    }
    for (ExecutionCommand followUp : prepared.followUpCommands()) {
      if (!followUp.key().equals(computing.key())) {
        throw new SecurityException("Computed transition follow-up targets another " + "execution");
      }
    }
    for (WorkflowEffect effect : prepared.outbox()) {
      if (!effect.key().equals(computing.key())) {
        throw new SecurityException("Computed transition effect targets another " + "execution");
      }
    }
  }

  private static ExecutionCommand rebaseContinuation(
      ExecutionCommand command, long committedRevision) {
    if (!(command instanceof AdvanceExecutionCommand advance)) {
      return command;
    }
    return new AdvanceExecutionCommand(
        advance.commandId(),
        advance.key(),
        committedRevision,
        advance.actor(),
        advance.requestedAt());
  }

  private WorkflowDefinitionBundle requireDefinition(StartExecutionCommand command) {
    WorkflowDefinitionBundle bundle = requireDefinition(command.definition());
    WorkflowPlan plan = bundle.plan();
    if (!command.definition().key().coordinates().equals(plan.coordinates())
        || !command.definition().sourceSha256().equals(plan.sourceSha256())
        || !command.definition().definitionSha256().equals(plan.definitionSha256())) {
      throw new IllegalArgumentException("Start command does not reference the supplied plan");
    }
    return bundle;
  }

  private WorkflowDefinitionBundle requireDefinition(
      com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionReference reference) {
    WorkflowDefinitionBundle bundle = definitions.resolve(reference);
    if (bundle == null) {
      throw new IllegalArgumentException("Referenced workflow definition is not admitted");
    }
    if (!bundle.reference().equals(reference)) {
      throw new IllegalArgumentException("Referenced workflow definition digest does not match");
    }
    return bundle;
  }

  private ExecutionSnapshot attachAdmittedPlan(ExecutionSnapshot current) {
    if (current == null || current.plan() != null) {
      return current;
    }
    return current.withPlan(requireDefinition(current.definition()).plan());
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      start(
          DurableProcessContext context, ExecutionSnapshot current, StartExecutionCommand command) {
    if (current != null) {
      throw new IllegalArgumentException("Execution already exists");
    }
    WorkflowDefinitionBundle bundle = requireDefinition(command);
    WorkflowPlan plan = bundle.plan();

    try {
      if (plan.dataFlow().inputSchema() != null) {
        schemas(plan).validate(plan.dataFlow().inputSchema(), inline(command.input()));
      }
    } catch (DataSchemaValidationException failure) {
      return failStart(command, plan, failure);
    }
    DataReference workflowInput;
    try {
      workflowInput = command.input();
      if (plan.dataFlow().inputFrom() != null) {
        workflowInput =
            transform(
                plan.dataFlow().inputFrom(),
                command.input(),
                new RuntimeExpressionArguments(
                    null,
                    null,
                    JsonNodeFactory.instance.objectNode(),
                    null,
                    null,
                    null,
                    workflowDescriptor(
                        plan,
                        command.key().executionId().value(),
                        inline(command.input()),
                        command.requestedAt()),
                    runtimeDescriptor()),
                plan);
      }
    } catch (RuntimeExpressionException failure) {
      return failStart(command, plan, expressionFailure("/input/from", command.input(), failure));
    }
    var event =
        event(
            command, 0, ExecutionEventType.EXECUTION_STARTED, null, command.input(), workflowInput);
    var snapshot =
        new ExecutionSnapshot(
            command.key(),
            command.definition(),
            plan,
            command.actor(),
            command.requestedAt(),
            ExecutionPhase.RUNNING,
            ExecutionCursor.start(workflowInput),
            command.input(),
            workflowInput,
            workflowInput,
            1);
    return changedWithContinuation(context, snapshot, List.of(event), command);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      advance(
          DurableProcessContext context,
          ExecutionSnapshot current,
          AdvanceExecutionCommand command) {
    requireSameExecution(current, command);
    if (current.phase() != ExecutionPhase.RUNNING) {
      return DurableTransition.unchanged(current);
    }
    if (current.pendingInteraction() != null) {
      return DurableTransition.unchanged(current);
    }
    if (current.activeFork() != null) {
      String readyInteraction = readyInteractionId(current.activeFork());
      if (readyInteraction != null) {
        return finalizeReadyForkInteraction(context, current, command, readyInteraction);
      }
      return advanceFork(context, current, command);
    }

    ExecutionFrame frame = current.cursor().current();
    List<PlanStep> children = childrenForFrame(current, frame);
    if (frame.nextChildIndex() < children.size()) {
      return enterOrExecute(
          context, current, command, frame, children, children.get(frame.nextChildIndex()));
    }
    return exitOrComplete(context, current, command, frame);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      receiveEvent(
          DurableProcessContext context, ExecutionSnapshot current, ReceiveEventCommand command) {
    requireSameExecution(current, command);
    if (current.phase() != ExecutionPhase.RUNNING) {
      throw new IllegalArgumentException(
          "Events cannot resume an execution in phase " + current.phase());
    }
    if (current.pendingInteraction() instanceof ActiveListenState listen
        && listen.subscriptionId().equals(command.subscriptionId())) {
      return receiveRootEvent(context, current, command, listen);
    }
    if (current.activeFork() != null) {
      return resumeForkInteraction(context, current, command, command.subscriptionId());
    }
    throw new IllegalArgumentException("Execution has no matching active event subscription");
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      receiveAsyncApiMessage(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ReceiveAsyncApiMessageCommand command) {
    requireSameExecution(current, command);
    if (current.phase() != ExecutionPhase.RUNNING
        && current.phase() != ExecutionPhase.PAUSED
        && current.phase() != ExecutionPhase.CANCEL_REQUESTED) {
      throw new IllegalArgumentException(
          "AsyncAPI messages cannot enter an execution in phase " + current.phase());
    }
    if (current.pendingInteraction() instanceof ActiveAsyncApiSubscriptionState subscription
        && subscription.subscriptionId().equals(command.subscriptionId())) {
      return receiveRootAsyncApiMessage(context, current, command, subscription);
    }
    if (current.pendingInteraction() instanceof ActiveCorrelatedWorkerState worker
        && worker.lifecycleId().equals(command.subscriptionId())) {
      return receiveRootCorrelatedWorkerMessage(context, current, command, worker);
    }
    if (current.activeFork() != null) {
      return resumeForkInteraction(context, current, command, command.subscriptionId());
    }
    throw new IllegalArgumentException("Execution has no matching active AsyncAPI subscription");
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      receiveRootCorrelatedWorkerMessage(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ReceiveAsyncApiMessageCommand command,
          ActiveCorrelatedWorkerState worker) {
    PlanStep step = current.plan().requireStep(worker.taskPath());
    WorkflowEffect ack = correlatedWorkerMessageAck(current, command, worker);
    if (worker.seenSourcePositions().contains(command.sourcePosition())) {
      return DurableTransition.changed(current, List.of(), List.of(), List.of(ack));
    }

    JsonNode message = inline(command.message());
    JsonNode payload = message.path("payload");
    String operationId = payload.path("operationId").asText();
    long sequence = current.nextSequence();
    if (!worker.lifecycleId().equals(operationId)) {
      ActiveCorrelatedWorkerState observed = worker.observe(command.sourcePosition(), null);
      ExecutionHistoryEvent filtered =
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.ASYNC_API_MESSAGE_FILTERED,
              step,
              command.message(),
              workerMessageAudit(command, false));
      return DurableTransition.changed(
          withPendingInteraction(
              current,
              current.phase(),
              current.cursor(),
              current.context(),
              current.data(),
              sequence,
              observed),
          List.of(filtered),
          List.of(),
          List.of(ack));
    }

    String status = payload.path("status").asText().toUpperCase(java.util.Locale.ROOT);
    if (!Set.of("ACCEPTED", "PROGRESS", "SUCCEEDED", "FAILED", "CANCELLED").contains(status)) {
      throw new IllegalArgumentException(
          "Correlated worker message has unsupported status " + status);
    }
    boolean terminal = Set.of("SUCCEEDED", "FAILED", "CANCELLED").contains(status);
    ActiveCorrelatedWorkerState observed =
        worker.observe(command.sourcePosition(), terminal ? command.message() : null);
    ExecutionEventType eventType =
        switch (status) {
          case "ACCEPTED" -> ExecutionEventType.CORRELATED_WORKER_ACCEPTED;
          case "PROGRESS" -> ExecutionEventType.CORRELATED_WORKER_PROGRESS;
          case "SUCCEEDED" -> ExecutionEventType.CORRELATED_WORKER_COMPLETED;
          case "FAILED" -> ExecutionEventType.CORRELATED_WORKER_FAILED;
          case "CANCELLED" -> ExecutionEventType.CORRELATED_WORKER_CANCELLED;
          default -> throw new IllegalStateException(status);
        };
    ExecutionHistoryEvent lifecycleEvent =
        event(current, command, sequence++, eventType, step, worker.taskInput(), command.message());
    if (!terminal || current.phase() == ExecutionPhase.PAUSED) {
      return DurableTransition.changed(
          withPendingInteraction(
              current,
              current.phase(),
              current.cursor(),
              current.context(),
              current.data(),
              sequence,
              observed),
          List.of(lifecycleEvent),
          List.of(),
          List.of(ack));
    }
    if (current.phase() == ExecutionPhase.CANCEL_REQUESTED) {
      return finalizeCorrelatedWorkerCancellation(
          current, command, observed, sequence, List.of(lifecycleEvent), List.of(ack));
    }
    return completeCorrelatedWorker(
        context,
        current,
        command,
        step,
        observed,
        payload,
        status,
        sequence,
        new ArrayList<>(List.of(lifecycleEvent)),
        List.of(ack));
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      receiveRootAsyncApiMessage(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ReceiveAsyncApiMessageCommand command,
          ActiveAsyncApiSubscriptionState subscription) {
    PlanStep step = current.plan().requireStep(subscription.taskPath());
    AsyncApiSubscriptionPlan plan = step.callPlan().asyncApiSubscription();
    if (plan == null) {
      throw new IllegalStateException("Active AsyncAPI subscription has no compiled plan");
    }
    if (subscription.seenSourcePositions().contains(command.sourcePosition())) {
      return DurableTransition.changed(
          current,
          List.of(),
          List.of(),
          List.of(asyncApiMessageAck(current, command, step, subscription)));
    }

    JsonNode message = inline(command.message());
    boolean accepted =
        plan.filter() == null
            || expressions.evaluateCondition(
                plan.filter(),
                message,
                expressionArguments(
                    current,
                    step,
                    subscription.rawInput(),
                    subscription.taskInput(),
                    command.message(),
                    null,
                    command),
                current.plan().expressions().mode());
    boolean complete = false;
    if (accepted && plan.consumption().mode() == AsyncApiSubscriptionPlan.Consumption.Mode.UNTIL) {
      complete =
          expressions.evaluateCondition(
              plan.consumption().condition(),
              message,
              expressionArguments(
                  current,
                  step,
                  subscription.rawInput(),
                  subscription.taskInput(),
                  command.message(),
                  null,
                  command),
              current.plan().expressions().mode());
      accepted = !complete;
    }

    List<DataReference> messages = new ArrayList<>(subscription.messages());
    if (accepted) {
      messages.add(command.message());
      complete =
          switch (plan.consumption().mode()) {
            case AMOUNT -> messages.size() >= plan.consumption().amount();
            case WHILE ->
                !expressions.evaluateCondition(
                    plan.consumption().condition(),
                    message,
                    expressionArguments(
                        current,
                        step,
                        subscription.rawInput(),
                        subscription.taskInput(),
                        reference(asyncApiMessages(messages)),
                        null,
                        command),
                    current.plan().expressions().mode());
            case UNTIL -> false;
          };
    }
    Set<String> seen = new java.util.LinkedHashSet<>(subscription.seenSourcePositions());
    seen.add(command.sourcePosition());
    ActiveAsyncApiSubscriptionState progressed =
        new ActiveAsyncApiSubscriptionState(
            subscription.subscriptionId(),
            subscription.taskPath(),
            subscription.rawInput(),
            subscription.taskInput(),
            subscription.resumeCursor(),
            subscription.descriptor(),
            messages,
            seen,
            subscription.deadlineTimerId(),
            subscription.deadlineAt(),
            complete);
    long sequence = current.nextSequence();
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    events.add(
        event(
            current,
            command,
            sequence++,
            accepted
                ? ExecutionEventType.ASYNC_API_MESSAGE_RECEIVED
                : ExecutionEventType.ASYNC_API_MESSAGE_FILTERED,
            step,
            command.message(),
            asyncApiMessageAudit(command, accepted)));
    WorkflowEffect ack = asyncApiMessageAck(current, command, step, progressed);
    if (!complete || current.phase() == ExecutionPhase.PAUSED) {
      ExecutionSnapshot waiting =
          withPendingInteraction(
              current,
              current.phase(),
              current.cursor(),
              current.context(),
              current.data(),
              sequence,
              progressed);
      return DurableTransition.changed(waiting, events, List.of(), List.of(ack));
    }
    return completeAsyncApiSubscription(
        context, current, command, step, progressed, sequence, events, List.of(ack));
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      observeAsyncApiSubscription(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ObserveAsyncApiSubscriptionCommand command) {
    requireSameExecution(current, command);
    if (current.phase().terminal()) {
      return DurableTransition.unchanged(current);
    }
    if (current.phase() != ExecutionPhase.RUNNING) {
      throw new IllegalArgumentException(
          "AsyncAPI observations cannot resume an execution in phase " + current.phase());
    }
    if (current.pendingInteraction() instanceof ActiveAsyncApiSubscriptionState subscription
        && subscription.subscriptionId().equals(command.subscriptionId())) {
      PlanStep step = current.plan().requireStep(subscription.taskPath());
      long sequence = current.nextSequence();
      List<ExecutionHistoryEvent> events = new ArrayList<>();
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.ERROR_RAISED,
              step,
              subscription.taskInput(),
              errorReference(command.error())));
      List<WorkflowEffect> effects = new ArrayList<>();
      effects.add(
          asyncApiSubscriptionEffect(
              current,
              command,
              step,
              subscription,
              WorkflowEffectType.DELETE_ASYNC_API_SUBSCRIPTION,
              ":failure"));
      if (subscription.deadlineTimerId() != null) {
        effects.add(
            asyncApiDeadlineEffect(
                current, command, step, subscription, WorkflowEffectType.CANCEL_TIMER, ":failure"));
      }
      ExecutionSnapshot observed =
          withPendingInteraction(
              current,
              ExecutionPhase.RUNNING,
              current.cursor(),
              current.context(),
              current.data(),
              sequence,
              null);
      return withOutbox(
          routeWorkflowError(context, observed, command, step, command.error(), sequence, events),
          effects);
    }
    if (current.activeFork() != null
        && current.activeFork().branches().stream()
            .anyMatch(branch -> containsInteraction(branch, command.subscriptionId()))) {
      return resumeForkInteraction(context, current, command, command.subscriptionId());
    }
    return DurableTransition.unchanged(current);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      resumeForkInteraction(
          DurableProcessContext context,
          ExecutionSnapshot outer,
          ExecutionCommand command,
          String interactionId) {
    ForkRuntimeState fork = outer.activeFork();
    int branchIndex = -1;
    for (int index = 0; index < fork.branches().size(); index++) {
      if (containsInteraction(fork.branches().get(index), interactionId)) {
        branchIndex = index;
        break;
      }
    }
    if (branchIndex < 0) {
      throw new IllegalArgumentException("Execution has no matching active event subscription");
    }

    ForkBranchState branch = fork.branches().get(branchIndex);
    PlanStep forkStep = outer.plan().requireStep(fork.taskPath());
    List<ForkPosition> positions = new ArrayList<>(outer.forkPositions());
    positions.add(
        new ForkPosition(
            forkStep.path(),
            forkStep.name(),
            branch.path(),
            branch.name(),
            branch.declarationIndex()));
    ExecutionSnapshot lane =
        new ExecutionSnapshot(
            outer.key(),
            outer.definition(),
            outer.plan(),
            outer.startedBy(),
            outer.startedAt(),
            outer.phase(),
            branch.cursor(),
            outer.initialInput(),
            outer.context(),
            branch.data(),
            outer.nextSequence(),
            null,
            branch.path(),
            branch.activeFork(),
            positions,
            branch.pendingInteraction(),
            outer.activeTimeouts());
    var laneTransition = decide(context, lane, command);
    return mergeForkLaneTransition(
        context, outer, command, fork, forkStep, branchIndex, branch, positions, laneTransition);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      resumeForkTimeout(
          DurableProcessContext context,
          ExecutionSnapshot outer,
          FireTimerCommand command,
          ActiveTimeoutState timeout) {
    ForkRuntimeState fork = outer.activeFork();
    int branchIndex = -1;
    for (int index = 0; index < fork.branches().size(); index++) {
      if (activeTaskPaths(fork.branches().get(index)).contains(timeout.taskPath())) {
        branchIndex = index;
        break;
      }
    }
    if (branchIndex < 0) {
      throw new IllegalStateException("Durable timeout has no owning fork branch");
    }

    ForkBranchState branch = fork.branches().get(branchIndex);
    PlanStep forkStep = outer.plan().requireStep(fork.taskPath());
    List<ForkPosition> positions = new ArrayList<>(outer.forkPositions());
    positions.add(
        new ForkPosition(
            forkStep.path(),
            forkStep.name(),
            branch.path(),
            branch.name(),
            branch.declarationIndex()));
    ExecutionSnapshot lane =
        new ExecutionSnapshot(
            outer.key(),
            outer.definition(),
            outer.plan(),
            outer.startedBy(),
            outer.startedAt(),
            outer.phase(),
            branch.cursor(),
            outer.initialInput(),
            outer.context(),
            branch.data(),
            outer.nextSequence(),
            null,
            branch.path(),
            branch.activeFork(),
            positions,
            branch.pendingInteraction(),
            outer.activeTimeouts());
    var laneTransition = decide(context, lane, command);
    return mergeForkLaneTransition(
        context, outer, command, fork, forkStep, branchIndex, branch, positions, laneTransition);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      mergeForkLaneTransition(
          DurableProcessContext context,
          ExecutionSnapshot outer,
          ExecutionCommand command,
          ForkRuntimeState fork,
          PlanStep forkStep,
          int branchIndex,
          ForkBranchState originalBranch,
          List<ForkPosition> positions,
          DurableTransition<
                  ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
              laneTransition) {
    ExecutionSnapshot progressed = laneTransition.state();
    List<ExecutionHistoryEvent> events = new ArrayList<>(laneTransition.events());
    List<WorkflowEffect> effects = new ArrayList<>(laneTransition.outbox());
    if (progressed.phase() == ExecutionPhase.FAILED) {
      effects.addAll(interactionEffects(outer, command, false, ":failure"));
      return DurableTransition.changed(
          failedOuterSnapshot(outer, progressed), events, List.of(), effects);
    }

    ForkBranchState branch =
        originalBranch.progressed(
            progressed.cursor(),
            progressed.data(),
            progressed.activeFork(),
            progressed.pendingInteraction());
    long sequence = progressed.nextSequence();
    boolean branchComplete =
        progressed.cursor().complete()
            && progressed.activeFork() == null
            && progressed.pendingInteraction() == null;
    if (branchComplete) {
      branch = branch.completed(fork.nextCompletionOrder());
      events.add(
          forkEvent(
              progressed,
              command,
              sequence++,
              ExecutionEventType.FORK_BRANCH_COMPLETED,
              forkStep,
              fork.input(),
              branch.data(),
              positions));
    }
    ForkRuntimeState progressedFork =
        fork.replace(
            branchIndex,
            branch,
            (branchIndex + 1) % fork.branches().size(),
            fork.nextCompletionOrder() + (branchComplete ? 1 : 0));
    if (branchComplete && progressedFork.compete()) {
      int before = events.size();
      progressedFork =
          abandonOtherBranches(
              progressed,
              command,
              forkStep,
              progressedFork,
              branchIndex,
              events,
              effects,
              sequence);
      sequence += events.size() - before;
      return withOutbox(
          completeFork(
              context,
              outer,
              progressed,
              command,
              forkStep,
              progressedFork,
              branch.data(),
              sequence,
              events),
          effects);
    }
    if (progressedFork.allCompleted()) {
      return withOutbox(
          completeFork(
              context,
              outer,
              progressed,
              command,
              forkStep,
              progressedFork,
              joinedForkOutput(progressedFork),
              sequence,
              events),
          effects);
    }
    ExecutionSnapshot snapshot =
        new ExecutionSnapshot(
            outer.key(),
            outer.definition(),
            outer.plan(),
            outer.startedBy(),
            outer.startedAt(),
            outer.phase(),
            outer.cursor(),
            outer.initialInput(),
            progressed.context(),
            outer.data(),
            sequence,
            null,
            outer.laneRootTaskPath(),
            progressedFork,
            outer.forkPositions(),
            outer.pendingInteraction(),
            progressed.activeTimeouts());
    if (outer.phase() == ExecutionPhase.PAUSED || progressedFork.nextRunnableIndex() < 0) {
      return DurableTransition.changed(snapshot, events, List.of(), effects);
    }
    return withOutbox(changedWithContinuation(context, snapshot, events, command), effects);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      finalizeReadyForkInteraction(
          DurableProcessContext context,
          ExecutionSnapshot outer,
          ExecutionCommand command,
          String interactionId) {
    ForkRuntimeState fork = outer.activeFork();
    int branchIndex = -1;
    for (int index = 0; index < fork.branches().size(); index++) {
      if (containsInteraction(fork.branches().get(index), interactionId)) {
        branchIndex = index;
        break;
      }
    }
    if (branchIndex < 0) {
      throw new IllegalStateException("Ready interaction disappeared from fork");
    }
    ForkBranchState branch = fork.branches().get(branchIndex);
    PlanStep forkStep = outer.plan().requireStep(fork.taskPath());
    List<ForkPosition> positions = new ArrayList<>(outer.forkPositions());
    positions.add(
        new ForkPosition(
            forkStep.path(),
            forkStep.name(),
            branch.path(),
            branch.name(),
            branch.declarationIndex()));
    ExecutionSnapshot lane =
        new ExecutionSnapshot(
            outer.key(),
            outer.definition(),
            outer.plan(),
            outer.startedBy(),
            outer.startedAt(),
            ExecutionPhase.RUNNING,
            branch.cursor(),
            outer.initialInput(),
            outer.context(),
            branch.data(),
            outer.nextSequence(),
            null,
            branch.path(),
            branch.activeFork(),
            positions,
            branch.pendingInteraction(),
            outer.activeTimeouts());
    final DurableTransition<
            ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
        laneTransition;
    if (lane.pendingInteraction() instanceof ActiveAsyncApiSubscriptionState subscription
        && subscription.subscriptionId().equals(interactionId)
        && subscription.completionReady()) {
      PlanStep step = lane.plan().requireStep(subscription.taskPath());
      laneTransition =
          completeAsyncApiSubscription(
              context,
              lane,
              command,
              step,
              subscription,
              lane.nextSequence(),
              new ArrayList<>(),
              List.of());
    } else if (lane.pendingInteraction() instanceof ActiveCorrelatedWorkerState worker
        && worker.lifecycleId().equals(interactionId)
        && worker.bufferedTerminalMessage() != null) {
      JsonNode payload = inline(worker.bufferedTerminalMessage()).required("payload");
      laneTransition =
          completeCorrelatedWorker(
              context,
              lane,
              command,
              lane.plan().requireStep(worker.taskPath()),
              worker,
              payload,
              payload.required("status").textValue().toUpperCase(java.util.Locale.ROOT),
              lane.nextSequence(),
              new ArrayList<>(),
              List.of());
    } else if (lane.pendingInteraction() instanceof ActiveOperationState operation
        && operation.operationId().equals(interactionId)
        && operation.completionReady()) {
      laneTransition =
          completeOperation(context, lane, command, operation, operation.terminalObservation());
    } else if (lane.pendingInteraction() instanceof ActiveHumanTaskState humanTask
        && humanTask.humanTaskId().equals(interactionId)
        && humanTask.completionReady()) {
      laneTransition =
          completeHumanTask(context, lane, command, humanTask, humanTask.terminalObservation());
    } else if (lane.activeFork() != null) {
      laneTransition = finalizeReadyForkInteraction(context, lane, command, interactionId);
    } else {
      throw new IllegalStateException("Fork interaction is not ready for completion");
    }
    return mergeForkLaneTransition(
        context, outer, command, fork, forkStep, branchIndex, branch, positions, laneTransition);
  }

  private static boolean containsInteraction(ForkBranchState branch, String interactionId) {
    if (branch.pendingInteraction() != null
        && branch.pendingInteraction().interactionId().equals(interactionId)) {
      return true;
    }
    if (branch.pendingInteraction() instanceof ActiveAsyncApiSubscriptionState subscription
        && interactionId.equals(subscription.deadlineTimerId())) {
      return true;
    }
    if (branch.pendingInteraction() instanceof ActiveCorrelatedWorkerState worker
        && interactionId.equals(worker.deadlineTimerId())) {
      return true;
    }
    if (branch.pendingInteraction() instanceof ActiveHumanTaskState humanTask
        && interactionId.equals(humanTask.dueTimerId())) {
      return true;
    }
    if (attemptDeadline(branch.cursor(), interactionId) != null) {
      return true;
    }
    if (branch.activeFork() == null) return false;
    return branch.activeFork().branches().stream()
        .anyMatch(nested -> containsInteraction(nested, interactionId));
  }

  private static String readyInteractionId(ForkRuntimeState fork) {
    if (fork == null) return null;
    for (ForkBranchState branch : fork.branches()) {
      if (branch.pendingInteraction() instanceof ActiveAsyncApiSubscriptionState subscription
          && subscription.completionReady()) {
        return subscription.subscriptionId();
      }
      if (branch.pendingInteraction() instanceof ActiveCorrelatedWorkerState worker
          && worker.bufferedTerminalMessage() != null) {
        return worker.lifecycleId();
      }
      if (branch.pendingInteraction() instanceof ActiveOperationState operation
          && operation.completionReady()) {
        return operation.operationId();
      }
      if (branch.pendingInteraction() instanceof ActiveHumanTaskState humanTask
          && humanTask.completionReady()) {
        return humanTask.humanTaskId();
      }
      String nested = readyInteractionId(branch.activeFork());
      if (nested != null) return nested;
    }
    return null;
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      fireTimer(
          DurableProcessContext context, ExecutionSnapshot current, FireTimerCommand command) {
    requireSameExecution(current, command);
    if (current.phase() == ExecutionPhase.CANCEL_REQUESTED
        && current.cancellation().timerId().equals(command.timerId())) {
      if (command.requestedAt().isBefore(current.cancellation().dueAt())) {
        throw new IllegalArgumentException(
            "Cancellation deadline fired before its durable " + "due time");
      }
      return finalizeCancellation(current, command, List.of(), true);
    }
    if (current.phase() == ExecutionPhase.PAUSED) {
      /*
       * A cancellation effect and an already-emitted timer command may
       * cross during pause. Resume re-materialises the original due
       * time, so the stale observation must not poison the command
       * partition.
       */
      return DurableTransition.unchanged(current);
    }
    if (current.phase() != ExecutionPhase.RUNNING) {
      throw new IllegalArgumentException(
          "Timers cannot resume an execution in phase " + current.phase());
    }
    ActiveTimeoutState timeout = activeTimeout(current.activeTimeouts(), command.timerId());
    if (timeout != null) {
      if (!timeout.workflowTimeout()
          && current.activeFork() != null
          && timeoutInFork(current.activeFork(), timeout.taskPath())) {
        return resumeForkTimeout(context, current, command, timeout);
      }
      return fireRootTimeout(context, current, command, timeout);
    }
    if (current.pendingInteraction() instanceof ActiveTimerState timer
        && timer.timerId().equals(command.timerId())) {
      return fireRootTimer(context, current, command, timer);
    }
    if (current.pendingInteraction() instanceof ActiveRetryState retry
        && retry.timerId().equals(command.timerId())) {
      return fireRootRetry(context, current, command, retry);
    }
    if (current.pendingInteraction() instanceof ActiveAsyncApiSubscriptionState subscription
        && command.timerId().equals(subscription.deadlineTimerId())) {
      return fireRootAsyncApiDeadline(context, current, command, subscription);
    }
    if (current.pendingInteraction() instanceof ActiveCorrelatedWorkerState worker
        && command.timerId().equals(worker.deadlineTimerId())) {
      return fireRootCorrelatedWorkerDeadline(context, current, command, worker);
    }
    if (current.pendingInteraction() instanceof ActiveHumanTaskState humanTask
        && command.timerId().equals(humanTask.dueTimerId())) {
      return fireRootHumanTaskDeadline(current, command, humanTask);
    }
    AttemptDeadline deadline = attemptDeadline(current.cursor(), command.timerId());
    if (deadline != null) {
      return fireRootAttemptDeadline(context, current, command, deadline);
    }
    if (current.activeFork() != null
        && current.activeFork().branches().stream()
            .anyMatch(branch -> containsInteraction(branch, command.timerId()))) {
      return resumeForkInteraction(context, current, command, command.timerId());
    }
    /*
     * Timer cancellation and a previously emitted fire command can cross
     * on different Kafka topology edges. Once the owning state has moved
     * on, that stable system command is an expected stale observation.
     */
    return DurableTransition.unchanged(current);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      fireRootHumanTaskDeadline(
          ExecutionSnapshot current, FireTimerCommand command, ActiveHumanTaskState humanTask) {
    if (command.requestedAt().isBefore(humanTask.dueAt())) {
      throw new IllegalArgumentException("Human-task deadline fired before its durable due time");
    }
    PlanStep step = current.plan().requireStep(humanTask.taskPath());
    long sequence = current.nextSequence();
    ExecutionHistoryEvent fired =
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.TIMER_FIRED,
            step,
            humanTask.taskInput(),
            humanTask.descriptor());
    ExecutionSnapshot waiting =
        withPendingInteraction(
            current,
            ExecutionPhase.RUNNING,
            current.cursor(),
            current.context(),
            current.data(),
            sequence,
            humanTask);
    return DurableTransition.changed(
        waiting,
        List.of(fired),
        List.of(),
        List.of(
            humanTaskEffect(
                current, command, step, humanTask, WorkflowEffectType.EXPIRE_HUMAN_TASK, ":due")));
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      fireRootTimeout(
          DurableProcessContext context,
          ExecutionSnapshot current,
          FireTimerCommand command,
          ActiveTimeoutState timeout) {
    if (command.requestedAt().isBefore(timeout.dueAt())) {
      throw new IllegalArgumentException("Timeout fired before its durable due time");
    }
    PlanStep step =
        timeout.workflowTimeout() ? null : current.plan().requireStep(timeout.taskPath());
    String instance = timeout.workflowTimeout() ? current.key().canonical() : step.path();
    String title = timeout.workflowTimeout() ? "Workflow timed out" : "Task timed out";
    String detail =
        timeout.workflowTimeout()
            ? "Workflow execution exceeded its durable deadline"
            : "Task " + step.name() + " exceeded its durable execution deadline";
    WorkflowError error =
        new WorkflowError(
            "https://open-workflow-specification.org/spec/1.0.0/errors/timeout",
            408,
            instance,
            title,
            detail);
    long sequence = current.nextSequence();
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.TIMER_FIRED,
            step,
            timeout.input(),
            timeoutDescriptor(current, timeout)));
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.ERROR_RAISED,
            step,
            timeout.input(),
            errorReference(error)));
    if (!timeout.workflowTimeout()) {
      return routeWorkflowError(context, current, command, step, error, sequence, events);
    }

    ExecutionFailure failure = ExecutionFailure.fromWorkflowError(error, "/", current.data());
    events.add(
        failedEvent(
            current.key(),
            current.plan().definitionSha256(),
            sequence++,
            null,
            current.data(),
            failure,
            iterationPositions(current.plan(), current.cursor()),
            current.forkPositions(),
            command));
    ExecutionSnapshot failed =
        new ExecutionSnapshot(
            current.key(),
            current.definition(),
            current.plan(),
            current.startedBy(),
            current.startedAt(),
            ExecutionPhase.FAILED,
            current.cursor(),
            current.initialInput(),
            current.context(),
            current.data(),
            sequence,
            failure,
            current.laneRootTaskPath(),
            null,
            current.forkPositions(),
            null,
            List.of());
    return DurableTransition.changed(
        failed,
        events,
        List.of(),
        interactionEffects(current, command, false, ":workflow-timeout"));
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      fireRootAsyncApiDeadline(
          DurableProcessContext context,
          ExecutionSnapshot current,
          FireTimerCommand command,
          ActiveAsyncApiSubscriptionState subscription) {
    if (command.requestedAt().isBefore(subscription.deadlineAt())) {
      throw new IllegalArgumentException("AsyncAPI deadline fired before its durable due time");
    }
    PlanStep step = current.plan().requireStep(subscription.taskPath());
    long sequence = current.nextSequence();
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.TIMER_FIRED,
            step,
            subscription.taskInput(),
            reference(asyncApiMessages(subscription.messages()))));
    ActiveAsyncApiSubscriptionState complete =
        new ActiveAsyncApiSubscriptionState(
            subscription.subscriptionId(),
            subscription.taskPath(),
            subscription.rawInput(),
            subscription.taskInput(),
            subscription.resumeCursor(),
            subscription.descriptor(),
            subscription.messages(),
            subscription.seenSourcePositions(),
            subscription.deadlineTimerId(),
            subscription.deadlineAt(),
            true);
    return completeAsyncApiSubscription(
        context, current, command, step, complete, sequence, events, List.of());
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      fireRootCorrelatedWorkerDeadline(
          DurableProcessContext context,
          ExecutionSnapshot current,
          FireTimerCommand command,
          ActiveCorrelatedWorkerState worker) {
    if (command.requestedAt().isBefore(worker.deadlineAt())) {
      throw new IllegalArgumentException(
          "Correlated-worker deadline fired before its durable " + "due time");
    }
    PlanStep step = current.plan().requireStep(worker.taskPath());
    WorkflowError error =
        new WorkflowError(
            "https://open-workflow-specification.org/spec/1.0.0/errors/timeout",
            408,
            worker.lifecycleId(),
            "Correlated worker timed out",
            "The external worker did not report a terminal outcome "
                + "before its durable deadline");
    long sequence = current.nextSequence();
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.TIMER_FIRED,
            step,
            worker.taskInput(),
            worker.subscriptionDescriptor()));
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.CORRELATED_WORKER_OUTCOME_UNKNOWN,
            step,
            worker.commandDescriptor(),
            errorReference(error)));
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.ERROR_RAISED,
            step,
            worker.taskInput(),
            errorReference(error)));
    List<WorkflowEffect> effects = new ArrayList<>();
    effects.add(
        correlatedWorkerSubscriptionEffect(
            current,
            command,
            worker,
            WorkflowEffectType.DELETE_ASYNC_API_SUBSCRIPTION,
            ":timeout"));
    if (worker.cancellationDescriptor() != null) {
      effects.add(
          correlatedWorkerOperationEffect(
              current, command, worker, worker.cancellationDescriptor(), ":timeout"));
    }
    ExecutionSnapshot timedOut =
        withPendingInteraction(
            current,
            ExecutionPhase.RUNNING,
            current.cursor(),
            current.context(),
            current.data(),
            sequence,
            null);
    return withOutbox(
        routeWorkflowError(context, timedOut, command, step, error, sequence, events), effects);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      fireRootAttemptDeadline(
          DurableProcessContext context,
          ExecutionSnapshot current,
          FireTimerCommand command,
          AttemptDeadline deadline) {
    if (command.requestedAt().isBefore(deadline.state().attemptDeadlineAt())) {
      throw new IllegalArgumentException(
          "Retry attempt deadline fired before its durable due time");
    }
    PlanStep step = current.plan().requireStep(deadline.taskPath());
    long sequence = current.nextSequence();
    DataReference descriptor = attemptDeadlineDescriptor(current, step, deadline.state());
    WorkflowError error =
        new WorkflowError(
            "https://open-workflow-specification.org/spec/1.0.0/errors/timeout",
            408,
            step.path(),
            "Retry attempt timed out",
            "Retry attempt "
                + deadline.state().attempt()
                + " exceeded its durable execution deadline");
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.TIMER_FIRED,
            step,
            current.data(),
            descriptor));
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.ERROR_RAISED,
            step,
            current.data(),
            errorReference(error)));
    return routeWorkflowError(context, current, command, step, error, sequence, events);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      fireRootTimer(
          DurableProcessContext context,
          ExecutionSnapshot current,
          FireTimerCommand command,
          ActiveTimerState timer) {
    if (command.requestedAt().isBefore(timer.dueAt())) {
      throw new IllegalArgumentException("Timer fired before its durable due time");
    }
    PlanStep step = current.plan().requireStep(timer.taskPath());
    long sequence = current.nextSequence();
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.TIMER_FIRED,
            step,
            timer.taskInput(),
            timer.taskInput()));
    ExecutionSnapshot resumed =
        withPendingInteraction(
            current,
            ExecutionPhase.RUNNING,
            timer.resumeCursor(),
            current.context(),
            timer.taskInput(),
            sequence,
            null);
    List<PlanStep> siblings = childrenForFrame(resumed, timer.resumeCursor().current());
    return withOutbox(
        completeTask(
            context,
            resumed,
            command,
            step,
            timer.rawInput(),
            timer.taskInput(),
            timer.taskInput(),
            timer.resumeCursor(),
            siblings,
            sequence,
            events),
        timerEffect(current, command, step, timer, WorkflowEffectType.CANCEL_TIMER, ":complete"));
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      fireRootRetry(
          DurableProcessContext context,
          ExecutionSnapshot current,
          FireTimerCommand command,
          ActiveRetryState retry) {
    if (command.requestedAt().isBefore(retry.dueAt())) {
      throw new IllegalArgumentException("Retry timer fired before its durable due time");
    }
    PlanStep step = current.plan().requireStep(retry.taskPath());
    long sequence = current.nextSequence();
    DataReference errorData = errorReference(retry.error());
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.RETRY_STARTED,
            step,
            errorData,
            retry.retryInput(),
            retry.retryCursor()));
    ExecutionSnapshot resumed =
        withPendingInteraction(
            current,
            ExecutionPhase.RUNNING,
            retry.retryCursor(),
            current.context(),
            retry.retryInput(),
            sequence,
            null);
    List<WorkflowEffect> effects = new ArrayList<>();
    effects.add(
        retryTimerEffect(
            current, command, step, retry, WorkflowEffectType.CANCEL_TIMER, ":started"));
    TryRuntimeState retryState = activeTryState(retry.retryCursor(), retry.taskPath());
    if (retryState.attemptDeadlineId() != null) {
      effects.add(
          attemptDeadlineEffect(
              current, command, step, retryState, WorkflowEffectType.SCHEDULE_TIMER, ""));
    }
    return withOutbox(changedWithContinuation(context, resumed, events, command), effects);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      receiveRootEvent(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ReceiveEventCommand command,
          ActiveListenState listen) {
    PlanStep step = current.plan().requireStep(listen.taskPath());
    ListenUpdate update = consume(current, command, step, listen);
    if (!update.accepted() && !update.complete()) {
      /*
       * A tenant event is an observation, not a malformed execution
       * command.  Multiple active subscriptions may inspect the same
       * CloudEvent, so a legitimate non-match must be a durable no-op
       * rather than a poison record.
       */
      return DurableTransition.unchanged(current);
    }
    long sequence = current.nextSequence();
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    if (update.accepted()) {
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.EVENT_RECEIVED,
              step,
              command.event(),
              update.readEvent()));
    }
    ActiveListenState progressedListen =
        new ActiveListenState(
            listen.subscriptionId(),
            listen.taskPath(),
            listen.rawInput(),
            listen.taskInput(),
            listen.resumeCursor(),
            update.consumed(),
            update.matchedStrategies(),
            update.correlations());
    if (!update.complete()) {
      ExecutionSnapshot progressed =
          withPendingInteraction(
              current,
              current.phase(),
              current.cursor(),
              current.context(),
              current.data(),
              sequence,
              progressedListen);
      return DurableTransition.changed(progressed, events, List.of());
    }

    DataReference result = listenResult(step.listenPlan(), update.consumed());
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.SUBSCRIPTION_COMPLETED,
            step,
            listen.taskInput(),
            result));
    WorkflowEffect delete =
        new WorkflowEffect(
            listen.subscriptionId() + ":delete",
            current.key(),
            WorkflowEffectType.DELETE_EVENT_SUBSCRIPTION,
            step.path(),
            subscriptionDescriptor(current, step, progressedListen),
            command.actor(),
            command.requestedAt());
    if (step.listenPlan().foreach()) {
      ArrayNode collection = JsonNodeFactory.instance.arrayNode();
      update.consumed().forEach(value -> collection.add(inline(value).deepCopy()));
      DataReference collectionReference = reference(collection);
      if (collection.isEmpty()) {
        ExecutionSnapshot resumed =
            withPendingInteraction(
                current,
                ExecutionPhase.RUNNING,
                listen.resumeCursor(),
                current.context(),
                result,
                sequence,
                null);
        List<PlanStep> siblings = childrenForFrame(resumed, listen.resumeCursor().current());
        return withOutbox(
            completeTask(
                context,
                resumed,
                command,
                step,
                listen.rawInput(),
                listen.taskInput(),
                result,
                listen.resumeCursor(),
                siblings,
                sequence,
                events),
            delete);
      }
      ExecutionCursor iterator =
          listen
              .resumeCursor()
              .enterFor(step.path(), listen.rawInput(), listen.taskInput(), collectionReference);
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.ITERATION_STARTED,
              step,
              listen.taskInput(),
              null,
              iterator));
      ExecutionSnapshot iterating =
          withPendingInteraction(
              current,
              ExecutionPhase.RUNNING,
              iterator,
              current.context(),
              listen.taskInput(),
              sequence,
              null);
      return withOutbox(changedWithContinuation(context, iterating, events, command), delete);
    }
    ExecutionSnapshot resumed =
        withPendingInteraction(
            current,
            ExecutionPhase.RUNNING,
            listen.resumeCursor(),
            current.context(),
            result,
            sequence,
            null);
    List<PlanStep> siblings = childrenForFrame(resumed, listen.resumeCursor().current());
    var completed =
        completeTask(
            context,
            resumed,
            command,
            step,
            listen.rawInput(),
            listen.taskInput(),
            result,
            listen.resumeCursor(),
            siblings,
            sequence,
            events);
    return withOutbox(completed, delete);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      observeOperation(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ObserveOperationCommand command) {
    requireSameExecution(current, command);
    if (current.phase().terminal()) {
      return DurableTransition.unchanged(current);
    }
    if (current.phase() == ExecutionPhase.PURGING) {
      if (current.pendingInteraction() instanceof ActiveExecutionPurgeState purge
          && purge.purgeId().equals(command.operationId())) {
        return observeExecutionPurge(current, command, purge);
      }
      return DurableTransition.unchanged(current);
    }
    if (current.phase() == ExecutionPhase.CANCEL_REQUESTED) {
      return observeCancellationOperation(current, command);
    }
    if (current.phase() != ExecutionPhase.RUNNING && current.phase() != ExecutionPhase.PAUSED) {
      throw new IllegalArgumentException(
          "Operations cannot enter an execution in phase " + current.phase());
    }
    if (current.pendingInteraction() instanceof ActiveOperationState operation
        && operation.operationId().equals(command.operationId())) {
      return observeRootOperation(context, current, command, operation);
    }
    if (current.pendingInteraction() instanceof ActiveCorrelatedWorkerState worker
        && worker.lifecycleId().equals(command.operationId())) {
      return observeRootCorrelatedWorkerPublication(context, current, command, worker);
    }
    if (current.activeFork() != null
        && current.activeFork().branches().stream()
            .anyMatch(branch -> containsInteraction(branch, command.operationId()))) {
      return resumeForkInteraction(context, current, command, command.operationId());
    }
    /*
     * Adapters can race a committed cancellation or terminal outcome.
     * A correctly scoped but now-stale observation is harmless.
     */
    return DurableTransition.unchanged(current);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      purge(ExecutionSnapshot current, PurgeExecutionCommand command) {
    requireSameExecution(current, command);
    if (current.phase() == ExecutionPhase.PURGING) {
      return DurableTransition.unchanged(current);
    }
    if (!current.phase().terminal()) {
      throw new IllegalArgumentException("Only a terminal execution can be purged");
    }
    if (!command.actor().roles().contains(PurgeExecutionCommand.REQUIRED_ROLE)) {
      throw new SecurityException(
          "Execution purge requires role " + PurgeExecutionCommand.REQUIRED_ROLE);
    }
    if (!current.key().equals(command.policyDecision().execution())) {
      throw new SecurityException("Purge policy decision targets another execution");
    }
    if (command.policyDecision().legalHold()) {
      throw new SecurityException("Execution is protected by a legal hold");
    }
    if (command.policyDecision().evaluatedAt().isAfter(command.requestedAt())) {
      throw new SecurityException("Purge policy decision was evaluated in the future");
    }
    if (command.policyDecision().eligibleAt().isAfter(command.requestedAt())) {
      throw new SecurityException("Execution has not reached its purge eligibility time");
    }

    String purgeId =
        stableId("purge", current.key().canonical() + "|" + command.policyDecision().decisionId());
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("operationId", purgeId);
    descriptor.put("operationKind", "executionPurge");
    descriptor.put("executionKey", current.key().canonical());
    descriptor.put("taskPath", ActiveExecutionPurgeState.TASK_PATH);
    descriptor.put("definitionReference", current.definition().canonical());
    descriptor.put("policyDecisionId", command.policyDecision().decisionId());
    descriptor.put("policyVersion", command.policyDecision().policyVersion());
    descriptor.put("retentionClass", command.policyDecision().retentionClass());
    descriptor.put("evaluatedAt", command.policyDecision().evaluatedAt().toString());

    DataReference purgeDescriptor = controlReference(descriptor);
    ActiveExecutionPurgeState pending =
        new ActiveExecutionPurgeState(
            purgeId, current.phase(), current.failure(), command.policyDecision(), purgeDescriptor);
    long sequence = current.nextSequence();
    ExecutionHistoryEvent requested =
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.EXECUTION_PURGE_REQUESTED,
            null,
            purgeDescriptor,
            purgeDescriptor);
    ExecutionSnapshot purging =
        new ExecutionSnapshot(
            current.key(),
            current.definition(),
            current.plan(),
            current.startedBy(),
            current.startedAt(),
            ExecutionPhase.PURGING,
            current.cursor(),
            current.initialInput(),
            current.context(),
            current.data(),
            sequence,
            null,
            current.laneRootTaskPath(),
            null,
            current.forkPositions(),
            pending,
            List.of(),
            null,
            null);
    WorkflowEffect dispatch =
        new WorkflowEffect(
            purgeId,
            current.key(),
            WorkflowEffectType.DISPATCH_OPERATION,
            ActiveExecutionPurgeState.TASK_PATH,
            purgeDescriptor,
            command.actor(),
            command.requestedAt());
    return DurableTransition.changed(purging, List.of(requested), List.of(), List.of(dispatch));
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      observeExecutionPurge(
          ExecutionSnapshot current,
          ObserveOperationCommand command,
          ActiveExecutionPurgeState purge) {
    OperationObservation observation = command.observation();
    if (observation.status() == OperationObservationStatus.PROGRESS) {
      return DurableTransition.unchanged(current);
    }
    long sequence = current.nextSequence();
    if (observation.status() != OperationObservationStatus.SUCCEEDED) {
      ExecutionHistoryEvent failed =
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.EXECUTION_PURGE_FAILED,
              null,
              purge.descriptor(),
              errorReference(observation.error()));
      ExecutionSnapshot restored =
          new ExecutionSnapshot(
              current.key(),
              current.definition(),
              current.plan(),
              current.startedBy(),
              current.startedAt(),
              purge.terminalPhase(),
              current.cursor(),
              current.initialInput(),
              current.context(),
              current.data(),
              sequence,
              purge.terminalFailure(),
              current.laneRootTaskPath(),
              null,
              current.forkPositions(),
              null,
              List.of(),
              null,
              null);
      return DurableTransition.changed(restored, List.of(failed), List.of());
    }

    ObjectNode receipt = JsonNodeFactory.instance.objectNode();
    receipt.put("executionKey", current.key().canonical());
    receipt.put("purgeId", purge.purgeId());
    receipt.put("policyDecisionId", purge.policyDecision().decisionId());
    receipt.put("policyVersion", purge.policyDecision().policyVersion());
    receipt.put("purgedAt", command.requestedAt().toString());
    DataReference receiptReference = reference(receipt);
    ExecutionHistoryEvent purged =
        event(
            current,
            command,
            sequence,
            ExecutionEventType.EXECUTION_PURGED,
            null,
            purge.descriptor(),
            receiptReference);
    WorkflowEffect cleanup =
        new WorkflowEffect(
            purge.purgeId() + ":projections",
            current.key(),
            WorkflowEffectType.PURGE_EXECUTION_PROJECTIONS,
            ActiveExecutionPurgeState.TASK_PATH,
            receiptReference,
            command.actor(),
            command.requestedAt());
    return DurableTransition.removed(List.of(purged), List.of(cleanup));
  }

  private static String stableId(String prefix, String identity) {
    try {
      return prefix
          + "-"
          + HexFormat.of()
              .formatHex(
                  MessageDigest.getInstance("SHA-256")
                      .digest(identity.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("JVM does not provide SHA-256", impossible);
    }
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      observeRootCorrelatedWorkerPublication(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ObserveOperationCommand command,
          ActiveCorrelatedWorkerState worker) {
    if (worker.commandPublished()
        && command.observation().status() == OperationObservationStatus.SUCCEEDED) {
      return DurableTransition.unchanged(current);
    }
    PlanStep step = current.plan().requireStep(worker.taskPath());
    long sequence = current.nextSequence();
    if (command.observation().status() == OperationObservationStatus.PROGRESS) {
      ExecutionHistoryEvent progress =
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.OPERATION_PROGRESS,
              step,
              worker.commandDescriptor(),
              command.observation().metadata() == null
                  ? reference(JsonNodeFactory.instance.objectNode())
                  : command.observation().metadata());
      return DurableTransition.changed(
          withPendingInteraction(
              current,
              current.phase(),
              current.cursor(),
              current.context(),
              current.data(),
              sequence,
              worker),
          List.of(progress),
          List.of());
    }
    if (command.observation().status() == OperationObservationStatus.SUCCEEDED) {
      ExecutionHistoryEvent published =
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.CORRELATED_WORKER_COMMAND_PUBLISHED,
              step,
              worker.taskInput(),
              command.observation().output());
      return DurableTransition.changed(
          withPendingInteraction(
              current,
              current.phase(),
              current.cursor(),
              current.context(),
              current.data(),
              sequence,
              worker.withCommandPublished()),
          List.of(published),
          List.of());
    }

    WorkflowError error = command.observation().error();
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.CORRELATED_WORKER_FAILED,
            step,
            worker.commandDescriptor(),
            errorReference(error)));
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.ERROR_RAISED,
            step,
            worker.taskInput(),
            errorReference(error)));
    ExecutionSnapshot observed =
        withPendingInteraction(
            current,
            ExecutionPhase.RUNNING,
            current.cursor(),
            current.context(),
            current.data(),
            sequence,
            null);
    return withOutbox(
        routeWorkflowError(context, observed, command, step, error, sequence, events),
        correlatedWorkerCleanupEffects(current, command, worker, List.of(), ":publish-failed"));
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      observeRootOperation(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ObserveOperationCommand command,
          ActiveOperationState operation) {
    PlanStep step = current.plan().requireStep(operation.taskPath());
    long sequence = current.nextSequence();
    DataReference metadata =
        command.observation().metadata() == null
            ? reference(JsonNodeFactory.instance.objectNode())
            : command.observation().metadata();
    if (command.observation().status() == OperationObservationStatus.PROGRESS) {
      ExecutionHistoryEvent progress =
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.OPERATION_PROGRESS,
              step,
              operation.descriptor(),
              metadata);
      return DurableTransition.changed(
          withPendingInteraction(
              current,
              current.phase(),
              current.cursor(),
              current.context(),
              current.data(),
              sequence,
              operation),
          List.of(progress),
          List.of());
    }

    if (current.phase() == ExecutionPhase.PAUSED) {
      OperationObservation outcome = command.observation();
      if (operation.completionReady() && operation.terminalObservation().equals(outcome)) {
        return DurableTransition.unchanged(current);
      }
      if (operation.completionReady()) {
        outcome = conflictingObservation(operation, command.observation());
      }
      DataReference audit =
          outcome.status() == OperationObservationStatus.SUCCEEDED
              ? outcome.output()
              : errorReference(outcome.error());
      ExecutionHistoryEvent buffered =
          event(
              current,
              command,
              sequence++,
              outcome.status() == OperationObservationStatus.SUCCEEDED
                  ? ExecutionEventType.OPERATION_RESULT_BUFFERED
                  : ExecutionEventType.OPERATION_FAILURE_BUFFERED,
              step,
              operation.descriptor(),
              audit);
      ActiveOperationState ready = operation.withTerminalObservation(outcome);
      return DurableTransition.changed(
          withPendingInteraction(
              current,
              ExecutionPhase.PAUSED,
              current.cursor(),
              current.context(),
              current.data(),
              sequence,
              ready),
          List.of(buffered),
          List.of());
    }

    return completeOperation(context, current, command, operation, command.observation());
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      observeCancellationOperation(ExecutionSnapshot current, ObserveOperationCommand command) {
    OperationLocation location = operationLocation(current, command.operationId());
    if (location == null) {
      // A terminal observation may race the committed cancellation
      // deadline. Once the operation has been accounted for, a replay is
      // an expected no-op.
      return DurableTransition.unchanged(current);
    }
    ActiveOperationState operation = location.operation();
    if (command.observation().status() == OperationObservationStatus.PROGRESS) {
      long sequence = current.nextSequence();
      ExecutionHistoryEvent progress =
          interactionEvent(
              current,
              command,
              sequence++,
              ExecutionEventType.OPERATION_PROGRESS,
              current.plan().requireStep(operation.taskPath()),
              operation.descriptor(),
              command.observation().metadata() == null
                  ? reference(JsonNodeFactory.instance.objectNode())
                  : command.observation().metadata(),
              location.cursor(),
              location.forkPositions());
      return DurableTransition.changed(
          copyForCancellation(
              current, current.pendingInteraction(), current.activeFork(), sequence),
          List.of(progress),
          List.of());
    }
    if (operation.completionReady()) {
      return DurableTransition.unchanged(current);
    }

    long sequence = current.nextSequence();
    OperationObservation observation = command.observation();
    DataReference audit;
    ExecutionEventType eventType;
    if (observation.status() == OperationObservationStatus.SUCCEEDED) {
      eventType = ExecutionEventType.OPERATION_COMPLETED;
      audit = observation.output();
    } else if (observation.status() == OperationObservationStatus.CANCELLED) {
      eventType = ExecutionEventType.OPERATION_CANCELLED;
      audit =
          observation.metadata() == null
              ? reference(JsonNodeFactory.instance.objectNode())
              : observation.metadata();
    } else {
      eventType = ExecutionEventType.ERROR_RAISED;
      audit = errorReference(observation.error());
    }
    ExecutionHistoryEvent terminal =
        interactionEvent(
            current,
            command,
            sequence++,
            eventType,
            current.plan().requireStep(operation.taskPath()),
            operation.descriptor(),
            audit,
            location.cursor(),
            location.forkPositions());

    PendingInteraction root = current.pendingInteraction();
    ForkRuntimeState fork = current.activeFork();
    ActiveOperationState observed = operation.withTerminalObservation(observation);
    if (root instanceof ActiveOperationState rootOperation
        && rootOperation.operationId().equals(command.operationId())) {
      root = observed;
    } else {
      fork = replaceOperation(fork, command.operationId(), observed);
    }
    ExecutionSnapshot progressed = copyForCancellation(current, root, fork, sequence);
    if (hasUnresolvedOperation(progressed)) {
      return DurableTransition.changed(progressed, List.of(terminal), List.of());
    }
    return finalizeCancellation(progressed, command, List.of(terminal), false);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      observeHumanTask(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ObserveHumanTaskCommand command) {
    requireSameExecution(current, command);
    if (current.phase().terminal()) {
      return DurableTransition.unchanged(current);
    }
    if (current.phase() != ExecutionPhase.RUNNING && current.phase() != ExecutionPhase.PAUSED) {
      /*
       * Human-task cancellation is asynchronous. An outcome that races
       * terminal workflow cancellation is expected and harmless.
       */
      if (current.phase() == ExecutionPhase.CANCEL_REQUESTED) {
        return DurableTransition.unchanged(current);
      }
      throw new IllegalArgumentException(
          "Human-task outcomes cannot enter an execution in phase " + current.phase());
    }
    if (current.pendingInteraction() instanceof ActiveHumanTaskState humanTask
        && humanTask.humanTaskId().equals(command.humanTaskId())) {
      return observeRootHumanTask(context, current, command, humanTask);
    }
    if (current.activeFork() != null
        && current.activeFork().branches().stream()
            .anyMatch(branch -> containsInteraction(branch, command.humanTaskId()))) {
      return resumeForkInteraction(context, current, command, command.humanTaskId());
    }
    return DurableTransition.unchanged(current);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      observeRootHumanTask(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ObserveHumanTaskCommand command,
          ActiveHumanTaskState humanTask) {
    if (!humanTask.correlationId().equals(command.correlationId())) {
      throw new IllegalArgumentException(
          "Human-task correlation ID does not match the active " + "workflow wait");
    }
    if (humanTask.completionReady()) {
      return DurableTransition.unchanged(current);
    }
    if (current.phase() == ExecutionPhase.PAUSED) {
      long sequence = current.nextSequence();
      ExecutionHistoryEvent buffered =
          humanTaskEvent(
              current,
              command,
              command.observation(),
              sequence++,
              ExecutionEventType.HUMAN_TASK_OUTCOME_BUFFERED,
              current.plan().requireStep(humanTask.taskPath()),
              humanTask.descriptor(),
              humanTaskOutcomeData(humanTask, command.observation()),
              current.cursor(),
              current.forkPositions());
      return DurableTransition.changed(
          withPendingInteraction(
              current,
              ExecutionPhase.PAUSED,
              current.cursor(),
              current.context(),
              current.data(),
              sequence,
              humanTask.withTerminalObservation(command.observation())),
          List.of(buffered),
          List.of());
    }
    return completeHumanTask(context, current, command, humanTask, command.observation());
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      completeHumanTask(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ExecutionCommand command,
          ActiveHumanTaskState humanTask,
          HumanTaskObservation observation) {
    PlanStep step = current.plan().requireStep(humanTask.taskPath());
    long sequence = current.nextSequence();
    DataReference outcome = humanTaskOutcomeData(humanTask, observation);
    ExecutionEventType type =
        switch (observation.status()) {
          case APPROVED -> ExecutionEventType.HUMAN_TASK_APPROVED;
          case REJECTED -> ExecutionEventType.HUMAN_TASK_REJECTED;
          case REWORK_REQUESTED -> ExecutionEventType.HUMAN_TASK_REWORK_REQUESTED;
          case EXPIRED -> ExecutionEventType.HUMAN_TASK_EXPIRED;
          case CANCELLED -> ExecutionEventType.HUMAN_TASK_CANCELLED;
        };
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    events.add(
        humanTaskEvent(
            current,
            command,
            observation,
            sequence++,
            type,
            step,
            humanTask.descriptor(),
            outcome,
            current.cursor(),
            current.forkPositions()));
    List<WorkflowEffect> effects = new ArrayList<>();
    if (humanTask.dueTimerId() != null) {
      effects.add(
          humanTaskDeadlineEffect(
              current, command, step, humanTask, WorkflowEffectType.CANCEL_TIMER, ":completed"));
    }
    ExecutionSnapshot resumed =
        withPendingInteraction(
            current,
            ExecutionPhase.RUNNING,
            humanTask.resumeCursor(),
            current.context(),
            outcome,
            sequence,
            null);
    if (observation.status() == HumanTaskObservationStatus.APPROVED) {
      List<PlanStep> siblings = childrenForFrame(resumed, humanTask.resumeCursor().current());
      return withOutbox(
          completeTask(
              context,
              resumed,
              command,
              step,
              humanTask.rawInput(),
              humanTask.taskInput(),
              outcome,
              humanTask.resumeCursor(),
              siblings,
              sequence,
              events),
          effects);
    }

    WorkflowError error = humanTaskError(humanTask, observation);
    events.add(
        humanTaskEvent(
            current,
            command,
            observation,
            sequence++,
            ExecutionEventType.ERROR_RAISED,
            step,
            humanTask.taskInput(),
            errorReference(error),
            current.cursor(),
            current.forkPositions()));
    return withOutbox(
        routeWorkflowError(
            context,
            withPendingInteraction(
                resumed,
                ExecutionPhase.RUNNING,
                humanTask.resumeCursor(),
                current.context(),
                current.data(),
                sequence,
                null),
            command,
            step,
            error,
            sequence,
            events),
        effects);
  }

  private static DataReference humanTaskOutcomeData(
      ActiveHumanTaskState humanTask, HumanTaskObservation observation) {
    return observation.data() == null ? humanTask.taskInput() : observation.data();
  }

  private static WorkflowError humanTaskError(
      ActiveHumanTaskState humanTask, HumanTaskObservation observation) {
    int status =
        switch (observation.status()) {
          case REJECTED, REWORK_REQUESTED -> 409;
          case EXPIRED -> 408;
          case CANCELLED -> 499;
          case APPROVED ->
              throw new IllegalArgumentException("An approved human task is not an error");
        };
    String outcome =
        observation.status().name().toLowerCase(java.util.Locale.ROOT).replace('_', '-');
    return new WorkflowError(
        "https://forwardmeasure.com/oks/errors/human-task/" + outcome,
        status,
        humanTask.taskPath(),
        "Human task " + outcome.replace('-', ' '),
        "Human task " + humanTask.humanTaskId() + " returned " + observation.status());
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      completeOperation(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ExecutionCommand command,
          ActiveOperationState operation,
          OperationObservation observation) {
    PlanStep step = current.plan().requireStep(operation.taskPath());
    long sequence = current.nextSequence();
    DataReference metadata =
        observation.metadata() == null
            ? reference(JsonNodeFactory.instance.objectNode())
            : observation.metadata();
    if (observation.status() == OperationObservationStatus.SUCCEEDED) {
      DataReference output = observation.output();
      List<ExecutionHistoryEvent> events = new ArrayList<>();
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.OPERATION_COMPLETED,
              step,
              operation.descriptor(),
              output));
      ExecutionSnapshot resumed =
          withPendingInteraction(
              current,
              ExecutionPhase.RUNNING,
              operation.resumeCursor(),
              current.context(),
              output,
              sequence,
              null);
      List<PlanStep> siblings = childrenForFrame(resumed, operation.resumeCursor().current());
      return completeTask(
          context,
          resumed,
          command,
          step,
          operation.rawInput(),
          operation.taskInput(),
          output,
          operation.resumeCursor(),
          siblings,
          sequence,
          events);
    }

    WorkflowError error = observation.error();
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    if (observation.status() == OperationObservationStatus.CANCELLED) {
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.OPERATION_CANCELLED,
              step,
              operation.descriptor(),
              metadata));
    }
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.ERROR_RAISED,
            step,
            operation.taskInput(),
            errorReference(error)));
    ExecutionSnapshot observed =
        withPendingInteraction(
            current,
            ExecutionPhase.RUNNING,
            current.cursor(),
            current.context(),
            current.data(),
            sequence,
            null);
    return routeWorkflowError(context, observed, command, step, error, sequence, events);
  }

  private static OperationObservation conflictingObservation(
      ActiveOperationState operation, OperationObservation conflicting) {
    WorkflowError error =
        new WorkflowError(
            RUNTIME_ERROR_TYPE,
            409,
            operation.taskPath(),
            "Conflicting terminal operation observations",
            "Operation "
                + operation.operationId()
                + " produced incompatible terminal outcomes: "
                + operation.terminalObservation().status()
                + " and "
                + conflicting.status());
    return new OperationObservation(OperationObservationStatus.FAILED, null, error, null);
  }

  private ListenUpdate consume(
      ExecutionSnapshot current,
      ReceiveEventCommand command,
      PlanStep step,
      ActiveListenState state) {
    JsonNode envelope = inline(command.event());
    Map<String, Set<Integer>> progress = new LinkedHashMap<>(state.matchedStrategies());
    Map<String, JsonNode> correlations = new LinkedHashMap<>(state.correlations());
    StrategyUpdate primary =
        consumeStrategy(
            current,
            command,
            step,
            step.listenPlan().consumption(),
            "primary",
            envelope,
            progress,
            correlations,
            state.consumedEvents(),
            state.taskInput());
    List<DataReference> consumed = new ArrayList<>(state.consumedEvents());
    DataReference read = readEvent(step.listenPlan(), envelope);
    if (primary.consumed()) consumed.add(read);

    boolean complete = primary.complete();
    String until = step.listenPlan().consumption().untilCondition();
    if (until != null && primary.consumed()) {
      ArrayNode values = JsonNodeFactory.instance.arrayNode();
      consumed.forEach(value -> values.add(inline(value).deepCopy()));
      complete =
          expressions.evaluateCondition(
              OpenWorkflowCompiler.requiredExpression(until),
              values,
              expressionArguments(
                  current,
                  step,
                  state.rawInput(),
                  state.taskInput(),
                  reference(values),
                  null,
                  command),
              current.plan().expressions().mode());
    }
    return new ListenUpdate(primary.observed(), complete, read, consumed, progress, correlations);
  }

  private StrategyUpdate consumeStrategy(
      ExecutionSnapshot current,
      ReceiveEventCommand command,
      PlanStep step,
      EventConsumptionPlan strategy,
      String strategyPath,
      JsonNode envelope,
      Map<String, Set<Integer>> progress,
      Map<String, JsonNode> correlations,
      List<DataReference> consumed,
      DataReference taskInput) {
    Set<Integer> matched =
        new java.util.LinkedHashSet<>(progress.getOrDefault(strategyPath, Set.of()));
    boolean accepted =
        strategy.mode() == EventConsumptionPlan.Mode.ANY && strategy.filters().isEmpty();
    for (int index = 0; index < strategy.filters().size(); index++) {
      if (strategy.mode() == EventConsumptionPlan.Mode.ALL && matched.contains(index)) {
        continue;
      }
      Map<String, JsonNode> candidateCorrelations =
          matchesFilter(
              current,
              command,
              step,
              strategy.filters().get(index),
              envelope,
              correlations,
              taskInput);
      if (candidateCorrelations == null) continue;
      correlations.clear();
      correlations.putAll(candidateCorrelations);
      matched.add(index);
      accepted = true;
      if (strategy.mode() != EventConsumptionPlan.Mode.ALL) break;
    }
    progress.put(strategyPath, Set.copyOf(matched));

    boolean complete =
        switch (strategy.mode()) {
          case ONE -> accepted;
          case ALL -> matched.size() == strategy.filters().size();
          case ANY ->
              accepted && strategy.untilCondition() == null && strategy.untilConsumed() == null;
        };
    boolean observed = accepted;
    if (strategy.mode() == EventConsumptionPlan.Mode.ANY && strategy.untilConsumed() != null) {
      StrategyUpdate until =
          consumeStrategy(
              current,
              command,
              step,
              strategy.untilConsumed(),
              strategyPath + "/until",
              envelope,
              progress,
              correlations,
              consumed,
              taskInput);
      complete = until.complete();
      observed = observed || until.observed();
    }
    return new StrategyUpdate(accepted, observed, complete);
  }

  private Map<String, JsonNode> matchesFilter(
      ExecutionSnapshot current,
      ReceiveEventCommand command,
      PlanStep step,
      EventFilterPlan filter,
      JsonNode envelope,
      Map<String, JsonNode> existingCorrelations,
      DataReference taskInput) {
    RuntimeExpressionArguments arguments =
        expressionArguments(current, step, taskInput, taskInput, null, null, command);
    var properties = filter.properties().properties().iterator();
    while (properties.hasNext()) {
      var property = properties.next();
      JsonNode actual =
          "data".equals(property.getKey())
              ? envelope.path("data")
              : envelope.path(property.getKey());
      JsonNode expected = property.getValue();
      if (expected.isTextual() && expected.textValue().trim().startsWith("${")) {
        if ("data".equals(property.getKey())) {
          if (!expressions.evaluateCondition(
              expected.textValue(), actual, arguments, current.plan().expressions().mode())) {
            return null;
          }
        } else {
          expected =
              expressions.evaluateExpression(
                  expected.textValue(),
                  inline(taskInput),
                  arguments,
                  current.plan().expressions().mode());
          if (!actual.equals(expected)) return null;
        }
      } else if (!actual.equals(expected)) {
        return null;
      }
    }

    schemas(current.plan()).validate(filter.dataSchema(), envelope.path("data"));

    Map<String, JsonNode> correlations = new LinkedHashMap<>(existingCorrelations);
    JsonNode eventData = envelope.path("data");
    for (var correlation : filter.correlations()) {
      JsonNode extracted =
          expressions.evaluateExpression(
              OpenWorkflowCompiler.requiredExpression(correlation.fromExpression()),
              eventData,
              arguments,
              current.plan().expressions().mode());
      JsonNode expected;
      if (correlation.expected() == null) {
        expected = correlations.get(correlation.name());
        if (expected == null) {
          correlations.put(correlation.name(), extracted.deepCopy());
          continue;
        }
      } else if (correlation.expected().trim().startsWith("${")) {
        expected =
            expressions.evaluateExpression(
                correlation.expected(),
                inline(taskInput),
                arguments,
                current.plan().expressions().mode());
      } else {
        expected = JsonNodeFactory.instance.textNode(correlation.expected());
      }
      if (!expected.equals(extracted)) return null;
    }
    return correlations;
  }

  private DataReference readEvent(ListenPlan plan, JsonNode envelope) {
    JsonNode value =
        switch (plan.readAs()) {
          case DATA -> envelope.path("data").deepCopy();
          case ENVELOPE, RAW -> envelope.deepCopy();
        };
    return reference(value);
  }

  private DataReference listenResult(ListenPlan plan, List<DataReference> consumed) {
    if (plan.consumption().mode() == EventConsumptionPlan.Mode.ONE && consumed.size() == 1) {
      return consumed.getFirst();
    }
    ArrayNode result = JsonNodeFactory.instance.arrayNode();
    consumed.forEach(value -> result.add(inline(value).deepCopy()));
    return reference(result);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      completeAsyncApiSubscription(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ExecutionCommand command,
          PlanStep step,
          ActiveAsyncApiSubscriptionState subscription,
          long sequence,
          List<ExecutionHistoryEvent> events,
          List<WorkflowEffect> precedingEffects) {
    DataReference result = reference(asyncApiMessages(subscription.messages()));
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.ASYNC_API_SUBSCRIPTION_COMPLETED,
            step,
            subscription.taskInput(),
            result));
    List<WorkflowEffect> effects = new ArrayList<>(precedingEffects);
    effects.add(
        asyncApiSubscriptionEffect(
            current,
            command,
            step,
            subscription,
            WorkflowEffectType.DELETE_ASYNC_API_SUBSCRIPTION,
            ":complete"));
    if (subscription.deadlineTimerId() != null) {
      effects.add(
          asyncApiDeadlineEffect(
              current, command, step, subscription, WorkflowEffectType.CANCEL_TIMER, ":complete"));
    }
    AsyncApiSubscriptionPlan plan = step.callPlan().asyncApiSubscription();
    if (plan.foreach() && !subscription.messages().isEmpty()) {
      ExecutionCursor iterator =
          subscription
              .resumeCursor()
              .enterFor(step.path(), subscription.rawInput(), subscription.taskInput(), result);
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.ITERATION_STARTED,
              step,
              subscription.taskInput(),
              null,
              iterator));
      ExecutionSnapshot iterating =
          withPendingInteraction(
              current,
              ExecutionPhase.RUNNING,
              iterator,
              current.context(),
              subscription.taskInput(),
              sequence,
              null);
      return withOutbox(changedWithContinuation(context, iterating, events, command), effects);
    }
    ExecutionSnapshot resumed =
        withPendingInteraction(
            current,
            ExecutionPhase.RUNNING,
            subscription.resumeCursor(),
            current.context(),
            result,
            sequence,
            null);
    List<PlanStep> siblings = childrenForFrame(resumed, subscription.resumeCursor().current());
    return withOutbox(
        completeTask(
            context,
            resumed,
            command,
            step,
            subscription.rawInput(),
            subscription.taskInput(),
            result,
            subscription.resumeCursor(),
            siblings,
            sequence,
            events),
        effects);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      completeCorrelatedWorker(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ExecutionCommand command,
          PlanStep step,
          ActiveCorrelatedWorkerState worker,
          JsonNode payload,
          String status,
          long sequence,
          List<ExecutionHistoryEvent> events,
          List<WorkflowEffect> precedingEffects) {
    List<WorkflowEffect> effects =
        correlatedWorkerCleanupEffects(current, command, worker, precedingEffects, ":terminal");
    if ("SUCCEEDED".equals(status)) {
      JsonNode value =
          payload.has("output")
              ? payload.required("output")
              : JsonNodeFactory.instance.objectNode();
      DataReference output = reference(value.deepCopy());
      ExecutionSnapshot resumed =
          withPendingInteraction(
              current,
              ExecutionPhase.RUNNING,
              worker.resumeCursor(),
              current.context(),
              output,
              sequence,
              null);
      List<PlanStep> siblings = childrenForFrame(resumed, worker.resumeCursor().current());
      return withOutbox(
          completeTask(
              context,
              resumed,
              command,
              step,
              worker.rawInput(),
              worker.taskInput(),
              output,
              worker.resumeCursor(),
              siblings,
              sequence,
              events),
          effects);
    }

    WorkflowError error = correlatedWorkerError(worker, payload, status);
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.ERROR_RAISED,
            step,
            worker.taskInput(),
            errorReference(error)));
    ExecutionSnapshot observed =
        withPendingInteraction(
            current,
            ExecutionPhase.RUNNING,
            current.cursor(),
            current.context(),
            current.data(),
            sequence,
            null);
    return withOutbox(
        routeWorkflowError(context, observed, command, step, error, sequence, events), effects);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      finalizeCorrelatedWorkerCancellation(
          ExecutionSnapshot current,
          ExecutionCommand command,
          ActiveCorrelatedWorkerState worker,
          long sequence,
          List<ExecutionHistoryEvent> events,
          List<WorkflowEffect> precedingEffects) {
    ExecutionSnapshot observed =
        withPendingInteraction(
            current,
            ExecutionPhase.CANCEL_REQUESTED,
            current.cursor(),
            current.context(),
            current.data(),
            sequence,
            null);
    return withOutbox(
        finalizeCancellation(observed, command, events, false),
        correlatedWorkerCleanupEffects(current, command, worker, precedingEffects, ":cancelled"));
  }

  private List<WorkflowEffect> correlatedWorkerCleanupEffects(
      ExecutionSnapshot current,
      ExecutionCommand command,
      ActiveCorrelatedWorkerState worker,
      List<WorkflowEffect> preceding,
      String suffix) {
    List<WorkflowEffect> effects = new ArrayList<>(preceding);
    effects.add(
        correlatedWorkerSubscriptionEffect(
            current, command, worker, WorkflowEffectType.DELETE_ASYNC_API_SUBSCRIPTION, suffix));
    effects.add(
        correlatedWorkerDeadlineEffect(
            current, command, worker, WorkflowEffectType.CANCEL_TIMER, suffix));
    return effects;
  }

  private static WorkflowError correlatedWorkerError(
      ActiveCorrelatedWorkerState worker, JsonNode payload, String status) {
    JsonNode declared = payload.path("error");
    String defaultType =
        "CANCELLED".equals(status)
            ? "https://open-workflow-specification.org/spec/1.0.0/errors/cancellation"
            : "https://open-workflow-specification.org/spec/1.0.0/errors/communication";
    int defaultStatus = "CANCELLED".equals(status) ? 499 : 500;
    return new WorkflowError(
        declared.path("type").asText(defaultType),
        declared.path("status").asInt(defaultStatus),
        declared.path("instance").asText(worker.lifecycleId()),
        declared
            .path("title")
            .asText("Correlated worker " + status.toLowerCase(java.util.Locale.ROOT)),
        declared.path("detail").asText("The correlated worker reported " + status));
  }

  private DataReference workerMessageAudit(
      ReceiveAsyncApiMessageCommand command, boolean accepted) {
    ObjectNode audit = JsonNodeFactory.instance.objectNode();
    audit.put("subscriptionId", command.subscriptionId());
    audit.put("sourcePosition", command.sourcePosition());
    audit.put("accepted", accepted);
    audit.set("message", inline(command.message()).deepCopy());
    return reference(audit);
  }

  private ArrayNode asyncApiMessages(List<DataReference> messages) {
    ArrayNode values = JsonNodeFactory.instance.arrayNode();
    messages.forEach(message -> values.add(inline(message).deepCopy()));
    return values;
  }

  private DataReference asyncApiMessageAudit(
      ReceiveAsyncApiMessageCommand command, boolean accepted) {
    ObjectNode audit = JsonNodeFactory.instance.objectNode();
    audit.put("subscriptionId", command.subscriptionId());
    audit.put("sourcePosition", command.sourcePosition());
    audit.put("accepted", accepted);
    audit.set("message", inline(command.message()).deepCopy());
    return reference(audit);
  }

  private static ExecutionSnapshot withPendingInteraction(
      ExecutionSnapshot current,
      ExecutionPhase phase,
      ExecutionCursor cursor,
      DataReference workflowContext,
      DataReference data,
      long nextSequence,
      PendingInteraction pending) {
    return new ExecutionSnapshot(
        current.key(),
        current.definition(),
        current.plan(),
        current.startedBy(),
        current.startedAt(),
        phase,
        cursor,
        current.initialInput(),
        workflowContext,
        data,
        nextSequence,
        null,
        current.laneRootTaskPath(),
        current.activeFork(),
        current.forkPositions(),
        pending,
        current.activeTimeouts(),
        current.cancellation());
  }

  private record StrategyUpdate(boolean consumed, boolean observed, boolean complete) {}

  private record ListenUpdate(
      boolean accepted,
      boolean complete,
      DataReference readEvent,
      List<DataReference> consumed,
      Map<String, Set<Integer>> matchedStrategies,
      Map<String, JsonNode> correlations) {}

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      control(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ControlExecutionCommand command) {
    requireSameExecution(current, command);
    return switch (command.action()) {
      case PAUSE -> pause(current, command);
      case RESUME -> resume(context, current, command);
      case CANCEL -> cancel(current, command);
    };
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      enterOrExecute(
          DurableProcessContext context,
          ExecutionSnapshot current,
          AdvanceExecutionCommand command,
          ExecutionFrame frame,
          List<PlanStep> siblings,
          PlanStep step) {
    DataReference rawInput = current.data();
    RuntimeExpressionArguments conditionArguments =
        step.dataFlow().condition() != null
                || expressions.requiresEvaluation(
                    step.dataFlow().inputFrom(), current.plan().expressions().mode())
            ? expressionArguments(current, step, rawInput, null, null, null, command)
            : null;
    ExecutionCursor parentAdvanced = current.cursor().replaceCurrent(frame.advance());
    long sequence = current.nextSequence();
    List<ExecutionHistoryEvent> events = new ArrayList<>();

    if (step.dataFlow().condition() != null
        && !expressions.evaluateCondition(
            step.dataFlow().condition(),
            inline(rawInput),
            conditionArguments,
            current.plan().expressions().mode())) {
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.TASK_SKIPPED,
              step,
              rawInput,
              rawInput));
      ExecutionCursor directed = applyFlowDirective(parentAdvanced, step, siblings);
      return finishTaskTransition(
          context, current, command, rawInput, current.context(), directed, sequence, events);
    }

    if (step.dataFlow().inputSchema() != null) {
      schemas(current.plan()).validate(step.dataFlow().inputSchema(), inline(rawInput));
    }
    DataReference taskInput =
        transform(step.dataFlow().inputFrom(), rawInput, conditionArguments, current.plan());
    events.add(
        event(
            current, command, sequence++, ExecutionEventType.TASK_STARTED, step, taskInput, null));

    ExecutionCursor cursor = parentAdvanced;
    DataReference output = taskInput;
    DataReference workflowContext = current.context();
    if (step.kind() == PlanStepKind.SET) {
      RuntimeExpressionArguments taskArguments =
          expressions.requiresEvaluation(step.configuration(), current.plan().expressions().mode())
              ? expressionArguments(current, step, rawInput, taskInput, null, null, command)
              : null;
      DataReference rawOutput =
          taskArguments == null
              ? reference(step.configuration().deepCopy())
              : reference(
                  expressions.evaluateTemplate(
                      step.configuration(),
                      inline(taskInput),
                      taskArguments,
                      current.plan().expressions().mode()));
      output =
          transform(
              step.dataFlow().outputAs(),
              rawOutput,
              expressionArguments(current, step, rawInput, taskInput, rawOutput, null, command),
              current.plan());
      schemas(current.plan()).validate(step.dataFlow().outputSchema(), inline(output));
      workflowContext =
          transform(
              step.dataFlow().exportAs(),
              output,
              expressionArguments(current, step, rawInput, taskInput, rawOutput, output, command),
              current.plan(),
              current.context());
      schemas(current.plan()).validate(step.dataFlow().exportSchema(), inline(workflowContext));
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.TASK_COMPLETED,
              step,
              taskInput,
              output));
      cursor = applyFlowDirective(parentAdvanced, step, siblings);
    } else if (step.kind() == PlanStepKind.SWITCH) {
      SwitchDecision decision = evaluateSwitch(current, step, rawInput, taskInput, command);
      DataReference rawOutput = taskInput;
      output =
          transform(
              step.dataFlow().outputAs(),
              rawOutput,
              expressionArguments(current, step, rawInput, taskInput, rawOutput, null, command),
              current.plan());
      schemas(current.plan()).validate(step.dataFlow().outputSchema(), inline(output));
      workflowContext =
          transform(
              step.dataFlow().exportAs(),
              output,
              expressionArguments(current, step, rawInput, taskInput, rawOutput, output, command),
              current.plan(),
              current.context());
      schemas(current.plan()).validate(step.dataFlow().exportSchema(), inline(workflowContext));
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.TASK_COMPLETED,
              step,
              taskInput,
              output,
              decision));
      if (isExtensionTarget(current, parentAdvanced, step)) {
        cursor =
            parentAdvanced.replaceCurrent(
                parentAdvanced.current().deferExtensionDirective(decision.flowDirective()));
      } else {
        cursor = applyFlowDirective(parentAdvanced, step, decision.flowDirective(), siblings);
      }
    } else if (step.kind() == PlanStepKind.EXTENSION) {
      cursor =
          parentAdvanced.enterExtension(
              step.path(),
              rawInput,
              taskInput,
              extensionDecisions(current, command, step, rawInput, taskInput));
    } else if (step.kind() == PlanStepKind.DO) {
      cursor = parentAdvanced.enter(step.path(), rawInput, taskInput);
    } else if (step.kind() == PlanStepKind.TRY) {
      cursor = parentAdvanced.enterTry(step.path(), rawInput, taskInput, command.requestedAt());
    } else if (step.kind() == PlanStepKind.FOR) {
      DataReference collection = forCollection(current, step, rawInput, taskInput, command);
      ExecutionCursor forCursor =
          parentAdvanced.enterFor(step.path(), rawInput, taskInput, collection);
      if (!continuesFor(current, step, taskInput, forCursor, command)) {
        return completeTask(
            context,
            current,
            command,
            step,
            rawInput,
            taskInput,
            taskInput,
            parentAdvanced,
            siblings,
            sequence,
            events);
      }
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.ITERATION_STARTED,
              step,
              taskInput,
              null,
              forCursor));
      cursor = forCursor;
    } else if (step.kind() == PlanStepKind.FORK) {
      ForkRuntimeState fork = startFork(step, rawInput, taskInput, parentAdvanced);
      ExecutionSnapshot snapshot =
          changed(
              current,
              ExecutionPhase.RUNNING,
              parentAdvanced,
              current.context(),
              taskInput,
              sequence,
              fork);
      return changedWithContinuation(context, snapshot, events, command);
    } else if (step.kind() == PlanStepKind.EMIT) {
      long emissionSequence = sequence;
      DataReference emitted =
          emittedCloudEvent(current, step, rawInput, taskInput, command, sequence);
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.EVENT_EMITTED,
              step,
              taskInput,
              emitted));
      var completed =
          completeTask(
              context,
              current,
              command,
              step,
              rawInput,
              taskInput,
              emitted,
              parentAdvanced,
              siblings,
              sequence,
              events);
      return withOutbox(
          completed,
          new WorkflowEffect(
              current.key().canonical() + ":" + emissionSequence,
              current.key(),
              WorkflowEffectType.EMIT_CLOUD_EVENT,
              step.path(),
              emitted,
              command.actor(),
              command.requestedAt()));
    } else if (step.kind() == PlanStepKind.LISTEN) {
      if (step.listenPlan().consumption().mode() == EventConsumptionPlan.Mode.ALL
          && step.listenPlan().consumption().filters().isEmpty()) {
        DataReference empty = reference(JsonNodeFactory.instance.arrayNode());
        return completeTask(
            context,
            current,
            command,
            step,
            rawInput,
            taskInput,
            empty,
            parentAdvanced,
            siblings,
            sequence,
            events);
      }
      String subscriptionId = current.key().canonical() + ":" + step.path() + ":" + sequence;
      ActiveListenState listen =
          ActiveListenState.start(subscriptionId, step.path(), rawInput, taskInput, parentAdvanced);
      DataReference descriptor = subscriptionDescriptor(current, step, listen);
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.SUBSCRIPTION_CREATED,
              step,
              taskInput,
              descriptor));
      ExecutionSnapshot waiting =
          new ExecutionSnapshot(
              current.key(),
              current.definition(),
              current.plan(),
              current.startedBy(),
              current.startedAt(),
              ExecutionPhase.RUNNING,
              parentAdvanced,
              current.initialInput(),
              current.context(),
              taskInput,
              sequence,
              null,
              current.laneRootTaskPath(),
              current.activeFork(),
              current.forkPositions(),
              listen,
              current.activeTimeouts(),
              current.cancellation());
      return DurableTransition.changed(
          waiting,
          events,
          List.of(),
          List.of(
              new WorkflowEffect(
                  subscriptionId,
                  current.key(),
                  WorkflowEffectType.UPSERT_EVENT_SUBSCRIPTION,
                  step.path(),
                  descriptor,
                  command.actor(),
                  command.requestedAt())));
    } else if (step.kind() == PlanStepKind.WAIT) {
      Duration duration = waitDuration(current, command, step, rawInput, taskInput);
      String timerId = current.key().canonical() + ":" + step.path() + ":" + sequence;
      ActiveTimerState timer =
          new ActiveTimerState(
              timerId,
              step.path(),
              rawInput,
              taskInput,
              parentAdvanced,
              command.requestedAt().plus(duration));
      DataReference descriptor = timerDescriptor(current, step, timer);
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.TIMER_SCHEDULED,
              step,
              taskInput,
              descriptor));
      ExecutionSnapshot waiting =
          new ExecutionSnapshot(
              current.key(),
              current.definition(),
              current.plan(),
              current.startedBy(),
              current.startedAt(),
              ExecutionPhase.RUNNING,
              parentAdvanced,
              current.initialInput(),
              current.context(),
              taskInput,
              sequence,
              null,
              current.laneRootTaskPath(),
              current.activeFork(),
              current.forkPositions(),
              timer,
              current.activeTimeouts(),
              current.cancellation());
      return DurableTransition.changed(
          waiting,
          events,
          List.of(),
          List.of(
              timerEffect(current, command, step, timer, WorkflowEffectType.SCHEDULE_TIMER, "")));
    } else if (step.kind() == PlanStepKind.CALL) {
      JsonNode argumentTemplate = callArgumentTemplate(step.callPlan());
      DataReference arguments =
          transform(
              argumentTemplate,
              taskInput,
              expressions.requiresEvaluation(argumentTemplate, current.plan().expressions().mode())
                  ? expressionArguments(current, step, rawInput, taskInput, null, null, command)
                  : null,
              current.plan());
      if (step.callPlan().kind() == CallPlan.Kind.HUMAN_TASK) {
        ObjectNode configuration = requireHumanTaskConfiguration(inline(arguments), step);
        String humanTaskId = humanTaskId(current, step, sequence);
        BusinessCorrelationId correlationId =
            new BusinessCorrelationId(
                configuration.hasNonNull("correlationId")
                    ? requiredText(configuration, "correlationId", step.path())
                    : humanTaskId);
        Instant dueAt = humanTaskDueAt(configuration, command.requestedAt(), step.path());
        String dueTimerId = dueAt == null ? null : humanTaskId + ":due";
        DataReference descriptor =
            humanTaskDescriptor(
                current, step, humanTaskId, correlationId, configuration, taskInput, dueAt);
        ActiveHumanTaskState humanTask =
            new ActiveHumanTaskState(
                humanTaskId,
                correlationId,
                step.path(),
                rawInput,
                taskInput,
                parentAdvanced,
                descriptor,
                dueTimerId,
                dueAt);
        events.add(
            event(
                current,
                command,
                sequence++,
                ExecutionEventType.HUMAN_TASK_CREATED,
                step,
                taskInput,
                descriptor));
        List<WorkflowEffect> effects = new ArrayList<>();
        effects.add(
            humanTaskEffect(
                current, command, step, humanTask, WorkflowEffectType.CREATE_HUMAN_TASK, ""));
        if (dueAt != null) {
          effects.add(
              humanTaskDeadlineEffect(
                  current, command, step, humanTask, WorkflowEffectType.SCHEDULE_TIMER, ""));
        }
        ExecutionSnapshot waiting =
            withPendingInteraction(
                current,
                ExecutionPhase.RUNNING,
                parentAdvanced,
                current.context(),
                taskInput,
                sequence,
                humanTask);
        return DurableTransition.changed(waiting, events, List.of(), effects);
      } else if (step.callPlan().kind() == CallPlan.Kind.CORRELATED_WORKER) {
        ObjectNode configuration = requireCorrelatedWorkerConfiguration(inline(arguments), step);
        String lifecycleId = current.key().canonical() + ":" + step.path() + ":worker:" + sequence;
        DataReference base =
            callDescriptor(current, step, lifecycleId, arguments, taskInput, command, null);
        DataReference commandDescriptor =
            correlatedWorkerOperationDescriptor(
                base, lifecycleId, configuration.required("command"));
        DataReference subscriptionDescriptor =
            asyncApiSubscriptionDescriptor(
                correlatedWorkerOperationDescriptor(
                    base, lifecycleId, configuration.required("events")),
                lifecycleId,
                lifecycleId + ":deadline",
                command
                    .requestedAt()
                    .plus(
                        resolveDuration(
                            step.callPlan().asyncApiSubscription().consumption().duration(),
                            current,
                            command,
                            step,
                            rawInput,
                            taskInput,
                            step.path() + "/with/events/subscription/consume/for")));
        Instant deadlineAt =
            Instant.parse(inline(subscriptionDescriptor).required("deadlineAt").textValue());
        DataReference cancellationDescriptor =
            configuration.has("cancellation")
                ? correlatedWorkerOperationDescriptor(
                    base, lifecycleId + ":cancel", configuration.required("cancellation"))
                : null;
        ActiveCorrelatedWorkerState worker =
            new ActiveCorrelatedWorkerState(
                lifecycleId,
                step.path(),
                rawInput,
                taskInput,
                parentAdvanced,
                commandDescriptor,
                subscriptionDescriptor,
                cancellationDescriptor,
                Set.of(),
                lifecycleId + ":deadline",
                deadlineAt,
                false,
                null);
        events.add(
            event(
                current,
                command,
                sequence++,
                ExecutionEventType.CORRELATED_WORKER_STARTED,
                step,
                taskInput,
                subscriptionDescriptor));
        ExecutionSnapshot waiting =
            withPendingInteraction(
                current,
                ExecutionPhase.RUNNING,
                parentAdvanced,
                current.context(),
                taskInput,
                sequence,
                worker);
        return DurableTransition.changed(
            waiting,
            events,
            List.of(),
            List.of(
                correlatedWorkerSubscriptionEffect(
                    current, command, worker, WorkflowEffectType.UPSERT_ASYNC_API_SUBSCRIPTION, ""),
                correlatedWorkerOperationEffect(current, command, worker, commandDescriptor, ""),
                correlatedWorkerDeadlineEffect(
                    current, command, worker, WorkflowEffectType.SCHEDULE_TIMER, "")));
      } else if (step.callPlan().kind() == CallPlan.Kind.FUNCTION) {
        cursor = parentAdvanced.enter(step.path(), rawInput, taskInput);
        output = arguments;
      } else if (step.callPlan().asyncApiSubscription() != null) {
        String subscriptionId =
            current.key().canonical() + ":" + step.path() + ":asyncapi:" + sequence;
        AsyncApiSubscriptionPlan plan = step.callPlan().asyncApiSubscription();
        String timerId = null;
        Instant deadlineAt = null;
        if (plan.consumption().duration() != null) {
          Duration duration =
              resolveDuration(
                  plan.consumption().duration(),
                  current,
                  command,
                  step,
                  rawInput,
                  taskInput,
                  step.path() + "/with/subscription/consume/for");
          timerId = subscriptionId + ":deadline";
          deadlineAt = command.requestedAt().plus(duration);
        }
        DataReference descriptor =
            callDescriptor(
                current, step, subscriptionId, arguments, taskInput, command, deadlineAt);
        ActiveAsyncApiSubscriptionState subscription =
            new ActiveAsyncApiSubscriptionState(
                subscriptionId,
                step.path(),
                rawInput,
                taskInput,
                parentAdvanced,
                asyncApiSubscriptionDescriptor(descriptor, subscriptionId, timerId, deadlineAt),
                List.of(),
                Set.of(),
                timerId,
                deadlineAt,
                false);
        events.add(
            event(
                current,
                command,
                sequence++,
                ExecutionEventType.ASYNC_API_SUBSCRIPTION_CREATED,
                step,
                taskInput,
                subscription.descriptor()));
        List<WorkflowEffect> effects = new ArrayList<>();
        effects.add(
            asyncApiSubscriptionEffect(
                current,
                command,
                step,
                subscription,
                WorkflowEffectType.UPSERT_ASYNC_API_SUBSCRIPTION,
                ""));
        if (deadlineAt != null) {
          effects.add(
              asyncApiDeadlineEffect(
                  current, command, step, subscription, WorkflowEffectType.SCHEDULE_TIMER, ""));
        }
        ExecutionSnapshot waiting =
            withPendingInteraction(
                current,
                ExecutionPhase.RUNNING,
                parentAdvanced,
                current.context(),
                taskInput,
                sequence,
                subscription);
        return DurableTransition.changed(waiting, events, List.of(), effects);
      } else {
        String operationId =
            current.key().canonical() + ":" + step.path() + ":operation:" + sequence;
        DataReference descriptor =
            callDescriptor(current, step, operationId, arguments, taskInput, command, null);
        ActiveOperationState operation =
            new ActiveOperationState(
                operationId, step.path(), "call", rawInput, taskInput, parentAdvanced, descriptor);
        events.add(
            event(
                current,
                command,
                sequence++,
                ExecutionEventType.OPERATION_DISPATCHED,
                step,
                taskInput,
                descriptor));
        ExecutionSnapshot waiting =
            withPendingInteraction(
                current,
                ExecutionPhase.RUNNING,
                parentAdvanced,
                current.context(),
                taskInput,
                sequence,
                operation);
        return DurableTransition.changed(
            waiting,
            events,
            List.of(),
            List.of(
                operationEffect(
                    current, command, step, operation, WorkflowEffectType.DISPATCH_OPERATION, "")));
      }
    } else if (step.kind() == PlanStepKind.RUN && step.runPlan().kind() == RunPlan.Kind.WORKFLOW) {
      return dispatchSubworkflow(
          context, current, command, step, rawInput, taskInput, parentAdvanced, sequence, events);
    } else if (step.kind() == PlanStepKind.RUN) {
      RunPlan run = step.runPlan();
      DataReference arguments =
          transform(
              run.configuration(),
              taskInput,
              expressions.requiresEvaluation(
                      run.configuration(), current.plan().expressions().mode())
                  ? expressionArguments(current, step, rawInput, taskInput, null, null, command)
                  : null,
              current.plan());
      String operationId = current.key().canonical() + ":" + step.path() + ":run:" + sequence;
      DataReference descriptor =
          runDescriptor(current, step, operationId, arguments, taskInput, command);
      ActiveOperationState operation =
          new ActiveOperationState(
              operationId, step.path(), "run", rawInput, taskInput, parentAdvanced, descriptor);
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.OPERATION_DISPATCHED,
              step,
              taskInput,
              descriptor));
      WorkflowEffect dispatch =
          operationEffect(
              current, command, step, operation, WorkflowEffectType.DISPATCH_OPERATION, "");
      if (!run.await()) {
        DataReference detachedResult = reference(JsonNodeFactory.instance.nullNode());
        ExecutionSnapshot detached =
            withPendingInteraction(
                current,
                ExecutionPhase.RUNNING,
                parentAdvanced,
                current.context(),
                detachedResult,
                sequence,
                null);
        List<PlanStep> postRunSiblings = childrenForFrame(detached, parentAdvanced.current());
        return withOutbox(
            completeTask(
                context,
                detached,
                command,
                step,
                rawInput,
                taskInput,
                detachedResult,
                parentAdvanced,
                postRunSiblings,
                sequence,
                events),
            dispatch);
      }
      ExecutionSnapshot waiting =
          withPendingInteraction(
              current,
              ExecutionPhase.RUNNING,
              parentAdvanced,
              current.context(),
              taskInput,
              sequence,
              operation);
      return DurableTransition.changed(waiting, events, List.of(), List.of(dispatch));
    } else if (step.kind() == PlanStepKind.RAISE) {
      WorkflowError error = workflowError(current, command, step, rawInput, taskInput);
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.ERROR_RAISED,
              step,
              taskInput,
              errorReference(error)));
      return routeWorkflowError(context, current, command, step, error, sequence, events);
    } else {
      throw new IllegalStateException("Runtime does not implement plan step " + step.kind());
    }

    return finishTaskTransition(
        context, current, command, output, workflowContext, cursor, sequence, events);
  }

  /**
   * Dispatches a {@code run: workflow:} step. The child is not reachable through {@link
   * ProtocolOperationMaterializer} - it is not a protocol call, it is another OpenWorkflow
   * execution started by this same runtime. The pending interaction is still an {@link
   * ActiveOperationState} (operation kind {@value #OPERATION_KIND_RUN_WORKFLOW}) so that fork
   * routing, cancellation racing and pause buffering are the same proven machinery used for every
   * other RUN kind; only effect materialization differs, via {@link
   * WorkflowEffectType#START_SUBWORKFLOW} and {@link WorkflowEffectType#CONTROL_SUBWORKFLOW}
   * instead of {@code DISPATCH_OPERATION}/{@code CANCEL_OPERATION}.
   */
  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      dispatchSubworkflow(
          DurableProcessContext context,
          ExecutionSnapshot current,
          AdvanceExecutionCommand command,
          PlanStep step,
          DataReference rawInput,
          DataReference taskInput,
          ExecutionCursor parentAdvanced,
          long sequence,
          List<ExecutionHistoryEvent> events) {
    RunPlan run = step.runPlan();
    com.forwardmeasure.openworkflow.definition.ResolvedSubflow subflow = run.subflow();
    DataReference arguments =
        transform(
            run.configuration(),
            taskInput,
            expressions.requiresEvaluation(run.configuration(), current.plan().expressions().mode())
                ? expressionArguments(current, step, rawInput, taskInput, null, null, command)
                : null,
            current.plan());
    JsonNode configuredInput = inline(arguments).get("input");
    DataReference childInput = configuredInput == null ? taskInput : reference(configuredInput);

    com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionReference
        childDefinition =
            new com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionReference(
                new com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionKey(
                    current.key().tenantId(), subflow.coordinates()),
                subflow.sourceSha256(),
                subflow.definitionSha256());
    // The compiler already pinned and admitted this exact child at compile time; this is a
    // defensive re-validation, matching every other durable dispatch in this class.
    requireDefinition(childDefinition);

    String childExecutionIdValue =
        UUID.nameUUIDFromBytes(
                (current.key().canonical()
                        + "|subworkflow|"
                        + step.path()
                        + "|"
                        + sequence
                        + "|"
                        + subflow.canonical())
                    .getBytes(StandardCharsets.UTF_8))
            .toString();
    com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey childKey =
        new com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey(
            current.key().tenantId(),
            new com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowExecutionId(
                childExecutionIdValue));
    String operationId = childKey.canonical();

    ObjectNode subworkflowDescriptor = JsonNodeFactory.instance.objectNode();
    subworkflowDescriptor.put("operationId", operationId);
    subworkflowDescriptor.put("operationKind", OPERATION_KIND_RUN_WORKFLOW);
    subworkflowDescriptor.put("executionKey", current.key().canonical());
    subworkflowDescriptor.put("taskPath", step.path());
    subworkflowDescriptor.put("parentExecutionKey", current.key().canonical());
    subworkflowDescriptor.put("childExecutionKey", childKey.canonical());
    subworkflowDescriptor.put("childNamespace", subflow.coordinates().namespace());
    subworkflowDescriptor.put("childName", subflow.coordinates().name());
    subworkflowDescriptor.put("childVersion", subflow.coordinates().version());
    subworkflowDescriptor.put("childDsl", subflow.coordinates().dsl());
    subworkflowDescriptor.put("childSourceSha256", subflow.sourceSha256());
    subworkflowDescriptor.put("childDefinitionSha256", subflow.definitionSha256());
    subworkflowDescriptor.put("awaitParent", run.await());
    subworkflowDescriptor.set("childInput", DataReferenceJson.encode(childInput));
    DataReference descriptor = controlReference(subworkflowDescriptor);

    ActiveOperationState operation =
        new ActiveOperationState(
            operationId,
            step.path(),
            OPERATION_KIND_RUN_WORKFLOW,
            rawInput,
            taskInput,
            parentAdvanced,
            descriptor);
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.OPERATION_DISPATCHED,
            step,
            taskInput,
            descriptor));
    WorkflowEffect dispatch =
        operationEffect(
            current, command, step, operation, WorkflowEffectType.START_SUBWORKFLOW, "");
    if (!run.await()) {
      DataReference detachedResult = reference(JsonNodeFactory.instance.nullNode());
      ExecutionSnapshot detached =
          withPendingInteraction(
              current,
              ExecutionPhase.RUNNING,
              parentAdvanced,
              current.context(),
              detachedResult,
              sequence,
              null);
      List<PlanStep> postRunSiblings = childrenForFrame(detached, parentAdvanced.current());
      return withOutbox(
          completeTask(
              context,
              detached,
              command,
              step,
              rawInput,
              taskInput,
              detachedResult,
              parentAdvanced,
              postRunSiblings,
              sequence,
              events),
          dispatch);
    }
    ExecutionSnapshot waiting =
        withPendingInteraction(
            current,
            ExecutionPhase.RUNNING,
            parentAdvanced,
            current.context(),
            taskInput,
            sequence,
            operation);
    return DurableTransition.changed(waiting, events, List.of(), List.of(dispatch));
  }

  private WorkflowError workflowError(
      ExecutionSnapshot current,
      ExecutionCommand command,
      PlanStep step,
      DataReference rawInput,
      DataReference taskInput) {
    ErrorPlan plan = step.raisePlan().error();
    RuntimeExpressionArguments arguments =
        expressionArguments(current, step, rawInput, taskInput, null, null, command);
    String type =
        dynamicErrorText(
            plan.type(), taskInput, arguments, current.plan(), step.path() + "/raise/error/type");
    String instance =
        plan.instance() == null
            ? step.path()
            : dynamicErrorText(
                plan.instance(),
                taskInput,
                arguments,
                current.plan(),
                step.path() + "/raise/error/instance");
    try {
      return new WorkflowError(
          type,
          plan.status(),
          instance,
          plan.title() == null
              ? null
              : dynamicErrorText(
                  plan.title(),
                  taskInput,
                  arguments,
                  current.plan(),
                  step.path() + "/raise/error/title"),
          plan.detail() == null
              ? null
              : dynamicErrorText(
                  plan.detail(),
                  taskInput,
                  arguments,
                  current.plan(),
                  step.path() + "/raise/error/detail"));
    } catch (IllegalArgumentException failure) {
      throw new RuntimeExpressionException(
          step.path()
              + "/raise/error produced invalid structured "
              + "error identifiers: "
              + failure.getMessage(),
          failure);
    }
  }

  private String dynamicErrorText(
      JsonNode value,
      DataReference input,
      RuntimeExpressionArguments arguments,
      WorkflowPlan workflow,
      String path) {
    JsonNode evaluated = value;
    if (value.isTextual() && value.textValue().trim().startsWith("${")) {
      evaluated =
          expressions.evaluateExpression(
              value.textValue(), inline(input), arguments, workflow.expressions().mode());
    }
    if (!evaluated.isTextual() || evaluated.textValue().isBlank()) {
      throw new RuntimeExpressionException(path + " must evaluate to non-blank text");
    }
    return evaluated.textValue();
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      routeWorkflowError(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ExecutionCommand command,
          PlanStep origin,
          WorkflowError error,
          long sequence,
          List<ExecutionHistoryEvent> precedingEvents) {
    return routeWorkflowError(
        context, current, command, origin, error, sequence, precedingEvents, null);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      routeWorkflowError(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ExecutionCommand command,
          PlanStep origin,
          WorkflowError error,
          long sequence,
          List<ExecutionHistoryEvent> precedingEvents,
          ExecutionFailure terminalFailure) {
    List<ExecutionFrame> frames = current.cursor().frames();
    for (int frameIndex = frames.size() - 1; frameIndex >= 0; frameIndex--) {
      ExecutionFrame frame = frames.get(frameIndex);
      if (frame.tryState() == null || frame.tryState().phase() != TryRuntimeState.Phase.BODY) {
        continue;
      }
      PlanStep tryStep = current.plan().requireStep(frame.taskPath());
      if (!catchMatches(current, command, tryStep, error, current.cursor())) {
        continue;
      }
      ExecutionCursor caughtCursor =
          current.cursor().truncateAndReplace(frameIndex, frame.catching(error));
      List<ExecutionHistoryEvent> events = new ArrayList<>(precedingEvents);
      DataReference errorData = errorReference(error);
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.ERROR_CAUGHT,
              tryStep,
              errorData,
              errorData,
              caughtCursor));
      List<WorkflowEffect> cancellation =
          interactionEffects(current, command, false, ":error-caught");
      ExecutionSnapshot caught =
          new ExecutionSnapshot(
              current.key(),
              current.definition(),
              current.plan(),
              current.startedBy(),
              current.startedAt(),
              ExecutionPhase.RUNNING,
              caughtCursor,
              current.initialInput(),
              current.context(),
              current.data(),
              sequence,
              null,
              current.laneRootTaskPath(),
              null,
              current.forkPositions(),
              null,
              current.activeTimeouts(),
              current.cancellation());
      return withOutbox(changedWithContinuation(context, caught, events, command), cancellation);
    }

    ExecutionFailure failure =
        terminalFailure == null
            ? ExecutionFailure.fromWorkflowError(
                error, origin == null ? "/" : origin.path(), current.data())
            : terminalFailure;
    List<ExecutionHistoryEvent> events = new ArrayList<>(precedingEvents);
    events.add(
        failedEvent(
            current.key(),
            current.plan().definitionSha256(),
            sequence++,
            origin,
            current.data(),
            failure,
            iterationPositions(current.plan(), current.cursor()),
            current.forkPositions(),
            command));
    ExecutionSnapshot failed =
        new ExecutionSnapshot(
            current.key(),
            current.definition(),
            current.plan(),
            current.startedBy(),
            current.startedAt(),
            ExecutionPhase.FAILED,
            current.cursor(),
            current.initialInput(),
            current.context(),
            current.data(),
            sequence,
            failure,
            current.laneRootTaskPath(),
            null,
            current.forkPositions(),
            null);
    return DurableTransition.changed(
        failed, events, List.of(), interactionEffects(current, command, false, ":failure"));
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      routeTechnicalFailure(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ExecutionCommand command,
          ExecutionFailure failure,
          PlanStep origin) {
    boolean insideTry =
        current.cursor().frames().stream()
            .anyMatch(
                frame ->
                    frame.tryState() != null
                        && frame.tryState().phase() == TryRuntimeState.Phase.BODY);
    if (!insideTry) {
      return fail(current, command, failure, origin);
    }
    WorkflowError error =
        new WorkflowError(
            failure.type(),
            ExecutionFailure.VALIDATION_ERROR.equals(failure.type()) ? 400 : 500,
            failure.definitionPath(),
            ExecutionFailure.VALIDATION_ERROR.equals(failure.type())
                ? "Data validation failed"
                : "Runtime expression failed",
            failure.message());
    long sequence = current.nextSequence();
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.ERROR_RAISED,
            origin,
            current.data(),
            errorReference(error)));
    return routeWorkflowError(context, current, command, origin, error, sequence, events, failure);
  }

  private boolean catchMatches(
      ExecutionSnapshot current,
      ExecutionCommand command,
      PlanStep tryStep,
      WorkflowError error,
      ExecutionCursor cursor) {
    var caught = tryStep.tryPlan().catchPlan();
    ErrorFilterPlan filter = caught.errors();
    if (filter != null
        && (!errorTypeMatches(filter.type(), error.type())
            || !equalsIfPresent(filter.status(), error.status())
            || !equalsIfPresent(filter.instance(), error.instance())
            || !equalsIfPresent(filter.title(), error.title())
            || !equalsIfPresent(filter.detail(), error.detail()))) {
      return false;
    }
    DataReference errorData = errorReference(error);
    RuntimeExpressionArguments arguments =
        expressionArguments(current, tryStep, errorData, errorData, null, null, command, cursor);
    if (caught.when() != null
        && !expressions.evaluateCondition(
            caught.when(), inline(errorData), arguments, current.plan().expressions().mode())) {
      return false;
    }
    return caught.exceptWhen() == null
        || !expressions.evaluateCondition(
            caught.exceptWhen(), inline(errorData), arguments, current.plan().expressions().mode());
  }

  private static boolean errorTypeMatches(String expected, String actual) {
    if (expected == null) {
      return true;
    }
    return canonicalErrorType(expected).equals(canonicalErrorType(actual));
  }

  private static String canonicalErrorType(String type) {
    if ("https://open-workflow-specification.org/dsl/errors/types/"
        .concat("communication")
        .equals(type)) {
      /*
       * The upstream 1.0.3 CTK retained this pre-standardisation URI
       * in its try scenarios. The normative 1.0.3 text requires the
       * /spec/1.0.0 URI. Accept the historical identifier in filters
       * while always emitting the normative identifier.
       */
      return "https://open-workflow-specification.org/spec/1.0.0/" + "errors/communication";
    }
    return type;
  }

  private static boolean equalsIfPresent(Object expected, Object actual) {
    return expected == null || Objects.equals(expected, actual);
  }

  private DataReference errorReference(WorkflowError error) {
    return reference(errorJson(error));
  }

  private static ObjectNode errorJson(WorkflowError error) {
    ObjectNode result = JsonNodeFactory.instance.objectNode();
    result.put("type", error.type());
    result.put("status", error.status());
    if (error.instance() != null) {
      result.put("instance", error.instance());
    }
    if (error.title() != null) {
      result.put("title", error.title());
    }
    if (error.detail() != null) {
      result.put("detail", error.detail());
    }
    return result;
  }

  private DataReference subscriptionDescriptor(
      ExecutionSnapshot current, PlanStep step, ActiveListenState listen) {
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("subscriptionId", listen.subscriptionId());
    descriptor.put("executionKey", current.key().canonical());
    descriptor.put("taskPath", step.path());
    descriptor.set("listen", step.configuration().deepCopy());
    return controlReference(descriptor);
  }

  private Duration waitDuration(
      ExecutionSnapshot current,
      ExecutionCommand command,
      PlanStep step,
      DataReference rawInput,
      DataReference taskInput) {
    return resolveDuration(
        step.waitPlan().duration(),
        current,
        command,
        step,
        rawInput,
        taskInput,
        step.path() + "/wait");
  }

  private Duration resolveDuration(
      DurationPlan plan,
      ExecutionSnapshot current,
      ExecutionCommand command,
      PlanStep step,
      DataReference rawInput,
      DataReference taskInput,
      String path) {
    if (plan.kind() == DurationPlan.Kind.INLINE) {
      JsonNode value = plan.value();
      Duration duration =
          Duration.ofDays(value.path("days").asLong())
              .plusHours(value.path("hours").asLong())
              .plusMinutes(value.path("minutes").asLong())
              .plusSeconds(value.path("seconds").asLong())
              .plusMillis(value.path("milliseconds").asLong());
      if (duration.isNegative()) {
        throw new RuntimeExpressionException(path + " duration must not be negative");
      }
      return duration;
    }
    String value = plan.value().textValue();
    if (plan.kind() == DurationPlan.Kind.EXPRESSION) {
      JsonNode evaluated =
          expressions.evaluateExpression(
              value,
              inline(taskInput),
              expressionArguments(current, step, rawInput, taskInput, null, null, command),
              current.plan().expressions().mode());
      if (!evaluated.isTextual()) {
        throw new RuntimeExpressionException(
            path + " duration expression must produce an " + "ISO 8601 string");
      }
      value = evaluated.textValue();
    }
    try {
      Duration duration = Iso8601Duration.between(command.requestedAt(), value);
      if (duration.isNegative()) {
        throw new RuntimeExpressionException(path + " duration must not be negative");
      }
      return duration;
    } catch (IllegalArgumentException | ArithmeticException failure) {
      throw new RuntimeExpressionException(
          path + " is not a valid ISO 8601 duration: " + value, failure);
    }
  }

  private DataReference timerDescriptor(
      ExecutionSnapshot current, PlanStep step, ActiveTimerState timer) {
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("timerId", timer.timerId());
    descriptor.put("executionKey", current.key().canonical());
    descriptor.put("taskPath", step.path());
    descriptor.put("dueAt", timer.dueAt().toString());
    return controlReference(descriptor);
  }

  private DataReference retryTimerDescriptor(
      ExecutionSnapshot current, PlanStep step, ActiveRetryState retry) {
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("timerId", retry.timerId());
    descriptor.put("executionKey", current.key().canonical());
    descriptor.put("taskPath", step.path());
    descriptor.put("dueAt", retry.dueAt().toString());
    descriptor.put("purpose", "retry");
    descriptor.set("error", errorJson(retry.error()));
    return controlReference(descriptor);
  }

  private DataReference attemptDeadlineDescriptor(
      ExecutionSnapshot current, PlanStep step, TryRuntimeState state) {
    if (state.attemptDeadlineId() == null) {
      throw new IllegalArgumentException("Try state has no active attempt deadline");
    }
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("timerId", state.attemptDeadlineId());
    descriptor.put("executionKey", current.key().canonical());
    descriptor.put("taskPath", step.path());
    descriptor.put("dueAt", state.attemptDeadlineAt().toString());
    descriptor.put("purpose", "retry-attempt-deadline");
    descriptor.put("attempt", state.attempt());
    return controlReference(descriptor);
  }

  private DataReference callDescriptor(
      ExecutionSnapshot current,
      PlanStep step,
      String operationId,
      DataReference arguments,
      DataReference taskInput,
      AdvanceExecutionCommand command,
      Instant subscriptionDeadline) {
    CallPlan call = step.callPlan();
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("operationId", operationId);
    descriptor.put("operationKind", "call");
    descriptor.put("executionKey", current.key().canonical());
    descriptor.put("definitionReference", current.definition().canonical());
    descriptor.put("definitionSha256", current.plan().definitionSha256());
    descriptor.put("taskPath", step.path());
    descriptor.put("callKind", call.kind().name());
    if (call.functionName() != null) {
      descriptor.put("functionName", call.functionName());
    }
    if (call.resource() != null) {
      descriptor.put("resourceKind", call.resource().kind().name());
      descriptor.put("resourceUri", call.resource().uri().toString());
      descriptor.put("resourceSha256", call.resource().sha256());
    }
    if (call.authentication() != null) {
      AuthenticationPlan authentication = call.authentication();
      ObjectNode reference = JsonNodeFactory.instance.objectNode();
      reference.put("kind", authentication.kind().name());
      if (authentication.secretBacked()) {
        reference.put("secretName", authentication.secretName());
      } else {
        reference.set("expressions", authentication.expressionConfiguration().deepCopy());
        if (!authentication.secretReferences().isEmpty()) {
          var authenticationReferences = reference.putArray("secretReferences");
          authentication.secretReferences().forEach(authenticationReferences::add);
        }
      }
      if (authentication.reusableName() != null) {
        reference.put("reusableName", authentication.reusableName());
      }
      descriptor.set("authentication", reference);
      if (!authentication.secretReferences().isEmpty()) {
        var references = descriptor.withArray("secretReferences");
        authentication.secretReferences().forEach(references::add);
      }
    }
    String supplementalSecret = mcpStdioEnvironmentSecret(call);
    if (supplementalSecret != null) {
      var references = descriptor.withArray("secretReferences");
      boolean present = false;
      for (JsonNode reference : references) {
        if (supplementalSecret.equals(reference.textValue())) {
          present = true;
          break;
        }
      }
      if (!present) {
        references.add(supplementalSecret);
      }
    }
    putDataReference(descriptor, "arguments", arguments);
    putDataReference(descriptor, "taskInput", taskInput);
    putDataReference(descriptor, "workflowContext", current.context());
    descriptor.put("expressionMode", current.plan().expressions().mode().name());
    if (call.kind() == CallPlan.Kind.ASYNC_API
        || call.kind() == CallPlan.Kind.GRPC
        || call.kind() == CallPlan.Kind.A2A
        || call.kind() == CallPlan.Kind.MCP) {
      RuntimeExpressionArguments scope =
          expressionArguments(current, step, taskInput, taskInput, null, null, command);
      descriptor.set(
          "protocolOperation",
          DURABLE_STATE_JSON.valueToTree(
              ProtocolOperationMaterializer.materialize(
                  current.plan(),
                  step,
                  inline(arguments),
                  operationId,
                  authenticationContext(scope),
                  subscriptionDeadline)));
    }
    return controlReference(descriptor);
  }

  private static String mcpStdioEnvironmentSecret(CallPlan call) {
    if (call.kind() != CallPlan.Kind.MCP) return null;
    JsonNode configured =
        call.arguments().path("transport").path("options").get("environmentSecret");
    return configured == null || !configured.isTextual() ? null : configured.textValue();
  }

  private static ObjectNode requireHumanTaskConfiguration(JsonNode value, PlanStep step) {
    if (!value.isObject()) {
      throw new RuntimeExpressionException(
          step.path() + "/with must evaluate to a human-task object");
    }
    ObjectNode configuration = (ObjectNode) value;
    requiredText(configuration, "title", step.path());
    JsonNode approvals = configuration.get("approvals");
    if (approvals == null
        || !approvals.isObject()
        || !approvals.path("stages").isArray()
        || approvals.path("stages").isEmpty()) {
      throw new RuntimeExpressionException(
          step.path() + "/with/approvals requires a non-empty stages " + "array");
    }
    JsonNode presentation = configuration.get("presentation");
    if (presentation != null && !presentation.isObject()) {
      throw new RuntimeExpressionException(
          step.path() + "/with/presentation must evaluate to an object");
    }
    return configuration;
  }

  private static String requiredText(ObjectNode object, String field, String taskPath) {
    JsonNode value = object.get(field);
    if (value == null || !value.isTextual() || value.textValue().isBlank()) {
      throw new RuntimeExpressionException(
          taskPath + "/with/" + field + " must evaluate to non-blank text");
    }
    return value.textValue();
  }

  private static String humanTaskId(ExecutionSnapshot current, PlanStep step, long sequence) {
    String identity =
        current.key().canonical()
            + "|"
            + current.plan().definitionSha256()
            + "|"
            + step.path()
            + "|"
            + sequence;
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(identity.getBytes(StandardCharsets.UTF_8));
      return "ht-" + HexFormat.of().formatHex(digest);
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("JVM does not provide SHA-256", impossible);
    }
  }

  private static Instant humanTaskDueAt(
      ObjectNode configuration, Instant requestedAt, String taskPath) {
    if (configuration.hasNonNull("dueAt")) {
      String value = requiredText(configuration, "dueAt", taskPath);
      try {
        return Instant.parse(value);
      } catch (java.time.format.DateTimeParseException invalid) {
        throw new RuntimeExpressionException(taskPath + "/with/dueAt must be an ISO-8601 instant");
      }
    }
    if (configuration.hasNonNull("dueAfter")) {
      String value = requiredText(configuration, "dueAfter", taskPath);
      try {
        return Iso8601Duration.addTo(requestedAt, value);
      } catch (IllegalArgumentException invalid) {
        throw new RuntimeExpressionException(
            taskPath + "/with/dueAfter must be an ISO-8601 " + "duration");
      }
    }
    return null;
  }

  private DataReference humanTaskDescriptor(
      ExecutionSnapshot current,
      PlanStep step,
      String humanTaskId,
      BusinessCorrelationId correlationId,
      ObjectNode configuration,
      DataReference taskInput,
      Instant dueAt) {
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("humanTaskId", humanTaskId);
    descriptor.put("correlationId", correlationId.value());
    descriptor.put("executionKey", current.key().canonical());
    descriptor.put("workflowTaskPath", step.path());
    descriptor.put("title", requiredText(configuration, "title", step.path()));
    if (configuration.hasNonNull("description")) {
      descriptor.put("description", requiredText(configuration, "description", step.path()));
    }
    if (configuration.has("input")) {
      putDataReference(descriptor, "input", reference(configuration.required("input")));
    } else {
      putDataReference(descriptor, "input", taskInput);
    }
    descriptor.set(
        "presentation",
        configuration.has("presentation")
            ? configuration.required("presentation").deepCopy()
            : JsonNodeFactory.instance.objectNode().put("kind", "RAW_JSON"));
    descriptor.set("approvals", configuration.required("approvals").deepCopy());
    if (dueAt != null) {
      descriptor.put("dueAt", dueAt.toString());
    }
    return controlReference(descriptor);
  }

  private DataReference runDescriptor(
      ExecutionSnapshot current,
      PlanStep step,
      String operationId,
      DataReference arguments,
      DataReference taskInput,
      AdvanceExecutionCommand command) {
    RunPlan run = step.runPlan();
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("operationId", operationId);
    descriptor.put("operationKind", "run");
    descriptor.put("executionKey", current.key().canonical());
    descriptor.put("definitionReference", current.definition().canonical());
    descriptor.put("definitionSha256", current.plan().definitionSha256());
    descriptor.put("taskPath", step.path());
    descriptor.put("runKind", run.kind().name());
    descriptor.put("await", run.await());
    descriptor.put("return", run.returnMode().name());
    if (run.resource() != null) {
      descriptor.put("resourceKind", run.resource().kind().name());
      descriptor.put("resourceUri", run.resource().uri().toString());
      descriptor.put("resourceSha256", run.resource().sha256());
    }
    if (run.subflow() != null) {
      descriptor.put("subflowNamespace", run.subflow().coordinates().namespace());
      descriptor.put("subflowName", run.subflow().coordinates().name());
      descriptor.put("subflowVersion", run.subflow().coordinates().version());
      descriptor.put("subflowDsl", run.subflow().coordinates().dsl());
      descriptor.put("subflowSourceSha256", run.subflow().sourceSha256());
      descriptor.put("subflowDefinitionSha256", run.subflow().definitionSha256());
    }
    putDataReference(descriptor, "configuration", arguments);
    putDataReference(descriptor, "taskInput", taskInput);
    putDataReference(descriptor, "workflowContext", current.context());
    descriptor.put("expressionMode", current.plan().expressions().mode().name());
    descriptor.set(
        "protocolOperation",
        DURABLE_STATE_JSON.valueToTree(
            ProtocolOperationMaterializer.materialize(
                current.plan(),
                step,
                inline(arguments),
                operationId,
                authenticationContext(
                    expressionArguments(
                        current, step, taskInput, taskInput, null, null, command)))));
    return controlReference(descriptor);
  }

  private DataReference asyncApiSubscriptionDescriptor(
      DataReference callDescriptor, String subscriptionId, String timerId, Instant deadlineAt) {
    ObjectNode descriptor = (ObjectNode) inline(callDescriptor).deepCopy();
    descriptor.put("subscriptionId", subscriptionId);
    if (timerId != null) {
      descriptor.put("deadlineTimerId", timerId);
      descriptor.put("deadlineAt", deadlineAt.toString());
    }
    return controlReference(descriptor);
  }

  private DataReference correlatedWorkerOperationDescriptor(
      DataReference base, String operationId, JsonNode operationArguments) {
    ObjectNode descriptor = (ObjectNode) inline(base).deepCopy();
    descriptor.put("operationId", operationId);
    /*
     * A cancellation is a distinct durable workflow effect, but it must
     * be ordered with the command that created the external operation.
     * Kafka adapters use this explicit correlation identity as their
     * partition key; operationId remains the immutable effect invocation
     * identity used by the OKS outbox and adapter dispatcher.
     */
    descriptor.put(
        "correlationId",
        operationId.endsWith(":cancel")
            ? operationId.substring(0, operationId.length() - ":cancel".length())
            : operationId);
    descriptor.put("callKind", CallPlan.Kind.ASYNC_API.name());
    ObjectNode arguments = (ObjectNode) operationArguments.deepCopy();
    if (arguments.has("message")) {
      ObjectNode message = (ObjectNode) arguments.required("message");
      ObjectNode payload = message.withObject("payload");
      payload.put(
          "operationId",
          operationId.endsWith(":cancel")
              ? operationId.substring(0, operationId.length() - ":cancel".length())
              : operationId);
    }
    putDataReference(descriptor, "arguments", reference(arguments));
    return controlReference(descriptor);
  }

  /** Runtime-owned AsyncAPI subscription semantics are not task templates. */
  private static JsonNode callArgumentTemplate(CallPlan call) {
    if (call.kind() == CallPlan.Kind.CORRELATED_WORKER) {
      ObjectNode arguments = ((ObjectNode) call.arguments()).deepCopy();
      ((ObjectNode) arguments.required("events"))
          .set("subscription", JsonNodeFactory.instance.objectNode());
      return arguments;
    }
    if (call.kind() != CallPlan.Kind.ASYNC_API || call.asyncApiSubscription() == null) {
      return call.arguments();
    }
    ObjectNode arguments = ((ObjectNode) call.arguments()).deepCopy();
    arguments.set("subscription", JsonNodeFactory.instance.objectNode());
    return arguments;
  }

  private static ObjectNode requireCorrelatedWorkerConfiguration(JsonNode value, PlanStep step) {
    if (!value.isObject()) {
      throw new RuntimeExpressionException(
          step.path() + "/with must evaluate to a correlated-worker " + "object");
    }
    ObjectNode configuration = (ObjectNode) value;
    if (!configuration.path("command").isObject() || !configuration.path("events").isObject()) {
      throw new RuntimeExpressionException(step.path() + "/with requires command and events");
    }
    return configuration;
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      withOutbox(
          DurableTransition<
                  ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
              transition,
          WorkflowEffect effect) {
    return withOutbox(transition, List.of(effect));
  }

  private static DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      withPriorEvents(
          DurableTransition<
                  ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
              transition,
          List<ExecutionHistoryEvent> prior) {
    if (!transition.stateChanged()) {
      throw new IllegalStateException("Prior events require a state-changing transition");
    }
    List<ExecutionHistoryEvent> events = new ArrayList<>(prior);
    events.addAll(transition.events());
    return DurableTransition.changed(
        transition.state(), events, transition.followUpCommands(), transition.outbox());
  }

  private static DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      withOutbox(
          DurableTransition<
                  ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
              transition,
          List<WorkflowEffect> effects) {
    if (!transition.stateChanged()) {
      throw new IllegalStateException("A workflow effect requires a state-changing transition");
    }
    List<WorkflowEffect> outbox = new ArrayList<>(transition.outbox());
    outbox.addAll(effects);
    return DurableTransition.changed(
        transition.state(), transition.events(), transition.followUpCommands(), outbox);
  }

  private DataReference emittedCloudEvent(
      ExecutionSnapshot current,
      PlanStep step,
      DataReference rawInput,
      DataReference taskInput,
      ExecutionCommand command,
      long sequence) {
    JsonNode evaluated =
        expressions.evaluateTemplate(
            step.configuration(),
            inline(taskInput),
            expressionArguments(current, step, rawInput, taskInput, null, null, command),
            current.plan().expressions().mode());
    if (!evaluated.isObject()) {
      throw new RuntimeExpressionException(
          step.path() + "/emit/event/with must evaluate to an object");
    }
    ObjectNode properties = (ObjectNode) evaluated;
    String type = requiredEventText(properties, "type", step.path());
    String id = optionalEventText(properties, "id", step.path());
    if (id == null) {
      id = current.key().executionId().value() + ":" + step.path() + ":" + sequence;
    }
    String source = optionalEventText(properties, "source", step.path());
    if (source == null) {
      source =
          "urn:openworkflow:"
              + current.plan().coordinates().namespace()
              + ":"
              + current.plan().coordinates().name()
              + ":"
              + current.plan().coordinates().version();
    }
    requireUri(source, step.path() + "/emit/event/with/source");
    String time = optionalEventText(properties, "time", step.path());
    if (time == null) time = command.requestedAt().toString();
    try {
      java.time.OffsetDateTime.parse(time);
    } catch (IllegalArgumentException | ArithmeticException failure) {
      throw new RuntimeExpressionException(
          step.path() + "/emit/event/with/time must be an RFC 3339 timestamp", failure);
    }

    ObjectNode event = JsonNodeFactory.instance.objectNode();
    event.put("specversion", "1.0");
    event.put("id", id);
    event.put("source", source);
    event.put("type", type);
    event.put("time", time);
    for (String attribute : List.of("subject", "datacontenttype", "dataschema")) {
      String value = optionalEventText(properties, attribute, step.path());
      if (value != null) {
        if ("dataschema".equals(attribute)) {
          requireUri(value, step.path() + "/emit/event/with/dataschema");
        }
        event.put(attribute, value);
      }
    }
    if (properties.has("data")) {
      event.set("data", properties.get("data").deepCopy());
    }
    Set<String> standard =
        Set.of(
            "specversion",
            "id",
            "source",
            "type",
            "time",
            "subject",
            "datacontenttype",
            "dataschema",
            "data");
    properties
        .properties()
        .iterator()
        .forEachRemaining(
            extension -> {
              if (standard.contains(extension.getKey())) return;
              if (!extension.getKey().matches("[a-z0-9]{1,20}")) {
                throw new RuntimeExpressionException(
                    step.path()
                        + "/emit/event/with extension attribute '"
                        + extension.getKey()
                        + "' is not a CloudEvents attribute name");
              }
              JsonNode value = extension.getValue();
              if (!value.isTextual() && !value.isBoolean() && !value.isNumber()) {
                throw new RuntimeExpressionException(
                    step.path()
                        + "/emit/event/with extension attribute '"
                        + extension.getKey()
                        + "' must be a CloudEvents scalar");
              }
              event.set(extension.getKey(), value.deepCopy());
            });
    return reference(event);
  }

  private static String requiredEventText(ObjectNode properties, String name, String taskPath) {
    String value = optionalEventText(properties, name, taskPath);
    if (value == null) {
      throw new RuntimeExpressionException(taskPath + "/emit/event/with/" + name + " is required");
    }
    return value;
  }

  private static String optionalEventText(ObjectNode properties, String name, String taskPath) {
    JsonNode value = properties.get(name);
    if (value == null || value.isNull()) return null;
    if (!value.isTextual() || value.textValue().isBlank()) {
      throw new RuntimeExpressionException(
          taskPath + "/emit/event/with/" + name + " must evaluate to non-blank text");
    }
    return value.textValue();
  }

  private static void requireUri(String value, String path) {
    try {
      if (!java.net.URI.create(value).isAbsolute()) {
        throw new IllegalArgumentException("URI is relative");
      }
    } catch (IllegalArgumentException failure) {
      throw new RuntimeExpressionException(path + " must be an absolute URI", failure);
    }
  }

  private DataReference forCollection(
      ExecutionSnapshot current,
      PlanStep step,
      DataReference rawInput,
      DataReference taskInput,
      ExecutionCommand command) {
    ForPlan plan = step.forPlan();
    JsonNode collection =
        plan.expressionCollection()
            ? expressions.evaluateExpression(
                plan.collection().textValue(),
                inline(taskInput),
                expressionArguments(current, step, rawInput, taskInput, null, null, command),
                current.plan().expressions().mode())
            : plan.collection().deepCopy();
    if (!collection.isArray()) {
      throw new RuntimeExpressionException(step.path() + "/for/in must evaluate to an array");
    }
    return reference(collection);
  }

  private static ForkRuntimeState startFork(
      PlanStep step,
      DataReference rawInput,
      DataReference taskInput,
      ExecutionCursor parentCursor) {
    List<ForkBranchState> branches = new ArrayList<>();
    for (int index = 0; index < step.children().size(); index++) {
      PlanStep branch = step.children().get(index);
      branches.add(ForkBranchState.pending(branch.name(), branch.path(), index, taskInput));
    }
    return new ForkRuntimeState(
        step.path(), rawInput, taskInput, parentCursor, step.forkPlan().compete(), branches, 0, 0);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      advanceFork(
          DurableProcessContext durableContext,
          ExecutionSnapshot current,
          AdvanceExecutionCommand command) {
    ForkRuntimeState fork = current.activeFork();
    PlanStep forkStep = current.plan().requireStep(fork.taskPath());
    int branchIndex = fork.nextRunnableIndex();
    if (branchIndex < 0) {
      throw new IllegalStateException("Active fork has no runnable branch");
    }

    ForkBranchState branch = fork.branches().get(branchIndex);
    long sequence = current.nextSequence();
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    List<ForkPosition> branchPositions = new ArrayList<>(current.forkPositions());
    ForkPosition position =
        new ForkPosition(
            forkStep.path(),
            forkStep.name(),
            branch.path(),
            branch.name(),
            branch.declarationIndex());
    branchPositions.add(position);

    if (branch.phase() == ForkBranchPhase.PENDING) {
      branch = branch.running();
      events.add(
          forkEvent(
              current,
              command,
              sequence++,
              ExecutionEventType.FORK_BRANCH_STARTED,
              forkStep,
              fork.input(),
              null,
              branchPositions));
    }

    ExecutionSnapshot lane =
        new ExecutionSnapshot(
            current.key(),
            current.definition(),
            current.plan(),
            current.startedBy(),
            current.startedAt(),
            ExecutionPhase.RUNNING,
            branch.cursor(),
            current.initialInput(),
            current.context(),
            branch.data(),
            sequence,
            null,
            branch.path(),
            branch.activeFork(),
            branchPositions,
            branch.pendingInteraction(),
            current.activeTimeouts(),
            current.cancellation());
    DurableTransition<ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
        laneTransition = decide(durableContext, lane, command);
    ExecutionSnapshot progressed = laneTransition.state();
    events.addAll(laneTransition.events());
    List<WorkflowEffect> effects = new ArrayList<>(laneTransition.outbox());
    sequence = progressed.nextSequence();

    if (progressed.phase() == ExecutionPhase.FAILED) {
      effects.addAll(interactionEffects(current, command, false, ":failure"));
      return DurableTransition.changed(
          failedOuterSnapshot(current, progressed), events, List.of(), effects);
    }
    if (progressed.phase() != ExecutionPhase.RUNNING) {
      throw new IllegalStateException(
          "A fork branch unexpectedly changed execution phase to " + progressed.phase());
    }

    branch =
        branch.progressed(
            progressed.cursor(),
            progressed.data(),
            progressed.activeFork(),
            progressed.pendingInteraction());
    boolean branchComplete =
        progressed.cursor().complete()
            && progressed.activeFork() == null
            && progressed.pendingInteraction() == null;
    if (branchComplete) {
      branch = branch.completed(fork.nextCompletionOrder());
      events.add(
          forkEvent(
              progressed,
              command,
              sequence++,
              ExecutionEventType.FORK_BRANCH_COMPLETED,
              forkStep,
              fork.input(),
              branch.data(),
              branchPositions));
    }

    int nextIndex = (branchIndex + 1) % fork.branches().size();
    long nextCompletionOrder = fork.nextCompletionOrder() + (branchComplete ? 1 : 0);
    ForkRuntimeState progressedFork =
        fork.replace(branchIndex, branch, nextIndex, nextCompletionOrder);

    if (branchComplete && endedWorkflow(current.plan(), laneTransition.events())) {
      progressedFork =
          abandonOtherBranches(
              progressed,
              command,
              forkStep,
              progressedFork,
              branchIndex,
              events,
              effects,
              sequence);
      sequence = current.nextSequence() + events.size();
      return withOutbox(
          completeExecutionFromFork(current, progressed, command, events, sequence), effects);
    }

    if (branchComplete && progressedFork.compete()) {
      int beforeAbandon = events.size();
      progressedFork =
          abandonOtherBranches(
              progressed,
              command,
              forkStep,
              progressedFork,
              branchIndex,
              events,
              effects,
              sequence);
      sequence += events.size() - beforeAbandon;
      return withOutbox(
          completeFork(
              durableContext,
              current,
              progressed,
              command,
              forkStep,
              progressedFork,
              branch.data(),
              sequence,
              events),
          effects);
    }
    if (progressedFork.allCompleted()) {
      return withOutbox(
          completeFork(
              durableContext,
              current,
              progressed,
              command,
              forkStep,
              progressedFork,
              joinedForkOutput(progressedFork),
              sequence,
              events),
          effects);
    }

    ExecutionSnapshot snapshot =
        new ExecutionSnapshot(
            current.key(),
            current.definition(),
            current.plan(),
            current.startedBy(),
            current.startedAt(),
            ExecutionPhase.RUNNING,
            current.cursor(),
            current.initialInput(),
            progressed.context(),
            current.data(),
            sequence,
            null,
            current.laneRootTaskPath(),
            progressedFork,
            current.forkPositions(),
            null,
            progressed.activeTimeouts());
    if (progressedFork.nextRunnableIndex() < 0) {
      return DurableTransition.changed(snapshot, events, List.of(), effects);
    }
    return withOutbox(changedWithContinuation(durableContext, snapshot, events, command), effects);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      completeFork(
          DurableProcessContext durableContext,
          ExecutionSnapshot outer,
          ExecutionSnapshot progressedLane,
          ExecutionCommand command,
          PlanStep forkStep,
          ForkRuntimeState fork,
          DataReference rawOutput,
          long sequence,
          List<ExecutionHistoryEvent> events) {
    ExecutionSnapshot resumed =
        new ExecutionSnapshot(
            outer.key(),
            outer.definition(),
            outer.plan(),
            outer.startedBy(),
            outer.startedAt(),
            ExecutionPhase.RUNNING,
            fork.parentCursor(),
            outer.initialInput(),
            progressedLane.context(),
            outer.data(),
            sequence,
            null,
            outer.laneRootTaskPath(),
            null,
            outer.forkPositions(),
            null,
            progressedLane.activeTimeouts());
    List<PlanStep> siblings = childrenForFrame(resumed, fork.parentCursor().current());
    return completeTask(
        durableContext,
        resumed,
        command,
        forkStep,
        fork.rawInput(),
        fork.input(),
        rawOutput,
        fork.parentCursor(),
        siblings,
        sequence,
        events);
  }

  private ForkRuntimeState abandonOtherBranches(
      ExecutionSnapshot current,
      ExecutionCommand command,
      PlanStep forkStep,
      ForkRuntimeState fork,
      int winnerIndex,
      List<ExecutionHistoryEvent> events,
      List<WorkflowEffect> effects,
      long firstSequence) {
    List<ForkBranchState> branches = new ArrayList<>(fork.branches());
    long sequence = firstSequence;
    for (int index = 0; index < branches.size(); index++) {
      if (index == winnerIndex || branches.get(index).phase().terminal()) {
        continue;
      }
      ForkBranchState loser = branches.get(index);
      List<ForkPosition> positions = new ArrayList<>(current.forkPositions());
      positions.add(
          new ForkPosition(
              forkStep.path(),
              forkStep.name(),
              loser.path(),
              loser.name(),
              loser.declarationIndex()));
      events.add(
          forkEvent(
              current,
              command,
              sequence++,
              ExecutionEventType.FORK_BRANCH_ABANDONED,
              forkStep,
              fork.input(),
              loser.data(),
              positions));
      collectBranchInteractionEffects(current, command, loser, false, ":abandon", effects);
      branches.set(index, loser.abandoned());
    }
    return fork.replaceBranches(branches);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      completeExecutionFromFork(
          ExecutionSnapshot outer,
          ExecutionSnapshot progressedLane,
          ExecutionCommand command,
          List<ExecutionHistoryEvent> events,
          long sequence) {
    DataReference workflowOutput = progressedLane.data();
    if (outer.plan().dataFlow().outputAs() != null) {
      workflowOutput =
          transform(
              outer.plan().dataFlow().outputAs(),
              progressedLane.data(),
              new RuntimeExpressionArguments(
                  inline(progressedLane.context()),
                  null,
                  null,
                  null,
                  null,
                  null,
                  workflowDescriptor(
                      outer.plan(),
                      outer.key().executionId().value(),
                      inline(outer.initialInput()),
                      outer.startedAt()),
                  runtimeDescriptor()),
              outer.plan());
    }
    if (outer.plan().dataFlow().outputSchema() != null) {
      schemas(outer.plan())
          .validate(outer.plan().dataFlow().outputSchema(), inline(workflowOutput));
    }
    ExecutionSnapshot terminal =
        new ExecutionSnapshot(
            outer.key(),
            outer.definition(),
            outer.plan(),
            outer.startedBy(),
            outer.startedAt(),
            ExecutionPhase.COMPLETED,
            new ExecutionCursor(List.of()),
            outer.initialInput(),
            progressedLane.context(),
            workflowOutput,
            sequence + 1,
            null,
            null,
            null,
            List.of());
    events.add(
        event(
            progressedLane,
            command,
            sequence,
            ExecutionEventType.EXECUTION_COMPLETED,
            null,
            outer.initialInput(),
            workflowOutput));
    return DurableTransition.changed(terminal, events, List.of());
  }

  private static ExecutionSnapshot failedOuterSnapshot(
      ExecutionSnapshot outer, ExecutionSnapshot failedLane) {
    return new ExecutionSnapshot(
        outer.key(),
        outer.definition(),
        outer.plan(),
        outer.startedBy(),
        outer.startedAt(),
        ExecutionPhase.FAILED,
        outer.cursor(),
        outer.initialInput(),
        failedLane.context(),
        failedLane.data(),
        failedLane.nextSequence(),
        failedLane.failure(),
        outer.laneRootTaskPath(),
        null,
        outer.forkPositions(),
        null);
  }

  private static boolean endedWorkflow(WorkflowPlan plan, List<ExecutionHistoryEvent> events) {
    for (ExecutionHistoryEvent event : events) {
      if (event.type() != ExecutionEventType.TASK_COMPLETED
          && event.type() != ExecutionEventType.TASK_SKIPPED) {
        continue;
      }
      String directive =
          event.switchDecision() == null
              ? plan.requireStep(event.taskPath()).dataFlow().thenDirective()
              : event.switchDecision().flowDirective();
      if ("end".equals(directive)) {
        return true;
      }
    }
    return false;
  }

  private DataReference joinedForkOutput(ForkRuntimeState fork) {
    ArrayNode result = JsonNodeFactory.instance.arrayNode();
    fork.branches().stream()
        .sorted(java.util.Comparator.comparingInt(ForkBranchState::declarationIndex))
        .forEach(
            branch -> {
              ObjectNode named = JsonNodeFactory.instance.objectNode();
              named.set(branch.name(), inline(branch.data()));
              result.add(named);
            });
    return reference(result);
  }

  private boolean continuesFor(
      ExecutionSnapshot current,
      PlanStep step,
      DataReference flowingInput,
      ExecutionCursor cursor,
      ExecutionCommand command) {
    ForIterationState iteration = cursor.current().iteration();
    JsonNode collection = inline(iteration.collection());
    if (iteration.index() >= collection.size()) {
      return false;
    }
    if (step.forPlan().whileCondition() == null) {
      return true;
    }
    return expressions.evaluateCondition(
        step.forPlan().whileCondition(),
        inline(flowingInput),
        expressionArguments(
            current,
            step,
            cursor.current().rawInput(),
            cursor.current().input(),
            null,
            null,
            command,
            cursor),
        current.plan().expressions().mode());
  }

  private SwitchDecision evaluateSwitch(
      ExecutionSnapshot current,
      PlanStep step,
      DataReference rawInput,
      DataReference taskInput,
      ExecutionCommand command) {
    List<SwitchCaseEvaluation> evaluations = new ArrayList<>();
    SwitchCasePlan defaultCase = null;
    int defaultIndex = -1;
    String selectedCase = null;
    String selectedDirective = null;
    RuntimeExpressionArguments arguments =
        expressionArguments(current, step, rawInput, taskInput, null, null, command);

    for (SwitchCasePlan candidate : step.switchCases()) {
      if (candidate.defaultCase()) {
        defaultCase = candidate;
        defaultIndex = evaluations.size();
        evaluations.add(new SwitchCaseEvaluation(candidate.name(), null, false));
      } else if (selectedCase != null) {
        evaluations.add(new SwitchCaseEvaluation(candidate.name(), candidate.condition(), null));
      } else {
        boolean matched =
            expressions.evaluateCondition(
                candidate.condition(),
                inline(taskInput),
                arguments,
                current.plan().expressions().mode());
        evaluations.add(new SwitchCaseEvaluation(candidate.name(), candidate.condition(), matched));
        if (matched) {
          selectedCase = candidate.name();
          selectedDirective = candidate.thenDirective();
        }
      }
    }

    if (selectedCase == null && defaultCase != null) {
      selectedCase = defaultCase.name();
      selectedDirective = defaultCase.thenDirective();
      evaluations.set(defaultIndex, new SwitchCaseEvaluation(defaultCase.name(), null, true));
    }
    if (selectedDirective == null) {
      selectedDirective = step.dataFlow().thenDirective();
    }
    return new SwitchDecision(evaluations, selectedCase, selectedDirective);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      exitOrComplete(
          DurableProcessContext context,
          ExecutionSnapshot current,
          AdvanceExecutionCommand command,
          ExecutionFrame frame) {
    long sequence = current.nextSequence();
    if (frame.taskPath() == null) {
      if (current.laneRootTaskPath() != null) {
        ExecutionSnapshot laneCompleted =
            new ExecutionSnapshot(
                current.key(),
                current.definition(),
                current.plan(),
                current.startedBy(),
                current.startedAt(),
                ExecutionPhase.RUNNING,
                current.cursor().exit(),
                current.initialInput(),
                current.context(),
                current.data(),
                sequence,
                null,
                current.laneRootTaskPath(),
                null,
                current.forkPositions(),
                null,
                current.activeTimeouts(),
                current.cancellation());
        return DurableTransition.changed(laneCompleted, List.of(), List.of());
      }
      DataReference workflowOutput = current.data();
      if (current.plan().dataFlow().outputAs() != null) {
        workflowOutput =
            transform(
                current.plan().dataFlow().outputAs(),
                current.data(),
                new RuntimeExpressionArguments(
                    inline(current.context()),
                    null,
                    null,
                    null,
                    null,
                    null,
                    workflowDescriptor(
                        current.plan(),
                        current.key().executionId().value(),
                        inline(current.initialInput()),
                        current.startedAt()),
                    runtimeDescriptor()),
                current.plan());
      }
      if (current.plan().dataFlow().outputSchema() != null) {
        schemas(current.plan())
            .validate(current.plan().dataFlow().outputSchema(), inline(workflowOutput));
      }
      var event =
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.EXECUTION_COMPLETED,
              null,
              current.initialInput(),
              workflowOutput);
      var snapshot =
          changed(
              current,
              ExecutionPhase.COMPLETED,
              current.cursor().exit(),
              current.context(),
              workflowOutput,
              sequence);
      return DurableTransition.changed(snapshot, List.of(event), List.of());
    }

    PlanStep step = current.plan().requireStep(frame.taskPath());
    if (step.kind() == PlanStepKind.FOR) {
      return advanceOrCompleteFor(context, current, command, frame, step, sequence);
    }
    if (step.kind() == PlanStepKind.LISTEN && step.listenPlan().foreach()) {
      return advanceOrCompleteSubscriptionForeach(context, current, command, frame, step, sequence);
    }
    if (step.kind() == PlanStepKind.CALL
        && step.callPlan().asyncApiSubscription() != null
        && step.callPlan().asyncApiSubscription().foreach()) {
      return advanceOrCompleteSubscriptionForeach(context, current, command, frame, step, sequence);
    }
    if (step.kind() == PlanStepKind.TRY
        && frame.tryState() != null
        && frame.tryState().phase() == TryRuntimeState.Phase.CATCH
        && step.tryPlan().catchPlan().retry() != null) {
      return scheduleOrExhaustRetry(context, current, command, frame, step, sequence);
    }
    DataReference rawOutput = current.data();
    DataReference output = rawOutput;
    if (step.dataFlow().outputAs() != null) {
      output =
          transform(
              step.dataFlow().outputAs(),
              rawOutput,
              expressionArguments(
                  current, step, frame.rawInput(), frame.input(), rawOutput, null, command),
              current.plan());
    }
    if (step.dataFlow().outputSchema() != null) {
      schemas(current.plan()).validate(step.dataFlow().outputSchema(), inline(output));
    }
    DataReference workflowContext = current.context();
    if (step.dataFlow().exportAs() != null) {
      workflowContext =
          transform(
              step.dataFlow().exportAs(),
              output,
              expressionArguments(
                  current, step, frame.rawInput(), frame.input(), rawOutput, output, command),
              current.plan(),
              current.context());
    }
    if (step.dataFlow().exportSchema() != null) {
      schemas(current.plan()).validate(step.dataFlow().exportSchema(), inline(workflowContext));
    }
    var event =
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.TASK_COMPLETED,
            step,
            frame.input(),
            output);
    ExecutionCursor parent = current.cursor().exit();
    List<PlanStep> siblings = childrenForFrame(current, parent.current());
    String directive = step.dataFlow().thenDirective();
    if (step.kind() == PlanStepKind.TRY
        && frame.tryState() != null
        && frame.tryState().phase() == TryRuntimeState.Phase.CATCH
        && step.tryPlan().catchPlan().thenDirective() != null) {
      directive = step.tryPlan().catchPlan().thenDirective();
    }
    ExecutionCursor directed;
    if (step.kind() == PlanStepKind.TRY
        && isExtensionTarget(current, parent, step)
        && step.tryPlan().catchPlan().thenDirective() != null) {
      directed = parent.replaceCurrent(parent.current().deferExtensionDirective(directive));
    } else {
      if (step.kind() == PlanStepKind.EXTENSION
          && frame.extensionState().deferredDirective() != null) {
        directive = frame.extensionState().deferredDirective();
      }
      directed = applyFlowDirective(parent, step, directive, siblings);
    }
    var completed =
        finishTaskTransition(
            context,
            current,
            command,
            output,
            workflowContext,
            directed,
            sequence,
            new ArrayList<>(List.of(event)));
    if (step.kind() == PlanStepKind.TRY
        && frame.tryState() != null
        && frame.tryState().attemptDeadlineId() != null) {
      return withOutbox(
          completed,
          attemptDeadlineEffect(
              current,
              command,
              step,
              frame.tryState(),
              WorkflowEffectType.CANCEL_TIMER,
              ":completed"));
    }
    return completed;
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      scheduleOrExhaustRetry(
          DurableProcessContext context,
          ExecutionSnapshot current,
          AdvanceExecutionCommand command,
          ExecutionFrame frame,
          PlanStep step,
          long sequence) {
    RetryPlan retry = step.tryPlan().catchPlan().retry();
    TryRuntimeState state = frame.tryState();
    WorkflowError error = state.error();
    DataReference errorData = errorReference(error);
    boolean eligible = retryConditionAllows(current, command, step, retry, errorData);
    if (retry.attemptCount() != null && state.attempt() >= retry.attemptCount()) {
      eligible = false;
    }
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    if (!eligible) {
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.RETRY_EXHAUSTED,
              step,
              errorData,
              errorData));
      return routeWorkflowError(context, current, command, step, error, sequence, events);
    }

    int retryOrdinal = Math.addExact(state.attempt(), 1);
    Duration delay = retryDelay(current, command, step, retry, errorData, retryOrdinal);
    Instant dueAt = command.requestedAt().plus(delay);
    Instant totalDeadline = null;
    if (retry.totalDuration() != null) {
      Duration totalLimit =
          resolveDuration(
              retry.totalDuration(),
              current,
              command,
              step,
              errorData,
              errorData,
              step.path() + "/catch/retry/limit/duration");
      totalDeadline = state.firstAttemptAt().plus(totalLimit);
      if (dueAt.isAfter(totalDeadline)) {
        events.add(
            event(
                current,
                command,
                sequence++,
                ExecutionEventType.RETRY_EXHAUSTED,
                step,
                errorData,
                errorData));
        return routeWorkflowError(context, current, command, step, error, sequence, events);
      }
    }

    Instant attemptDeadlineAt = totalDeadline;
    if (retry.attemptDuration() != null) {
      Duration attemptLimit =
          resolveDuration(
              retry.attemptDuration(),
              current,
              command,
              step,
              errorData,
              errorData,
              step.path() + "/catch/retry/limit/attempt/duration");
      Instant perAttemptDeadline = dueAt.plus(attemptLimit);
      if (attemptDeadlineAt == null || perAttemptDeadline.isBefore(attemptDeadlineAt)) {
        attemptDeadlineAt = perAttemptDeadline;
      }
    }
    String attemptDeadlineId =
        attemptDeadlineAt == null
            ? null
            : current.key().canonical()
                + ":"
                + step.path()
                + ":retry-attempt:"
                + retryOrdinal
                + ":deadline";
    ExecutionCursor retryCursor =
        current
            .cursor()
            .replaceCurrent(frame.retrying(dueAt, attemptDeadlineId, attemptDeadlineAt));
    String timerId = current.key().canonical() + ":" + step.path() + ":retry:" + retryOrdinal;
    ActiveRetryState pending =
        new ActiveRetryState(timerId, step.path(), current.data(), retryCursor, dueAt, error);
    DataReference descriptor = retryTimerDescriptor(current, step, pending);
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.RETRY_SCHEDULED,
            step,
            errorData,
            descriptor));
    ExecutionSnapshot waiting =
        withPendingInteraction(
            current,
            ExecutionPhase.RUNNING,
            current.cursor(),
            current.context(),
            current.data(),
            sequence,
            pending);
    return DurableTransition.changed(
        waiting,
        events,
        List.of(),
        List.of(
            retryTimerEffect(
                current, command, step, pending, WorkflowEffectType.SCHEDULE_TIMER, "")));
  }

  private boolean retryConditionAllows(
      ExecutionSnapshot current,
      ExecutionCommand command,
      PlanStep step,
      RetryPlan retry,
      DataReference errorData) {
    RuntimeExpressionArguments arguments =
        expressionArguments(current, step, errorData, errorData, null, null, command);
    if (retry.when() != null
        && !expressions.evaluateCondition(
            retry.when(), inline(errorData), arguments, current.plan().expressions().mode())) {
      return false;
    }
    return retry.exceptWhen() == null
        || !expressions.evaluateCondition(
            retry.exceptWhen(), inline(errorData), arguments, current.plan().expressions().mode());
  }

  private Duration retryDelay(
      ExecutionSnapshot current,
      ExecutionCommand command,
      PlanStep step,
      RetryPlan retry,
      DataReference errorData,
      int retryOrdinal) {
    Duration base =
        retry.delay() == null
            ? Duration.ZERO
            : resolveDuration(
                retry.delay(),
                current,
                command,
                step,
                errorData,
                errorData,
                step.path() + "/catch/retry/delay");
    BigInteger multiplier =
        switch (retry.backoff()) {
          case CONSTANT -> BigInteger.ONE;
          case LINEAR -> BigInteger.valueOf(retryOrdinal);
          case EXPONENTIAL -> BigInteger.ONE.shiftLeft(retryOrdinal - 1);
        };
    BigInteger nanos = durationNanos(base).multiply(multiplier);
    if (retry.jitterFrom() != null) {
      Duration from =
          resolveDuration(
              retry.jitterFrom(),
              current,
              command,
              step,
              errorData,
              errorData,
              step.path() + "/catch/retry/jitter/from");
      Duration to =
          resolveDuration(
              retry.jitterTo(),
              current,
              command,
              step,
              errorData,
              errorData,
              step.path() + "/catch/retry/jitter/to");
      BigInteger lower = durationNanos(from);
      BigInteger upper = durationNanos(to);
      if (lower.signum() < 0 || upper.compareTo(lower) < 0) {
        throw new RuntimeExpressionException("Retry jitter requires 0 <= from <= to");
      }
      BigInteger width = upper.subtract(lower).add(BigInteger.ONE);
      BigInteger selected =
          deterministicNumber(current.key().canonical() + "|" + step.path() + "|" + retryOrdinal)
              .mod(width)
              .add(lower);
      nanos = nanos.add(selected);
    }
    return durationFromNanos(nanos);
  }

  private static BigInteger deterministicNumber(String identity) {
    try {
      return new BigInteger(
          1,
          MessageDigest.getInstance("SHA-256").digest(identity.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("JVM does not provide SHA-256", impossible);
    }
  }

  private static BigInteger durationNanos(Duration duration) {
    return BigInteger.valueOf(duration.getSeconds())
        .multiply(BigInteger.valueOf(1_000_000_000L))
        .add(BigInteger.valueOf(duration.getNano()));
  }

  private static Duration durationFromNanos(BigInteger nanos) {
    if (nanos.signum() < 0) {
      throw new RuntimeExpressionException("Retry delay must not be negative");
    }
    BigInteger[] parts = nanos.divideAndRemainder(BigInteger.valueOf(1_000_000_000L));
    try {
      return Duration.ofSeconds(parts[0].longValueExact(), parts[1].longValueExact());
    } catch (ArithmeticException failure) {
      throw new RuntimeExpressionException("Retry delay exceeds the runtime time range", failure);
    }
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      advanceOrCompleteFor(
          DurableProcessContext context,
          ExecutionSnapshot current,
          AdvanceExecutionCommand command,
          ExecutionFrame frame,
          PlanStep step,
          long sequence) {
    ForIterationState iteration = Objects.requireNonNull(frame.iteration(), "FOR frame iteration");
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.ITERATION_COMPLETED,
            step,
            iteration.input(),
            current.data(),
            current.cursor()));

    ExecutionCursor nextCursor =
        current.cursor().replaceCurrent(frame.nextIteration(current.data()));
    if (continuesFor(current, step, current.data(), nextCursor, command)) {
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.ITERATION_STARTED,
              step,
              current.data(),
              null,
              nextCursor));
      ExecutionSnapshot snapshot =
          changed(
              current,
              ExecutionPhase.RUNNING,
              nextCursor,
              current.context(),
              current.data(),
              sequence);
      return changedWithContinuation(context, snapshot, events, command);
    }

    ExecutionCursor parent = current.cursor().exit();
    List<PlanStep> siblings = childrenForFrame(current, parent.current());
    return completeTask(
        context,
        current,
        command,
        step,
        frame.rawInput(),
        frame.input(),
        current.data(),
        parent,
        siblings,
        sequence,
        events);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      advanceOrCompleteSubscriptionForeach(
          DurableProcessContext context,
          ExecutionSnapshot current,
          AdvanceExecutionCommand command,
          ExecutionFrame frame,
          PlanStep step,
          long sequence) {
    ForIterationState iteration =
        Objects.requireNonNull(frame.iteration(), "subscription foreach iteration");
    var flow =
        step.kind() == PlanStepKind.LISTEN
            ? step.listenPlan().iteratorDataFlow()
            : step.callPlan().asyncApiSubscription().iteratorDataFlow();
    DataReference rawOutput = current.data();
    DataReference itemOutput =
        transform(
            flow.outputAs(),
            rawOutput,
            expressionArguments(
                current,
                step,
                frame.rawInput(),
                frame.input(),
                rawOutput,
                null,
                command,
                current.cursor()),
            current.plan());
    schemas(current.plan()).validate(flow.outputSchema(), inline(itemOutput));
    DataReference workflowContext =
        transform(
            flow.exportAs(),
            itemOutput,
            expressionArguments(
                current,
                step,
                frame.rawInput(),
                frame.input(),
                rawOutput,
                itemOutput,
                command,
                current.cursor()),
            current.plan(),
            current.context());
    schemas(current.plan()).validate(flow.exportSchema(), inline(workflowContext));
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.ITERATION_COMPLETED,
            step,
            iteration.input(),
            itemOutput,
            current.cursor()));

    ExecutionFrame nextFrame = frame.nextIteration(itemOutput, itemOutput);
    ExecutionCursor nextCursor = current.cursor().replaceCurrent(nextFrame);
    JsonNode collection = inline(iteration.collection());
    if (nextFrame.iteration().index() < collection.size()) {
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.ITERATION_STARTED,
              step,
              itemOutput,
              null,
              nextCursor));
      ExecutionSnapshot snapshot =
          changed(
              current, ExecutionPhase.RUNNING, nextCursor, workflowContext, itemOutput, sequence);
      return changedWithContinuation(context, snapshot, events, command);
    }

    ArrayNode outputs = JsonNodeFactory.instance.arrayNode();
    nextFrame.iteration().outputs().forEach(output -> outputs.add(inline(output).deepCopy()));
    DataReference aggregate = reference(outputs);
    ExecutionCursor parent = current.cursor().exit();
    ExecutionSnapshot progressed =
        changed(
            current,
            ExecutionPhase.RUNNING,
            current.cursor(),
            workflowContext,
            aggregate,
            sequence);
    List<PlanStep> siblings = childrenForFrame(progressed, parent.current());
    return completeTask(
        context,
        progressed,
        command,
        step,
        frame.rawInput(),
        frame.input(),
        aggregate,
        parent,
        siblings,
        sequence,
        events);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      completeTask(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ExecutionCommand command,
          PlanStep step,
          DataReference rawInput,
          DataReference taskInput,
          DataReference rawOutput,
          ExecutionCursor parent,
          List<PlanStep> siblings,
          long sequence,
          List<ExecutionHistoryEvent> events) {
    DataReference output = rawOutput;
    if (step.dataFlow().outputAs() != null) {
      output =
          transform(
              step.dataFlow().outputAs(),
              rawOutput,
              expressionArguments(current, step, rawInput, taskInput, rawOutput, null, command),
              current.plan());
    }
    if (step.dataFlow().outputSchema() != null) {
      schemas(current.plan()).validate(step.dataFlow().outputSchema(), inline(output));
    }
    DataReference workflowContext = current.context();
    if (step.dataFlow().exportAs() != null) {
      workflowContext =
          transform(
              step.dataFlow().exportAs(),
              output,
              expressionArguments(current, step, rawInput, taskInput, rawOutput, output, command),
              current.plan(),
              current.context());
    }
    if (step.dataFlow().exportSchema() != null) {
      schemas(current.plan()).validate(step.dataFlow().exportSchema(), inline(workflowContext));
    }
    events.add(
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.TASK_COMPLETED,
            step,
            taskInput,
            output));
    ExecutionCursor directed = applyFlowDirective(parent, step, siblings);
    return finishTaskTransition(
        context, current, command, output, workflowContext, directed, sequence, events);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      pause(ExecutionSnapshot current, ControlExecutionCommand command) {
    if (current.phase() == ExecutionPhase.PAUSED) {
      return DurableTransition.unchanged(current);
    }
    if (current.phase().terminal()) {
      throw new IllegalArgumentException("Cannot pause terminal execution " + current.phase());
    }
    if (current.phase() != ExecutionPhase.RUNNING) {
      throw new IllegalArgumentException("Cannot pause execution in phase " + current.phase());
    }
    long sequence = current.nextSequence();
    var event =
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.EXECUTION_PAUSED,
            null,
            current.data(),
            current.data());
    var snapshot =
        changed(
            current,
            ExecutionPhase.PAUSED,
            current.cursor(),
            current.context(),
            current.data(),
            sequence);
    List<WorkflowEffect> effects = interactionEffects(current, command, false, ":pause");
    if (!effects.isEmpty()) {
      return DurableTransition.changed(snapshot, List.of(event), List.of(), effects);
    }
    return DurableTransition.changed(snapshot, List.of(event), List.of());
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      resume(
          DurableProcessContext context,
          ExecutionSnapshot current,
          ControlExecutionCommand command) {
    if (current.phase() != ExecutionPhase.PAUSED) {
      throw new IllegalArgumentException("Only a paused execution can resume");
    }
    long sequence = current.nextSequence();
    var event =
        event(
            current,
            command,
            sequence++,
            ExecutionEventType.EXECUTION_RESUMED,
            null,
            current.data(),
            current.data());
    var snapshot =
        changed(
            current,
            ExecutionPhase.RUNNING,
            current.cursor(),
            current.context(),
            current.data(),
            sequence);
    if (current.pendingInteraction() instanceof ActiveAsyncApiSubscriptionState subscription
        && subscription.completionReady()) {
      PlanStep step = current.plan().requireStep(subscription.taskPath());
      return completeAsyncApiSubscription(
          context,
          snapshot,
          command,
          step,
          subscription,
          sequence,
          new ArrayList<>(List.of(event)),
          List.of());
    }
    if (current.pendingInteraction() instanceof ActiveCorrelatedWorkerState worker
        && worker.bufferedTerminalMessage() != null) {
      PlanStep step = current.plan().requireStep(worker.taskPath());
      JsonNode payload = inline(worker.bufferedTerminalMessage()).required("payload");
      String status = payload.required("status").textValue().toUpperCase(java.util.Locale.ROOT);
      return completeCorrelatedWorker(
          context,
          snapshot,
          command,
          step,
          worker,
          payload,
          status,
          sequence,
          new ArrayList<>(List.of(event)),
          List.of());
    }
    if (current.pendingInteraction() instanceof ActiveOperationState operation
        && operation.completionReady()) {
      return withPriorEvents(
          completeOperation(context, snapshot, command, operation, operation.terminalObservation()),
          List.of(event));
    }
    if (current.pendingInteraction() instanceof ActiveHumanTaskState humanTask
        && humanTask.completionReady()) {
      return withPriorEvents(
          completeHumanTask(context, snapshot, command, humanTask, humanTask.terminalObservation()),
          List.of(event));
    }
    List<WorkflowEffect> effects = interactionEffects(current, command, true, ":resume");
    boolean rootWaiting = current.pendingInteraction() != null;
    boolean forkCanRun =
        current.activeFork() != null && current.activeFork().nextRunnableIndex() >= 0;
    boolean forkCompletionReady = readyInteractionId(current.activeFork()) != null;
    if (rootWaiting || (current.activeFork() != null && !forkCanRun && !forkCompletionReady)) {
      return DurableTransition.changed(snapshot, List.of(event), List.of(), effects);
    }
    return withOutbox(changedWithContinuation(context, snapshot, List.of(event), command), effects);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      cancel(ExecutionSnapshot current, ControlExecutionCommand command) {
    if (current.phase() == ExecutionPhase.CANCELLED) {
      return DurableTransition.unchanged(current);
    }
    if (current.phase() == ExecutionPhase.CANCEL_REQUESTED) {
      return DurableTransition.unchanged(current);
    }
    if (current.phase().terminal()) {
      throw new IllegalArgumentException("Cannot cancel terminal execution " + current.phase());
    }
    // PURGING is deliberately not terminal() (it only starts once the execution already
    // reached a terminal phase, and that phase must be restorable if the purge itself fails -
    // see ActiveExecutionPurgeState) - but it is exactly as uncancellable as a terminal phase:
    // there is no in-flight workflow logic left to cancel, only an external cleanup operation
    // already in progress. Reject it with the same clear message shape as the terminal check
    // above, instead of falling through to a confusing "Compiled plan does not contain task
    // $purge" crash from requireStep() on the purge state's synthetic task path.
    if (current.phase() == ExecutionPhase.PURGING) {
      throw new IllegalArgumentException("Cannot cancel a purging execution");
    }
    long sequence = current.nextSequence();
    List<ExecutionHistoryEvent> events = new ArrayList<>();
    sequence =
        appendInteractionCancellationEvents(
            current,
            command,
            current.pendingInteraction(),
            current.cursor(),
            current.forkPositions(),
            events,
            sequence);
    sequence =
        appendForkInteractionCancellationEvents(
            current, command, current.activeFork(), current.forkPositions(), events, sequence);
    if (current.activeFork() != null) {
      ForkRuntimeState fork = current.activeFork();
      PlanStep forkStep = current.plan().requireStep(fork.taskPath());
      for (ForkBranchState branch : fork.branches()) {
        if (branch.phase().terminal()) continue;
        List<ForkPosition> positions = new ArrayList<>(current.forkPositions());
        positions.add(
            new ForkPosition(
                forkStep.path(),
                forkStep.name(),
                branch.path(),
                branch.name(),
                branch.declarationIndex()));
        events.add(
            forkEvent(
                current,
                command,
                sequence++,
                ExecutionEventType.FORK_BRANCH_ABANDONED,
                forkStep,
                fork.input(),
                branch.data(),
                positions));
      }
    }
    List<WorkflowEffect> effects =
        new ArrayList<>(interactionEffects(current, command, false, ":cancel"));
    if (hasUnresolvedOperation(current)) {
      events.add(
          event(
              current,
              command,
              sequence++,
              ExecutionEventType.EXECUTION_CANCEL_REQUESTED,
              null,
              current.data(),
              current.data()));
      CancellationState cancellation =
          new CancellationState(
              current.key().canonical() + ":timer:cancellation",
              command.requestedAt(),
              command.requestedAt().plus(cancellationGracePeriod),
              command.actor());
      ExecutionSnapshot requested =
          new ExecutionSnapshot(
              current.key(),
              current.definition(),
              current.plan(),
              current.startedBy(),
              current.startedAt(),
              ExecutionPhase.CANCEL_REQUESTED,
              current.cursor(),
              current.initialInput(),
              current.context(),
              current.data(),
              sequence,
              null,
              current.laneRootTaskPath(),
              current.activeFork(),
              current.forkPositions(),
              current.pendingInteraction(),
              List.of(),
              cancellation);
      effects.add(
          cancellationTimerEffect(
              requested, command, WorkflowEffectType.SCHEDULE_TIMER, ":schedule"));
      return DurableTransition.changed(requested, events, List.of(), effects);
    }
    return terminalCancellation(current, command, events, effects, sequence, command.actor());
  }

  private long appendForkInteractionCancellationEvents(
      ExecutionSnapshot current,
      ExecutionCommand command,
      ForkRuntimeState fork,
      List<ForkPosition> parentPositions,
      List<ExecutionHistoryEvent> events,
      long sequence) {
    if (fork == null) return sequence;
    PlanStep forkStep = current.plan().requireStep(fork.taskPath());
    for (ForkBranchState branch : fork.branches()) {
      if (branch.phase().terminal()) continue;
      List<ForkPosition> positions = new ArrayList<>(parentPositions);
      positions.add(
          new ForkPosition(
              forkStep.path(),
              forkStep.name(),
              branch.path(),
              branch.name(),
              branch.declarationIndex()));
      sequence =
          appendInteractionCancellationEvents(
              current,
              command,
              branch.pendingInteraction(),
              branch.cursor(),
              positions,
              events,
              sequence);
      sequence =
          appendForkInteractionCancellationEvents(
              current, command, branch.activeFork(), positions, events, sequence);
    }
    return sequence;
  }

  private long appendInteractionCancellationEvents(
      ExecutionSnapshot current,
      ExecutionCommand command,
      PendingInteraction interaction,
      ExecutionCursor cursor,
      List<ForkPosition> forks,
      List<ExecutionHistoryEvent> events,
      long sequence) {
    if (interaction == null) return sequence;
    PlanStep step = current.plan().requireStep(interaction.taskPath());
    ExecutionEventType type;
    DataReference input;
    DataReference output;
    switch (interaction) {
      case ActiveListenState listen -> {
        type = ExecutionEventType.SUBSCRIPTION_CANCELLED;
        input = listen.taskInput();
        output = subscriptionDescriptor(current, step, listen);
      }
      case ActiveAsyncApiSubscriptionState subscription -> {
        type = ExecutionEventType.ASYNC_API_SUBSCRIPTION_CANCELLED;
        input = subscription.taskInput();
        output = subscription.descriptor();
      }
      case ActiveCorrelatedWorkerState worker -> {
        type = ExecutionEventType.CORRELATED_WORKER_CANCELLATION_REQUESTED;
        input = worker.taskInput();
        output = worker.subscriptionDescriptor();
      }
      case ActiveTimerState timer -> {
        type = ExecutionEventType.TIMER_CANCELLED;
        input = timer.taskInput();
        output = timerDescriptor(current, step, timer);
      }
      case ActiveRetryState retry -> {
        type = ExecutionEventType.TIMER_CANCELLED;
        input = errorReference(retry.error());
        output = retryTimerDescriptor(current, step, retry);
      }
      case ActiveOperationState operation -> {
        type = ExecutionEventType.OPERATION_CANCELLATION_REQUESTED;
        input = operation.taskInput();
        output = operation.descriptor();
      }
      case ActiveHumanTaskState humanTask -> {
        type = ExecutionEventType.HUMAN_TASK_CANCELLATION_REQUESTED;
        input = humanTask.taskInput();
        output = humanTask.descriptor();
      }
      // A purge is a one-shot administrative action on completed/terminal executions, not
      // itself a cancellable interaction - preserves the exact pre-refactor behavior for this
      // case, but now as a compiler-checked arm: adding a 9th PendingInteraction variant fails
      // the build here instead of silently reaching this same throw only at runtime.
      case ActiveExecutionPurgeState purge ->
          throw new IllegalStateException(
              "Unknown pending interaction " + purge.getClass().getName());
    }
    events.add(
        interactionEvent(current, command, sequence++, type, step, input, output, cursor, forks));
    return sequence;
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      finalizeCancellation(
          ExecutionSnapshot current,
          ExecutionCommand command,
          List<ExecutionHistoryEvent> priorEvents,
          boolean deadlineExpired) {
    List<ExecutionHistoryEvent> events = new ArrayList<>(priorEvents);
    long sequence = current.nextSequence();
    if (deadlineExpired) {
      for (OperationLocation location : operationLocations(current)) {
        if (location.operation().completionReady()) continue;
        ObjectNode unknown = JsonNodeFactory.instance.objectNode();
        unknown.put("operationId", location.operation().operationId());
        unknown.put("reason", "Cancellation acknowledgement deadline expired");
        unknown.put("dueAt", current.cancellation().dueAt().toString());
        events.add(
            interactionEvent(
                current,
                command,
                sequence++,
                ExecutionEventType.OPERATION_OUTCOME_UNKNOWN,
                current.plan().requireStep(location.operation().taskPath()),
                location.operation().descriptor(),
                reference(unknown),
                location.cursor(),
                location.forkPositions()));
      }
      for (CorrelatedWorkerLocation location : correlatedWorkerLocations(current)) {
        ObjectNode unknown = JsonNodeFactory.instance.objectNode();
        unknown.put("operationId", location.worker().lifecycleId());
        unknown.put("reason", "Cancellation acknowledgement deadline expired");
        unknown.put("dueAt", current.cancellation().dueAt().toString());
        events.add(
            interactionEvent(
                current,
                command,
                sequence++,
                ExecutionEventType.CORRELATED_WORKER_OUTCOME_UNKNOWN,
                current.plan().requireStep(location.worker().taskPath()),
                location.worker().subscriptionDescriptor(),
                reference(unknown),
                location.cursor(),
                location.forkPositions()));
      }
    }
    List<WorkflowEffect> effects = new ArrayList<>();
    effects.add(
        cancellationTimerEffect(
            current,
            command,
            WorkflowEffectType.CANCEL_TIMER,
            deadlineExpired ? ":expired" : ":acknowledged"));
    ActorContext cancellationActor =
        Objects.requireNonNull(
                current.cancellation(),
                "Terminal cancellation requires durable " + "cancellation state")
            .requestedBy();
    return terminalCancellation(current, command, events, effects, sequence, cancellationActor);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      terminalCancellation(
          ExecutionSnapshot current,
          ExecutionCommand command,
          List<ExecutionHistoryEvent> priorEvents,
          List<WorkflowEffect> effects,
          long sequence,
          ActorContext cancellationActor) {
    List<ExecutionHistoryEvent> events = new ArrayList<>(priorEvents);
    Objects.requireNonNull(cancellationActor, "Terminal cancellation requires a requesting actor");
    long terminalSequence = sequence++;
    events.add(
        new ExecutionHistoryEvent(
            current.key().canonical() + ":" + terminalSequence,
            current.key(),
            terminalSequence,
            ExecutionEventType.EXECUTION_CANCELLED,
            current.plan().definitionSha256(),
            null,
            null,
            null,
            current.data(),
            current.data(),
            iterationPositions(current.plan(), current.cursor()),
            current.forkPositions(),
            null,
            null,
            cancellationActor,
            command.requestedAt()));
    ExecutionSnapshot cancelled =
        new ExecutionSnapshot(
            current.key(),
            current.definition(),
            current.plan(),
            current.startedBy(),
            current.startedAt(),
            ExecutionPhase.CANCELLED,
            new ExecutionCursor(List.of()),
            current.initialInput(),
            current.context(),
            current.data(),
            sequence,
            null,
            null,
            null,
            List.of());
    return DurableTransition.changed(cancelled, events, List.of(), effects);
  }

  private static boolean hasUnresolvedOperation(ExecutionSnapshot current) {
    return operationLocations(current).stream()
            .anyMatch(location -> !location.operation().completionReady())
        || !correlatedWorkerLocations(current).isEmpty();
  }

  private static List<CorrelatedWorkerLocation> correlatedWorkerLocations(
      ExecutionSnapshot current) {
    List<CorrelatedWorkerLocation> workers = new ArrayList<>();
    if (current.pendingInteraction() instanceof ActiveCorrelatedWorkerState worker) {
      workers.add(new CorrelatedWorkerLocation(worker, current.cursor(), current.forkPositions()));
    }
    collectCorrelatedWorkerLocations(
        current, current.activeFork(), current.forkPositions(), workers);
    return List.copyOf(workers);
  }

  private static void collectCorrelatedWorkerLocations(
      ExecutionSnapshot current,
      ForkRuntimeState fork,
      List<ForkPosition> parentPositions,
      List<CorrelatedWorkerLocation> workers) {
    if (fork == null) return;
    PlanStep forkStep = current.plan().requireStep(fork.taskPath());
    for (ForkBranchState branch : fork.branches()) {
      List<ForkPosition> positions = new ArrayList<>(parentPositions);
      positions.add(
          new ForkPosition(
              forkStep.path(),
              forkStep.name(),
              branch.path(),
              branch.name(),
              branch.declarationIndex()));
      if (branch.pendingInteraction() instanceof ActiveCorrelatedWorkerState worker) {
        workers.add(new CorrelatedWorkerLocation(worker, branch.cursor(), positions));
      }
      collectCorrelatedWorkerLocations(current, branch.activeFork(), positions, workers);
    }
  }

  private static List<OperationLocation> operationLocations(ExecutionSnapshot current) {
    List<OperationLocation> operations = new ArrayList<>();
    if (current.pendingInteraction() instanceof ActiveOperationState operation) {
      operations.add(new OperationLocation(operation, current.cursor(), current.forkPositions()));
    }
    collectOperationLocations(current, current.activeFork(), current.forkPositions(), operations);
    return List.copyOf(operations);
  }

  private static void collectOperationLocations(
      ExecutionSnapshot current,
      ForkRuntimeState fork,
      List<ForkPosition> parentPositions,
      List<OperationLocation> operations) {
    if (fork == null) return;
    PlanStep forkStep = current.plan().requireStep(fork.taskPath());
    for (ForkBranchState branch : fork.branches()) {
      List<ForkPosition> positions = new ArrayList<>(parentPositions);
      positions.add(
          new ForkPosition(
              forkStep.path(),
              forkStep.name(),
              branch.path(),
              branch.name(),
              branch.declarationIndex()));
      if (branch.pendingInteraction() instanceof ActiveOperationState operation) {
        operations.add(new OperationLocation(operation, branch.cursor(), positions));
      }
      collectOperationLocations(current, branch.activeFork(), positions, operations);
    }
  }

  private static OperationLocation operationLocation(
      ExecutionSnapshot current, String operationId) {
    return operationLocations(current).stream()
        .filter(location -> location.operation().operationId().equals(operationId))
        .findFirst()
        .orElse(null);
  }

  private static ForkRuntimeState replaceOperation(
      ForkRuntimeState fork, String operationId, ActiveOperationState replacement) {
    if (fork == null) return null;
    List<ForkBranchState> branches = new ArrayList<>();
    boolean changed = false;
    for (ForkBranchState branch : fork.branches()) {
      PendingInteraction interaction = branch.pendingInteraction();
      ForkRuntimeState nested = branch.activeFork();
      if (interaction instanceof ActiveOperationState operation
          && operation.operationId().equals(operationId)) {
        interaction = replacement;
        changed = true;
      } else if (nested != null && containsInteraction(branch, operationId)) {
        ForkRuntimeState replaced = replaceOperation(nested, operationId, replacement);
        changed |= replaced != nested;
        nested = replaced;
      }
      branches.add(
          new ForkBranchState(
              branch.name(),
              branch.path(),
              branch.declarationIndex(),
              branch.phase(),
              branch.cursor(),
              branch.data(),
              nested,
              interaction,
              branch.completedOrder()));
    }
    return changed ? fork.replaceBranches(branches) : fork;
  }

  private static ExecutionSnapshot copyForCancellation(
      ExecutionSnapshot current, PendingInteraction pending, ForkRuntimeState fork, long sequence) {
    return new ExecutionSnapshot(
        current.key(),
        current.definition(),
        current.plan(),
        current.startedBy(),
        current.startedAt(),
        ExecutionPhase.CANCEL_REQUESTED,
        current.cursor(),
        current.initialInput(),
        current.context(),
        current.data(),
        sequence,
        null,
        current.laneRootTaskPath(),
        fork,
        current.forkPositions(),
        pending,
        current.activeTimeouts(),
        current.cancellation());
  }

  private DataReference cancellationTimerDescriptor(ExecutionSnapshot current) {
    CancellationState cancellation = current.cancellation();
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("timerId", cancellation.timerId());
    descriptor.put("executionKey", current.key().canonical());
    descriptor.put("purpose", "cancellation-deadline");
    descriptor.put("taskPath", "/");
    descriptor.put("requestedAt", cancellation.requestedAt().toString());
    descriptor.put("dueAt", cancellation.dueAt().toString());
    return controlReference(descriptor);
  }

  private WorkflowEffect cancellationTimerEffect(
      ExecutionSnapshot current, ExecutionCommand command, WorkflowEffectType type, String suffix) {
    return new WorkflowEffect(
        current.cancellation().timerId() + suffix,
        current.key(),
        type,
        "/",
        cancellationTimerDescriptor(current),
        command.actor(),
        command.requestedAt());
  }

  private record OperationLocation(
      ActiveOperationState operation, ExecutionCursor cursor, List<ForkPosition> forkPositions) {
    private OperationLocation {
      forkPositions = List.copyOf(forkPositions);
    }
  }

  private record CorrelatedWorkerLocation(
      ActiveCorrelatedWorkerState worker,
      ExecutionCursor cursor,
      List<ForkPosition> forkPositions) {
    private CorrelatedWorkerLocation {
      forkPositions = List.copyOf(forkPositions);
    }
  }

  private List<WorkflowEffect> interactionEffects(
      ExecutionSnapshot current, ExecutionCommand command, boolean activate, String suffix) {
    List<WorkflowEffect> effects = new ArrayList<>();
    appendInteractionEffect(
        current, command, current.pendingInteraction(), activate, suffix, effects);
    collectAttemptDeadlineEffects(current, command, current.cursor(), activate, suffix, effects);
    collectInteractionEffects(current, command, current.activeFork(), activate, suffix, effects);
    return List.copyOf(effects);
  }

  private void collectInteractionEffects(
      ExecutionSnapshot current,
      ExecutionCommand command,
      ForkRuntimeState fork,
      boolean activate,
      String suffix,
      List<WorkflowEffect> effects) {
    if (fork == null) return;
    for (ForkBranchState branch : fork.branches()) {
      collectBranchInteractionEffects(current, command, branch, activate, suffix, effects);
    }
  }

  private void collectBranchInteractionEffects(
      ExecutionSnapshot current,
      ExecutionCommand command,
      ForkBranchState branch,
      boolean activate,
      String suffix,
      List<WorkflowEffect> effects) {
    appendInteractionEffect(
        current, command, branch.pendingInteraction(), activate, suffix, effects);
    collectAttemptDeadlineEffects(current, command, branch.cursor(), activate, suffix, effects);
    collectInteractionEffects(current, command, branch.activeFork(), activate, suffix, effects);
  }

  private void collectAttemptDeadlineEffects(
      ExecutionSnapshot current,
      ExecutionCommand command,
      ExecutionCursor cursor,
      boolean activate,
      String suffix,
      List<WorkflowEffect> effects) {
    for (ExecutionFrame frame : cursor.frames()) {
      TryRuntimeState state = frame.tryState();
      if (state == null || state.attemptDeadlineId() == null) {
        continue;
      }
      effects.add(
          attemptDeadlineEffect(
              current,
              command,
              current.plan().requireStep(frame.taskPath()),
              state,
              activate ? WorkflowEffectType.SCHEDULE_TIMER : WorkflowEffectType.CANCEL_TIMER,
              suffix));
    }
  }

  private void appendInteractionEffect(
      ExecutionSnapshot current,
      ExecutionCommand command,
      PendingInteraction interaction,
      boolean activate,
      String suffix,
      List<WorkflowEffect> effects) {
    // The old if/instanceof chain silently did nothing for a null interaction (no pending
    // interaction to act on); a bare switch on a reference type throws NPE on null instead of
    // matching no case, so this guard is required to preserve that behavior, not optional.
    if (interaction == null) return;
    switch (interaction) {
      case ActiveListenState listen ->
          effects.add(
              subscriptionEffect(
                  current,
                  command,
                  current.plan().requireStep(listen.taskPath()),
                  listen,
                  activate
                      ? WorkflowEffectType.UPSERT_EVENT_SUBSCRIPTION
                      : WorkflowEffectType.DELETE_EVENT_SUBSCRIPTION,
                  suffix));
      case ActiveTimerState timer ->
          effects.add(
              timerEffect(
                  current,
                  command,
                  current.plan().requireStep(timer.taskPath()),
                  timer,
                  activate ? WorkflowEffectType.SCHEDULE_TIMER : WorkflowEffectType.CANCEL_TIMER,
                  suffix));
      case ActiveRetryState retry ->
          effects.add(
              retryTimerEffect(
                  current,
                  command,
                  current.plan().requireStep(retry.taskPath()),
                  retry,
                  activate ? WorkflowEffectType.SCHEDULE_TIMER : WorkflowEffectType.CANCEL_TIMER,
                  suffix));
      // A subworkflow is a whole other durable execution, not a one-shot
      // external call - it genuinely can be paused and resumed, and a
      // cancellation must not leave the child running orphaned. Unlike a
      // plain operation, pause always propagates to the child too.
      case ActiveOperationState operation
          when OPERATION_KIND_RUN_WORKFLOW.equals(operation.operationKind()) -> {
        if (!operation.completionReady()) {
          ExecutionControlAction action =
              activate
                  ? ExecutionControlAction.RESUME
                  : suffix.startsWith(":pause")
                      ? ExecutionControlAction.PAUSE
                      : ExecutionControlAction.CANCEL;
          effects.add(
              subworkflowControlEffect(
                  current,
                  command,
                  current.plan().requireStep(operation.taskPath()),
                  operation,
                  action,
                  suffix));
        }
      }
      case ActiveOperationState operation -> {
        if (!operation.completionReady()) {
          // Pausing controls workflow advancement; it is not business
          // cancellation.  Cancelling an in-flight one-shot operation
          // here produces a terminal CANCELLED observation which is
          // buffered while paused and then incorrectly consumed as the
          // operation outcome on resume.  Leave the operation running
          // instead.  Its terminal observation is durably buffered, and
          // resume either consumes that result or redispatches the same
          // stable operation identity for adapter recovery.
          if (activate || !suffix.startsWith(":pause")) {
            effects.add(
                operationEffect(
                    current,
                    command,
                    current.plan().requireStep(operation.taskPath()),
                    operation,
                    activate
                        ? WorkflowEffectType.DISPATCH_OPERATION
                        : WorkflowEffectType.CANCEL_OPERATION,
                    suffix));
          }
        }
      }
      case ActiveAsyncApiSubscriptionState subscription -> {
        PlanStep step = current.plan().requireStep(subscription.taskPath());
        if (!activate || !subscription.completionReady()) {
          effects.add(
              asyncApiSubscriptionEffect(
                  current,
                  command,
                  step,
                  subscription,
                  activate
                      ? WorkflowEffectType.UPSERT_ASYNC_API_SUBSCRIPTION
                      : WorkflowEffectType.DELETE_ASYNC_API_SUBSCRIPTION,
                  suffix));
        }
        if (subscription.deadlineTimerId() != null
            && (!activate || !subscription.completionReady())) {
          effects.add(
              asyncApiDeadlineEffect(
                  current,
                  command,
                  step,
                  subscription,
                  activate ? WorkflowEffectType.SCHEDULE_TIMER : WorkflowEffectType.CANCEL_TIMER,
                  suffix));
        }
      }
      case ActiveCorrelatedWorkerState worker -> {
        effects.add(
            correlatedWorkerSubscriptionEffect(
                current,
                command,
                worker,
                activate
                    ? WorkflowEffectType.UPSERT_ASYNC_API_SUBSCRIPTION
                    : WorkflowEffectType.DELETE_ASYNC_API_SUBSCRIPTION,
                suffix));
        effects.add(
            correlatedWorkerDeadlineEffect(
                current,
                command,
                worker,
                activate ? WorkflowEffectType.SCHEDULE_TIMER : WorkflowEffectType.CANCEL_TIMER,
                suffix));
        if (!activate && !suffix.startsWith(":pause") && worker.cancellationDescriptor() != null) {
          effects.add(
              correlatedWorkerOperationEffect(
                  current, command, worker, worker.cancellationDescriptor(), suffix));
        }
      }
      case ActiveHumanTaskState humanTask -> {
        PlanStep step = current.plan().requireStep(humanTask.taskPath());
        // Pause preserves already-visible governed work. The workflow
        // only dematerialises its local deadline and restores that same
        // deadline on resume. Cancellation and failure close the task.
        if (!activate && !suffix.startsWith(":pause") && !humanTask.completionReady()) {
          effects.add(
              humanTaskEffect(
                  current, command, step, humanTask, WorkflowEffectType.CANCEL_HUMAN_TASK, suffix));
        }
        if (humanTask.dueTimerId() != null && !humanTask.completionReady()) {
          effects.add(
              humanTaskDeadlineEffect(
                  current,
                  command,
                  step,
                  humanTask,
                  activate ? WorkflowEffectType.SCHEDULE_TIMER : WorkflowEffectType.CANCEL_TIMER,
                  suffix));
        }
      }
      // A purge's terminalPhase is required to already be terminal (see
      // ActiveExecutionPurgeState's compact constructor) - it only ever attaches to an
      // execution that has already completed/failed/cancelled, so pause/resume/cancel
      // side-effects genuinely don't apply. Explicit no-op, not an accidental omission: a
      // future 9th PendingInteraction variant now fails the build here instead of silently
      // producing no effect.
      case ActiveExecutionPurgeState ignored -> {}
    }
  }

  private static WorkflowEffect asyncApiSubscriptionEffect(
      ExecutionSnapshot current,
      ExecutionCommand command,
      PlanStep step,
      ActiveAsyncApiSubscriptionState subscription,
      WorkflowEffectType type,
      String suffix) {
    return new WorkflowEffect(
        subscription.subscriptionId() + suffix,
        current.key(),
        type,
        step.path(),
        subscription.descriptor(),
        type == WorkflowEffectType.UPSERT_ASYNC_API_SUBSCRIPTION
            ? current.startedBy()
            : command.actor(),
        command.requestedAt());
  }

  private static WorkflowEffect correlatedWorkerSubscriptionEffect(
      ExecutionSnapshot current,
      ExecutionCommand command,
      ActiveCorrelatedWorkerState worker,
      WorkflowEffectType type,
      String suffix) {
    return new WorkflowEffect(
        worker.lifecycleId() + ":subscription" + suffix,
        current.key(),
        type,
        worker.taskPath(),
        worker.subscriptionDescriptor(),
        type == WorkflowEffectType.UPSERT_ASYNC_API_SUBSCRIPTION
            ? current.startedBy()
            : command.actor(),
        command.requestedAt());
  }

  private WorkflowEffect correlatedWorkerDeadlineEffect(
      ExecutionSnapshot current,
      ExecutionCommand command,
      ActiveCorrelatedWorkerState worker,
      WorkflowEffectType type,
      String suffix) {
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("timerId", worker.deadlineTimerId());
    descriptor.put("executionKey", current.key().canonical());
    descriptor.put("taskPath", worker.taskPath());
    descriptor.put("dueAt", worker.deadlineAt().toString());
    descriptor.put("purpose", "correlated-worker-deadline");
    descriptor.put("lifecycleId", worker.lifecycleId());
    return new WorkflowEffect(
        worker.deadlineTimerId() + suffix,
        current.key(),
        type,
        worker.taskPath(),
        controlReference(descriptor),
        command.actor(),
        command.requestedAt());
  }

  private static WorkflowEffect correlatedWorkerOperationEffect(
      ExecutionSnapshot current,
      ExecutionCommand command,
      ActiveCorrelatedWorkerState worker,
      DataReference descriptor,
      String suffix) {
    String operationId = descriptor.inlineValue().required("operationId").textValue();
    return new WorkflowEffect(
        operationId + suffix,
        current.key(),
        WorkflowEffectType.DISPATCH_OPERATION,
        worker.taskPath(),
        descriptor,
        operationId.equals(worker.lifecycleId()) ? current.startedBy() : command.actor(),
        command.requestedAt());
  }

  private WorkflowEffect correlatedWorkerMessageAck(
      ExecutionSnapshot current,
      ReceiveAsyncApiMessageCommand command,
      ActiveCorrelatedWorkerState worker) {
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("subscriptionId", worker.lifecycleId());
    descriptor.put("sourcePosition", command.sourcePosition());
    descriptor.put("commandId", command.commandId());
    return new WorkflowEffect(
        worker.lifecycleId() + ":ack:" + command.commandId(),
        current.key(),
        WorkflowEffectType.ACK_ASYNC_API_MESSAGE,
        worker.taskPath(),
        controlReference(descriptor),
        command.actor(),
        command.requestedAt());
  }

  private WorkflowEffect asyncApiDeadlineEffect(
      ExecutionSnapshot current,
      ExecutionCommand command,
      PlanStep step,
      ActiveAsyncApiSubscriptionState subscription,
      WorkflowEffectType type,
      String suffix) {
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("timerId", subscription.deadlineTimerId());
    descriptor.put("executionKey", current.key().canonical());
    descriptor.put("taskPath", step.path());
    descriptor.put("dueAt", subscription.deadlineAt().toString());
    descriptor.put("purpose", "asyncapi-subscription-deadline");
    descriptor.put("subscriptionId", subscription.subscriptionId());
    return new WorkflowEffect(
        subscription.deadlineTimerId() + suffix,
        current.key(),
        type,
        step.path(),
        controlReference(descriptor),
        command.actor(),
        command.requestedAt());
  }

  private WorkflowEffect asyncApiMessageAck(
      ExecutionSnapshot current,
      ReceiveAsyncApiMessageCommand command,
      PlanStep step,
      ActiveAsyncApiSubscriptionState subscription) {
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("subscriptionId", subscription.subscriptionId());
    descriptor.put("sourcePosition", command.sourcePosition());
    descriptor.put("commandId", command.commandId());
    return new WorkflowEffect(
        subscription.subscriptionId() + ":ack:" + command.commandId(),
        current.key(),
        WorkflowEffectType.ACK_ASYNC_API_MESSAGE,
        step.path(),
        controlReference(descriptor),
        command.actor(),
        command.requestedAt());
  }

  private WorkflowEffect subscriptionEffect(
      ExecutionSnapshot current,
      ExecutionCommand command,
      PlanStep step,
      ActiveListenState listen,
      WorkflowEffectType type,
      String suffix) {
    return new WorkflowEffect(
        listen.subscriptionId() + suffix,
        current.key(),
        type,
        step.path(),
        subscriptionDescriptor(current, step, listen),
        command.actor(),
        command.requestedAt());
  }

  private WorkflowEffect timerEffect(
      ExecutionSnapshot current,
      ExecutionCommand command,
      PlanStep step,
      ActiveTimerState timer,
      WorkflowEffectType type,
      String suffix) {
    return new WorkflowEffect(
        timer.timerId() + suffix,
        current.key(),
        type,
        step.path(),
        timerDescriptor(current, step, timer),
        command.actor(),
        command.requestedAt());
  }

  private WorkflowEffect retryTimerEffect(
      ExecutionSnapshot current,
      ExecutionCommand command,
      PlanStep step,
      ActiveRetryState retry,
      WorkflowEffectType type,
      String suffix) {
    return new WorkflowEffect(
        retry.timerId() + suffix,
        current.key(),
        type,
        step.path(),
        retryTimerDescriptor(current, step, retry),
        command.actor(),
        command.requestedAt());
  }

  private WorkflowEffect attemptDeadlineEffect(
      ExecutionSnapshot current,
      ExecutionCommand command,
      PlanStep step,
      TryRuntimeState state,
      WorkflowEffectType type,
      String suffix) {
    return new WorkflowEffect(
        state.attemptDeadlineId() + suffix,
        current.key(),
        type,
        step.path(),
        attemptDeadlineDescriptor(current, step, state),
        command.actor(),
        command.requestedAt());
  }

  private static WorkflowEffect operationEffect(
      ExecutionSnapshot current,
      ExecutionCommand command,
      PlanStep step,
      ActiveOperationState operation,
      WorkflowEffectType type,
      String suffix) {
    return new WorkflowEffect(
        operation.operationId() + suffix,
        current.key(),
        type,
        step.path(),
        operation.descriptor(),
        type == WorkflowEffectType.DISPATCH_OPERATION
                || type == WorkflowEffectType.START_SUBWORKFLOW
            ? current.startedBy()
            : command.actor(),
        command.requestedAt());
  }

  /**
   * Propagates the parent's own pause, resume or cancellation onto the child execution a {@code
   * run: workflow:} step is waiting on. The routing processor that turns this into a {@link
   * com.forwardmeasure.openworkflow.workflow.runtime.api.ControlExecutionCommand} for the child
   * execution reads {@code childExecutionKey} and {@code action} straight off this descriptor - see
   * {@code OksSubworkflowControlProcessor}.
   */
  private WorkflowEffect subworkflowControlEffect(
      ExecutionSnapshot current,
      ExecutionCommand command,
      PlanStep step,
      ActiveOperationState operation,
      ExecutionControlAction action,
      String suffix) {
    ObjectNode control = JsonNodeFactory.instance.objectNode();
    control.put("operationId", operation.operationId());
    control.put(
        "childExecutionKey",
        inline(operation.descriptor()).required("childExecutionKey").textValue());
    control.put("action", action.name());
    return new WorkflowEffect(
        operation.operationId() + ":" + action.name().toLowerCase(java.util.Locale.ROOT) + suffix,
        current.key(),
        WorkflowEffectType.CONTROL_SUBWORKFLOW,
        step.path(),
        controlReference(control),
        command.actor(),
        command.requestedAt());
  }

  private static WorkflowEffect humanTaskEffect(
      ExecutionSnapshot current,
      ExecutionCommand command,
      PlanStep step,
      ActiveHumanTaskState humanTask,
      WorkflowEffectType type,
      String suffix) {
    if (type != WorkflowEffectType.CREATE_HUMAN_TASK
        && type != WorkflowEffectType.EXPIRE_HUMAN_TASK
        && type != WorkflowEffectType.CANCEL_HUMAN_TASK) {
      throw new IllegalArgumentException("Unsupported human-task effect " + type);
    }
    return new WorkflowEffect(
        humanTask.humanTaskId() + ":" + type.name().toLowerCase(java.util.Locale.ROOT) + suffix,
        current.key(),
        type,
        step.path(),
        humanTask.descriptor(),
        type == WorkflowEffectType.CREATE_HUMAN_TASK || type == WorkflowEffectType.CANCEL_HUMAN_TASK
            ? current.startedBy()
            : command.actor(),
        command.requestedAt());
  }

  private WorkflowEffect humanTaskDeadlineEffect(
      ExecutionSnapshot current,
      ExecutionCommand command,
      PlanStep step,
      ActiveHumanTaskState humanTask,
      WorkflowEffectType type,
      String suffix) {
    if (humanTask.dueTimerId() == null) {
      throw new IllegalArgumentException("Human task has no due timer");
    }
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("timerId", humanTask.dueTimerId());
    descriptor.put("executionKey", current.key().canonical());
    descriptor.put("taskPath", step.path());
    descriptor.put("dueAt", humanTask.dueAt().toString());
    descriptor.put("purpose", "human-task-deadline");
    descriptor.put("humanTaskId", humanTask.humanTaskId());
    return new WorkflowEffect(
        humanTask.dueTimerId() + suffix,
        current.key(),
        type,
        step.path(),
        controlReference(descriptor),
        command.actor(),
        command.requestedAt());
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      changedWithContinuation(
          DurableProcessContext context,
          ExecutionSnapshot snapshot,
          List<ExecutionHistoryEvent> events,
          ExecutionCommand cause) {
    if (snapshot.phase() != ExecutionPhase.RUNNING) {
      throw new IllegalArgumentException("Only a running execution may request continuation");
    }
    var runtimeActor =
        Actors.systemCorrelated(
            snapshot.key().tenantId(),
            runtimeActorId,
            runtimeComponent,
            cause.actor().correlationId(),
            cause.requestedAt());
    var continuation =
        new AdvanceExecutionCommand(
            "advance:" + context.nextRevision(),
            snapshot.key(),
            context.nextRevision(),
            runtimeActor,
            cause.requestedAt());
    return DurableTransition.changed(snapshot, events, List.of(continuation));
  }

  private static ExecutionSnapshot changed(
      ExecutionSnapshot current,
      ExecutionPhase phase,
      ExecutionCursor cursor,
      DataReference workflowContext,
      DataReference data,
      long nextSequence) {
    boolean terminal = phase.terminal();
    return new ExecutionSnapshot(
        current.key(),
        current.definition(),
        current.plan(),
        current.startedBy(),
        current.startedAt(),
        phase,
        cursor,
        current.initialInput(),
        workflowContext,
        data,
        nextSequence,
        null,
        current.laneRootTaskPath(),
        terminal ? null : current.activeFork(),
        current.forkPositions(),
        terminal ? null : current.pendingInteraction(),
        terminal ? List.of() : current.activeTimeouts(),
        terminal ? null : current.cancellation());
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      reconcileTimeouts(
          ExecutionSnapshot previous,
          ExecutionCommand command,
          DurableTransition<
                  ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
              transition) {
    if (!transition.stateChanged()) {
      return transition;
    }
    ExecutionSnapshot state = transition.state();
    List<ActiveTimeoutState> before = previous == null ? List.of() : previous.activeTimeouts();
    List<ActiveTimeoutState> active = new ArrayList<>(state.activeTimeouts());
    List<ExecutionHistoryEvent> events = new ArrayList<>(transition.events());
    long sequence = state.nextSequence();

    if (previous == null
        && state.phase() == ExecutionPhase.RUNNING
        && state.plan().timeout() != null) {
      Duration duration = resolveWorkflowTimeout(state, command, state.plan().timeout().after());
      ActiveTimeoutState timeout =
          new ActiveTimeoutState(
              state.key().canonical() + ":timeout:workflow",
              null,
              state.data(),
              command.requestedAt().plus(duration));
      active.add(timeout);
      events.add(
          event(
              state,
              command,
              sequence++,
              ExecutionEventType.TIMER_SCHEDULED,
              null,
              state.data(),
              timeoutDescriptor(state, timeout)));
    }

    Set<String> activePaths = activeTaskPaths(state);
    active.removeIf(
        timeout ->
            !timeout.workflowTimeout()
                && timeoutOwnedByLane(state, timeout)
                && !activePaths.contains(timeout.taskPath()));
    if (command instanceof FireTimerCommand fired) {
      active.removeIf(timeout -> timeout.timerId().equals(fired.timerId()));
    }
    if (!state.phase().terminal()) {
      for (ExecutionHistoryEvent started : transition.events()) {
        if (started.type() != ExecutionEventType.TASK_STARTED
            || started.taskPath() == null
            || !activePaths.contains(started.taskPath())
            || active.stream().anyMatch(timeout -> started.taskPath().equals(timeout.taskPath()))) {
          continue;
        }
        PlanStep step = state.plan().requireStep(started.taskPath());
        if (step.timeout() == null) continue;
        DataReference taskInput = started.input();
        Duration duration =
            resolveDuration(
                step.timeout().after(),
                previous == null ? state : previous,
                command,
                step,
                previous == null ? state.data() : previous.data(),
                taskInput,
                step.path() + "/timeout/after");
        ActiveTimeoutState timeout =
            new ActiveTimeoutState(
                state.key().canonical() + ":timeout:task:" + started.sequence(),
                step.path(),
                taskInput,
                command.requestedAt().plus(duration));
        active.add(timeout);
        events.add(
            event(
                state,
                command,
                sequence++,
                ExecutionEventType.TIMER_SCHEDULED,
                step,
                taskInput,
                timeoutDescriptor(state, timeout)));
      }
    }
    if (state.phase().terminal()) {
      active.clear();
    }

    ExecutionSnapshot reconciled = withActiveTimeouts(state, active, sequence);
    Map<String, ActiveTimeoutState> materializedBefore =
        timeoutIndex(
            previous != null && previous.phase() == ExecutionPhase.RUNNING ? before : List.of());
    Map<String, ActiveTimeoutState> materializedAfter =
        timeoutIndex(reconciled.phase() == ExecutionPhase.RUNNING ? active : List.of());
    List<WorkflowEffect> outbox = new ArrayList<>(transition.outbox());
    materializedBefore.forEach(
        (timerId, timeout) -> {
          if (!materializedAfter.containsKey(timerId)) {
            outbox.add(
                timeoutEffect(
                    previous, command, timeout, WorkflowEffectType.CANCEL_TIMER, ":cancel"));
          }
        });
    materializedAfter.forEach(
        (timerId, timeout) -> {
          if (!materializedBefore.containsKey(timerId)) {
            outbox.add(
                timeoutEffect(reconciled, command, timeout, WorkflowEffectType.SCHEDULE_TIMER, ""));
          }
        });
    return DurableTransition.changed(reconciled, events, transition.followUpCommands(), outbox);
  }

  private Duration resolveWorkflowTimeout(
      ExecutionSnapshot state, ExecutionCommand command, DurationPlan duration) {
    if (duration.kind() != DurationPlan.Kind.EXPRESSION) {
      return resolveDuration(
          duration,
          state,
          command,
          state.plan().steps().isEmpty() ? null : state.plan().steps().getFirst(),
          state.data(),
          state.data(),
          "/timeout/after");
    }
    JsonNode evaluated =
        expressions.evaluateExpression(
            duration.value().textValue(),
            inline(state.data()),
            new RuntimeExpressionArguments(
                inline(state.context()),
                inline(state.data()),
                JsonNodeFactory.instance.objectNode(),
                null,
                null,
                null,
                workflowDescriptor(
                    state.plan(),
                    state.key().executionId().value(),
                    inline(state.initialInput()),
                    state.startedAt()),
                runtimeDescriptor()),
            state.plan().expressions().mode());
    if (!evaluated.isTextual()) {
      throw new RuntimeExpressionException(
          "/timeout/after duration expression must produce an " + "ISO 8601 string");
    }
    try {
      Duration result = Iso8601Duration.between(command.requestedAt(), evaluated.textValue());
      if (result.isNegative()) {
        throw new RuntimeExpressionException("/timeout/after duration must not be negative");
      }
      return result;
    } catch (java.time.format.DateTimeParseException failure) {
      throw new RuntimeExpressionException(
          "/timeout/after is not a valid ISO 8601 duration: " + evaluated.textValue(), failure);
    }
  }

  private static Map<String, ActiveTimeoutState> timeoutIndex(List<ActiveTimeoutState> timeouts) {
    Map<String, ActiveTimeoutState> result = new LinkedHashMap<>();
    timeouts.forEach(timeout -> result.put(timeout.timerId(), timeout));
    return result;
  }

  private static ActiveTimeoutState activeTimeout(
      List<ActiveTimeoutState> timeouts, String timerId) {
    return timeouts.stream()
        .filter(timeout -> timeout.timerId().equals(timerId))
        .findFirst()
        .orElse(null);
  }

  private static Set<String> activeTaskPaths(ExecutionSnapshot state) {
    Set<String> paths = new HashSet<>();
    addCursorTaskPaths(state.cursor(), paths);
    if (state.pendingInteraction() != null) {
      paths.add(state.pendingInteraction().taskPath());
    }
    addForkTaskPaths(state.activeFork(), paths);
    return paths;
  }

  private static Set<String> activeTaskPaths(ForkBranchState branch) {
    Set<String> paths = new HashSet<>();
    addCursorTaskPaths(branch.cursor(), paths);
    if (branch.pendingInteraction() != null) {
      paths.add(branch.pendingInteraction().taskPath());
    }
    addForkTaskPaths(branch.activeFork(), paths);
    return paths;
  }

  private static void addCursorTaskPaths(ExecutionCursor cursor, Set<String> paths) {
    cursor.frames().stream()
        .map(ExecutionFrame::taskPath)
        .filter(Objects::nonNull)
        .forEach(paths::add);
  }

  private static void addForkTaskPaths(ForkRuntimeState fork, Set<String> paths) {
    if (fork == null) return;
    paths.add(fork.taskPath());
    for (ForkBranchState branch : fork.branches()) {
      paths.addAll(activeTaskPaths(branch));
    }
  }

  private static boolean timeoutInFork(ForkRuntimeState fork, String taskPath) {
    if (fork == null || fork.taskPath().equals(taskPath)) {
      return false;
    }
    return fork.branches().stream().anyMatch(branch -> activeTaskPaths(branch).contains(taskPath));
  }

  private static boolean timeoutOwnedByLane(ExecutionSnapshot state, ActiveTimeoutState timeout) {
    if (state.laneRootTaskPath() == null) return true;
    return timeout.taskPath().equals(state.laneRootTaskPath())
        || timeout.taskPath().startsWith(state.laneRootTaskPath() + "/");
  }

  private static ExecutionSnapshot withActiveTimeouts(
      ExecutionSnapshot current, List<ActiveTimeoutState> activeTimeouts, long nextSequence) {
    return new ExecutionSnapshot(
        current.key(),
        current.definition(),
        current.plan(),
        current.startedBy(),
        current.startedAt(),
        current.phase(),
        current.cursor(),
        current.initialInput(),
        current.context(),
        current.data(),
        nextSequence,
        current.failure(),
        current.laneRootTaskPath(),
        current.activeFork(),
        current.forkPositions(),
        current.pendingInteraction(),
        activeTimeouts,
        current.cancellation(),
        current.pendingComputation());
  }

  private DataReference timeoutDescriptor(ExecutionSnapshot current, ActiveTimeoutState timeout) {
    ObjectNode descriptor = JsonNodeFactory.instance.objectNode();
    descriptor.put("timerId", timeout.timerId());
    descriptor.put("executionKey", current.key().canonical());
    descriptor.put("purpose", timeout.workflowTimeout() ? "workflow-timeout" : "task-timeout");
    descriptor.put("taskPath", timeout.workflowTimeout() ? "/" : timeout.taskPath());
    descriptor.put("dueAt", timeout.dueAt().toString());
    return controlReference(descriptor);
  }

  private WorkflowEffect timeoutEffect(
      ExecutionSnapshot current,
      ExecutionCommand command,
      ActiveTimeoutState timeout,
      WorkflowEffectType type,
      String suffix) {
    return new WorkflowEffect(
        timeout.timerId() + suffix,
        current.key(),
        type,
        timeout.workflowTimeout() ? "/" : timeout.taskPath(),
        timeoutDescriptor(current, timeout),
        command.actor(),
        command.requestedAt());
  }

  private static List<PlanStep> childrenForFrame(ExecutionSnapshot current, ExecutionFrame frame) {
    if (frame.taskPath() != null) {
      PlanStep entered = current.plan().requireStep(frame.taskPath());
      if (entered.kind() == PlanStepKind.EXTENSION) {
        if (frame.extensionState() == null) {
          throw new IllegalStateException("An EXTENSION frame requires durable decisions");
        }
        return entered.extensionPlan().selectedChildren(frame.extensionState().applies());
      }
      if (entered.kind() == PlanStepKind.TRY) {
        if (frame.tryState() == null) {
          throw new IllegalStateException("A TRY frame requires durable try state");
        }
        return frame.tryState().phase() == TryRuntimeState.Phase.BODY
            ? entered.tryPlan().steps()
            : entered.tryPlan().catchPlan().steps();
      }
      return entered.children();
    }
    if (current.laneRootTaskPath() != null) {
      return List.of(current.plan().requireStep(current.laneRootTaskPath()));
    }
    return current.plan().steps();
  }

  private static TryRuntimeState activeTryState(ExecutionCursor cursor, String taskPath) {
    for (int index = cursor.frames().size() - 1; index >= 0; index--) {
      ExecutionFrame frame = cursor.frames().get(index);
      if (taskPath.equals(frame.taskPath()) && frame.tryState() != null) {
        return frame.tryState();
      }
    }
    throw new IllegalStateException("Cursor has no active try frame for " + taskPath);
  }

  private static AttemptDeadline attemptDeadline(ExecutionCursor cursor, String timerId) {
    for (int index = cursor.frames().size() - 1; index >= 0; index--) {
      ExecutionFrame frame = cursor.frames().get(index);
      TryRuntimeState state = frame.tryState();
      if (state != null && timerId.equals(state.attemptDeadlineId())) {
        return new AttemptDeadline(frame.taskPath(), state);
      }
    }
    return null;
  }

  private record AttemptDeadline(String taskPath, TryRuntimeState state) {}

  private static ExecutionSnapshot changed(
      ExecutionSnapshot current,
      ExecutionPhase phase,
      ExecutionCursor cursor,
      DataReference workflowContext,
      DataReference data,
      long nextSequence,
      ForkRuntimeState activeFork) {
    return new ExecutionSnapshot(
        current.key(),
        current.definition(),
        current.plan(),
        current.startedBy(),
        current.startedAt(),
        phase,
        cursor,
        current.initialInput(),
        workflowContext,
        data,
        nextSequence,
        null,
        current.laneRootTaskPath(),
        activeFork,
        current.forkPositions(),
        current.pendingInteraction(),
        current.activeTimeouts(),
        current.cancellation());
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      finishTaskTransition(
          DurableProcessContext durableContext,
          ExecutionSnapshot current,
          ExecutionCommand command,
          DataReference output,
          DataReference workflowContext,
          ExecutionCursor cursor,
          long nextSequence,
          List<ExecutionHistoryEvent> events) {
    ExecutionSnapshot snapshot =
        changed(
            current,
            cursor.complete() && current.laneRootTaskPath() == null
                ? ExecutionPhase.COMPLETED
                : ExecutionPhase.RUNNING,
            cursor,
            workflowContext,
            output,
            nextSequence);
    if (cursor.complete() && current.laneRootTaskPath() != null) {
      return DurableTransition.changed(snapshot, events, List.of());
    }
    if (cursor.complete()) {
      DataReference workflowOutput = output;
      if (current.plan().dataFlow().outputAs() != null) {
        workflowOutput =
            transform(
                current.plan().dataFlow().outputAs(),
                output,
                new RuntimeExpressionArguments(
                    inline(workflowContext),
                    null,
                    null,
                    null,
                    null,
                    null,
                    workflowDescriptor(
                        current.plan(),
                        current.key().executionId().value(),
                        inline(current.initialInput()),
                        current.startedAt()),
                    runtimeDescriptor()),
                current.plan());
      }
      if (current.plan().dataFlow().outputSchema() != null) {
        schemas(current.plan())
            .validate(current.plan().dataFlow().outputSchema(), inline(workflowOutput));
      }
      events.add(
          event(
              snapshot,
              command,
              nextSequence,
              ExecutionEventType.EXECUTION_COMPLETED,
              null,
              snapshot.initialInput(),
              workflowOutput));
      return DurableTransition.changed(
          changed(
              snapshot,
              ExecutionPhase.COMPLETED,
              cursor,
              workflowContext,
              workflowOutput,
              nextSequence + 1),
          events,
          List.of());
    }
    return changedWithContinuation(durableContext, snapshot, events, command);
  }

  private DataSchemaValidator schemas(WorkflowPlan plan) {
    return schemaValidators.computeIfAbsent(
        plan.definitionSha256(), ignored -> new DataSchemaValidator(plan.resources()));
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      failStart(
          StartExecutionCommand command, WorkflowPlan plan, DataSchemaValidationException cause) {
    return failStart(command, plan, failure(cause));
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      failStart(StartExecutionCommand command, WorkflowPlan plan, ExecutionFailure failure) {
    var snapshot =
        new ExecutionSnapshot(
            command.key(),
            command.definition(),
            plan,
            command.actor(),
            command.requestedAt(),
            ExecutionPhase.FAILED,
            ExecutionCursor.start(command.input()),
            command.input(),
            command.input(),
            command.input(),
            1,
            failure);
    var event =
        failedEvent(
            command.key(),
            plan.definitionSha256(),
            0,
            null,
            command.input(),
            failure,
            List.of(),
            List.of(),
            command);
    return DurableTransition.changed(snapshot, List.of(event), List.of());
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      fail(
          ExecutionSnapshot current,
          ExecutionCommand command,
          DataSchemaValidationException cause) {
    ExecutionFailure failure = failure(cause);
    PlanStep step = owningStep(current.plan().steps(), cause.schema().definitionPath());
    return fail(current, command, failure, step);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      failExpression(
          ExecutionSnapshot current, ExecutionCommand command, RuntimeExpressionException cause) {
    PlanStep step = nextOrActiveStep(current);
    String definitionPath = step == null ? "/output/as" : step.path();
    return fail(current, command, expressionFailure(definitionPath, current.data(), cause), step);
  }

  private DurableTransition<
          ExecutionSnapshot, ExecutionCommand, ExecutionHistoryEvent, WorkflowEffect>
      fail(
          ExecutionSnapshot current,
          ExecutionCommand command,
          ExecutionFailure failure,
          PlanStep step) {
    long sequence = current.nextSequence();
    var snapshot =
        new ExecutionSnapshot(
            current.key(),
            current.definition(),
            current.plan(),
            current.startedBy(),
            current.startedAt(),
            ExecutionPhase.FAILED,
            current.cursor(),
            current.initialInput(),
            current.context(),
            current.data(),
            sequence + 1,
            failure,
            current.laneRootTaskPath(),
            null,
            current.forkPositions(),
            null);
    var event =
        failedEvent(
            current.key(),
            current.plan().definitionSha256(),
            sequence,
            step,
            current.data(),
            failure,
            iterationPositions(current.plan(), current.cursor()),
            current.forkPositions(),
            command);
    return DurableTransition.changed(
        snapshot,
        List.of(event),
        List.of(),
        interactionEffects(current, command, false, ":failure"));
  }

  private static ExecutionFailure expressionFailure(
      String definitionPath, DataReference rejectedData, RuntimeExpressionException cause) {
    return new ExecutionFailure(
        ExecutionFailure.EXPRESSION_ERROR,
        cause.getMessage() == null ? "Runtime expression evaluation failed" : cause.getMessage(),
        definitionPath,
        rejectedData,
        List.of());
  }

  private static PlanStep nextOrActiveStep(ExecutionSnapshot current) {
    ExecutionFrame frame = current.cursor().current();
    List<PlanStep> children = childrenForFrame(current, frame);
    if (frame.nextChildIndex() < children.size()) {
      return children.get(frame.nextChildIndex());
    }
    return frame.taskPath() == null ? null : current.plan().requireStep(frame.taskPath());
  }

  private ExecutionFailure failure(DataSchemaValidationException cause) {
    return new ExecutionFailure(
        ExecutionFailure.VALIDATION_ERROR,
        cause.getMessage(),
        cause.schema().definitionPath(),
        reference(cause.rejectedValue()),
        cause.violations());
  }

  private static PlanStep owningStep(List<PlanStep> steps, String definitionPath) {
    PlanStep result = null;
    for (PlanStep candidate : steps) {
      if (definitionPath.startsWith(candidate.path() + "/")
          && (result == null || candidate.path().length() > result.path().length())) {
        result = candidate;
      }
      PlanStep nested = owningStep(candidate.children(), definitionPath);
      if (nested != null && (result == null || nested.path().length() > result.path().length())) {
        result = nested;
      }
    }
    return result;
  }

  private static ExecutionHistoryEvent failedEvent(
      com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey key,
      String definitionSha256,
      long sequence,
      PlanStep step,
      DataReference input,
      ExecutionFailure failure,
      List<IterationPosition> iterations,
      List<ForkPosition> forks,
      ExecutionCommand command) {
    return new ExecutionHistoryEvent(
        key.canonical() + ":" + sequence,
        key,
        sequence,
        ExecutionEventType.EXECUTION_FAILED,
        definitionSha256,
        step == null ? null : step.path(),
        step == null ? null : step.name(),
        step == null ? null : step.kind(),
        input,
        failure.rejectedData(),
        iterations,
        forks,
        failure,
        null,
        command.actor(),
        command.requestedAt());
  }

  private static ExecutionCursor applyFlowDirective(
      ExecutionCursor cursor, PlanStep completed, List<PlanStep> siblings) {
    return applyFlowDirective(cursor, completed, completed.dataFlow().thenDirective(), siblings);
  }

  private static ExecutionCursor applyFlowDirective(
      ExecutionCursor cursor, PlanStep completed, String directive, List<PlanStep> siblings) {
    if ("continue".equals(directive)) {
      return cursor;
    }
    if ("end".equals(directive)) {
      return new ExecutionCursor(List.of());
    }
    if ("exit".equals(directive)) {
      return cursor.replaceCurrent(cursor.current().moveTo(siblings.size()));
    }
    String completedScope = taskListPath(completed.path());
    for (int index = 0; index < siblings.size(); index++) {
      PlanStep sibling = siblings.get(index);
      if (sibling.name().equals(directive) && taskListPath(sibling.path()).equals(completedScope)) {
        return cursor.replaceCurrent(cursor.current().moveTo(index));
      }
    }
    for (int index = 0; index < siblings.size(); index++) {
      if (siblings.get(index).name().equals(directive)) {
        return cursor.replaceCurrent(cursor.current().moveTo(index));
      }
    }
    throw new IllegalStateException("Compiled flow target is missing from its scope: " + directive);
  }

  private static String taskListPath(String taskPath) {
    int nameSeparator = taskPath.lastIndexOf('/');
    if (nameSeparator < 0) return taskPath;
    int indexSeparator = taskPath.lastIndexOf('/', nameSeparator - 1);
    return indexSeparator < 0 ? taskPath : taskPath.substring(0, indexSeparator);
  }

  private DataReference transform(
      JsonNode transformation,
      DataReference evaluatedOn,
      RuntimeExpressionArguments arguments,
      WorkflowPlan plan) {
    return transform(transformation, evaluatedOn, arguments, plan, evaluatedOn);
  }

  private DataReference transform(
      JsonNode transformation,
      DataReference evaluatedOn,
      RuntimeExpressionArguments arguments,
      WorkflowPlan plan,
      DataReference defaultValue) {
    if (transformation == null) {
      return defaultValue;
    }
    if (!transformation.isTextual()
        && !expressions.requiresEvaluation(transformation, plan.expressions().mode())) {
      return reference(transformation.deepCopy());
    }
    JsonNode transformed =
        transformation.isTextual()
            ? expressions.evaluateExpression(
                transformation.textValue(),
                inline(evaluatedOn),
                arguments,
                plan.expressions().mode())
            : expressions.evaluateTemplate(
                transformation, inline(evaluatedOn), arguments, plan.expressions().mode());
    return reference(transformed);
  }

  private RuntimeExpressionArguments expressionArguments(
      ExecutionSnapshot current,
      PlanStep step,
      DataReference rawInput,
      DataReference transformedInput,
      DataReference rawOutput,
      DataReference outputArgument,
      ExecutionCommand command) {
    return expressionArguments(
        current,
        step,
        rawInput,
        transformedInput,
        rawOutput,
        outputArgument,
        command,
        current.cursor());
  }

  private RuntimeExpressionArguments expressionArguments(
      ExecutionSnapshot current,
      PlanStep step,
      DataReference rawInput,
      DataReference transformedInput,
      DataReference rawOutput,
      DataReference outputArgument,
      ExecutionCommand command,
      ExecutionCursor cursor) {
    return new RuntimeExpressionArguments(
        inline(current.context()),
        transformedInput == null ? null : inline(transformedInput),
        outputArgument == null ? null : inline(outputArgument),
        JsonNodeFactory.instance.objectNode(),
        null,
        taskDescriptor(
            descriptorStep(current, step, cursor),
            inline(rawInput),
            rawOutput == null ? null : inline(rawOutput),
            command.requestedAt().toString()),
        workflowDescriptor(
            current.plan(),
            current.key().executionId().value(),
            inline(current.initialInput()),
            current.startedAt()),
        runtimeDescriptor(),
        iterationVariables(current.plan(), cursor));
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

  private static PlanStep descriptorStep(
      ExecutionSnapshot current, PlanStep executing, ExecutionCursor cursor) {
    for (int index = cursor.frames().size() - 1; index >= 0; index--) {
      ExecutionFrame frame = cursor.frames().get(index);
      if (frame.extensionState() == null) continue;
      String prefix = frame.taskPath() + "/$";
      if (executing.path().startsWith(prefix)) {
        return current.plan().requireStep(frame.taskPath());
      }
    }
    return executing;
  }

  private static boolean isExtensionTarget(
      ExecutionSnapshot current, ExecutionCursor cursor, PlanStep candidate) {
    if (cursor.complete() || cursor.current().extensionState() == null) {
      return false;
    }
    PlanStep wrapper = current.plan().requireStep(cursor.current().taskPath());
    return wrapper.kind() == PlanStepKind.EXTENSION
        && wrapper.extensionPlan().target().path().equals(candidate.path());
  }

  private ExtensionRuntimeState extensionDecisions(
      ExecutionSnapshot current,
      ExecutionCommand command,
      PlanStep step,
      DataReference rawInput,
      DataReference taskInput) {
    List<Boolean> applies = new ArrayList<>();
    for (var application : step.extensionPlan().applications()) {
      boolean selected =
          application.condition() == null
              || expressions.evaluateCondition(
                  application.condition(),
                  inline(taskInput),
                  expressionArguments(current, step, rawInput, taskInput, null, null, command),
                  current.plan().expressions().mode());
      applies.add(selected);
    }
    return new ExtensionRuntimeState(applies);
  }

  private Map<String, JsonNode> iterationVariables(WorkflowPlan plan, ExecutionCursor cursor) {
    Map<String, JsonNode> result = new LinkedHashMap<>();
    for (ExecutionFrame frame : cursor.frames()) {
      if (frame.tryState() != null && frame.tryState().phase() == TryRuntimeState.Phase.CATCH) {
        PlanStep tryStep = plan.requireStep(frame.taskPath());
        result.put(tryStep.tryPlan().catchPlan().as(), errorJson(frame.tryState().error()));
      }
      if (frame.iteration() == null) continue;
      PlanStep step = plan.requireStep(frame.taskPath());
      ForIterationState iteration = frame.iteration();
      JsonNode collection = inline(iteration.collection());
      if (!collection.isArray() || iteration.index() >= collection.size()) {
        throw new IllegalStateException("Durable FOR cursor does not identify an item");
      }
      result.put(iterationItemVariable(step), collection.get(iteration.index()));
      result.put(
          iterationIndexVariable(step), JsonNodeFactory.instance.numberNode(iteration.index()));
    }
    return Map.copyOf(result);
  }

  private List<IterationPosition> iterationPositions(WorkflowPlan plan, ExecutionCursor cursor) {
    List<IterationPosition> result = new ArrayList<>();
    for (ExecutionFrame frame : cursor.frames()) {
      if (frame.iteration() == null) continue;
      PlanStep step = plan.requireStep(frame.taskPath());
      ForIterationState iteration = frame.iteration();
      JsonNode collection = inline(iteration.collection());
      if (!collection.isArray() || iteration.index() >= collection.size()) {
        throw new IllegalStateException("Durable FOR cursor does not identify an item");
      }
      result.add(
          new IterationPosition(
              step.path(),
              iterationItemVariable(step),
              iterationIndexVariable(step),
              iteration.index(),
              reference(collection.get(iteration.index()))));
    }
    return List.copyOf(result);
  }

  private static String iterationItemVariable(PlanStep step) {
    if (step.kind() == PlanStepKind.FOR) {
      return step.forPlan().itemVariable();
    }
    if (step.kind() == PlanStepKind.LISTEN) {
      return step.listenPlan().itemVariable();
    }
    return step.callPlan().asyncApiSubscription().itemVariable();
  }

  private static String iterationIndexVariable(PlanStep step) {
    if (step.kind() == PlanStepKind.FOR) {
      return step.forPlan().indexVariable();
    }
    if (step.kind() == PlanStepKind.LISTEN) {
      return step.listenPlan().indexVariable();
    }
    return step.callPlan().asyncApiSubscription().indexVariable();
  }

  private JsonNode runtimeDescriptor() {
    ObjectNode result = JsonNodeFactory.instance.objectNode();
    result.put("name", runtimeComponent);
    result.put("version", RUNTIME_VERSION);
    result.set("metadata", JsonNodeFactory.instance.objectNode());
    return result;
  }

  private static JsonNode workflowDescriptor(
      WorkflowPlan plan, String executionId, JsonNode rawInput, java.time.Instant startedAt) {
    ObjectNode result = JsonNodeFactory.instance.objectNode();
    result.put("id", executionId);
    result.set("definition", plan.definition());
    result.set("input", rawInput);
    result.put("startedAt", startedAt.toString());
    return result;
  }

  private static JsonNode taskDescriptor(
      PlanStep step, JsonNode rawInput, JsonNode rawOutput, String startedAt) {
    ObjectNode result = JsonNodeFactory.instance.objectNode();
    /*
     * Open Workflow extensions inspect the extended task through members
     * such as $task.call and $task.with.  Those members are the task
     * definition itself, not an implementation-specific nested object.
     * Copy them first and then add the standard execution descriptor
     * fields below.
     */
    step.definition()
        .properties()
        .iterator()
        .forEachRemaining(entry -> result.set(entry.getKey(), entry.getValue().deepCopy()));
    result.put("name", step.name());
    result.put("reference", step.path());
    result.set("definition", step.definition());
    result.set("input", rawInput);
    result.put("startedAt", startedAt);
    result.set("output", rawOutput == null ? JsonNodeFactory.instance.nullNode() : rawOutput);
    return result;
  }

  private JsonNode inline(DataReference reference) {
    return dataAccess.resolve(reference);
  }

  private DataReference reference(JsonNode value) {
    return dataAccess.reference(value);
  }

  private DataReference controlReference(JsonNode value) {
    return dataAccess.controlReference(value);
  }

  private static void putDataReference(ObjectNode target, String field, DataReference reference) {
    if (reference.storage() == DataReference.Storage.INLINE) {
      target.set(field, reference.inlineValue().deepCopy());
    } else {
      target.set(field + "Reference", DataReferenceJson.encode(reference));
    }
  }

  private static ExecutionSnapshot requireCurrent(ExecutionSnapshot current) {
    if (current == null) {
      throw new IllegalArgumentException("Execution does not exist");
    }
    return current;
  }

  private static void requireSameExecution(ExecutionSnapshot current, ExecutionCommand command) {
    if (!current.key().equals(command.key())) {
      throw new IllegalArgumentException("Command execution key does not match state");
    }
  }

  private ExecutionHistoryEvent event(
      StartExecutionCommand command,
      long sequence,
      ExecutionEventType type,
      PlanStep step,
      DataReference input,
      DataReference output) {
    return new ExecutionHistoryEvent(
        command.key().canonical() + ":" + sequence,
        command.key(),
        sequence,
        type,
        command.definition().definitionSha256(),
        step == null ? null : step.path(),
        step == null ? null : step.name(),
        step == null ? null : step.kind(),
        input,
        output,
        List.of(),
        List.of(),
        null,
        null,
        command.actor(),
        command.requestedAt());
  }

  private ExecutionHistoryEvent event(
      ExecutionSnapshot current,
      ExecutionCommand command,
      long sequence,
      ExecutionEventType type,
      PlanStep step,
      DataReference input,
      DataReference output) {
    return new ExecutionHistoryEvent(
        current.key().canonical() + ":" + sequence,
        current.key(),
        sequence,
        type,
        current.plan().definitionSha256(),
        step == null ? null : step.path(),
        step == null ? null : step.name(),
        step == null ? null : step.kind(),
        input,
        output,
        iterationPositions(current.plan(), current.cursor()),
        current.forkPositions(),
        null,
        null,
        command.actor(),
        command.requestedAt());
  }

  private ExecutionHistoryEvent event(
      ExecutionSnapshot current,
      ExecutionCommand command,
      long sequence,
      ExecutionEventType type,
      PlanStep step,
      DataReference input,
      DataReference output,
      ExecutionCursor cursor) {
    return new ExecutionHistoryEvent(
        current.key().canonical() + ":" + sequence,
        current.key(),
        sequence,
        type,
        current.plan().definitionSha256(),
        step == null ? null : step.path(),
        step == null ? null : step.name(),
        step == null ? null : step.kind(),
        input,
        output,
        iterationPositions(current.plan(), cursor),
        current.forkPositions(),
        null,
        null,
        command.actor(),
        command.requestedAt());
  }

  private ExecutionHistoryEvent event(
      ExecutionSnapshot current,
      ExecutionCommand command,
      long sequence,
      ExecutionEventType type,
      PlanStep step,
      DataReference input,
      DataReference output,
      SwitchDecision switchDecision) {
    return new ExecutionHistoryEvent(
        current.key().canonical() + ":" + sequence,
        current.key(),
        sequence,
        type,
        current.plan().definitionSha256(),
        step == null ? null : step.path(),
        step == null ? null : step.name(),
        step == null ? null : step.kind(),
        input,
        output,
        iterationPositions(current.plan(), current.cursor()),
        current.forkPositions(),
        null,
        switchDecision,
        command.actor(),
        command.requestedAt());
  }

  private ExecutionHistoryEvent forkEvent(
      ExecutionSnapshot current,
      ExecutionCommand command,
      long sequence,
      ExecutionEventType type,
      PlanStep step,
      DataReference input,
      DataReference output,
      List<ForkPosition> forks) {
    return new ExecutionHistoryEvent(
        current.key().canonical() + ":" + sequence,
        current.key(),
        sequence,
        type,
        current.plan().definitionSha256(),
        step.path(),
        step.name(),
        step.kind(),
        input,
        output,
        iterationPositions(current.plan(), current.cursor()),
        forks,
        null,
        null,
        command.actor(),
        command.requestedAt());
  }

  private ExecutionHistoryEvent interactionEvent(
      ExecutionSnapshot current,
      ExecutionCommand command,
      long sequence,
      ExecutionEventType type,
      PlanStep step,
      DataReference input,
      DataReference output,
      ExecutionCursor cursor,
      List<ForkPosition> forks) {
    return new ExecutionHistoryEvent(
        current.key().canonical() + ":" + sequence,
        current.key(),
        sequence,
        type,
        current.plan().definitionSha256(),
        step.path(),
        step.name(),
        step.kind(),
        input,
        output,
        iterationPositions(current.plan(), cursor),
        forks,
        null,
        null,
        command.actor(),
        command.requestedAt());
  }

  private ExecutionHistoryEvent humanTaskEvent(
      ExecutionSnapshot current,
      ExecutionCommand command,
      HumanTaskObservation observation,
      long sequence,
      ExecutionEventType type,
      PlanStep step,
      DataReference input,
      DataReference output,
      ExecutionCursor cursor,
      List<ForkPosition> forks) {
    Objects.requireNonNull(observation, "observation");
    return new ExecutionHistoryEvent(
        current.key().canonical() + ":" + sequence,
        current.key(),
        sequence,
        type,
        current.plan().definitionSha256(),
        step.path(),
        step.name(),
        step.kind(),
        input,
        output,
        iterationPositions(current.plan(), cursor),
        forks,
        null,
        null,
        observation.actor(),
        observation.occurredAt());
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
