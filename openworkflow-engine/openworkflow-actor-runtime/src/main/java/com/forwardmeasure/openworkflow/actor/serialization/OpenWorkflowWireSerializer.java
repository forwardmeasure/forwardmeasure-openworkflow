package com.forwardmeasure.openworkflow.actor.serialization;

import com.forwardmeasure.openworkflow.actor.ProtocolOperationCoordinatorCommand;
import com.forwardmeasure.openworkflow.actor.ProtocolOperationCoordinatorReply;
import com.forwardmeasure.openworkflow.actor.ScheduleCommand;
import com.forwardmeasure.openworkflow.actor.ScheduleEvent;
import com.forwardmeasure.openworkflow.actor.ScheduleReply;
import com.forwardmeasure.openworkflow.actor.ScheduleState;
import com.forwardmeasure.openworkflow.actor.ScheduledExecutionRequest;
import com.forwardmeasure.openworkflow.actor.SubworkflowCoordinatorCommand;
import com.forwardmeasure.openworkflow.actor.SubworkflowCoordinatorEvent;
import com.forwardmeasure.openworkflow.actor.SubworkflowCoordinatorReply;
import com.forwardmeasure.openworkflow.actor.SubworkflowCoordinatorState;
import com.forwardmeasure.openworkflow.actor.WorkflowCommand;
import com.forwardmeasure.openworkflow.actor.WorkflowReply;
import com.forwardmeasure.openworkflow.actor.WorkflowState;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import java.io.NotSerializableException;
import java.util.Map;
import java.util.Objects;
import org.apache.pekko.actor.ExtendedActorSystem;
import org.apache.pekko.serialization.SerializerWithStringManifest;
import org.apache.pekko.serialization.jackson.JacksonCborSerializer;

/**
 * Stable public manifests around Pekko Jackson CBOR. Java class names are an implementation detail
 * and never enter journals or remoting envelopes.
 */
public final class OpenWorkflowWireSerializer extends SerializerWithStringManifest {
  public static final int IDENTIFIER = 771_003;

  private static final Map<Class<?>, String> MANIFESTS =
      Map.ofEntries(
          Map.entry(WorkflowCommand.Start.class, "ow.command.start.v1"),
          Map.entry(WorkflowCommand.RunNext.class, "ow.command.run-next.v1"),
          Map.entry(WorkflowCommand.Pause.class, "ow.command.pause.v1"),
          Map.entry(WorkflowCommand.Resume.class, "ow.command.resume.v1"),
          Map.entry(WorkflowCommand.Cancel.class, "ow.command.cancel.v1"),
          Map.entry(WorkflowCommand.TimerElapsed.class, "ow.command.timer-elapsed.v1"),
          Map.entry(WorkflowCommand.RetryElapsed.class, "ow.command.retry-elapsed.v1"),
          Map.entry(WorkflowCommand.DeadlineElapsed.class, "ow.command.deadline-elapsed.v1"),
          Map.entry(WorkflowCommand.RecheckTimers.class, "ow.command.recheck-timers.v1"),
          Map.entry(WorkflowCommand.EffectAcknowledged.class, "ow.command.effect-acknowledged.v2"),
          Map.entry(WorkflowCommand.HttpCallCompleted.class, "ow.command.http-call-completed.v1"),
          Map.entry(
              WorkflowCommand.ProtocolCallObserved.class, "ow.command.protocol-call-observed.v1"),
          Map.entry(
              WorkflowCommand.SubworkflowCompleted.class, "ow.command.subworkflow-completed.v1"),
          Map.entry(WorkflowCommand.CloudEventReceived.class, "ow.command.cloud-event-received.v2"),
          Map.entry(WorkflowCommand.GetState.class, "ow.command.get-state.v1"),
          Map.entry(WorkflowCommand.GetRuntimeState.class, "ow.command.get-runtime-state.v1"),
          Map.entry(ScheduleCommand.Register.class, "ow.schedule.command.register.v1"),
          Map.entry(ScheduleCommand.Due.class, "ow.schedule.command.due.v1"),
          Map.entry(
              ScheduleCommand.ExecutionCompleted.class,
              "ow.schedule.command.execution-completed.v1"),
          Map.entry(ScheduleCommand.EventReceived.class, "ow.schedule.command.event-received.v2"),
          Map.entry(
              ScheduleCommand.DispatchAcknowledged.class, "ow.schedule.command.dispatch-ack.v1"),
          Map.entry(ScheduleCommand.Recheck.class, "ow.schedule.command.recheck.v1"),
          Map.entry(ScheduleCommand.GetState.class, "ow.schedule.command.get-state.v1"),
          Map.entry(EngineEvent.Started.class, "ow.event.started.v1"),
          Map.entry(EngineEvent.TaskEntered.class, "ow.event.task-entered.v1"),
          Map.entry(EngineEvent.ExtensionEntered.class, "ow.event.extension-entered.v1"),
          Map.entry(EngineEvent.FunctionEntered.class, "ow.event.function-entered.v1"),
          Map.entry(EngineEvent.ForEntered.class, "ow.event.for-entered.v1"),
          Map.entry(EngineEvent.ForIterationAdvanced.class, "ow.event.for-advanced.v1"),
          Map.entry(EngineEvent.WaitScheduled.class, "ow.event.wait-scheduled.v1"),
          Map.entry(EngineEvent.DeadlineScheduled.class, "ow.event.deadline-scheduled.v1"),
          Map.entry(EngineEvent.TryEntered.class, "ow.event.try-entered.v1"),
          Map.entry(EngineEvent.ForkEntered.class, "ow.event.fork-entered.v1"),
          Map.entry(EngineEvent.ForkBranchAdvanced.class, "ow.event.fork-branch-advanced.v1"),
          Map.entry(
              EngineEvent.ForkBranchTaskEntered.class, "ow.event.fork-branch-task-entered.v1"),
          Map.entry(
              EngineEvent.ForkBranchExtensionEntered.class,
              "ow.event.fork-branch-extension-entered.v1"),
          Map.entry(
              EngineEvent.ForkBranchFunctionEntered.class,
              "ow.event.fork-branch-function-entered.v1"),
          Map.entry(
              EngineEvent.ForkBranchTaskCompleted.class, "ow.event.fork-branch-task-completed.v1"),
          Map.entry(EngineEvent.ForkBranchForEntered.class, "ow.event.fork-branch-for-entered.v1"),
          Map.entry(
              EngineEvent.ForkBranchForAdvanced.class, "ow.event.fork-branch-for-advanced.v1"),
          Map.entry(EngineEvent.ForkNestedEntered.class, "ow.event.fork-nested-entered.v1"),
          Map.entry(
              EngineEvent.ForkNestedBranchAdvanced.class,
              "ow.event.fork-nested-branch-advanced.v1"),
          Map.entry(EngineEvent.ForkNestedCompleted.class, "ow.event.fork-nested-completed.v1"),
          Map.entry(
              EngineEvent.ForkNestedTaskEntered.class, "ow.event.fork-nested-task-entered.v1"),
          Map.entry(
              EngineEvent.ForkNestedExtensionEntered.class,
              "ow.event.fork-nested-extension-entered.v1"),
          Map.entry(
              EngineEvent.ForkNestedFunctionEntered.class,
              "ow.event.fork-nested-function-entered.v1"),
          Map.entry(
              EngineEvent.ForkNestedTaskCompleted.class, "ow.event.fork-nested-task-completed.v1"),
          Map.entry(EngineEvent.ForkNestedForEntered.class, "ow.event.fork-nested-for-entered.v1"),
          Map.entry(
              EngineEvent.ForkNestedForAdvanced.class, "ow.event.fork-nested-for-advanced.v1"),
          Map.entry(
              EngineEvent.ForkBranchWaitScheduled.class, "ow.event.fork-branch-wait-scheduled.v1"),
          Map.entry(
              EngineEvent.ForkBranchWaitCompleted.class, "ow.event.fork-branch-wait-completed.v1"),
          Map.entry(EngineEvent.ForkBranchesWaiting.class, "ow.event.fork-branches-waiting.v1"),
          Map.entry(
              EngineEvent.ForkBranchContextUpdated.class,
              "ow.event.fork-branch-context-updated.v1"),
          Map.entry(EngineEvent.ForkBranchTryEntered.class, "ow.event.fork-branch-try-entered.v1"),
          Map.entry(
              EngineEvent.ForkBranchTryCompleted.class, "ow.event.fork-branch-try-completed.v1"),
          Map.entry(
              EngineEvent.ForkBranchErrorCaught.class, "ow.event.fork-branch-error-caught.v1"),
          Map.entry(
              EngineEvent.ForkBranchRetryScheduled.class,
              "ow.event.fork-branch-retry-scheduled.v1"),
          Map.entry(
              EngineEvent.ForkBranchRetryStarted.class, "ow.event.fork-branch-retry-started.v1"),
          Map.entry(
              EngineEvent.ForkBranchEmitRequested.class, "ow.event.fork-branch-emit-requested.v1"),
          Map.entry(
              EngineEvent.ForkBranchEmitAcknowledged.class,
              "ow.event.fork-branch-emit-acknowledged.v1"),
          Map.entry(
              EngineEvent.ForkBranchHttpCallRequested.class,
              "ow.event.fork-branch-http-call-requested.v1"),
          Map.entry(
              EngineEvent.ForkBranchHttpCallCompleted.class,
              "ow.event.fork-branch-http-call-completed.v1"),
          Map.entry(
              EngineEvent.ForkBranchProtocolCallRequested.class,
              "ow.event.fork-branch-protocol-call-requested.v1"),
          Map.entry(
              EngineEvent.ForkBranchProtocolCallItemAccepted.class,
              "ow.event.fork-branch-protocol-call-item.v1"),
          Map.entry(
              EngineEvent.ForkBranchProtocolCallCompleted.class,
              "ow.event.fork-branch-protocol-call-completed.v1"),
          Map.entry(
              EngineEvent.ForkBranchProtocolCallIterationStarted.class,
              "ow.event.fork-branch-protocol-call-iteration-started.v1"),
          Map.entry(
              EngineEvent.ForkBranchProtocolCallIterationAdvanced.class,
              "ow.event.fork-branch-protocol-call-iteration-advanced.v1"),
          Map.entry(
              EngineEvent.ForkBranchListenStarted.class, "ow.event.fork-branch-listen-started.v1"),
          Map.entry(
              EngineEvent.ForkBranchListenAccepted.class,
              "ow.event.fork-branch-listen-accepted.v1"),
          Map.entry(
              EngineEvent.ForkBranchListenIterationAdvanced.class,
              "ow.event.fork-branch-listen-iteration-advanced.v1"),
          Map.entry(
              EngineEvent.ForkBranchEffectSkipped.class, "ow.event.fork-branch-effect-skipped.v1"),
          Map.entry(
              EngineEvent.ForkBranchSubworkflowRequested.class,
              "ow.event.fork-branch-subworkflow-requested.v1"),
          Map.entry(
              EngineEvent.ForkBranchSubworkflowCompleted.class,
              "ow.event.fork-branch-subworkflow-completed.v1"),
          Map.entry(EngineEvent.SubworkflowRequested.class, "ow.event.subworkflow-requested.v1"),
          Map.entry(EngineEvent.SubworkflowCompleted.class, "ow.event.subworkflow-completed.v1"),
          Map.entry(EngineEvent.EmitRequested.class, "ow.event.emit-requested.v1"),
          Map.entry(EngineEvent.EmitAcknowledged.class, "ow.event.emit-acknowledged.v1"),
          Map.entry(EngineEvent.HttpCallRequested.class, "ow.event.http-call-requested.v1"),
          Map.entry(EngineEvent.HttpCallCompleted.class, "ow.event.http-call-completed.v1"),
          Map.entry(EngineEvent.ProtocolCallRequested.class, "ow.event.protocol-call-requested.v1"),
          Map.entry(EngineEvent.ProtocolCallItemAccepted.class, "ow.event.protocol-call-item.v1"),
          Map.entry(EngineEvent.ProtocolCallCompleted.class, "ow.event.protocol-call-completed.v1"),
          Map.entry(
              EngineEvent.ProtocolCallIterationStarted.class,
              "ow.event.protocol-call-iteration-started.v1"),
          Map.entry(
              EngineEvent.ProtocolCallIterationAdvanced.class,
              "ow.event.protocol-call-iteration-advanced.v1"),
          Map.entry(EngineEvent.ListenStarted.class, "ow.event.listen-started.v2"),
          Map.entry(EngineEvent.ListenEventAccepted.class, "ow.event.listen-event-accepted.v1"),
          Map.entry(EngineEvent.ListenUntilAdvanced.class, "ow.event.listen-until-advanced.v1"),
          Map.entry(
              EngineEvent.ListenIterationStarted.class, "ow.event.listen-iteration-started.v1"),
          Map.entry(
              EngineEvent.ListenIterationAdvanced.class, "ow.event.listen-iteration-advanced.v1"),
          Map.entry(EngineEvent.ErrorRaised.class, "ow.event.error-raised.v1"),
          Map.entry(EngineEvent.ErrorCaught.class, "ow.event.error-caught.v1"),
          Map.entry(EngineEvent.RetryScheduled.class, "ow.event.retry-scheduled.v1"),
          Map.entry(EngineEvent.RetryStarted.class, "ow.event.retry-started.v1"),
          Map.entry(EngineEvent.TaskCompleted.class, "ow.event.task-completed.v2"),
          Map.entry(EngineEvent.PauseRequested.class, "ow.event.pause-requested.v1"),
          Map.entry(EngineEvent.Paused.class, "ow.event.paused.v1"),
          Map.entry(EngineEvent.Resumed.class, "ow.event.resumed.v1"),
          Map.entry(EngineEvent.CancellationRequested.class, "ow.event.cancel-requested.v1"),
          Map.entry(EngineEvent.Cancelled.class, "ow.event.cancelled.v1"),
          Map.entry(EngineEvent.Completed.class, "ow.event.completed.v1"),
          Map.entry(EngineEvent.Failed.class, "ow.event.failed.v1"),
          Map.entry(ScheduleEvent.Registered.class, "ow.schedule.event.registered.v1"),
          Map.entry(ScheduleEvent.AfterScheduled.class, "ow.schedule.event.after-scheduled.v1"),
          Map.entry(ScheduleEvent.EventAccepted.class, "ow.schedule.event.event-accepted.v1"),
          Map.entry(ScheduleEvent.LaunchRequested.class, "ow.schedule.event.launch-requested.v1"),
          Map.entry(ScheduleEvent.DispatchAcknowledged.class, "ow.schedule.event.dispatch-ack.v1"),
          Map.entry(WorkflowReply.Accepted.class, "ow.reply.accepted.v1"),
          Map.entry(WorkflowReply.Rejected.class, "ow.reply.rejected.v1"),
          Map.entry(WorkflowReply.StateSnapshot.class, "ow.reply.state.v1"),
          Map.entry(WorkflowReply.RuntimeState.class, "ow.reply.runtime-state.v1"),
          Map.entry(ScheduleReply.Accepted.class, "ow.schedule.reply.accepted.v1"),
          Map.entry(ScheduleReply.Rejected.class, "ow.schedule.reply.rejected.v1"),
          Map.entry(ScheduleReply.Snapshot.class, "ow.schedule.reply.snapshot.v1"),
          Map.entry(WorkflowState.New.class, "ow.state.new.v1"),
          Map.entry(WorkflowState.Running.class, "ow.state.running.v4"),
          Map.entry(WorkflowState.Waiting.class, "ow.state.waiting.v4"),
          Map.entry(WorkflowState.Pausing.class, "ow.state.pausing.v4"),
          Map.entry(WorkflowState.Paused.class, "ow.state.paused.v4"),
          Map.entry(WorkflowState.Cancelling.class, "ow.state.cancelling.v4"),
          Map.entry(WorkflowState.Cancelled.class, "ow.state.cancelled.v1"),
          Map.entry(WorkflowState.Completed.class, "ow.state.completed.v1"),
          Map.entry(WorkflowState.Failed.class, "ow.state.failed.v1"),
          Map.entry(ScheduleState.Unregistered.class, "ow.schedule.state.unregistered.v1"),
          Map.entry(ScheduleState.Active.class, "ow.schedule.state.active.v2"),
          Map.entry(ScheduledExecutionRequest.class, "ow.schedule.dispatch.request.v1"),
          Map.entry(SubworkflowCoordinatorCommand.Launch.class, "ow.subflow.command.launch.v1"),
          Map.entry(SubworkflowCoordinatorCommand.Poll.class, "ow.subflow.command.poll.v1"),
          Map.entry(
              SubworkflowCoordinatorCommand.ParentObserved.class,
              "ow.subflow.command.parent-observed.v1"),
          Map.entry(
              SubworkflowCoordinatorCommand.ChildObserved.class,
              "ow.subflow.command.child-observed.v2"),
          Map.entry(
              SubworkflowCoordinatorCommand.ParentDeliveryObserved.class,
              "ow.subflow.command.parent-delivery-observed.v1"),
          Map.entry(SubworkflowCoordinatorEvent.Launched.class, "ow.subflow.event.launched.v1"),
          Map.entry(
              SubworkflowCoordinatorEvent.ChildTerminalObserved.class,
              "ow.subflow.event.child-terminal-observed.v1"),
          Map.entry(
              SubworkflowCoordinatorEvent.ParentNotified.class,
              "ow.subflow.event.parent-notified.v1"),
          Map.entry(SubworkflowCoordinatorReply.class, "ow.subflow.reply.v1"),
          Map.entry(SubworkflowCoordinatorState.Empty.class, "ow.subflow.state.empty.v1"),
          Map.entry(SubworkflowCoordinatorState.Active.class, "ow.subflow.state.active.v1"),
          Map.entry(SubworkflowCoordinatorState.Terminal.class, "ow.subflow.state.terminal.v1"),
          Map.entry(SubworkflowCoordinatorState.Delivered.class, "ow.subflow.state.delivered.v1"),
          Map.entry(
              ProtocolOperationCoordinatorCommand.Start.class, "ow.protocol.command.start.v1"),
          Map.entry(ProtocolOperationCoordinatorCommand.Poll.class, "ow.protocol.command.poll.v1"),
          Map.entry(
              ProtocolOperationCoordinatorCommand.StateObserved.class,
              "ow.protocol.command.state-observed.v1"),
          Map.entry(
              ProtocolOperationCoordinatorCommand.TransportEnded.class,
              "ow.protocol.command.transport-ended.v1"),
          Map.entry(
              ProtocolOperationCoordinatorCommand.DeadlineObserved.class,
              "ow.protocol.command.deadline-observed.v1"),
          Map.entry(ProtocolOperationCoordinatorReply.class, "ow.protocol.reply.v1"));
  private static final Map<String, Class<?>> TYPES =
      MANIFESTS.entrySet().stream()
          .collect(
              java.util.stream.Collectors.toUnmodifiableMap(
                  Map.Entry::getValue, Map.Entry::getKey));
  private static final Map<String, Class<?>> LEGACY_TYPES =
      Map.ofEntries(
          Map.entry("ow.event.task-completed.v1", EngineEvent.TaskCompleted.class),
          Map.entry("ow.command.effect-acknowledged.v1", WorkflowCommand.EffectAcknowledged.class),
          Map.entry("ow.event.listen-started.v1", EngineEvent.ListenStarted.class),
          Map.entry("ow.command.cloud-event-received.v1", WorkflowCommand.CloudEventReceived.class),
          Map.entry("ow.schedule.command.event-received.v1", ScheduleCommand.EventReceived.class),
          Map.entry(
              "ow.subflow.command.child-observed.v1",
              SubworkflowCoordinatorCommand.ChildObserved.class),
          Map.entry("ow.state.running.v3", WorkflowState.Running.class),
          Map.entry("ow.state.waiting.v3", WorkflowState.Waiting.class),
          Map.entry("ow.state.pausing.v3", WorkflowState.Pausing.class),
          Map.entry("ow.state.paused.v3", WorkflowState.Paused.class),
          Map.entry("ow.state.cancelling.v3", WorkflowState.Cancelling.class),
          Map.entry("ow.state.running.v2", WorkflowState.Running.class),
          Map.entry("ow.state.waiting.v2", WorkflowState.Waiting.class),
          Map.entry("ow.state.pausing.v2", WorkflowState.Pausing.class),
          Map.entry("ow.state.paused.v2", WorkflowState.Paused.class),
          Map.entry("ow.state.cancelling.v2", WorkflowState.Cancelling.class),
          Map.entry("ow.state.running.v1", WorkflowState.Running.class),
          Map.entry("ow.state.waiting.v1", WorkflowState.Waiting.class),
          Map.entry("ow.state.pausing.v1", WorkflowState.Pausing.class),
          Map.entry("ow.state.paused.v1", WorkflowState.Paused.class),
          Map.entry("ow.state.cancelling.v1", WorkflowState.Cancelling.class),
          Map.entry("ow.schedule.state.active.v1", ScheduleState.Active.class));

  private final JacksonCborSerializer delegate;

  public OpenWorkflowWireSerializer(ExtendedActorSystem system) {
    delegate = new JacksonCborSerializer(Objects.requireNonNull(system, "system"), "jackson-cbor");
  }

  @Override
  public int identifier() {
    return IDENTIFIER;
  }

  @Override
  public String manifest(Object value) {
    String manifest = MANIFESTS.get(Objects.requireNonNull(value, "value").getClass());
    if (manifest == null) {
      throw new IllegalArgumentException(
          "Unsupported OpenWorkflow wire type: " + value.getClass().getName());
    }
    return manifest;
  }

  @Override
  public byte[] toBinary(Object value) {
    manifest(value);
    return delegate.toBinary(value);
  }

  @Override
  public Object fromBinary(byte[] bytes, String manifest) throws NotSerializableException {
    Class<?> type = TYPES.get(manifest);
    if (type == null) type = LEGACY_TYPES.get(manifest);
    if (type == null) {
      throw new NotSerializableException("Unknown OpenWorkflow wire manifest: " + manifest);
    }
    return delegate.fromBinary(bytes, type.getName());
  }

  public static Map<Class<?>, String> manifests() {
    return MANIFESTS;
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
