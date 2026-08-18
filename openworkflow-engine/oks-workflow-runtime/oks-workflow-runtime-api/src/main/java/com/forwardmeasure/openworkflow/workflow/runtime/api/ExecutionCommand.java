package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.time.Instant;

/** Durable, authenticated intent applied to one execution aggregate. */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "commandType")
@JsonSubTypes({
  @JsonSubTypes.Type(value = StartExecutionCommand.class, name = "start"),
  @JsonSubTypes.Type(value = ControlExecutionCommand.class, name = "control"),
  @JsonSubTypes.Type(value = AdvanceExecutionCommand.class, name = "advance"),
  @JsonSubTypes.Type(value = ReceiveEventCommand.class, name = "event"),
  @JsonSubTypes.Type(value = ReceiveAsyncApiMessageCommand.class, name = "asyncApiMessage"),
  @JsonSubTypes.Type(
      value = ObserveAsyncApiSubscriptionCommand.class,
      name = "asyncApiObservation"),
  @JsonSubTypes.Type(value = FireTimerCommand.class, name = "timer"),
  @JsonSubTypes.Type(value = ObserveOperationCommand.class, name = "operation"),
  @JsonSubTypes.Type(value = ObserveHumanTaskCommand.class, name = "humanTask"),
  @JsonSubTypes.Type(value = ObserveWorkflowComputationCommand.class, name = "workflowComputation"),
  @JsonSubTypes.Type(
      value = ObserveWorkflowComputationFailureCommand.class,
      name = "workflowComputationFailure"),
  @JsonSubTypes.Type(value = ReapplyExecutionCommand.class, name = "reapply"),
  @JsonSubTypes.Type(value = PurgeExecutionCommand.class, name = "purge")
})
public sealed interface ExecutionCommand
    permits StartExecutionCommand,
        ControlExecutionCommand,
        AdvanceExecutionCommand,
        ReceiveEventCommand,
        ReceiveAsyncApiMessageCommand,
        ObserveAsyncApiSubscriptionCommand,
        FireTimerCommand,
        ObserveOperationCommand,
        ObserveHumanTaskCommand,
        ObserveWorkflowComputationCommand,
        ObserveWorkflowComputationFailureCommand,
        ReapplyExecutionCommand,
        PurgeExecutionCommand {

  String commandId();

  ExecutionKey key();

  ActorContext actor();

  Instant requestedAt();
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
