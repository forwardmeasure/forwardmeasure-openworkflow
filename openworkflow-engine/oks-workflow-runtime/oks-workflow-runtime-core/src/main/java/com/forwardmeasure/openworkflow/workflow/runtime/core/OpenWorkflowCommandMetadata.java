package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.forwardmeasure.durableprocessing.api.DurableCommandMetadata;
import com.forwardmeasure.openworkflow.workflow.runtime.api.AdvanceExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ControlExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.PurgeExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ReapplyExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.StartExecutionCommand;
import java.time.Instant;
import java.util.OptionalLong;

/** Maps OpenWorkflow commands onto the transport-neutral durability contract. */
public final class OpenWorkflowCommandMetadata implements DurableCommandMetadata<ExecutionCommand> {

  @Override
  public String aggregateKey(ExecutionCommand command) {
    return command.key().canonical();
  }

  @Override
  public String commandId(ExecutionCommand command) {
    return command.commandId();
  }

  @Override
  public Instant requestedAt(ExecutionCommand command) {
    return command.requestedAt();
  }

  @Override
  public String commandType(ExecutionCommand command) {
    return switch (command) {
      case StartExecutionCommand ignored -> "START";
      case ControlExecutionCommand control -> control.action().name();
      case PurgeExecutionCommand ignored -> "PURGE";
      default -> command.getClass().getSimpleName();
    };
  }

  @Override
  public OptionalLong expectedRevision(ExecutionCommand command) {
    return switch (command) {
      case AdvanceExecutionCommand advance -> OptionalLong.of(advance.expectedRevision());
      case ReapplyExecutionCommand reapply -> OptionalLong.of(reapply.expectedRevision());
      case ControlExecutionCommand control when control.expectedRevision() != null ->
          OptionalLong.of(control.expectedRevision());
      case PurgeExecutionCommand purge when purge.expectedRevision() != null ->
          OptionalLong.of(purge.expectedRevision());
      default -> OptionalLong.empty();
    };
  }

  @Override
  public boolean deduplicate(ExecutionCommand command) {
    /*
     * Runtime continuations are guarded by aggregate revision and therefore
     * need no unbounded command-receipt entry.
     */
    return !(command instanceof AdvanceExecutionCommand
        || command instanceof ReapplyExecutionCommand);
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
