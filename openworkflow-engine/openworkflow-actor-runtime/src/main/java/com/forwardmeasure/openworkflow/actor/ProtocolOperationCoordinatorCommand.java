package com.forwardmeasure.openworkflow.actor;

import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import java.util.Objects;
import org.apache.pekko.actor.typed.ActorRef;

/** Cluster messages for one recoverable external protocol operation. */
public sealed interface ProtocolOperationCoordinatorCommand
    permits ProtocolOperationCoordinatorCommand.Start,
        ProtocolOperationCoordinatorCommand.Poll,
        ProtocolOperationCoordinatorCommand.StateObserved,
        ProtocolOperationCoordinatorCommand.TransportEnded,
        ProtocolOperationCoordinatorCommand.DeadlineObserved {

  record Start(
      ExecutionId executionId,
      String operationId,
      ActorRef<ProtocolOperationCoordinatorReply> replyTo)
      implements ProtocolOperationCoordinatorCommand {
    public Start {
      Objects.requireNonNull(executionId, "executionId");
      operationId = require(operationId, "operationId");
      Objects.requireNonNull(replyTo, "replyTo");
    }
  }

  record Poll() implements ProtocolOperationCoordinatorCommand {}

  record StateObserved(WorkflowRuntimeState state, String failure)
      implements ProtocolOperationCoordinatorCommand {}

  record TransportEnded(long generation, String failure)
      implements ProtocolOperationCoordinatorCommand {}

  record DeadlineObserved(String failure) implements ProtocolOperationCoordinatorCommand {}

  private static String require(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
    return value;
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
