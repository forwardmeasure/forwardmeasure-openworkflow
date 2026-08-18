package com.forwardmeasure.openworkflow.workflow.runtime.api;

import java.time.Instant;
import java.util.Objects;

/** Authorized request to irreversibly purge one terminal execution. */
public record PurgeExecutionCommand(
    String commandId,
    ExecutionKey key,
    ExecutionPurgePolicyDecision policyDecision,
    ActorContext actor,
    Instant requestedAt,
    Long expectedRevision)
    implements ExecutionCommand {

  public static final String REQUIRED_ROLE = "workflow-execution-purge";

  public PurgeExecutionCommand {
    ControlExecutionCommand.requireCommand(commandId, key, actor, requestedAt);
    Objects.requireNonNull(policyDecision, "policyDecision");
    if (!key.equals(policyDecision.execution())) {
      throw new SecurityException("Purge policy decision targets another execution");
    }
    if (expectedRevision != null && expectedRevision < 0) {
      throw new IllegalArgumentException("expectedRevision must not be negative");
    }
  }

  public PurgeExecutionCommand(
      String commandId,
      ExecutionKey key,
      ExecutionPurgePolicyDecision policyDecision,
      ActorContext actor,
      Instant requestedAt) {
    this(commandId, key, policyDecision, actor, requestedAt, null);
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
