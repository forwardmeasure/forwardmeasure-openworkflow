package com.forwardmeasure.openworkflow.workflow.runtime.core;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * One externally-resumed durable boundary in an execution lane.
 *
 * <p>A root lane owns at most one interaction. Fork branches each own their own interaction, so
 * waits compose without turning Kafka Streams threads into blocked worker threads.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "interactionType")
@JsonSubTypes({
  @JsonSubTypes.Type(value = ActiveListenState.class, name = "listen"),
  @JsonSubTypes.Type(value = ActiveTimerState.class, name = "timer"),
  @JsonSubTypes.Type(value = ActiveRetryState.class, name = "retry"),
  @JsonSubTypes.Type(value = ActiveOperationState.class, name = "operation"),
  @JsonSubTypes.Type(value = ActiveAsyncApiSubscriptionState.class, name = "asyncApiSubscription"),
  @JsonSubTypes.Type(value = ActiveCorrelatedWorkerState.class, name = "correlatedWorker"),
  @JsonSubTypes.Type(value = ActiveHumanTaskState.class, name = "humanTask"),
  @JsonSubTypes.Type(value = ActiveExecutionPurgeState.class, name = "executionPurge")
})
public sealed interface PendingInteraction
    permits ActiveListenState,
        ActiveTimerState,
        ActiveRetryState,
        ActiveOperationState,
        ActiveAsyncApiSubscriptionState,
        ActiveCorrelatedWorkerState,
        ActiveHumanTaskState,
        ActiveExecutionPurgeState {
  String interactionId();

  String taskPath();
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
