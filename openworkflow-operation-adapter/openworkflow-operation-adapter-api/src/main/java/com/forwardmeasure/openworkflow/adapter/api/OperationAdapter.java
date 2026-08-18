package com.forwardmeasure.openworkflow.adapter.api;

import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservation;
import java.util.concurrent.CompletionStage;

/**
 * Protocol adapter boundary. Implementations perform I/O outside Streams threads and return
 * observations; they never mutate workflow state.
 */
public interface OperationAdapter {
  boolean supports(OperationRequest request);

  CompletionStage<OperationObservation> execute(
      OperationRequest request, OperationProgressSink progress);

  /**
   * Recovers an operation for which the adapter dispatcher durably published at least one progress
   * observation before losing its local process state.
   *
   * <p>Stateless and intrinsically idempotent adapters can use the default replay behaviour.
   * Stateful protocols can use the checkpoint to resume or reconcile the already-created remote
   * operation.
   */
  default CompletionStage<OperationObservation> recover(
      OperationRequest request, OperationObservation checkpoint, OperationProgressSink progress) {
    return execute(request, progress);
  }

  CompletionStage<Void> cancel(OperationRequest request);

  /**
   * Cancels an operation after local adapter state was lost. The checkpoint is the last committed,
   * redacted progress observation.
   */
  default CompletionStage<Void> cancel(OperationRequest request, OperationObservation checkpoint) {
    return cancel(request);
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
