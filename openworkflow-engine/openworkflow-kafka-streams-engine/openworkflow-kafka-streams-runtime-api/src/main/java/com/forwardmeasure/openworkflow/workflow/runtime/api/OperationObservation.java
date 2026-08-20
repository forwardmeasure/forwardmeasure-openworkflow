package com.forwardmeasure.openworkflow.workflow.runtime.api;

import java.util.Objects;

/** Correlated, immutable observation returned by an external operation adapter. */
public record OperationObservation(
    OperationObservationStatus status,
    DataReference output,
    WorkflowError error,
    DataReference metadata) {

  public OperationObservation {
    Objects.requireNonNull(status, "status");
    boolean succeeded = status == OperationObservationStatus.SUCCEEDED;
    boolean failed =
        status == OperationObservationStatus.FAILED
            || status == OperationObservationStatus.CANCELLED;
    if (succeeded != (output != null)) {
      throw new IllegalArgumentException("Only a successful observation carries output");
    }
    if (failed != (error != null)) {
      throw new IllegalArgumentException("A failed or cancelled observation requires one error");
    }
    if (status == OperationObservationStatus.PROGRESS && (output != null || error != null)) {
      throw new IllegalArgumentException("Progress carries metadata, not terminal output/error");
    }
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
