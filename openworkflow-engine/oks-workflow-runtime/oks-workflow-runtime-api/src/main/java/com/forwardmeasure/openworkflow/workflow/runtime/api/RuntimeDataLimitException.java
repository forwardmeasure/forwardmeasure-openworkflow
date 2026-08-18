package com.forwardmeasure.openworkflow.workflow.runtime.api;

/**
 * A deterministic runtime value cannot be embedded safely in the Kafka command/state/history
 * boundary.
 */
public final class RuntimeDataLimitException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private final long actualBytes;
  private final long maximumBytes;

  public RuntimeDataLimitException(long actualBytes, long maximumBytes) {
    super(
        "Inline runtime data is " + actualBytes + " bytes; maximum is " + maximumBytes + " bytes");
    if (actualBytes <= maximumBytes || maximumBytes < 1) {
      throw new IllegalArgumentException("Runtime data limit requires actual > maximum > 0");
    }
    this.actualBytes = actualBytes;
    this.maximumBytes = maximumBytes;
  }

  public long actualBytes() {
    return actualBytes;
  }

  public long maximumBytes() {
    return maximumBytes;
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
