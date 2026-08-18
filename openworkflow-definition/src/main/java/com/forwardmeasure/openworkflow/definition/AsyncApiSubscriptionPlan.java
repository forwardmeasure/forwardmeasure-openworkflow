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
package com.forwardmeasure.openworkflow.definition;

import java.util.Objects;

/**
 * Immutable, runtime-owned semantics for an AsyncAPI subscription call.
 *
 * <p>The transport adapter owns only protocol I/O. Filtering, termination, iteration and data flow
 * remain in the durable workflow state machine.
 */
public record AsyncApiSubscriptionPlan(
    String filter,
    Consumption consumption,
    String itemVariable,
    String indexVariable,
    TaskDataFlow iteratorDataFlow) {

  public AsyncApiSubscriptionPlan {
    Objects.requireNonNull(consumption, "consumption");
    if ((itemVariable == null) != (indexVariable == null)) {
      throw new IllegalArgumentException("AsyncAPI foreach requires both item and index variables");
    }
    if (itemVariable != null && itemVariable.equals(indexVariable)) {
      throw new IllegalArgumentException("AsyncAPI foreach variables must be distinct");
    }
    if (itemVariable == null && iteratorDataFlow != null) {
      throw new IllegalArgumentException("Iterator data flow requires AsyncAPI foreach");
    }
  }

  public boolean foreach() {
    return itemVariable != null;
  }

  public record Consumption(Mode mode, Integer amount, String condition, DurationPlan duration) {
    public Consumption {
      Objects.requireNonNull(mode, "mode");
      if (mode == Mode.AMOUNT && (amount == null || amount < 1)) {
        throw new IllegalArgumentException("AsyncAPI consume.amount must be positive");
      }
      if (mode != Mode.AMOUNT && amount != null) {
        throw new IllegalArgumentException("Only AMOUNT carries an amount");
      }
      if ((mode == Mode.WHILE || mode == Mode.UNTIL) != (condition != null)) {
        throw new IllegalArgumentException("WHILE and UNTIL require exactly one condition");
      }
    }

    public enum Mode {
      AMOUNT,
      WHILE,
      UNTIL
    }
  }
}
