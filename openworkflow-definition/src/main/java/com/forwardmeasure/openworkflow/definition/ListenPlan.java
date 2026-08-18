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

/** Compiled Open Workflow listen task. */
public record ListenPlan(
    EventConsumptionPlan consumption,
    EventReadMode readAs,
    String itemVariable,
    String indexVariable,
    TaskDataFlow iteratorDataFlow) {

  public ListenPlan {
    Objects.requireNonNull(consumption, "consumption");
    readAs = readAs == null ? EventReadMode.DATA : readAs;
    if ((itemVariable == null) != (indexVariable == null)) {
      throw new IllegalArgumentException("Listen foreach requires both item and index variables");
    }
    if (itemVariable != null && itemVariable.equals(indexVariable)) {
      throw new IllegalArgumentException("Listen foreach variables must be distinct");
    }
    if (itemVariable == null && iteratorDataFlow != null) {
      throw new IllegalArgumentException("Iterator data flow requires listen foreach");
    }
  }

  public boolean foreach() {
    return itemVariable != null;
  }
}
