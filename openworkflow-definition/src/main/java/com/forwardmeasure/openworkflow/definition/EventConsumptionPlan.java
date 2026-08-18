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

import java.util.List;
import java.util.Objects;

/** Compiled one/all/any event-consumption strategy. */
public record EventConsumptionPlan(
    Mode mode,
    List<EventFilterPlan> filters,
    String untilCondition,
    EventConsumptionPlan untilConsumed) {

  public EventConsumptionPlan {
    Objects.requireNonNull(mode, "mode");
    filters = List.copyOf(Objects.requireNonNull(filters, "filters"));
    if (mode == Mode.ONE && filters.size() != 1) {
      throw new IllegalArgumentException("ONE requires exactly one event filter");
    }
    if (mode != Mode.ANY && (untilCondition != null || untilConsumed != null)) {
      throw new IllegalArgumentException("Only ANY supports an until condition");
    }
    if (untilCondition != null && untilConsumed != null) {
      throw new IllegalArgumentException("ANY until must be a condition or a strategy");
    }
  }

  public enum Mode {
    ONE,
    ALL,
    ANY
  }
}
