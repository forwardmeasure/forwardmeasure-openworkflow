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

/** Compiled forms of the normative workflow schedule. */
public record SchedulePlan(
    DurationPlan every,
    String cron,
    DurationPlan after,
    EventConsumptionPlan on,
    EventReadMode readAs) {
  public SchedulePlan {
    int configured =
        (every == null ? 0 : 1)
            + (cron == null ? 0 : 1)
            + (after == null ? 0 : 1)
            + (on == null ? 0 : 1);
    if (configured == 0) {
      throw new IllegalArgumentException("A schedule requires at least one trigger");
    }
    if (cron != null && cron.isBlank()) {
      throw new IllegalArgumentException("cron must not be blank");
    }
    readAs = readAs == null ? EventReadMode.DATA : readAs;
    if (on == null && readAs != EventReadMode.DATA) {
      throw new IllegalArgumentException("A schedule read mode requires an event trigger");
    }
  }
}
