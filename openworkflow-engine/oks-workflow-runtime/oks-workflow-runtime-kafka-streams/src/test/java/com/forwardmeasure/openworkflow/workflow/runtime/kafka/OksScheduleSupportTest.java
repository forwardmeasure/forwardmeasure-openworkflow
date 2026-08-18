package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.DurationPlan;
import com.forwardmeasure.openworkflow.expression.ExpressionMode;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class OksScheduleSupportTest {

  @Test
  void unixCronUsesUtcAndReturnsTheStrictlyNextSlot() {
    assertEquals(
        Instant.parse("2026-07-30T12:05:00Z"),
        OksScheduleSupport.nextCron("*/5 * * * *", Instant.parse("2026-07-30T12:03:47Z")));
    assertEquals(
        Instant.parse("2026-07-31T00:00:00Z"),
        OksScheduleSupport.nextCron("0 0 * * *", Instant.parse("2026-07-30T00:00:00Z")));
  }

  @Test
  void calendarDurationUsesTheConcreteScheduleAnchor() {
    Instant anchor = Instant.parse("2024-01-31T12:00:00Z");

    assertEquals(
        Duration.between(anchor, Instant.parse("2024-02-29T12:00:00Z")),
        OksScheduleSupport.duration(
            new DurationPlan(DurationPlan.Kind.LITERAL, JsonNodeFactory.instance.textNode("P1M")),
            JsonNodeFactory.instance.objectNode(),
            ExpressionMode.STRICT,
            anchor));
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
