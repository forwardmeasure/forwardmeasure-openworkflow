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
package com.forwardmeasure.openworkflow.actor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.expression.RuntimeExpressionArguments;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class ScheduleTemporalPlannerTest {
  private static final Instant AT = Instant.parse("2026-01-31T23:59:00Z");

  @Test
  void calculatesEveryAndAfterFromTheirNormativeAnchors() {
    var plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: forwardmeasure
                  name: temporal-schedule
                  version: '1.0.0'
                schedule:
                  every: P1M
                  after: '${ .delay }'
                do:
                  - work:
                      set:
                        accepted: true
                """
                    .getBytes(StandardCharsets.UTF_8));
    var input = JsonNodeFactory.instance.objectNode().put("delay", "PT90S");
    var planner = new ScheduleTemporalPlanner();

    assertEquals(
        Instant.parse("2026-02-28T23:59:00Z"),
        planner.nextDuration(
            plan.schedule().every(),
            input,
            RuntimeExpressionArguments.empty(),
            plan.expressions().mode(),
            AT,
            "/schedule/every"));
    assertEquals(
        AT.plusSeconds(90),
        planner.nextDuration(
            plan.schedule().after(),
            input,
            RuntimeExpressionArguments.empty(),
            plan.expressions().mode(),
            AT,
            "/schedule/after"));
  }

  @Test
  void calculatesStrictlyNextUnixCronInstantInUtc() {
    var planner = new ScheduleTemporalPlanner();
    assertEquals(
        Instant.parse("2026-08-16T00:00:00Z"),
        planner.nextCron("0 0 * * *", Instant.parse("2026-08-15T12:34:56Z")));
    assertThrows(IllegalArgumentException.class, () -> planner.nextCron("not a cron", AT));
  }
}
