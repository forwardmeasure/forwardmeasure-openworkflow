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

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.openworkflow.definition.DurationPlan;
import com.forwardmeasure.openworkflow.expression.ExpressionMode;
import com.forwardmeasure.openworkflow.expression.JqRuntimeExpressionEvaluator;
import com.forwardmeasure.openworkflow.expression.RuntimeExpressionArguments;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;

/** Deterministic temporal calculations used by the durable schedule entity. */
public final class ScheduleTemporalPlanner {
  private static final CronParser UNIX_CRON =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));

  private final JqRuntimeExpressionEvaluator expressions;

  public ScheduleTemporalPlanner() {
    this(new JqRuntimeExpressionEvaluator());
  }

  ScheduleTemporalPlanner(JqRuntimeExpressionEvaluator expressions) {
    this.expressions = Objects.requireNonNull(expressions, "expressions");
  }

  /** Returns the next fixed/calendar interval from the previous scheduled instant. */
  public Instant nextDuration(
      DurationPlan duration,
      JsonNode evaluatedOn,
      RuntimeExpressionArguments arguments,
      ExpressionMode mode,
      Instant previousScheduledAt,
      String path) {
    return OpenWorkflowDurationResolver.resolve(
        duration, evaluatedOn, arguments, mode, previousScheduledAt, path, expressions);
  }

  /** Open Workflow cron schedules use the five-field UNIX form and UTC. */
  public Instant nextCron(String expression, Instant after) {
    Objects.requireNonNull(expression, "expression");
    Objects.requireNonNull(after, "after");
    Cron cron = UNIX_CRON.parse(expression);
    cron.validate();
    return ExecutionTime.forCron(cron)
        .nextExecution(ZonedDateTime.ofInstant(after, ZoneOffset.UTC))
        .orElseThrow(
            () -> new IllegalArgumentException("Cron expression has no execution after " + after))
        .toInstant();
  }
}
