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

import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.openworkflow.definition.DurationPlan;
import com.forwardmeasure.openworkflow.definition.Iso8601Duration;
import com.forwardmeasure.openworkflow.expression.ExpressionMode;
import com.forwardmeasure.openworkflow.expression.JqRuntimeExpressionEvaluator;
import com.forwardmeasure.openworkflow.expression.RuntimeExpressionArguments;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/** Shared execution-time resolver for waits, timeouts, retries, and schedules. */
public final class OpenWorkflowDurationResolver {
  private OpenWorkflowDurationResolver() {}

  public static Instant resolve(
      DurationPlan duration,
      JsonNode evaluatedOn,
      RuntimeExpressionArguments arguments,
      ExpressionMode expressionMode,
      Instant anchor,
      String path,
      JqRuntimeExpressionEvaluator expressions) {
    Objects.requireNonNull(duration, "duration");
    Objects.requireNonNull(anchor, "anchor");
    JsonNode resolved =
        switch (duration.kind()) {
          case LITERAL -> duration.value();
          case EXPRESSION, INLINE ->
              Objects.requireNonNull(expressions, "expressions")
                  .evaluateTemplate(duration.value(), evaluatedOn, arguments, expressionMode);
        };
    String literal = resolved.isTextual() ? resolved.textValue() : inlineDuration(resolved, path);
    return Iso8601Duration.addTo(anchor, literal);
  }

  private static String inlineDuration(JsonNode value, String path) {
    if (!value.isObject()) {
      throw new IllegalArgumentException("duration must resolve to text or an object at " + path);
    }
    BigDecimal years = durationPart(value, "years", path);
    BigDecimal months = durationPart(value, "months", path);
    BigDecimal weeks = durationPart(value, "weeks", path);
    BigDecimal days = durationPart(value, "days", path);
    BigDecimal hours = durationPart(value, "hours", path);
    BigDecimal minutes = durationPart(value, "minutes", path);
    BigDecimal seconds =
        durationPart(value, "seconds", path)
            .add(durationPart(value, "milliseconds", path).movePointLeft(3));
    String date =
        component(years, "Y")
            + component(months, "M")
            + component(weeks, "W")
            + component(days, "D");
    String time = component(hours, "H") + component(minutes, "M") + component(seconds, "S");
    if (date.isEmpty() && time.isEmpty()) return "PT0S";
    return "P" + date + (time.isEmpty() ? "" : "T" + time);
  }

  private static BigDecimal durationPart(JsonNode value, String name, String path) {
    JsonNode part = value.get(name);
    if (part == null) return BigDecimal.ZERO;
    if (!part.isNumber() || part.decimalValue().signum() < 0) {
      throw new IllegalArgumentException(
          name + " must resolve to a non-negative number at " + path);
    }
    return part.decimalValue();
  }

  private static String component(BigDecimal value, String suffix) {
    return value.signum() == 0 ? "" : value.stripTrailingZeros().toPlainString() + suffix;
  }
}
