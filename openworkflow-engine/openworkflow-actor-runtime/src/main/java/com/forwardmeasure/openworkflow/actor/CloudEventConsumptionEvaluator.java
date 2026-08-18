package com.forwardmeasure.openworkflow.actor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.definition.EventConsumptionPlan;
import com.forwardmeasure.openworkflow.definition.EventFilterPlan;
import com.forwardmeasure.openworkflow.definition.EventReadMode;
import com.forwardmeasure.openworkflow.engine.api.EventConsumptionWindow;
import com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent;
import com.forwardmeasure.openworkflow.expression.ExpressionMode;
import com.forwardmeasure.openworkflow.expression.JqRuntimeExpressionEvaluator;
import com.forwardmeasure.openworkflow.expression.RuntimeExpressionArguments;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Shared deterministic Open Workflow CloudEvent filtering and correlation semantics. */
final class CloudEventConsumptionEvaluator {
  private final JqRuntimeExpressionEvaluator expressions = new JqRuntimeExpressionEvaluator();

  Offer offer(
      EventConsumptionPlan plan,
      EventReadMode readMode,
      EventConsumptionWindow window,
      WorkflowCloudEvent event,
      JsonNode evaluatedOn,
      RuntimeExpressionArguments arguments,
      ExpressionMode expressionMode) {
    if (contains(window, event)) return null;
    if (plan.untilConsumed() != null) {
      EventConsumptionWindow until =
          window.untilWindow() == null ? EventConsumptionWindow.empty() : window.untilWindow();
      Offer terminating =
          offer(
              plan.untilConsumed(), readMode, until, event, evaluatedOn, arguments, expressionMode);
      if (terminating != null) {
        var next =
            new EventConsumptionWindow(
                window.accepted(), window.correlations(),
                window.matchedFilters(), terminating.window());
        return new Offer(next, terminating.complete(), terminating.complete(), true);
      }
    }
    Match match =
        match(
            plan,
            window.correlations(),
            window.matchedFilters(),
            event,
            evaluatedOn,
            arguments,
            expressionMode);
    if (match == null) return null;
    var nextAccepted = new ArrayList<>(window.accepted());
    nextAccepted.add(event);
    var nextMatched = new LinkedHashSet<>(window.matchedFilters());
    nextMatched.add(match.filterIndex());
    boolean complete =
        switch (plan.mode()) {
          case ONE -> true;
          case ALL -> nextMatched.size() == plan.filters().size();
          case ANY ->
              plan.untilCondition() == null
                  ? plan.untilConsumed() == null
                  : expressions.evaluateCondition(
                      explicitExpression(plan.untilCondition()),
                      read(nextAccepted, readMode),
                      arguments,
                      expressionMode);
        };
    return new Offer(
        new EventConsumptionWindow(
            nextAccepted, match.correlations(), nextMatched, window.untilWindow()),
        complete,
        false,
        false);
  }

  private static boolean contains(EventConsumptionWindow window, WorkflowCloudEvent event) {
    if (window.accepted().stream()
        .anyMatch(
            previous ->
                previous.id().equals(event.id()) && previous.source().equals(event.source())))
      return true;
    return window.untilWindow() != null && contains(window.untilWindow(), event);
  }

  private Match match(
      EventConsumptionPlan plan,
      Map<String, JsonNode> existingCorrelations,
      Set<Integer> matchedFilters,
      WorkflowCloudEvent event,
      JsonNode evaluatedOn,
      RuntimeExpressionArguments arguments,
      ExpressionMode expressionMode) {
    if (plan.mode() == EventConsumptionPlan.Mode.ANY && plan.filters().isEmpty()) {
      return new Match(-1, existingCorrelations);
    }
    ObjectNode envelope = envelope(event);
    for (int index = 0; index < plan.filters().size(); index++) {
      if (plan.mode() == EventConsumptionPlan.Mode.ALL && matchedFilters.contains(index)) continue;
      EventFilterPlan filter = plan.filters().get(index);
      if (!propertiesMatch(filter.properties(), envelope, arguments, expressionMode)) continue;
      var next = new LinkedHashMap<>(existingCorrelations);
      boolean matches = true;
      for (var correlation : filter.correlations()) {
        JsonNode actual =
            expressions.evaluateExpression(
                explicitExpression(correlation.fromExpression()),
                event.data(),
                arguments,
                expressionMode);
        JsonNode expected = next.get(correlation.name());
        if (expected == null && correlation.expected() != null) {
          expected =
              isExpression(correlation.expected())
                  ? expressions.evaluateExpression(
                      correlation.expected(), evaluatedOn, arguments, expressionMode)
                  : JsonNodeFactory.instance.textNode(correlation.expected());
        }
        if (expected != null && !expected.equals(actual)) {
          matches = false;
          break;
        }
        next.putIfAbsent(correlation.name(), actual);
      }
      if (matches) return new Match(index, next);
    }
    return null;
  }

  private boolean propertiesMatch(
      JsonNode expected,
      JsonNode actual,
      RuntimeExpressionArguments arguments,
      ExpressionMode mode) {
    var fields = expected.properties().iterator();
    while (fields.hasNext()) {
      var field = fields.next();
      JsonNode value = actual.get(field.getKey());
      if (value == null) return false;
      JsonNode pattern = field.getValue();
      if (pattern.isTextual() && isExpression(pattern.textValue())) {
        if (!expressions.evaluateCondition(pattern.textValue(), value, arguments, mode))
          return false;
      } else if (pattern.isObject() && value.isObject()) {
        if (!propertiesMatch(pattern, value, arguments, mode)) return false;
      } else if (!pattern.equals(value)) return false;
    }
    return true;
  }

  static ObjectNode envelope(WorkflowCloudEvent event) {
    ObjectNode value = JsonNodeFactory.instance.objectNode();
    value.put("specversion", event.specVersion());
    value.put("id", event.id());
    value.put("source", event.source().toString());
    value.put("type", event.type());
    if (event.subject() != null) value.put("subject", event.subject());
    if (event.time() != null) value.put("time", event.time().toString());
    if (event.dataContentType() != null) {
      value.put("datacontenttype", event.dataContentType());
    }
    event.extensions().forEach(value::set);
    value.set("data", event.data());
    return value;
  }

  static JsonNode read(List<WorkflowCloudEvent> events, EventReadMode mode) {
    var output = JsonNodeFactory.instance.arrayNode();
    for (WorkflowCloudEvent event : events) {
      output.add(mode == EventReadMode.ENVELOPE ? envelope(event) : event.data());
    }
    return output;
  }

  private static String explicitExpression(String expression) {
    return isExpression(expression) ? expression : "${ " + expression + " }";
  }

  private static boolean isExpression(String value) {
    String trimmed = value.trim();
    return trimmed.startsWith("${") && trimmed.endsWith("}");
  }

  record Offer(
      EventConsumptionWindow window,
      boolean complete,
      boolean terminatingEvent,
      boolean untilProgress) {}

  private record Match(int filterIndex, Map<String, JsonNode> correlations) {}
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
