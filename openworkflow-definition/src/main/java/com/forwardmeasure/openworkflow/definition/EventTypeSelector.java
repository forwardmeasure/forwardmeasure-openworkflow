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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/** Conservative event-type prefilter extraction; empty represents wildcard/dynamic input. */
public final class EventTypeSelector {
  private EventTypeSelector() {}

  public static Set<String> literalTypes(EventConsumptionPlan consumption) {
    Objects.requireNonNull(consumption, "consumption");
    var types = new LinkedHashSet<String>();
    if (!collect(consumption, types)) return Set.of();
    return Set.copyOf(types);
  }

  private static boolean collect(EventConsumptionPlan consumption, Set<String> types) {
    for (var filter : consumption.filters()) {
      JsonNode type = filter.properties().get("type");
      if (type == null || !type.isTextual() || type.textValue().contains("${")) {
        return false;
      }
      types.add(type.textValue());
    }
    return consumption.untilConsumed() == null || collect(consumption.untilConsumed(), types);
  }
}
