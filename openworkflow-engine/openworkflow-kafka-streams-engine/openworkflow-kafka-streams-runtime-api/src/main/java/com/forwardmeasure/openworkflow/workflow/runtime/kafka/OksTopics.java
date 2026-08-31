package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import java.util.Objects;
import java.util.Set;

public record OksTopics(
    String definitionCommands,
    String definitionHistory,
    String definitionCatalogue,
    String definitions,
    String commands,
    String history,
    String effects,
    String operationCheckpoints,
    String subscriptionEffects,
    String timerEffects,
    String subworkflowEffects,
    String inboundEvents,
    String emittedEvents,
    String deadLetters) {
  public OksTopics {
    requireText(definitionCommands, "definitionCommands");
    requireText(definitionHistory, "definitionHistory");
    requireText(definitionCatalogue, "definitionCatalogue");
    requireText(definitions, "definitions");
    requireText(commands, "commands");
    requireText(history, "history");
    requireText(effects, "effects");
    requireText(operationCheckpoints, "operationCheckpoints");
    requireText(subscriptionEffects, "subscriptionEffects");
    requireText(timerEffects, "timerEffects");
    requireText(subworkflowEffects, "subworkflowEffects");
    requireText(inboundEvents, "inboundEvents");
    requireText(emittedEvents, "emittedEvents");
    requireText(deadLetters, "deadLetters");
    if (Set.of(
                definitionCommands,
                definitionHistory,
                definitionCatalogue,
                definitions,
                commands,
                history,
                effects,
                operationCheckpoints,
                subscriptionEffects,
                timerEffects,
                subworkflowEffects,
                inboundEvents,
                emittedEvents,
                deadLetters)
            .size()
        != 14) {
      throw new IllegalArgumentException("All topic names must be distinct");
    }
  }

  public static OksTopics withPrefix(String prefix) {
    requireText(prefix, "prefix");
    return new OksTopics(
        prefix + ".definition-commands",
        prefix + ".definition-history",
        prefix + ".definition-catalogue",
        prefix + ".definitions",
        prefix + ".commands",
        prefix + ".history",
        prefix + ".effects",
        prefix + ".operation-checkpoints",
        prefix + ".subscription-effects",
        prefix + ".timer-effects",
        prefix + ".subworkflow-effects",
        prefix + ".inbound-events",
        prefix + ".emitted-events",
        prefix + ".dead-letters");
  }

  private static void requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
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
