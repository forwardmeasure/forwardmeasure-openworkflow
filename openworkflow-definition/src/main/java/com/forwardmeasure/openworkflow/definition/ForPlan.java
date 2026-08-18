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
import java.util.Objects;

/**
 * Immutable compiled configuration for an Open Workflow {@code for} task.
 *
 * <p>The collection is either a definition-owned inline array or a runtime expression evaluated
 * once when the task starts.
 */
public record ForPlan(
    String itemVariable, String indexVariable, JsonNode collection, String whileCondition) {

  public ForPlan {
    itemVariable = requireText(itemVariable, "itemVariable");
    indexVariable = requireText(indexVariable, "indexVariable");
    if (itemVariable.equals(indexVariable)) {
      throw new IllegalArgumentException("itemVariable and indexVariable must differ");
    }
    Objects.requireNonNull(collection, "collection");
    if (!collection.isTextual() && !collection.isArray()) {
      throw new IllegalArgumentException("collection must be an expression or inline array");
    }
    collection = collection.deepCopy();
    if (whileCondition != null && whileCondition.isBlank()) {
      throw new IllegalArgumentException("whileCondition must not be blank");
    }
  }

  public boolean expressionCollection() {
    return collection.isTextual();
  }

  @Override
  public JsonNode collection() {
    return collection.deepCopy();
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
