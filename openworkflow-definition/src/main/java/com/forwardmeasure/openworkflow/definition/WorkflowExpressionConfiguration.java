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

import com.forwardmeasure.openworkflow.expression.ExpressionMode;
import java.util.Objects;

/** Expression language and recognition mode pinned into a compiled plan. */
public record WorkflowExpressionConfiguration(String language, ExpressionMode mode) {

  public WorkflowExpressionConfiguration {
    Objects.requireNonNull(language, "language");
    Objects.requireNonNull(mode, "mode");
    if (!"jq".equals(language)) {
      throw new IllegalArgumentException("Only the mandatory jq expression language is supported");
    }
  }

  public static WorkflowExpressionConfiguration defaults() {
    return new WorkflowExpressionConfiguration("jq", ExpressionMode.STRICT);
  }
}
