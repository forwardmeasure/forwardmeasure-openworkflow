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
import java.util.Locale;
import java.util.Objects;

/** Immutable, transport-free plan for one normative Open Workflow run task. */
public record RunPlan(
    Kind kind,
    boolean await,
    ReturnMode returnMode,
    JsonNode configuration,
    WorkflowResourceReference resource,
    ResolvedSubflow subflow) {

  public RunPlan {
    Objects.requireNonNull(kind, "kind");
    Objects.requireNonNull(returnMode, "returnMode");
    Objects.requireNonNull(configuration, "configuration");
    if (!configuration.isObject()) {
      throw new IllegalArgumentException("Run configuration must be an object");
    }
    configuration = configuration.deepCopy();
    if ((kind == Kind.SCRIPT && configuration.has("source")) != (resource != null)) {
      throw new IllegalArgumentException("Only an external script carries a resolved resource");
    }
    if (resource != null && resource.kind() != WorkflowResourceKind.SCRIPT_SOURCE) {
      throw new IllegalArgumentException("External script resource must be SCRIPT_SOURCE");
    }
    if ((kind == Kind.WORKFLOW) != (subflow != null)) {
      throw new IllegalArgumentException("Only a workflow run carries a pinned subflow");
    }
    if (kind == Kind.WORKFLOW && resource != null) {
      throw new IllegalArgumentException("A workflow run cannot carry a script resource");
    }
  }

  public enum Kind {
    CONTAINER,
    SCRIPT,
    SHELL,
    WORKFLOW
  }

  /** Returns an isolated copy so admitted runner configuration remains immutable. */
  @Override
  public JsonNode configuration() {
    return configuration.deepCopy();
  }

  public enum ReturnMode {
    STDOUT,
    STDERR,
    CODE,
    ALL,
    NONE;

    public static ReturnMode parse(String value) {
      return valueOf(value.toUpperCase(Locale.ROOT));
    }
  }
}
