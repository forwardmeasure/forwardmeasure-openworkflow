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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Objects;

/** Immutable standard call-task variant and its unevaluated arguments. */
public record CallPlan(
    Kind kind,
    String functionName,
    WorkflowResourceReference resource,
    JsonNode arguments,
    AsyncApiSubscriptionPlan asyncApiSubscription,
    AuthenticationPlan authentication) {

  /**
   * Reserved custom-function name for the governed human-task extension.
   *
   * <p>The upstream 1.0.3 schema deliberately permits implementation-defined function names. Using
   * that standards-defined extension point keeps human work in an otherwise valid OpenWorkflow
   * document without changing or forking the upstream schema.
   *
   * <p>Not a "legacy-compatible" name carried over from anywhere - {@code oks} (the {@code
   * openworkflow-kafka-streams} source repo this compiler was originally consolidated from, see
   * {@code docs/source-provenance.md}) was never itself deployed, so there is no real document
   * anywhere depending on that branding to keep reading. Named to match this product's own {@code
   * com.forwardmeasure.openworkflow} package convention instead.
   */
  public static final String HUMAN_TASK_FUNCTION = "com.forwardmeasure.openworkflow.human-task";

  /**
   * Reserved custom-function name for one durable request/progress/result lifecycle implemented
   * over AsyncAPI operations.
   *
   * <p>The function remains a schema-valid Open Workflow custom call while giving the runtime one
   * authoritative cancellation and recovery boundary instead of asking authors to coordinate an
   * independent send task and receive task.
   */
  public static final String CORRELATED_WORKER_FUNCTION =
      "com.forwardmeasure.openworkflow.correlated-worker";

  public CallPlan(
      Kind kind, String functionName, WorkflowResourceReference resource, JsonNode arguments) {
    this(kind, functionName, resource, arguments, null, null);
  }

  public CallPlan(
      Kind kind,
      String functionName,
      WorkflowResourceReference resource,
      JsonNode arguments,
      AsyncApiSubscriptionPlan asyncApiSubscription) {
    this(kind, functionName, resource, arguments, asyncApiSubscription, null);
  }

  public enum Kind {
    ASYNC_API,
    GRPC,
    HTTP,
    OPEN_API,
    A2A,
    MCP,
    HUMAN_TASK,
    CORRELATED_WORKER,
    FUNCTION
  }

  public CallPlan {
    Objects.requireNonNull(kind, "kind");
    if (kind == Kind.FUNCTION) {
      if (functionName == null || functionName.isBlank()) {
        throw new IllegalArgumentException("A function call requires its catalogue name");
      }
    } else if (functionName != null) {
      throw new IllegalArgumentException("Only a function call carries a function name");
    }
    boolean requiresResource =
        kind == Kind.ASYNC_API
            || kind == Kind.CORRELATED_WORKER
            || kind == Kind.GRPC
            || kind == Kind.OPEN_API;
    if (requiresResource && resource == null) {
      throw new IllegalArgumentException(kind + " requires a resolved external resource");
    }
    if ((kind == Kind.HTTP || kind == Kind.MCP || kind == Kind.HUMAN_TASK) && resource != null) {
      throw new IllegalArgumentException(kind + " does not use an external call document");
    }
    if (kind == Kind.FUNCTION
        && resource != null
        && resource.kind() != WorkflowResourceKind.FUNCTION_DEFINITION) {
      throw new IllegalArgumentException(
          "A catalogued function resource must be a function " + "definition");
    }
    if (kind == Kind.A2A
        && resource != null
        && resource.kind() != WorkflowResourceKind.A2A_AGENT_CARD) {
      throw new IllegalArgumentException("A2A resource must be an agent card");
    }
    arguments =
        arguments == null || arguments.isNull()
            ? JsonNodeFactory.instance.objectNode()
            : arguments.deepCopy();
    if (!arguments.isObject()) {
      throw new IllegalArgumentException("Call arguments must be an object");
    }
    boolean declaresSubscription = arguments.path("subscription").isObject();
    boolean requiresSubscriptionPlan =
        kind == Kind.ASYNC_API && declaresSubscription || kind == Kind.CORRELATED_WORKER;
    if ((asyncApiSubscription != null) != requiresSubscriptionPlan) {
      throw new IllegalArgumentException(
          "A compiled AsyncAPI subscription plan must exactly match "
              + "an AsyncAPI subscription or correlated-worker "
              + "call");
    }
    if (kind == Kind.FUNCTION && authentication != null) {
      throw new IllegalArgumentException("Reusable functions cannot carry adapter authentication");
    }
  }

  /** Returns an isolated copy so a compiled operation cannot be mutated. */
  @Override
  public JsonNode arguments() {
    return arguments.deepCopy();
  }
}
