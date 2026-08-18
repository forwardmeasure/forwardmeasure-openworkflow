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
package com.forwardmeasure.openworkflow.engine.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.AsyncApiSubscriptionPlan;
import com.forwardmeasure.openworkflow.definition.AuthenticationPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceKind;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceReference;
import java.net.URI;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Credential-free durable intent for AsyncAPI, gRPC, A2A, or MCP transport work. */
public record ProtocolOperationDescriptor(
    String operationId,
    Kind kind,
    Mode mode,
    WorkflowResourceReference document,
    String protocol,
    URI endpoint,
    String operation,
    JsonNode request,
    AsyncApiSubscriptionPlan subscription,
    AuthenticationPlan authentication,
    AuthenticationExpressionContext authenticationContext,
    @JsonInclude(JsonInclude.Include.NON_EMPTY) String protocolSchema,
    @JsonInclude(JsonInclude.Include.NON_NULL) Instant subscriptionDeadline,
    @JsonInclude(JsonInclude.Include.NON_EMPTY) Map<String, String> protocolDependencies,
    @JsonInclude(JsonInclude.Include.NON_NULL) ActorIdentity requestedBy) {

  public ProtocolOperationDescriptor(
      String operationId,
      Kind kind,
      Mode mode,
      WorkflowResourceReference document,
      String protocol,
      URI endpoint,
      String operation,
      JsonNode request,
      AsyncApiSubscriptionPlan subscription,
      AuthenticationPlan authentication,
      AuthenticationExpressionContext authenticationContext,
      String protocolSchema,
      Instant subscriptionDeadline,
      Map<String, String> protocolDependencies) {
    this(
        operationId,
        kind,
        mode,
        document,
        protocol,
        endpoint,
        operation,
        request,
        subscription,
        authentication,
        authenticationContext,
        protocolSchema,
        subscriptionDeadline,
        protocolDependencies,
        null);
  }

  public ProtocolOperationDescriptor(
      String operationId,
      Kind kind,
      Mode mode,
      WorkflowResourceReference document,
      String protocol,
      URI endpoint,
      String operation,
      JsonNode request,
      AsyncApiSubscriptionPlan subscription,
      AuthenticationPlan authentication,
      AuthenticationExpressionContext authenticationContext) {
    this(
        operationId,
        kind,
        mode,
        document,
        protocol,
        endpoint,
        operation,
        request,
        subscription,
        authentication,
        authenticationContext,
        null,
        null,
        Map.of(),
        null);
  }

  public ProtocolOperationDescriptor(
      String operationId,
      Kind kind,
      Mode mode,
      WorkflowResourceReference document,
      String protocol,
      URI endpoint,
      String operation,
      JsonNode request,
      AsyncApiSubscriptionPlan subscription,
      AuthenticationPlan authentication,
      AuthenticationExpressionContext authenticationContext,
      String protocolSchema) {
    this(
        operationId,
        kind,
        mode,
        document,
        protocol,
        endpoint,
        operation,
        request,
        subscription,
        authentication,
        authenticationContext,
        protocolSchema,
        null,
        Map.of(),
        null);
  }

  public ProtocolOperationDescriptor(
      String operationId,
      Kind kind,
      Mode mode,
      WorkflowResourceReference document,
      String protocol,
      URI endpoint,
      String operation,
      JsonNode request,
      AsyncApiSubscriptionPlan subscription,
      AuthenticationPlan authentication,
      AuthenticationExpressionContext authenticationContext,
      String protocolSchema,
      Instant subscriptionDeadline) {
    this(
        operationId,
        kind,
        mode,
        document,
        protocol,
        endpoint,
        operation,
        request,
        subscription,
        authentication,
        authenticationContext,
        protocolSchema,
        subscriptionDeadline,
        Map.of(),
        null);
  }

  public enum Kind {
    ASYNC_API,
    GRPC,
    A2A,
    MCP,
    RUN
  }

  public enum Mode {
    PUBLISH,
    SUBSCRIBE,
    GRPC_UNARY,
    GRPC_SERVER_STREAM,
    GRPC_CLIENT_STREAM,
    GRPC_BIDI_STREAM,
    RPC_UNARY,
    RPC_STREAM,
    RUN_AWAIT,
    RUN_DETACHED
  }

  public ProtocolOperationDescriptor {
    operationId = requireText(operationId, "operationId");
    Objects.requireNonNull(kind, "kind");
    Objects.requireNonNull(mode, "mode");
    protocol = requireText(protocol, "protocol").toLowerCase(Locale.ROOT);
    endpoint = Objects.requireNonNull(endpoint, "endpoint").normalize();
    if (!endpoint.isAbsolute()) {
      throw new IllegalArgumentException("Protocol endpoint must be absolute");
    }
    operation = requireText(operation, "operation");
    request = request == null ? JsonNodeFactory.instance.objectNode() : request.deepCopy();
    protocolDependencies =
        protocolDependencies == null ? Map.of() : Map.copyOf(protocolDependencies);
    if (kind == Kind.ASYNC_API) {
      Objects.requireNonNull(document, "document");
      if (document.kind() != WorkflowResourceKind.ASYNC_API_DOCUMENT) {
        throw new IllegalArgumentException("AsyncAPI intent requires an AsyncAPI document");
      }
      if ((mode == Mode.SUBSCRIBE) != (subscription != null)) {
        throw new IllegalArgumentException(
            "Only an AsyncAPI subscription carries its consumption plan");
      }
      if (mode != Mode.PUBLISH && mode != Mode.SUBSCRIBE) {
        throw new IllegalArgumentException("Invalid AsyncAPI operation mode");
      }
      if (protocolSchema != null)
        throw new IllegalArgumentException("AsyncAPI intent does not carry a protobuf schema");
      if (!protocolDependencies.isEmpty())
        throw new IllegalArgumentException("AsyncAPI intent does not carry protobuf dependencies");
      if (mode != Mode.SUBSCRIBE && subscriptionDeadline != null) {
        throw new IllegalArgumentException("Only an AsyncAPI subscription carries a deadline");
      }
    } else if (kind == Kind.GRPC) {
      Objects.requireNonNull(document, "document");
      if (document.kind() != WorkflowResourceKind.GRPC_PROTO) {
        throw new IllegalArgumentException("gRPC intent requires a pinned proto resource");
      }
      if (mode == Mode.PUBLISH || mode == Mode.SUBSCRIBE || subscription != null) {
        throw new IllegalArgumentException("Invalid gRPC operation mode");
      }
      protocolSchema = requireText(protocolSchema, "protocolSchema");
      if (subscriptionDeadline != null)
        throw new IllegalArgumentException(
            "gRPC intent does not carry an AsyncAPI subscription deadline");
    } else if (kind == Kind.A2A || kind == Kind.MCP) {
      if (kind == Kind.A2A
          && document != null
          && document.kind() != WorkflowResourceKind.A2A_AGENT_CARD) {
        throw new IllegalArgumentException("A2A intent document must be a pinned agent card");
      }
      if (kind == Kind.MCP && document != null) {
        throw new IllegalArgumentException("MCP intent does not carry an external call document");
      }
      if (kind == Kind.A2A && (document == null) != (protocolSchema == null)) {
        throw new IllegalArgumentException(
            "An AgentCard call carries both its reference and pinned content");
      }
      if (mode != Mode.RPC_UNARY && mode != Mode.RPC_STREAM) {
        throw new IllegalArgumentException("A2A/MCP intent requires an RPC mode");
      }
      if (kind == Kind.MCP && mode != Mode.RPC_UNARY) {
        throw new IllegalArgumentException("Open Workflow MCP calls are unary method calls");
      }
      if (subscription != null
          || subscriptionDeadline != null
          || (kind == Kind.MCP && protocolSchema != null)
          || !protocolDependencies.isEmpty()) {
        throw new IllegalArgumentException(
            "A2A/MCP intent carries no AsyncAPI or protobuf metadata");
      }
    } else {
      if (mode != Mode.RUN_AWAIT && mode != Mode.RUN_DETACHED) {
        throw new IllegalArgumentException("Run intent requires an awaited or detached mode");
      }
      if (document != null && document.kind() != WorkflowResourceKind.SCRIPT_SOURCE) {
        throw new IllegalArgumentException("Run intent document must be a pinned script source");
      }
      if ((document != null) != (protocolSchema != null)) {
        throw new IllegalArgumentException(
            "An external run script carries both its reference and pinned source");
      }
      if (subscription != null || subscriptionDeadline != null || !protocolDependencies.isEmpty()) {
        throw new IllegalArgumentException(
            "Run intent carries no subscription or protobuf metadata");
      }
      if (authentication != null || authenticationContext != null) {
        throw new IllegalArgumentException(
            "Run credentials belong to runner policy, not workflow state");
      }
    }
    if (authentication != null && !authentication.secretBacked() && authenticationContext == null) {
      throw new IllegalArgumentException(
          "Expression-backed authentication requires a runtime context");
    }
    if ((authentication == null || authentication.secretBacked())
        && authenticationContext != null) {
      throw new IllegalArgumentException(
          "Only expression-backed authentication can carry a runtime context");
    }
  }

  @Override
  public JsonNode request() {
    return request.deepCopy();
  }

  public ProtocolOperationDescriptor requestedBy(ActorIdentity actor) {
    return new ProtocolOperationDescriptor(
        operationId,
        kind,
        mode,
        document,
        protocol,
        endpoint,
        operation,
        request,
        subscription,
        authentication,
        authenticationContext,
        protocolSchema,
        subscriptionDeadline,
        protocolDependencies,
        Objects.requireNonNull(actor));
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
    return value;
  }
}
