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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.forwardmeasure.openworkflow.definition.CallPlan;
import com.forwardmeasure.openworkflow.definition.PlanStep;
import com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource;
import com.forwardmeasure.openworkflow.definition.RunPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Deterministically materializes durable protocol-call and secured-run intents. */
public final class ProtocolOperationMaterializer {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

  private ProtocolOperationMaterializer() {}

  public static ProtocolOperationDescriptor materialize(
      WorkflowPlan plan,
      PlanStep step,
      JsonNode arguments,
      String operationId,
      AuthenticationExpressionContext authenticationContext) {
    return materialize(plan, step, arguments, operationId, authenticationContext, null);
  }

  /**
   * Decomposes one {@code com.forwardmeasure.openworkflow.correlated-worker} call into its command
   * (PUBLISH), events (SUBSCRIBE), and optional cancellation (PUBLISH) operations - each an
   * ordinary {@link ProtocolOperationDescriptor} that every AsyncAPI transport executor already
   * runs unchanged. {@code lifecycleId} is the durable identity the workflow uses to correlate the
   * three operations with each other and, injected into each outbound message payload's {@code
   * operationId}, with the external worker's own lifecycle. Each descriptor's own {@code
   * operationId} is suffixed ({@code :events}/{@code :cancel}) so the three can be dispatched and
   * recovered as independent durable operations.
   */
  public static CorrelatedWorkerOperations materializeCorrelatedWorker(
      WorkflowPlan plan,
      PlanStep step,
      JsonNode arguments,
      String lifecycleId,
      AuthenticationExpressionContext authenticationContext,
      java.time.Instant subscriptionDeadline) {
    CallPlan call = step.callPlan();
    if (call == null || call.kind() != CallPlan.Kind.CORRELATED_WORKER) {
      throw new IllegalArgumentException("Not a correlated-worker call at " + step.path());
    }
    JsonNode document = parse(resource(plan, call));
    ProtocolOperationDescriptor command =
        correlatedWorkerOperation(
            call,
            document,
            arguments.required("command"),
            lifecycleId,
            lifecycleId,
            authenticationContext,
            false,
            null);
    ProtocolOperationDescriptor events =
        correlatedWorkerOperation(
            call,
            document,
            arguments.required("events"),
            lifecycleId + ":events",
            lifecycleId,
            authenticationContext,
            true,
            subscriptionDeadline);
    ProtocolOperationDescriptor cancellation =
        arguments.hasNonNull("cancellation")
            ? correlatedWorkerOperation(
                call,
                document,
                arguments.required("cancellation"),
                lifecycleId + ":cancel",
                lifecycleId,
                authenticationContext,
                false,
                null)
            : null;
    return new CorrelatedWorkerOperations(command, events, cancellation);
  }

  /** The command/events/cancellation operations decomposed from one correlated-worker call. */
  public record CorrelatedWorkerOperations(
      ProtocolOperationDescriptor command,
      ProtocolOperationDescriptor events,
      ProtocolOperationDescriptor cancellation) {}

  private static ProtocolOperationDescriptor correlatedWorkerOperation(
      CallPlan call,
      JsonNode document,
      JsonNode block,
      String operationId,
      String correlationId,
      AuthenticationExpressionContext authenticationContext,
      boolean subscribe,
      java.time.Instant subscriptionDeadline) {
    String channel = block.required("channel").asText();
    String action = subscribe ? "subscribe" : "publish";
    JsonNode channelNode = document.path("channels").path(channel);
    JsonNode operation = channelNode.path(action);
    if (operation.isMissingNode()) {
      throw new IllegalArgumentException(
          "Pinned AsyncAPI channel has no " + action + " operation: " + channel);
    }
    String wanted = operation.path("operationId").asText(channel + "/" + action);
    LocatedServer server = asyncServer(document, operation);
    URI endpoint = asyncEndpoint(server.endpoint(), channelNode.path("address").asText(channel));
    JsonNode request =
        subscribe ? block.path("subscription") : correlatedWorkerMessage(block, correlationId);
    return new ProtocolOperationDescriptor(
        operationId,
        ProtocolOperationDescriptor.Kind.ASYNC_API,
        subscribe
            ? ProtocolOperationDescriptor.Mode.SUBSCRIBE
            : ProtocolOperationDescriptor.Mode.PUBLISH,
        call.resource(),
        server.protocol(),
        endpoint,
        wanted,
        request,
        subscribe ? call.asyncApiSubscription() : null,
        call.authentication(),
        runtimeAuthentication(call, authenticationContext),
        null,
        subscribe ? subscriptionDeadline : null);
  }

  private static ObjectNode correlatedWorkerMessage(JsonNode block, String correlationId) {
    JsonNode messageNode = block.path("message");
    if (!messageNode.isObject()) {
      throw new IllegalArgumentException(
          "correlated-worker command/cancellation requires a message");
    }
    ObjectNode message = (ObjectNode) messageNode.deepCopy();
    ObjectNode payload =
        message.has("payload") && message.get("payload").isObject()
            ? (ObjectNode) message.get("payload")
            : message.putObject("payload");
    payload.put("operationId", correlationId);
    return message;
  }

  public static ProtocolOperationDescriptor materialize(
      WorkflowPlan plan,
      PlanStep step,
      JsonNode arguments,
      String operationId,
      AuthenticationExpressionContext authenticationContext,
      java.time.Instant subscriptionDeadline) {
    CallPlan call = step.callPlan();
    if (step.runPlan() != null && step.runPlan().kind() != RunPlan.Kind.WORKFLOW) {
      return run(plan, step.runPlan(), arguments, operationId);
    }
    return switch (call.kind()) {
      case ASYNC_API ->
          asyncApi(
              call,
              resource(plan, call),
              arguments,
              operationId,
              authenticationContext,
              subscriptionDeadline);
      case GRPC ->
          grpc(plan, call, resource(plan, call), arguments, operationId, authenticationContext);
      case A2A -> a2a(plan, call, arguments, operationId, authenticationContext);
      case MCP -> mcp(call, arguments, operationId, authenticationContext);
      default ->
          throw new IllegalArgumentException("Not a durable protocol call at " + step.path());
    };
  }

  private static ProtocolOperationDescriptor run(
      WorkflowPlan plan, RunPlan run, JsonNode configuration, String operationId) {
    String protocol =
        switch (run.kind()) {
          case SHELL -> "run-shell";
          case SCRIPT -> "run-script";
          case CONTAINER -> "run-container";
          case WORKFLOW ->
              throw new IllegalArgumentException("A workflow run uses the subworkflow coordinator");
        };
    String operation =
        switch (run.kind()) {
          case SHELL -> configuration.required("command").asText();
          case SCRIPT -> configuration.required("language").asText();
          case CONTAINER -> configuration.required("image").asText();
          case WORKFLOW -> throw new IllegalStateException();
        };
    ObjectNode request = JsonNodeFactory.instance.objectNode();
    request.set("configuration", configuration.deepCopy());
    request.put("return", run.returnMode().name().toLowerCase(Locale.ROOT));
    ResolvedWorkflowResource source =
        run.resource() == null ? null : resource(plan, run.resource());
    return new ProtocolOperationDescriptor(
        operationId,
        ProtocolOperationDescriptor.Kind.RUN,
        run.await()
            ? ProtocolOperationDescriptor.Mode.RUN_AWAIT
            : ProtocolOperationDescriptor.Mode.RUN_DETACHED,
        run.resource(),
        protocol,
        URI.create("runner://local"),
        operation,
        request,
        null,
        null,
        null,
        source == null ? null : source.content());
  }

  private static ProtocolOperationDescriptor a2a(
      WorkflowPlan plan,
      CallPlan call,
      JsonNode arguments,
      String operationId,
      AuthenticationExpressionContext authenticationContext) {
    String method = arguments.required("method").asText();
    ResolvedWorkflowResource pinnedCard = call.resource() == null ? null : resource(plan, call);
    URI endpoint;
    if (arguments.has("server")) {
      endpoint = endpoint(arguments.required("server"));
    } else {
      if (call.resource() == null)
        throw new IllegalArgumentException("A2A call requires a server or agent card");
      JsonNode card = parse(pinnedCard);
      JsonNode supported = card.path("supportedInterfaces");
      String url = card.path("url").asText();
      if (url.isBlank() && supported.isArray() && !supported.isEmpty()) {
        url = supported.get(0).path("url").asText();
      }
      if (url.isBlank())
        throw new IllegalArgumentException("Pinned A2A agent card has no server URL");
      endpoint = URI.create(url);
    }
    boolean streaming = method.equals("message/stream") || method.equals("tasks/resubscribe");
    JsonNode parameters = a2aParameters(method, arguments.path("parameters"), operationId);
    return new ProtocolOperationDescriptor(
        operationId,
        ProtocolOperationDescriptor.Kind.A2A,
        streaming
            ? ProtocolOperationDescriptor.Mode.RPC_STREAM
            : ProtocolOperationDescriptor.Mode.RPC_UNARY,
        call.resource(),
        "a2a-jsonrpc",
        endpoint,
        method,
        parameters,
        null,
        call.authentication(),
        runtimeAuthentication(call, authenticationContext),
        pinnedCard == null ? null : pinnedCard.content());
  }

  private static JsonNode a2aParameters(String method, JsonNode configured, String operationId) {
    if (!method.equals("message/send") && !method.equals("message/stream")) {
      return configured.deepCopy();
    }
    if (!configured.isObject()) {
      throw new IllegalArgumentException(method + " parameters must be an object");
    }
    ObjectNode parameters = (ObjectNode) configured.deepCopy();
    JsonNode candidate = parameters.get("message");
    if (candidate == null || !candidate.isObject()) {
      throw new IllegalArgumentException(method + " parameters.message must be an object");
    }
    ObjectNode message = (ObjectNode) candidate;
    if (message.path("messageId").asText().isBlank()) {
      message.put(
          "messageId",
          java.util
              .UUID
              .nameUUIDFromBytes((operationId + ":a2a-message").getBytes(StandardCharsets.UTF_8))
              .toString());
    }
    if (message.path("role").asText().isBlank()) {
      message.put("role", "user");
    }
    return parameters;
  }

  private static ProtocolOperationDescriptor mcp(
      CallPlan call,
      JsonNode arguments,
      String operationId,
      AuthenticationExpressionContext authenticationContext) {
    JsonNode transport = arguments.required("transport");
    boolean http = transport.path("http").isObject();
    boolean stdio = transport.path("stdio").isObject();
    if (http == stdio)
      throw new IllegalArgumentException("MCP call requires exactly one HTTP or stdio transport");
    URI endpoint =
        http
            ? endpoint(transport.required("http").required("endpoint"))
            : URI.create("stdio://local");
    return new ProtocolOperationDescriptor(
        operationId,
        ProtocolOperationDescriptor.Kind.MCP,
        ProtocolOperationDescriptor.Mode.RPC_UNARY,
        null,
        http ? "mcp-http" : "mcp-stdio",
        endpoint,
        arguments.required("method").asText(),
        arguments,
        null,
        call.authentication(),
        runtimeAuthentication(call, authenticationContext));
  }

  private static URI endpoint(JsonNode configured) {
    String value =
        configured.isObject() ? configured.required("uri").asText() : configured.asText();
    return URI.create(value);
  }

  private static ProtocolOperationDescriptor asyncApi(
      CallPlan call,
      ResolvedWorkflowResource resource,
      JsonNode arguments,
      String operationId,
      AuthenticationExpressionContext authenticationContext,
      java.time.Instant subscriptionDeadline) {
    JsonNode document = parse(resource);
    boolean subscribe = call.asyncApiSubscription() != null;
    String wanted;
    LocatedAsyncOperation located;
    if (arguments.hasNonNull("operation")) {
      wanted = arguments.required("operation").asText();
      located = locateAsyncOperation(document, wanted);
    } else {
      String channel = arguments.required("channel").asText();
      String action = subscribe ? "subscribe" : "publish";
      JsonNode operation = document.path("channels").path(channel).path(action);
      if (operation.isMissingNode()) {
        throw new IllegalArgumentException(
            "Pinned AsyncAPI channel has no " + action + " operation: " + channel);
      }
      wanted = operation.path("operationId").asText(channel + "/" + action);
      located = new LocatedAsyncOperation(channel + "/" + action, operation, channel);
    }
    LocatedServer server = asyncServer(document, located.operation());
    URI endpoint = asyncEndpoint(server.endpoint(), channelAddress(document, located));
    String action = located.operation().path("action").asText();
    if (!action.isBlank() && !action.equals(subscribe ? "receive" : "send")) {
      throw new IllegalArgumentException(
          "AsyncAPI operation action does not match call mode: " + wanted);
    }
    JsonNode request = subscribe ? arguments.path("subscription") : arguments.path("message");
    return new ProtocolOperationDescriptor(
        operationId,
        ProtocolOperationDescriptor.Kind.ASYNC_API,
        subscribe
            ? ProtocolOperationDescriptor.Mode.SUBSCRIBE
            : ProtocolOperationDescriptor.Mode.PUBLISH,
        call.resource(),
        server.protocol(),
        endpoint,
        wanted,
        request,
        call.asyncApiSubscription(),
        call.authentication(),
        runtimeAuthentication(call, authenticationContext),
        null,
        subscriptionDeadline);
  }

  private static ProtocolOperationDescriptor grpc(
      WorkflowPlan plan,
      CallPlan call,
      ResolvedWorkflowResource resource,
      JsonNode arguments,
      String operationId,
      AuthenticationExpressionContext authenticationContext) {
    JsonNode service = arguments.required("service");
    String serviceName = service.required("name").asText();
    String method = arguments.required("method").asText();
    String host = service.required("host").asText();
    int port = service.path("port").asInt(443);
    boolean tls = service.path("tls").asBoolean(port == 443);
    String protocol = tls ? "grpcs" : "grpc";
    URI endpoint = URI.create(protocol + "://" + host + ":" + port);
    ProtocolOperationDescriptor.Mode mode = grpcMode(resource.content(), serviceName, method);
    return new ProtocolOperationDescriptor(
        operationId,
        ProtocolOperationDescriptor.Kind.GRPC,
        mode,
        call.resource(),
        protocol,
        endpoint,
        serviceName + "/" + method,
        arguments.path("arguments"),
        null,
        call.authentication(),
        runtimeAuthentication(call, authenticationContext),
        resource.content(),
        null,
        protoDependencies(plan, resource));
  }

  private static Map<String, String> protoDependencies(
      WorkflowPlan plan, ResolvedWorkflowResource root) {
    var resources =
        plan.resources().stream()
            .collect(
                java.util.stream.Collectors.toMap(
                    ResolvedWorkflowResource::uri,
                    java.util.function.Function.identity(),
                    (left, right) -> left));
    var dependencies = new java.util.LinkedHashMap<String, String>();
    collectProtoDependencies(root.uri(), root.content(), resources, dependencies);
    return Map.copyOf(dependencies);
  }

  private static void collectProtoDependencies(
      URI sourceUri,
      String source,
      Map<URI, ResolvedWorkflowResource> resources,
      Map<String, String> dependencies) {
    Matcher imports =
        Pattern.compile("(?m)^\\s*import\\s+(?:public\\s+|weak\\s+)?\"([^\"]+)\"\\s*;")
            .matcher(source);
    while (imports.find()) {
      String path = imports.group(1);
      if (dependencies.containsKey(path)) continue;
      URI resolvedUri = sourceUri.resolve(path);
      ResolvedWorkflowResource dependency = resources.get(resolvedUri);
      if (dependency == null)
        throw new IllegalArgumentException("Pinned proto import was not resolved: " + path);
      dependencies.put(path, dependency.content());
      collectProtoDependencies(resolvedUri, dependency.content(), resources, dependencies);
    }
  }

  private static AuthenticationExpressionContext runtimeAuthentication(
      CallPlan call, AuthenticationExpressionContext authenticationContext) {
    return call.authentication() != null && !call.authentication().secretBacked()
        ? authenticationContext
        : null;
  }

  private static ProtocolOperationDescriptor.Mode grpcMode(
      String proto, String serviceName, String method) {
    String simpleService = serviceName.substring(serviceName.lastIndexOf('.') + 1);
    Matcher serviceMatcher =
        Pattern.compile("(?s)\\bservice\\s+" + Pattern.quote(simpleService) + "\\s*\\{(.*?)\\}")
            .matcher(proto);
    if (!serviceMatcher.find()) {
      throw new IllegalArgumentException("Pinned proto service was not found: " + serviceName);
    }
    Matcher rpc =
        Pattern.compile(
                "(?s)\\brpc\\s+"
                    + Pattern.quote(method)
                    + "\\s*\\(\\s*(stream\\s+)?[^)]+\\)\\s*returns\\s*"
                    + "\\(\\s*(stream\\s+)?[^)]+\\)")
            .matcher(serviceMatcher.group(1));
    if (!rpc.find()) {
      throw new IllegalArgumentException(
          "Pinned proto method was not found: " + serviceName + "/" + method);
    }
    boolean client = rpc.group(1) != null;
    boolean server = rpc.group(2) != null;
    if (client && server) return ProtocolOperationDescriptor.Mode.GRPC_BIDI_STREAM;
    if (client) return ProtocolOperationDescriptor.Mode.GRPC_CLIENT_STREAM;
    if (server) return ProtocolOperationDescriptor.Mode.GRPC_SERVER_STREAM;
    return ProtocolOperationDescriptor.Mode.GRPC_UNARY;
  }

  private static LocatedAsyncOperation locateAsyncOperation(JsonNode document, String wanted) {
    JsonNode operations = document.path("operations");
    JsonNode direct = operations.get(wanted);
    if (direct != null)
      return new LocatedAsyncOperation(
          wanted, direct, referenceName(direct.path("channel").path("$ref").asText()));
    LocatedAsyncOperation found = null;
    for (Iterator<Map.Entry<String, JsonNode>> entries = operations.properties().iterator();
        entries.hasNext(); ) {
      Map.Entry<String, JsonNode> entry = entries.next();
      if (wanted.equals(entry.getValue().path("operationId").asText())) {
        if (found != null)
          throw new IllegalArgumentException("AsyncAPI operation is ambiguous: " + wanted);
        found =
            new LocatedAsyncOperation(
                entry.getKey(),
                entry.getValue(),
                referenceName(entry.getValue().path("channel").path("$ref").asText()));
      }
    }
    if (found != null) return found;

    // AsyncAPI 2.x operations live below channels.
    for (Iterator<Map.Entry<String, JsonNode>> channels =
            document.path("channels").properties().iterator();
        channels.hasNext(); ) {
      Map.Entry<String, JsonNode> channel = channels.next();
      for (String action : java.util.List.of("publish", "subscribe")) {
        JsonNode candidate = channel.getValue().path(action);
        if (wanted.equals(candidate.path("operationId").asText())) {
          if (found != null)
            throw new IllegalArgumentException("AsyncAPI operation is ambiguous: " + wanted);
          found =
              new LocatedAsyncOperation(
                  channel.getKey() + "/" + action, candidate, channel.getKey());
        }
      }
    }
    if (found == null)
      throw new IllegalArgumentException("Pinned AsyncAPI operation was not found: " + wanted);
    return found;
  }

  private static LocatedServer asyncServer(JsonNode document, JsonNode operation) {
    String serverName = referenceName(operation.path("server").path("$ref").asText());
    if (serverName == null
        && operation.path("servers").isArray()
        && !operation.path("servers").isEmpty()) {
      JsonNode first = operation.path("servers").get(0);
      serverName = first.isTextual() ? first.asText() : referenceName(first.path("$ref").asText());
    }
    JsonNode servers = document.path("servers");
    JsonNode server = serverName == null ? firstValue(servers) : servers.path(serverName);
    if (server.isMissingNode() || server.isNull())
      throw new IllegalArgumentException("AsyncAPI document has no usable server");
    String protocol = server.path("protocol").asText();
    String address = server.path("url").asText(server.path("host").asText());
    if (protocol.isBlank() && address.contains(":")) {
      protocol = URI.create(address).getScheme();
    }
    if (protocol == null || protocol.isBlank())
      throw new IllegalArgumentException("AsyncAPI server has no protocol");
    if (!address.contains("://")) address = protocol + "://" + address;
    return new LocatedServer(protocol.toLowerCase(Locale.ROOT), URI.create(address));
  }

  private static String channelAddress(JsonNode document, LocatedAsyncOperation operation) {
    if (operation.channel() == null) return "";
    JsonNode channel = document.path("channels").path(operation.channel());
    return channel.path("address").asText(operation.channel());
  }

  private static URI asyncEndpoint(URI server, String channel) {
    if (channel == null || channel.isBlank()) return server;
    String base = server.toString();
    if (!base.endsWith("/")) base += "/";
    String relative = channel.startsWith("/") ? channel.substring(1) : channel;
    return URI.create(base + relative.replace(" ", "%20"));
  }

  private static JsonNode firstValue(JsonNode object) {
    Iterator<JsonNode> values = object.elements();
    return values.hasNext()
        ? values.next()
        : com.fasterxml.jackson.databind.node.MissingNode.getInstance();
  }

  private static String referenceName(String reference) {
    if (reference == null || reference.isBlank()) return null;
    int slash = reference.lastIndexOf('/');
    return slash < 0 ? reference : reference.substring(slash + 1);
  }

  private static JsonNode parse(ResolvedWorkflowResource resource) {
    try {
      return (resource.mediaType().toLowerCase(Locale.ROOT).contains("yaml") ? YAML : JSON)
          .readTree(resource.content());
    } catch (Exception failure) {
      throw new IllegalArgumentException("Pinned protocol document cannot be parsed", failure);
    }
  }

  private static ResolvedWorkflowResource resource(WorkflowPlan plan, CallPlan call) {
    return resource(plan, call.resource());
  }

  private static ResolvedWorkflowResource resource(
      WorkflowPlan plan,
      com.forwardmeasure.openworkflow.definition.WorkflowResourceReference reference) {
    return plan.resources().stream()
        .filter(
            candidate ->
                candidate.uri().equals(reference.uri())
                    && candidate.sha256().equals(reference.sha256()))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Pinned protocol document is absent from the executable plan"));
  }

  private record LocatedAsyncOperation(String key, JsonNode operation, String channel) {}

  private record LocatedServer(String protocol, URI endpoint) {}
}
