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
package com.forwardmeasure.openworkflow.adapter.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.operation.AsyncApiHttpOperationExecutor;
import com.forwardmeasure.openworkflow.operation.AsyncApiWebSocketOperationExecutor;
import com.forwardmeasure.openworkflow.operation.AuthorizedProtocolOperationExecutor;
import com.forwardmeasure.openworkflow.operation.OperationAdapterConfiguration;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor;
import com.forwardmeasure.openworkflow.operation.RoutingProtocolOperationExecutor;
import com.forwardmeasure.openworkflow.operation.agent.JsonRpcHttpOperationExecutor;
import com.forwardmeasure.openworkflow.operation.agent.McpStdioCommandPolicy;
import com.forwardmeasure.openworkflow.operation.agent.McpStdioOperationExecutor;
import com.forwardmeasure.openworkflow.operation.amqp.AsyncApiAmqpOperationExecutor;
import com.forwardmeasure.openworkflow.operation.cloud.AsyncApiAnypointMqOperationExecutor;
import com.forwardmeasure.openworkflow.operation.cloud.AsyncApiCloudOperationExecutor;
import com.forwardmeasure.openworkflow.operation.grpc.DynamicGrpcOperationExecutor;
import com.forwardmeasure.openworkflow.operation.jms.AsyncApiJmsOperationExecutor;
import com.forwardmeasure.openworkflow.operation.kafka.AsyncApiKafkaOperationExecutor;
import com.forwardmeasure.openworkflow.operation.mqtt.AsyncApiMqttOperationExecutor;
import com.forwardmeasure.openworkflow.operation.nats.AsyncApiNatsOperationExecutor;
import com.forwardmeasure.openworkflow.operation.pulsar.AsyncApiPulsarOperationExecutor;
import com.forwardmeasure.openworkflow.operation.redis.AsyncApiRedisOperationExecutor;
import com.forwardmeasure.openworkflow.operation.runner.LocalProcessOperationExecutor;
import com.forwardmeasure.openworkflow.operation.runner.OciContainerOperationExecutor;
import com.forwardmeasure.openworkflow.operation.runner.RunPolicyConfiguration;
import com.forwardmeasure.openworkflow.operation.stomp.AsyncApiStompOperationExecutor;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Builds the identical complete protocol routing table for every Kafka host binding. */
public final class KafkaProtocolOperationExecutors {
  private KafkaProtocolOperationExecutors() {}

  public static ProtocolOperationExecutor create(
      AuthorizationService authorization, ObjectMapper json, Configuration configuration) {
    Objects.requireNonNull(configuration, "configuration");
    Duration timeout = Duration.ofMillis(configuration.timeoutMillis());
    var egress = OperationAdapterConfiguration.egressPolicy(configuration.httpEgressAllowlist());
    var secrets = OperationAdapterConfiguration.secretProvider(configuration.secretDirectory());
    Map<RoutingProtocolOperationExecutor.DriverKey, ProtocolOperationExecutor> drivers =
        new LinkedHashMap<>();
    var grpc = new DynamicGrpcOperationExecutor(timeout, egress, secrets);
    var asyncHttp =
        new AsyncApiHttpOperationExecutor(
            OperationAdapterConfiguration.executor(
                json,
                timeout.toMillis(),
                configuration.httpEgressAllowlist(),
                configuration.secretDirectory()),
            timeout,
            egress,
            secrets);
    var kafka = new AsyncApiKafkaOperationExecutor(Duration.ofMillis(500), egress, secrets);
    var nats = new AsyncApiNatsOperationExecutor(timeout, egress, secrets);
    var webSocket = new AsyncApiWebSocketOperationExecutor(timeout, egress, secrets);
    var amqp = new AsyncApiAmqpOperationExecutor(timeout, egress, secrets);
    var cloud = new AsyncApiCloudOperationExecutor(timeout, egress, secrets);
    var anypoint = new AsyncApiAnypointMqOperationExecutor(timeout, egress, secrets);
    var jms = new AsyncApiJmsOperationExecutor(timeout, egress, secrets);
    var mqtt = new AsyncApiMqttOperationExecutor(timeout, egress, secrets);
    var pulsar = new AsyncApiPulsarOperationExecutor(timeout, egress, secrets);
    var redis = new AsyncApiRedisOperationExecutor(timeout, egress, secrets);
    var stomp = new AsyncApiStompOperationExecutor(timeout, egress, secrets);
    var agent = new JsonRpcHttpOperationExecutor(timeout, egress, secrets);
    var mcp =
        new McpStdioOperationExecutor(
            timeout,
            McpStdioCommandPolicy.configured(configuration.mcpCommandAllowlist()),
            secrets);
    var runPolicy =
        RunPolicyConfiguration.policy(
            configuration.runCommandAllowlist(),
            configuration.runInterpreterAllowlist(),
            configuration.runImageAllowlist(),
            configuration.runVolumeAllowlist(),
            configuration.runPortAllowlist());
    var localRun = new LocalProcessOperationExecutor(timeout, 1_048_576, runPolicy);
    var containerRun =
        new OciContainerOperationExecutor(
            configuration.ociRuntime(), timeout, 1_048_576, runPolicy);

    put(drivers, ProtocolOperationDescriptor.Kind.GRPC, List.of("grpc", "grpcs"), grpc);
    put(
        drivers,
        ProtocolOperationDescriptor.Kind.ASYNC_API,
        List.of("http", "https", "mercure"),
        asyncHttp);
    put(
        drivers,
        ProtocolOperationDescriptor.Kind.ASYNC_API,
        List.of("kafka", "kafka-secure"),
        kafka);
    put(drivers, ProtocolOperationDescriptor.Kind.ASYNC_API, List.of("nats"), nats);
    put(drivers, ProtocolOperationDescriptor.Kind.ASYNC_API, List.of("ws"), webSocket);
    put(drivers, ProtocolOperationDescriptor.Kind.ASYNC_API, List.of("amqp", "amqp1"), amqp);
    put(
        drivers,
        ProtocolOperationDescriptor.Kind.ASYNC_API,
        List.of("googlepubsub", "sns", "sqs"),
        cloud);
    put(drivers, ProtocolOperationDescriptor.Kind.ASYNC_API, List.of("anypointmq"), anypoint);
    put(
        drivers,
        ProtocolOperationDescriptor.Kind.ASYNC_API,
        List.of("jms", "ibmmq", "solace"),
        jms);
    put(drivers, ProtocolOperationDescriptor.Kind.ASYNC_API, List.of("mqtt", "mqtt5"), mqtt);
    put(drivers, ProtocolOperationDescriptor.Kind.ASYNC_API, List.of("pulsar"), pulsar);
    put(drivers, ProtocolOperationDescriptor.Kind.ASYNC_API, List.of("redis"), redis);
    put(drivers, ProtocolOperationDescriptor.Kind.ASYNC_API, List.of("stomp"), stomp);
    put(drivers, ProtocolOperationDescriptor.Kind.A2A, List.of("a2a-jsonrpc"), agent);
    put(drivers, ProtocolOperationDescriptor.Kind.MCP, List.of("mcp-http"), agent);
    put(drivers, ProtocolOperationDescriptor.Kind.MCP, List.of("mcp-stdio"), mcp);
    put(
        drivers,
        ProtocolOperationDescriptor.Kind.RUN,
        List.of("run-shell", "run-script"),
        localRun);
    put(drivers, ProtocolOperationDescriptor.Kind.RUN, List.of("run-container"), containerRun);
    return new AuthorizedProtocolOperationExecutor(
        authorization, new RoutingProtocolOperationExecutor(drivers));
  }

  private static void put(
      Map<RoutingProtocolOperationExecutor.DriverKey, ProtocolOperationExecutor> drivers,
      ProtocolOperationDescriptor.Kind kind,
      List<String> protocols,
      ProtocolOperationExecutor executor) {
    protocols.forEach(
        protocol ->
            drivers.put(new RoutingProtocolOperationExecutor.DriverKey(kind, protocol), executor));
  }

  public record Configuration(
      long timeoutMillis,
      String httpEgressAllowlist,
      String secretDirectory,
      String mcpCommandAllowlist,
      String runCommandAllowlist,
      String runInterpreterAllowlist,
      String runImageAllowlist,
      String runVolumeAllowlist,
      String runPortAllowlist,
      String ociRuntime) {
    public Configuration {
      if (timeoutMillis < 1) throw new IllegalArgumentException("timeoutMillis must be positive");
      httpEgressAllowlist = value(httpEgressAllowlist);
      secretDirectory = value(secretDirectory);
      mcpCommandAllowlist = value(mcpCommandAllowlist);
      runCommandAllowlist = value(runCommandAllowlist);
      runInterpreterAllowlist = value(runInterpreterAllowlist);
      runImageAllowlist = value(runImageAllowlist);
      runVolumeAllowlist = value(runVolumeAllowlist);
      runPortAllowlist = value(runPortAllowlist);
      ociRuntime = ociRuntime == null || ociRuntime.isBlank() ? "podman" : ociRuntime;
    }

    private static String value(String configured) {
      return configured == null ? "" : configured;
    }
  }
}
