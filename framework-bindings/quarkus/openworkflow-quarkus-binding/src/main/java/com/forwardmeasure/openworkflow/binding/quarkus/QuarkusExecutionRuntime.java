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
package com.forwardmeasure.openworkflow.binding.quarkus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.actor.PekkoClusterRuntime;
import com.forwardmeasure.openworkflow.actor.PekkoExecutionEngineProvider;
import com.forwardmeasure.openworkflow.actor.ProtocolOperationCoordinatorSharding;
import com.forwardmeasure.openworkflow.actor.WorkflowSharding;
import com.forwardmeasure.openworkflow.adapter.kafka.KafkaOperationAdapterRuntime;
import com.forwardmeasure.openworkflow.adapter.kafka.KafkaProtocolOperationExecutors;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.engine.api.ActorId;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProviders;
import com.forwardmeasure.openworkflow.engine.api.TenantActorContext;
import com.forwardmeasure.openworkflow.execution.jaxrs.ExecutionContextProvider;
import com.forwardmeasure.openworkflow.execution.management.AuthzenExecutionAuthorizer;
import com.forwardmeasure.openworkflow.execution.management.ExecutionManagementService;
import com.forwardmeasure.openworkflow.execution.persistence.JpaExecutionPersistenceFactory;
import com.forwardmeasure.openworkflow.execution.query.persistence.JpaTenantRoutingExecutionStore;
import com.forwardmeasure.openworkflow.operation.AsyncApiHttpOperationExecutor;
import com.forwardmeasure.openworkflow.operation.AsyncApiWebSocketOperationExecutor;
import com.forwardmeasure.openworkflow.operation.AuthorizedHttpOperationExecutor;
import com.forwardmeasure.openworkflow.operation.AuthorizedProtocolOperationExecutor;
import com.forwardmeasure.openworkflow.operation.OperationAdapterConfiguration;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor;
import com.forwardmeasure.openworkflow.operation.RoutingProtocolOperationExecutor;
import com.forwardmeasure.openworkflow.operation.agent.JsonRpcHttpOperationExecutor;
import com.forwardmeasure.openworkflow.operation.agent.McpStdioCommandPolicy;
import com.forwardmeasure.openworkflow.operation.agent.McpStdioOperationExecutor;
import com.forwardmeasure.openworkflow.operation.amqp.AsyncApiAmqpOperationExecutor;
import com.forwardmeasure.openworkflow.operation.cassandra.CassandraOperationOutboxes;
import com.forwardmeasure.openworkflow.operation.cloud.AsyncApiAnypointMqOperationExecutor;
import com.forwardmeasure.openworkflow.operation.cloud.AsyncApiCloudOperationExecutor;
import com.forwardmeasure.openworkflow.operation.grpc.DynamicGrpcOperationExecutor;
import com.forwardmeasure.openworkflow.operation.jms.AsyncApiJmsOperationExecutor;
import com.forwardmeasure.openworkflow.operation.kafka.AsyncApiKafkaOperationExecutor;
import com.forwardmeasure.openworkflow.operation.mqtt.AsyncApiMqttOperationExecutor;
import com.forwardmeasure.openworkflow.operation.nats.AsyncApiNatsOperationExecutor;
import com.forwardmeasure.openworkflow.operation.postgresql.PostgresqlOperationOutboxes;
import com.forwardmeasure.openworkflow.operation.pulsar.AsyncApiPulsarOperationExecutor;
import com.forwardmeasure.openworkflow.operation.redis.AsyncApiRedisOperationExecutor;
import com.forwardmeasure.openworkflow.operation.runner.LocalProcessOperationExecutor;
import com.forwardmeasure.openworkflow.operation.runner.OciContainerOperationExecutor;
import com.forwardmeasure.openworkflow.operation.runner.RunPolicyConfiguration;
import com.forwardmeasure.openworkflow.operation.stomp.AsyncApiStompOperationExecutor;
import com.forwardmeasure.openworkflow.persistence.PersistenceConfigLoader;
import com.forwardmeasure.openworkflow.persistence.PersistenceProfile;
import com.forwardmeasure.openworkflow.persistence.postgresql.PostgresqlDataSources;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.KafkaStreamsExecutionEngineProvider;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.OksKafkaCommandGateway;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.OksKafkaRuntime;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.OksTopics;
import com.typesafe.config.ConfigFactory;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.persistence.EntityManager;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.eclipse.microprofile.config.Config;

/** Quarkus composition for the shared execution plane and clustered Pekko provider. */
@ApplicationScoped
public class QuarkusExecutionRuntime {
  private ActorSystem<Void> actorSystem;
  private OksKafkaRuntime kafkaRuntime;
  private OksKafkaCommandGateway kafkaGateway;
  private KafkaOperationAdapterRuntime operationAdapters;
  private HikariDataSource operationDataSource;

  @Produces
  @ApplicationScoped
  JpaExecutionPersistenceFactory executionPersistence(
      EntityManager entityManager, ObjectMapper objectMapper) {
    return new JpaExecutionPersistenceFactory(entityManager, objectMapper);
  }

  @Produces
  @ApplicationScoped
  JpaTenantRoutingExecutionStore executionStore(
      EntityManager entityManager, ObjectMapper objectMapper) {
    return new JpaTenantRoutingExecutionStore(entityManager, objectMapper);
  }

  @Produces
  @ApplicationScoped
  ExecutionContextProvider executionContext(ActiveOrganizationProvider organizations) {
    return () -> context(organizations.current());
  }

  private synchronized ActorSystem<Void> actorSystem(Config config) {
    if (actorSystem != null) {
      return actorSystem;
    }
    PersistenceProfile profile =
        PersistenceProfile.parse(
            configValue(config, "openworkflow.persistence.profile", "postgresql"));
    var settings =
        new PekkoClusterRuntime.Settings(
            configValue(config, "openworkflow.cluster.discovery-service", ""),
            configValue(config, "openworkflow.cluster.pod-ip", ""),
            Integer.parseInt(configValue(config, "openworkflow.cluster.artery-port", "25520")),
            Integer.parseInt(configValue(config, "openworkflow.cluster.management-port", "8558")),
            Integer.parseInt(
                configValue(config, "openworkflow.cluster.required-contact-points", "1")),
            configValue(config, "openworkflow.cluster.role", "operation-adapter"));
    var selected =
        PersistenceConfigLoader.withConnection(
            PersistenceConfigLoader.select(ConfigFactory.load(), profile),
            profile,
            required(config, "openworkflow.persistence.endpoint"),
            configValue(config, "openworkflow.persistence.username", ""),
            configValue(config, "openworkflow.persistence.password", ""),
            configValue(config, "openworkflow.persistence.local-datacenter", "datacenter1"));
    actorSystem =
        PekkoClusterRuntime.create(
            Behaviors.empty(),
            configValue(config, "openworkflow.cluster.system-name", "openworkflow-pekko"),
            PekkoClusterRuntime.configure(selected, settings));
    PekkoClusterRuntime.start(actorSystem, settings);
    return actorSystem;
  }

  @Produces
  @ApplicationScoped
  synchronized ExecutionEngineProvider engineProvider(
      QuarkusExecutionEventSink events,
      AuthorizationService authorization,
      ObjectMapper objectMapper,
      Config config) {
    String engine = configValue(config, "openworkflow.engine.profile", "pekko");
    if ("kafka-streams".equals(engine)) {
      String bootstrap = required(config, "openworkflow.kafka.bootstrap-servers");
      String applicationId =
          configValue(config, "openworkflow.kafka.application-id", "openworkflow-kafka");
      String instanceId =
          configValue(config, "openworkflow.kafka.instance-id", UUID.randomUUID().toString());
      OksTopics topics =
          OksTopics.withPrefix(
              configValue(config, "openworkflow.kafka.topic-prefix", "openworkflow"));
      kafkaRuntime =
          new OksKafkaRuntime(
              bootstrap,
              applicationId,
              instanceId,
              Path.of(
                  configValue(config, "openworkflow.kafka.state-dir", "/tmp/openworkflow-kafka")),
              topics,
              com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId.parse(
                  "did:forwardmeasure:actor:kafka-runtime"),
              events);
      kafkaRuntime.start();
      kafkaGateway =
          new OksKafkaCommandGateway(
              bootstrap, applicationId + '-' + instanceId + "-ingress", topics, Clock.systemUTC());
      operationAdapters =
          new KafkaOperationAdapterRuntime(
              bootstrap,
              topics.effects(),
              topics.definitions(),
              topics.commands(),
              topics.deadLetters(),
              configValue(
                  config, "openworkflow.adapters.consumer-group", applicationId + "-operations"),
              instanceId,
              authorization,
              objectMapper,
              configValue(
                  config,
                  "openworkflow.adapters.secret-directory",
                  "/var/run/secrets/openworkflow"),
              configValue(config, "openworkflow.adapters.http-egress-allowlist", ""),
              kafkaProtocolExecutor(authorization, objectMapper, config));
      operationAdapters.start();
      return new KafkaStreamsExecutionEngineProvider(kafkaGateway, events, Clock.systemUTC(), true);
    }
    if (!"pekko".equals(engine)) {
      throw new IllegalArgumentException("Unsupported execution engine profile: " + engine);
    }
    WorkflowSharding sharding = WorkflowSharding.initialize(actorSystem(config));
    Duration askTimeout =
        Duration.ofMillis(
            Long.parseLong(configValue(config, "openworkflow.pekko.ask-timeout-ms", "10000")));
    var http =
        new AuthorizedHttpOperationExecutor(
            authorization,
            OperationAdapterConfiguration.executor(
                objectMapper,
                Long.parseLong(
                    configValue(config, "openworkflow.operations.http.timeout-ms", "30000")),
                configValue(config, "openworkflow.adapters.http-egress-allowlist", ""),
                configValue(
                    config,
                    "openworkflow.adapters.secret-directory",
                    "/var/run/secrets/openworkflow")));
    PersistenceProfile profile =
        PersistenceProfile.parse(
            configValue(config, "openworkflow.persistence.profile", "postgresql"));
    if (profile == PersistenceProfile.POSTGRESQL) {
      operationDataSource =
          PostgresqlDataSources.pooled(
              required(config, "openworkflow.persistence.endpoint"),
              configValue(config, "openworkflow.persistence.username", ""),
              configValue(config, "openworkflow.persistence.password", ""),
              8);
      PostgresqlOperationOutboxes.startHttp(
          actorSystem(config), operationDataSource, sharding, http, askTimeout);
      PostgresqlOperationOutboxes.startProtocol(
          actorSystem(config),
          operationDataSource,
          ProtocolOperationCoordinatorSharding.initialize(
              actorSystem(config), sharding, protocolExecutor(authorization, objectMapper, config)),
          askTimeout);
    } else {
      CassandraOperationOutboxes.startHttp(actorSystem(config), sharding, http, askTimeout);
      CassandraOperationOutboxes.startProtocol(
          actorSystem(config),
          ProtocolOperationCoordinatorSharding.initialize(
              actorSystem(config), sharding, protocolExecutor(authorization, objectMapper, config)),
          askTimeout);
    }
    return new PekkoExecutionEngineProvider(sharding, askTimeout, Clock.systemUTC(), events);
  }

  @Produces
  @ApplicationScoped
  ExecutionManagementService executionManagement(
      JpaExecutionPersistenceFactory persistence,
      ExecutionEngineProvider provider,
      AuthorizationService authorization,
      ActiveOrganizationProvider organizations,
      Config config) {
    var authorizer =
        new AuthzenExecutionAuthorizer(authorization, ignored -> organizations.current());
    return new ExecutionManagementService(
        persistence,
        authorizer,
        persistence,
        new ExecutionEngineProviders(List.of(provider)),
        ignored ->
            "kafka-streams".equals(configValue(config, "openworkflow.engine.profile", "pekko"))
                ? EngineId.KAFKA_STREAMS
                : EngineId.PEKKO,
        Clock.systemUTC(),
        UUID::randomUUID);
  }

  @PreDestroy
  void stop() {
    if (operationAdapters != null) {
      operationAdapters.close();
    }
    if (kafkaGateway != null) {
      kafkaGateway.close();
    }
    if (kafkaRuntime != null) {
      kafkaRuntime.close();
    }
    if (actorSystem != null) {
      actorSystem.terminate();
    }
    if (operationDataSource != null) {
      operationDataSource.close();
    }
  }

  private static TenantActorContext context(ActiveOrganization organization) {
    return new TenantActorContext(
        new com.forwardmeasure.openworkflow.engine.api.TenantId(organization.tenantId().value()),
        organization.organizationId(),
        new ActorId(organization.actorId()),
        organization.organizationRoles());
  }

  private static ProtocolOperationExecutor protocolExecutor(
      AuthorizationService authorization, ObjectMapper json, Config config) {
    Duration timeout =
        Duration.ofMillis(
            Long.parseLong(
                configValue(config, "openworkflow.operations.protocol.timeout-ms", "30000")));
    var egress =
        OperationAdapterConfiguration.egressPolicy(
            configValue(config, "openworkflow.adapters.http-egress-allowlist", ""));
    var secrets =
        OperationAdapterConfiguration.secretProvider(
            configValue(
                config, "openworkflow.adapters.secret-directory", "/var/run/secrets/openworkflow"));
    var drivers =
        new java.util.LinkedHashMap<
            RoutingProtocolOperationExecutor.DriverKey, ProtocolOperationExecutor>();
    var grpc = new DynamicGrpcOperationExecutor(timeout, egress, secrets);
    var asyncHttp =
        new AsyncApiHttpOperationExecutor(
            OperationAdapterConfiguration.executor(
                json,
                timeout.toMillis(),
                configValue(config, "openworkflow.adapters.http-egress-allowlist", ""),
                configValue(config, "openworkflow.adapters.secret-directory", "")),
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
            McpStdioCommandPolicy.configured(
                configValue(config, "openworkflow.operations.mcp-command-allowlist", "")),
            secrets);
    var runPolicy =
        RunPolicyConfiguration.policy(
            configValue(config, "openworkflow.operations.run.command-allowlist", ""),
            configValue(config, "openworkflow.operations.run.interpreter-allowlist", ""),
            configValue(config, "openworkflow.operations.run.image-allowlist", ""),
            configValue(config, "openworkflow.operations.run.volume-allowlist", ""),
            configValue(config, "openworkflow.operations.run.port-allowlist", ""));
    var localRun = new LocalProcessOperationExecutor(timeout, 1_048_576, runPolicy);
    var containerRun =
        new OciContainerOperationExecutor(
            configValue(config, "openworkflow.operations.run.oci-runtime", "podman"),
            timeout,
            1_048_576,
            runPolicy);
    java.util.function.BiConsumer<
            com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind, String>
        grpcRoute =
            (kind, protocol) ->
                drivers.put(new RoutingProtocolOperationExecutor.DriverKey(kind, protocol), grpc);
    grpcRoute.accept(
        com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.GRPC, "grpc");
    grpcRoute.accept(
        com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.GRPC, "grpcs");
    for (String protocol : java.util.List.of("http", "https", "mercure"))
      drivers.put(
          new RoutingProtocolOperationExecutor.DriverKey(
              com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.ASYNC_API,
              protocol),
          asyncHttp);
    for (String protocol : java.util.List.of("kafka", "kafka-secure"))
      drivers.put(
          new RoutingProtocolOperationExecutor.DriverKey(
              com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.ASYNC_API,
              protocol),
          kafka);
    drivers.put(
        new RoutingProtocolOperationExecutor.DriverKey(
            com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.ASYNC_API,
            "nats"),
        nats);
    drivers.put(
        new RoutingProtocolOperationExecutor.DriverKey(
            com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.ASYNC_API,
            "ws"),
        webSocket);
    for (String protocol : java.util.List.of("amqp", "amqp1"))
      drivers.put(
          new RoutingProtocolOperationExecutor.DriverKey(
              com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.ASYNC_API,
              protocol),
          amqp);
    for (String protocol : java.util.List.of("googlepubsub", "sns", "sqs"))
      drivers.put(
          new RoutingProtocolOperationExecutor.DriverKey(
              com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.ASYNC_API,
              protocol),
          cloud);
    drivers.put(
        new RoutingProtocolOperationExecutor.DriverKey(
            com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.ASYNC_API,
            "anypointmq"),
        anypoint);
    for (String protocol : java.util.List.of("jms", "ibmmq", "solace"))
      drivers.put(
          new RoutingProtocolOperationExecutor.DriverKey(
              com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.ASYNC_API,
              protocol),
          jms);
    for (String protocol : java.util.List.of("mqtt", "mqtt5"))
      drivers.put(
          new RoutingProtocolOperationExecutor.DriverKey(
              com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.ASYNC_API,
              protocol),
          mqtt);
    drivers.put(
        new RoutingProtocolOperationExecutor.DriverKey(
            com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.ASYNC_API,
            "pulsar"),
        pulsar);
    drivers.put(
        new RoutingProtocolOperationExecutor.DriverKey(
            com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.ASYNC_API,
            "redis"),
        redis);
    drivers.put(
        new RoutingProtocolOperationExecutor.DriverKey(
            com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.ASYNC_API,
            "stomp"),
        stomp);
    drivers.put(
        new RoutingProtocolOperationExecutor.DriverKey(
            com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.A2A,
            "a2a-jsonrpc"),
        agent);
    drivers.put(
        new RoutingProtocolOperationExecutor.DriverKey(
            com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.MCP,
            "mcp-http"),
        agent);
    drivers.put(
        new RoutingProtocolOperationExecutor.DriverKey(
            com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.MCP,
            "mcp-stdio"),
        mcp);
    for (String protocol : java.util.List.of("run-shell", "run-script"))
      drivers.put(
          new RoutingProtocolOperationExecutor.DriverKey(
              com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.RUN,
              protocol),
          localRun);
    drivers.put(
        new RoutingProtocolOperationExecutor.DriverKey(
            com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor.Kind.RUN,
            "run-container"),
        containerRun);
    return new AuthorizedProtocolOperationExecutor(
        authorization, new RoutingProtocolOperationExecutor(drivers));
  }

  private static ProtocolOperationExecutor kafkaProtocolExecutor(
      AuthorizationService authorization, ObjectMapper json, Config config) {
    return KafkaProtocolOperationExecutors.create(
        authorization,
        json,
        new KafkaProtocolOperationExecutors.Configuration(
            Long.parseLong(
                configValue(config, "openworkflow.operations.protocol.timeout-ms", "30000")),
            configValue(config, "openworkflow.adapters.http-egress-allowlist", ""),
            configValue(
                config, "openworkflow.adapters.secret-directory", "/var/run/secrets/openworkflow"),
            configValue(config, "openworkflow.operations.mcp-command-allowlist", ""),
            configValue(config, "openworkflow.operations.run.command-allowlist", ""),
            configValue(config, "openworkflow.operations.run.interpreter-allowlist", ""),
            configValue(config, "openworkflow.operations.run.image-allowlist", ""),
            configValue(config, "openworkflow.operations.run.volume-allowlist", ""),
            configValue(config, "openworkflow.operations.run.port-allowlist", ""),
            configValue(config, "openworkflow.operations.run.oci-runtime", "podman")));
  }

  private static String required(Config config, String name) {
    return config
        .getOptionalValue(name, String.class)
        .filter(value -> !value.isBlank())
        .orElseThrow(() -> new IllegalStateException(name + " is required"));
  }

  private static String configValue(Config config, String name, String fallback) {
    return config.getOptionalValue(name, String.class).orElse(fallback);
  }
}
