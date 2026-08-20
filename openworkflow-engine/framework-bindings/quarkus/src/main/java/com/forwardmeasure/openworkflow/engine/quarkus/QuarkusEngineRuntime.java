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
package com.forwardmeasure.openworkflow.engine.quarkus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.actor.PekkoClusterRuntime;
import com.forwardmeasure.openworkflow.actor.PekkoExecutionEngineProvider;
import com.forwardmeasure.openworkflow.actor.ProtocolOperationCoordinatorSharding;
import com.forwardmeasure.openworkflow.actor.WorkflowSharding;
import com.forwardmeasure.openworkflow.adapter.kafka.KafkaOperationAdapterRuntime;
import com.forwardmeasure.openworkflow.adapter.kafka.KafkaProtocolOperationExecutors;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProviders;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEventSink;
import com.forwardmeasure.openworkflow.execution.management.AuthzenExecutionAuthorizer;
import com.forwardmeasure.openworkflow.execution.management.ExecutionManagementService;
import com.forwardmeasure.openworkflow.execution.persistence.JpaExecutionPersistenceFactory;
import com.forwardmeasure.openworkflow.operation.AuthorizedHttpOperationExecutor;
import com.forwardmeasure.openworkflow.operation.OperationAdapterConfiguration;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor;
import com.forwardmeasure.openworkflow.operation.cassandra.CassandraOperationOutboxes;
import com.forwardmeasure.openworkflow.operation.postgresql.PostgresqlOperationOutboxes;
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

/**
 * Quarkus composition for the selectable Pekko and Kafka Streams execution engines and the
 * command-orchestration service built on top of them.
 */
@ApplicationScoped
public class QuarkusEngineRuntime {
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
    com.typesafe.config.Config selected =
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
      ExecutionEventSink events,
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

  private static ProtocolOperationExecutor protocolExecutor(
      AuthorizationService authorization, ObjectMapper json, Config config) {
    return kafkaProtocolExecutor(authorization, json, config);
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
