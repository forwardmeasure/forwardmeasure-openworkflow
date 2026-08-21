/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.adapter.pekko;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.actor.PekkoClusterRuntime;
import com.forwardmeasure.openworkflow.actor.ProtocolOperationCoordinatorSharding;
import com.forwardmeasure.openworkflow.actor.WorkflowSharding;
import com.forwardmeasure.openworkflow.adapter.kafka.KafkaProtocolOperationExecutors;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.operation.AuthorizedHttpOperationExecutor;
import com.forwardmeasure.openworkflow.operation.OperationAdapterConfiguration;
import com.forwardmeasure.openworkflow.operation.cassandra.CassandraOperationOutboxes;
import com.forwardmeasure.openworkflow.operation.postgresql.PostgresqlOperationOutboxes;
import com.forwardmeasure.openworkflow.persistence.PersistenceConfigLoader;
import com.forwardmeasure.openworkflow.persistence.PersistenceProfile;
import com.forwardmeasure.openworkflow.persistence.postgresql.PostgresqlDataSources;
import com.typesafe.config.ConfigFactory;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;

/** Owns Pekko outbox projections on adapter-role nodes without hosting workflow engine entities. */
public final class PekkoOperationAdapterRuntime implements AutoCloseable {
  private final ActorSystem<Void> actorSystem;
  private final HikariDataSource dataSource;

  public PekkoOperationAdapterRuntime(
      Configuration configuration, AuthorizationService authorization, ObjectMapper mapper) {
    Objects.requireNonNull(configuration, "configuration");
    Objects.requireNonNull(authorization, "authorization");
    Objects.requireNonNull(mapper, "mapper");
    if (!configuration.enabled()) {
      actorSystem = null;
      dataSource = null;
      return;
    }
    PersistenceProfile profile = PersistenceProfile.parse(configuration.persistenceProfile());
    var persistence =
        PersistenceConfigLoader.withConnection(
            PersistenceConfigLoader.select(ConfigFactory.load(), profile), profile,
            configuration.persistenceEndpoint(), configuration.persistenceUsername(),
            configuration.persistencePassword(), configuration.localDatacenter());
    var cluster =
        new PekkoClusterRuntime.Settings(
            configuration.discoveryService(),
            configuration.podIp(),
            configuration.arteryPort(),
            configuration.managementPort(),
            configuration.requiredContactPoints(),
            "operation-adapter");
    actorSystem =
        PekkoClusterRuntime.create(
            Behaviors.empty(),
            configuration.systemName(),
            PekkoClusterRuntime.configure(persistence, cluster));
    PekkoClusterRuntime.start(actorSystem, cluster);
    WorkflowSharding workflows = WorkflowSharding.initialize(actorSystem, "workflow-engine");
    var protocol =
        KafkaProtocolOperationExecutors.create(
            authorization, mapper, configuration.protocolConfiguration());
    var coordinators =
        ProtocolOperationCoordinatorSharding.initialize(actorSystem, workflows, protocol);
    var http =
        new AuthorizedHttpOperationExecutor(
            authorization,
            OperationAdapterConfiguration.executor(
                mapper,
                configuration.httpTimeoutMillis(),
                configuration.httpEgressAllowlist(),
                configuration.secretDirectory()));
    if (profile == PersistenceProfile.POSTGRESQL) {
      dataSource =
          PostgresqlDataSources.pooled(
              configuration.persistenceEndpoint(),
              configuration.persistenceUsername(),
              configuration.persistencePassword(),
              8);
      PostgresqlOperationOutboxes.startHttp(
          actorSystem, dataSource, workflows, http, configuration.askTimeout());
      PostgresqlOperationOutboxes.startProtocol(
          actorSystem, dataSource, coordinators, configuration.askTimeout());
    } else {
      dataSource = null;
      CassandraOperationOutboxes.startHttp(
          actorSystem, workflows, http, configuration.askTimeout());
      CassandraOperationOutboxes.startProtocol(
          actorSystem, coordinators, configuration.askTimeout());
    }
  }

  @Override
  public void close() {
    if (actorSystem != null) actorSystem.terminate();
    if (dataSource != null) dataSource.close();
  }

  public record Configuration(
      boolean enabled,
      String systemName,
      String persistenceProfile,
      String persistenceEndpoint,
      String persistenceUsername,
      String persistencePassword,
      String localDatacenter,
      String discoveryService,
      String podIp,
      int arteryPort,
      int managementPort,
      int requiredContactPoints,
      Duration askTimeout,
      long httpTimeoutMillis,
      String httpEgressAllowlist,
      String secretDirectory,
      KafkaProtocolOperationExecutors.Configuration protocolConfiguration) {
    public Configuration {
      Objects.requireNonNull(systemName);
      Objects.requireNonNull(askTimeout);
      Objects.requireNonNull(protocolConfiguration);
    }

    public static Configuration from(Function<String, String> value) {
      String allowlist = configured(value, "openworkflow.adapters.http-egress-allowlist", "");
      String secrets =
          configured(
              value, "openworkflow.adapters.secret-directory", "/var/run/secrets/openworkflow");
      long protocolTimeout = number(value, "openworkflow.operations.protocol.timeout-ms", 30_000);
      return new Configuration(
          Boolean.parseBoolean(configured(value, "openworkflow.adapters.pekko-enabled", "true")),
          configured(value, "openworkflow.pekko.system-name", "openworkflow-pekko"),
          configured(value, "openworkflow.persistence.profile", "postgresql"),
          configured(
              value,
              "openworkflow.persistence.endpoint",
              "jdbc:postgresql://postgresql:5432/openworkflow"),
          configured(value, "openworkflow.persistence.username", "openworkflow"),
          configured(value, "openworkflow.persistence.password", "openworkflow"),
          configured(value, "openworkflow.persistence.local-datacenter", "datacenter1"),
          configured(value, "openworkflow.cluster.discovery-service", ""),
          configured(value, "openworkflow.cluster.pod-ip", ""),
          (int) number(value, "openworkflow.cluster.artery-port", 25_520),
          (int) number(value, "openworkflow.cluster.management-port", 8_558),
          (int) number(value, "openworkflow.cluster.required-contact-points", 1),
          Duration.ofMillis(number(value, "openworkflow.pekko.ask-timeout-ms", 10_000)),
          number(value, "openworkflow.operations.http.timeout-ms", 30_000),
          allowlist,
          secrets,
          new KafkaProtocolOperationExecutors.Configuration(
              protocolTimeout,
              allowlist,
              secrets,
              configured(value, "openworkflow.operations.mcp-command-allowlist", ""),
              configured(value, "openworkflow.operations.run.command-allowlist", ""),
              configured(value, "openworkflow.operations.run.interpreter-allowlist", ""),
              configured(value, "openworkflow.operations.run.image-allowlist", ""),
              configured(value, "openworkflow.operations.run.volume-allowlist", ""),
              configured(value, "openworkflow.operations.run.port-allowlist", ""),
              configured(value, "openworkflow.operations.run.oci-runtime", "podman")));
    }

    private static String configured(Function<String, String> value, String name, String fallback) {
      String configured = value.apply(name);
      return configured == null || configured.isBlank() ? fallback : configured;
    }

    private static long number(Function<String, String> value, String name, long fallback) {
      return Long.parseLong(configured(value, name, Long.toString(fallback)));
    }
  }
}
