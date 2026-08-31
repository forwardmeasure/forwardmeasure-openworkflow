/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.actor;

import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEventSink;
import com.typesafe.config.Config;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;

/** Lifecycle owner for one Pekko engine cluster member without embedded operation adapters. */
public final class PekkoEngineRuntime implements AutoCloseable {
  private final ActorSystem<Void> actorSystem;
  private final WorkflowSharding workflows;
  private final ExecutionEngineProvider provider;
  private final Optional<PostgresConnectionSettings> postgresConnection;

  public PekkoEngineRuntime(
      String systemName,
      Config configuration,
      PekkoClusterRuntime.Settings clusterSettings,
      Duration askTimeout,
      ExecutionEventSink events) {
    this(systemName, configuration, clusterSettings, askTimeout, events, Optional.empty());
  }

  /**
   * @param postgresConnection empty for the Cassandra profile; present for the Postgres profile so
   *     every entity this runtime hosts resolves its own tenant-scoped journal/snapshot plugin -
   *     see {@link WorkflowEntity}'s plugin overrides. Callers wiring up the outbox/subscription
   *     projections and the schedule/subworkflow shardings must reuse this same instance (via
   *     {@link #postgresConnection()}), not re-derive an independent one.
   */
  public PekkoEngineRuntime(
      String systemName,
      Config configuration,
      PekkoClusterRuntime.Settings clusterSettings,
      Duration askTimeout,
      ExecutionEventSink events,
      Optional<PostgresConnectionSettings> postgresConnection) {
    Objects.requireNonNull(systemName, "systemName");
    Objects.requireNonNull(configuration, "configuration");
    Objects.requireNonNull(clusterSettings, "clusterSettings");
    Objects.requireNonNull(askTimeout, "askTimeout");
    Objects.requireNonNull(events, "events");
    Objects.requireNonNull(postgresConnection, "postgresConnection");
    this.postgresConnection = postgresConnection;
    actorSystem =
        PekkoClusterRuntime.create(
            Behaviors.empty(),
            systemName,
            PekkoClusterRuntime.configure(configuration, clusterSettings));
    PekkoClusterRuntime.start(actorSystem, clusterSettings);
    workflows =
        WorkflowSharding.initialize(actorSystem, clusterSettings.role(), postgresConnection);
    provider = new PekkoExecutionEngineProvider(workflows, askTimeout, Clock.systemUTC(), events);
  }

  public ExecutionEngineProvider provider() {
    return provider;
  }

  /**
   * The same live cluster member both {@link #provider} and the outbox/subscription projections
   * this runtime doesn't start itself must share - {@code ShardedDaemonProcess} needs the actual
   * running {@link ActorSystem}, not a new one, so a caller wiring up
   * PostgresqlCloudEventOutbox/PostgresqlSubworkflowOutbox/etc. must reuse these, not construct
   * their own {@link WorkflowSharding}.
   */
  public ActorSystem<Void> actorSystem() {
    return actorSystem;
  }

  public WorkflowSharding workflows() {
    return workflows;
  }

  /**
   * The same connection settings {@link #workflows} was built from - callers wiring up {@code
   * SubworkflowCoordinatorSharding}/{@code WorkflowScheduleSharding}/the outbox and subscription
   * projections must reuse this, not construct their own independent instance.
   */
  public Optional<PostgresConnectionSettings> postgresConnection() {
    return postgresConnection;
  }

  @Override
  public void close() {
    actorSystem.terminate();
  }
}
