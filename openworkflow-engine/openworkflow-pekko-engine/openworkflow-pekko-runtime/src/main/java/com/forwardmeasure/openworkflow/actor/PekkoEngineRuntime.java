/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.actor;

import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEventSink;
import com.typesafe.config.Config;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;

/** Lifecycle owner for one Pekko engine cluster member without embedded operation adapters. */
public final class PekkoEngineRuntime implements AutoCloseable {
  private final ActorSystem<Void> actorSystem;
  private final ExecutionEngineProvider provider;

  public PekkoEngineRuntime(
      String systemName,
      Config configuration,
      PekkoClusterRuntime.Settings clusterSettings,
      Duration askTimeout,
      ExecutionEventSink events) {
    Objects.requireNonNull(systemName, "systemName");
    Objects.requireNonNull(configuration, "configuration");
    Objects.requireNonNull(clusterSettings, "clusterSettings");
    Objects.requireNonNull(askTimeout, "askTimeout");
    Objects.requireNonNull(events, "events");
    actorSystem =
        PekkoClusterRuntime.create(
            Behaviors.empty(),
            systemName,
            PekkoClusterRuntime.configure(configuration, clusterSettings));
    PekkoClusterRuntime.start(actorSystem, clusterSettings);
    WorkflowSharding sharding = WorkflowSharding.initialize(actorSystem, clusterSettings.role());
    provider = new PekkoExecutionEngineProvider(sharding, askTimeout, Clock.systemUTC(), events);
  }

  public ExecutionEngineProvider provider() {
    return provider;
  }

  @Override
  public void close() {
    actorSystem.terminate();
  }
}
