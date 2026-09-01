/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEventSink;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Objects;

/** Lifecycle owner for the Kafka Streams engine only; operation adapters are separate workloads. */
public final class KafkaStreamsEngineRuntime implements AutoCloseable {
  private final OksKafkaRuntime runtime;
  private final OksKafkaCommandGateway gateway;
  private final OksInboundCloudEventGateway inboundEvents;
  private final ExecutionEngineProvider provider;

  public KafkaStreamsEngineRuntime(Configuration configuration, ExecutionEventSink events) {
    Objects.requireNonNull(configuration, "configuration");
    Objects.requireNonNull(events, "events");
    OksTopics topics = OksTopics.withPrefix(configuration.topicPrefix());
    runtime =
        new OksKafkaRuntime(
            configuration.bootstrapServers(),
            configuration.applicationId(),
            configuration.instanceId(),
            configuration.stateDirectory(),
            topics,
            com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId.parse(
                "did:forwardmeasure:actor:kafka-runtime"),
            events);
    gateway =
        new OksKafkaCommandGateway(
            configuration.bootstrapServers(),
            configuration.applicationId() + '-' + configuration.instanceId() + "-ingress",
            topics,
            Clock.systemUTC());
    inboundEvents =
        new OksInboundCloudEventGateway(
            configuration.bootstrapServers(),
            configuration.applicationId() + '-' + configuration.instanceId() + "-inbound-events",
            topics);
    provider = new KafkaStreamsExecutionEngineProvider(gateway, events, Clock.systemUTC(), true);
  }

  public void start() {
    runtime.start();
  }

  public ExecutionEngineProvider provider() {
    return provider;
  }

  /**
   * The CloudEvents HTTP ingress's only path onto {@code inbound-events} - see {@link
   * OksInboundCloudEventGateway}. Framework bindings pull this through to construct the JAX-RS
   * resource that accepts external CloudEvents, the same way {@link #provider()} is pulled through
   * to construct {@code EngineCommandResource}.
   */
  public OksInboundCloudEventGateway inboundEvents() {
    return inboundEvents;
  }

  @Override
  public void close() {
    gateway.close();
    inboundEvents.close();
    runtime.close();
  }

  public record Configuration(
      String bootstrapServers,
      String applicationId,
      String instanceId,
      String topicPrefix,
      Path stateDirectory) {
    public Configuration {
      Objects.requireNonNull(bootstrapServers, "bootstrapServers");
      Objects.requireNonNull(applicationId, "applicationId");
      Objects.requireNonNull(instanceId, "instanceId");
      Objects.requireNonNull(topicPrefix, "topicPrefix");
      Objects.requireNonNull(stateDirectory, "stateDirectory");
    }
  }
}
