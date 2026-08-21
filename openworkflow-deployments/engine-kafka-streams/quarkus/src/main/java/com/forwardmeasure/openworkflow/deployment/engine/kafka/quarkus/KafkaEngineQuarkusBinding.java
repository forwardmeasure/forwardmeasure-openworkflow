/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.engine.kafka.quarkus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEventSink;
import com.forwardmeasure.openworkflow.engine.http.HttpExecutionEventSink;
import com.forwardmeasure.openworkflow.engine.http.server.EngineCommandResource;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.KafkaStreamsEngineRuntime;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class KafkaEngineQuarkusBinding {
  @Produces
  @ApplicationScoped
  ExecutionEventSink events(
      ObjectMapper mapper,
      @ConfigProperty(name = "openworkflow.execution-events.url") URI url,
      @ConfigProperty(name = "openworkflow.execution-events.timeout") Duration timeout) {
    return new HttpExecutionEventSink(
        url, HttpClient.newBuilder().connectTimeout(timeout).build(), mapper, timeout);
  }

  @Produces
  @ApplicationScoped
  KafkaStreamsEngineRuntime runtime(
      ExecutionEventSink events,
      @ConfigProperty(name = "openworkflow.kafka.bootstrap-servers") String bootstrap,
      @ConfigProperty(name = "openworkflow.kafka.application-id") String applicationId,
      @ConfigProperty(name = "openworkflow.kafka.instance-id") String instanceId,
      @ConfigProperty(name = "openworkflow.kafka.topic-prefix") String topicPrefix,
      @ConfigProperty(name = "openworkflow.kafka.state-dir") Path stateDirectory) {
    var runtime =
        new KafkaStreamsEngineRuntime(
            new KafkaStreamsEngineRuntime.Configuration(
                bootstrap, applicationId, instanceId, topicPrefix, stateDirectory),
            events);
    runtime.start();
    return runtime;
  }

  @Produces
  @ApplicationScoped
  ExecutionEngineProvider provider(KafkaStreamsEngineRuntime runtime) {
    return runtime.provider();
  }

  @Produces
  @ApplicationScoped
  EngineCommandResource commands(ExecutionEngineProvider provider) {
    return new EngineCommandResource(provider);
  }
}
