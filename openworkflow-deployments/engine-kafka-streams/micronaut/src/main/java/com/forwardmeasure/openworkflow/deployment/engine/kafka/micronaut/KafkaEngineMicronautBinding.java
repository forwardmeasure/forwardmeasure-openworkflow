/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.engine.kafka.micronaut;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEventSink;
import com.forwardmeasure.openworkflow.engine.http.HttpExecutionEventSink;
import com.forwardmeasure.openworkflow.engine.http.server.EngineCommandResource;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.KafkaStreamsEngineRuntime;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.jaxrs.OksCloudEventIngressResource;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;

/**
 * {@code OksCloudEventIngressResource} below is, like the existing, unchanged {@code
 * EngineCommandResource}, a {@code @Path}-annotated HTTP resource Micronaut's own HTTP server
 * eagerly discovers and constructs at startup - no separate startup-event forcing trigger is needed
 * for it to run, same reasoning already documented on {@code PekkoEngineMicronautBinding}'s {@code
 * startEventing}.
 */
@Factory
public class KafkaEngineMicronautBinding {
  @Singleton
  ExecutionEventSink executionEvents(
      ObjectMapper mapper,
      @Value("${openworkflow.execution-events.url}") URI url,
      @Value("${openworkflow.execution-events.timeout}") Duration timeout) {
    return new HttpExecutionEventSink(
        url, HttpClient.newBuilder().connectTimeout(timeout).build(), mapper, timeout);
  }

  /**
   * Neither {@code ActiveOrganizationProvider} nor {@code AuthorizationService} is produced here -
   * both come from {@code openworkflow-micronaut-binding} (which this module now depends on):
   * {@code MicronautActiveOrganizationProvider} and {@code OpenWorkflowMicronautBinding} are both
   * {@code @Singleton}/{@code @Factory} beans Micronaut already discovers, same as {@code
   * openworkflow-engine-pekko-micronaut} already relies on for {@code CloudEventIngressResource}'s
   * identical needs. Duplicating either producer here would give the context two beans for the same
   * type and fail startup with an ambiguous/non-unique bean error.
   */
  @Singleton
  KafkaStreamsEngineRuntime kafkaRuntime(
      ExecutionEventSink events,
      @Value("${openworkflow.kafka.bootstrap-servers}") String bootstrap,
      @Value("${openworkflow.kafka.application-id}") String applicationId,
      @Value("${openworkflow.kafka.instance-id}") String instanceId,
      @Value("${openworkflow.kafka.topic-prefix}") String topicPrefix,
      @Value("${openworkflow.kafka.state-dir}") Path stateDirectory) {
    var runtime =
        new KafkaStreamsEngineRuntime(
            new KafkaStreamsEngineRuntime.Configuration(
                bootstrap, applicationId, instanceId, topicPrefix, stateDirectory),
            events);
    runtime.start();
    return runtime;
  }

  @Singleton
  ExecutionEngineProvider engine(KafkaStreamsEngineRuntime runtime) {
    return runtime.provider();
  }

  @Singleton
  EngineCommandResource commands(ExecutionEngineProvider engine) {
    return new EngineCommandResource(engine);
  }

  @Singleton
  OksCloudEventIngressResource cloudEvents(
      KafkaStreamsEngineRuntime runtime,
      ActiveOrganizationProvider organizations,
      AuthorizationService authorization,
      ObjectMapper mapper) {
    return new OksCloudEventIngressResource(
        runtime.inboundEvents(), organizations, authorization, mapper);
  }
}
