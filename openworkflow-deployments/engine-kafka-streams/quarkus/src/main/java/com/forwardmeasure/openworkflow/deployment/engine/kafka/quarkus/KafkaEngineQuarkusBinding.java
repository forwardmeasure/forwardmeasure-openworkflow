/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.engine.kafka.quarkus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEventSink;
import com.forwardmeasure.openworkflow.engine.http.HttpExecutionEventSink;
import com.forwardmeasure.openworkflow.engine.http.server.EngineCommandResource;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.KafkaStreamsEngineRuntime;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.jaxrs.OksCloudEventIngressResource;
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

  /**
   * Neither {@code ActiveOrganizationProvider} nor {@code AuthorizationService} is produced here -
   * both come from {@code openworkflow-quarkus-binding} (which this module now depends on): {@code
   * QuarkusActiveOrganizationProvider} is a {@code @RequestScoped} CDI bean Quarkus auto-discovers,
   * and {@code OpenWorkflowQuarkusBinding} produces {@code AuthorizationService} the same way.
   * Duplicating either producer here (as an earlier pass at this file did) would give CDI two beans
   * for the same type and fail deployment validation with an ambiguous-resolution error; {@code
   * openworkflow-deployments/operation-adapter/quarkus}'s own {@code authorization(...)} producer
   * is not a counterexample - that module deliberately does NOT depend on {@code
   * openworkflow-quarkus-binding}, so nothing else is producing that bean there.
   */
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

  /**
   * The production path onto {@code inbound-events} - see {@code OksInboundCloudEventGateway} and
   * {@code KafkaStreamsEngineRuntime.inboundEvents()}. Depending on {@code runtime} (not just
   * {@code runtime.inboundEvents()}) also forces {@code runtime}'s CDI producer - and therefore
   * {@code runtime.start()} - to run even though {@code provider}/{@code commands} above already do
   * so today; this is redundant with that existing forcing path, not a replacement for it.
   */
  @Produces
  @ApplicationScoped
  OksCloudEventIngressResource cloudEvents(
      KafkaStreamsEngineRuntime runtime,
      ActiveOrganizationProvider organizations,
      AuthorizationService authorization,
      ObjectMapper mapper) {
    return new OksCloudEventIngressResource(
        runtime.inboundEvents(), organizations, authorization, mapper);
  }
}
