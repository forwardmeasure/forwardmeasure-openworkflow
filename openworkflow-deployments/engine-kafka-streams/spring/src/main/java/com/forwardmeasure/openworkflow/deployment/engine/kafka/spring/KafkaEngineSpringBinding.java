/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.engine.kafka.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEventSink;
import com.forwardmeasure.openworkflow.engine.http.HttpExecutionEventSink;
import com.forwardmeasure.openworkflow.engine.http.server.EngineCommandResource;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.KafkaStreamsEngineRuntime;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.jaxrs.OksCloudEventIngressResource;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jersey.autoconfigure.ResourceConfigCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class KafkaEngineSpringBinding {
  @Bean
  ExecutionEventSink executionEvents(
      ObjectMapper mapper,
      @Value("${openworkflow.execution-events.url}") URI url,
      @Value("${openworkflow.execution-events.timeout}") Duration timeout) {
    return new HttpExecutionEventSink(
        url, HttpClient.newBuilder().connectTimeout(timeout).build(), mapper, timeout);
  }

  /**
   * Neither {@code ActiveOrganizationProvider} nor {@code AuthorizationService} is produced here -
   * both come from {@code openworkflow-spring-binding} (which this module now depends on): {@code
   * SpringActiveOrganizationProvider} is a {@code @Component} and {@code OpenWorkflowSpringBinding}
   * is a {@code @Configuration} class producing {@code AuthorizationService}, same as {@code
   * openworkflow-engine-pekko-spring} already relies on for {@code CloudEventIngressResource}'s
   * identical needs. Duplicating either bean here would give Spring two definitions for the same
   * name/type and fail context startup with a conflicting-bean error. Requires {@code
   * KafkaEngineSpringApplication} to scan {@code com.forwardmeasure.openworkflow} (not just its own
   * sub-package) so both of those classes - and {@code openWorkflowSecurity}'s {@code
   * SecurityFilterChain} bean - are actually found.
   */
  @Bean(destroyMethod = "close")
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

  @Bean
  ExecutionEngineProvider engine(KafkaStreamsEngineRuntime runtime) {
    return runtime.provider();
  }

  @Bean
  EngineCommandResource commands(ExecutionEngineProvider engine) {
    return new EngineCommandResource(engine);
  }

  @Bean
  OksCloudEventIngressResource cloudEvents(
      KafkaStreamsEngineRuntime runtime,
      ActiveOrganizationProvider organizations,
      AuthorizationService authorization,
      ObjectMapper mapper) {
    return new OksCloudEventIngressResource(
        runtime.inboundEvents(), organizations, authorization, mapper);
  }

  @Bean
  ResourceConfigCustomizer engineResources(
      EngineCommandResource commands, OksCloudEventIngressResource cloudEvents) {
    return resourceConfig -> resourceConfig.register(commands).register(cloudEvents);
  }
}
