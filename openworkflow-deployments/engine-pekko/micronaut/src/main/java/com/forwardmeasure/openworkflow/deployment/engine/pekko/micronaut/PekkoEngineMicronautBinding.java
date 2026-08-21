/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.engine.pekko.micronaut;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.actor.PekkoClusterRuntime;
import com.forwardmeasure.openworkflow.actor.PekkoEngineRuntime;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEventSink;
import com.forwardmeasure.openworkflow.engine.http.HttpExecutionEventSink;
import com.forwardmeasure.openworkflow.engine.http.server.EngineCommandResource;
import com.forwardmeasure.openworkflow.persistence.PersistenceConfigLoader;
import com.forwardmeasure.openworkflow.persistence.PersistenceProfile;
import com.typesafe.config.ConfigFactory;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;

@Factory
public class PekkoEngineMicronautBinding {
  @Singleton
  ExecutionEventSink events(
      ObjectMapper mapper,
      @Value("${openworkflow.execution-events.url}") URI url,
      @Value("${openworkflow.execution-events.timeout}") Duration timeout) {
    return new HttpExecutionEventSink(
        url, HttpClient.newBuilder().connectTimeout(timeout).build(), mapper, timeout);
  }

  @Singleton
  PekkoEngineRuntime runtime(
      ExecutionEventSink events,
      @Value("${openworkflow.pekko.system-name}") String systemName,
      @Value("${openworkflow.pekko.ask-timeout}") Duration askTimeout,
      @Value("${openworkflow.persistence.profile}") String profileName,
      @Value("${openworkflow.persistence.endpoint}") String endpoint,
      @Value("${openworkflow.persistence.username:}") String username,
      @Value("${openworkflow.persistence.password:}") String password,
      @Value("${openworkflow.persistence.local-datacenter:datacenter1}") String datacenter,
      @Value("${openworkflow.cluster.discovery-service:}") String discovery,
      @Value("${openworkflow.cluster.pod-ip:}") String podIp,
      @Value("${openworkflow.cluster.artery-port}") int arteryPort,
      @Value("${openworkflow.cluster.management-port}") int managementPort,
      @Value("${openworkflow.cluster.required-contact-points}") int contacts,
      @Value("${openworkflow.cluster.role:workflow-engine}") String role) {
    PersistenceProfile profile = PersistenceProfile.parse(profileName);
    var configuration =
        PersistenceConfigLoader.withConnection(
            PersistenceConfigLoader.select(ConfigFactory.load(), profile),
            profile,
            endpoint,
            username,
            password,
            datacenter);
    return new PekkoEngineRuntime(
        systemName,
        configuration,
        new PekkoClusterRuntime.Settings(
            discovery, podIp, arteryPort, managementPort, contacts, role),
        askTimeout,
        events);
  }

  @Singleton
  ExecutionEngineProvider provider(PekkoEngineRuntime runtime) {
    return runtime.provider();
  }

  @Singleton
  EngineCommandResource commands(ExecutionEngineProvider provider) {
    return new EngineCommandResource(provider);
  }
}
