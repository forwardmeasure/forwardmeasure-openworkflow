/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.deployment.engine.pekko.quarkus;

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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class PekkoEngineQuarkusBinding {
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
  PekkoEngineRuntime runtime(
      ExecutionEventSink events,
      @ConfigProperty(name = "openworkflow.pekko.system-name") String systemName,
      @ConfigProperty(name = "openworkflow.pekko.ask-timeout") Duration askTimeout,
      @ConfigProperty(name = "openworkflow.persistence.profile") String profileName,
      @ConfigProperty(name = "openworkflow.persistence.endpoint") String endpoint,
      @ConfigProperty(name = "openworkflow.persistence.username", defaultValue = "")
          String username,
      @ConfigProperty(name = "openworkflow.persistence.password", defaultValue = "")
          String password,
      @ConfigProperty(
              name = "openworkflow.persistence.local-datacenter",
              defaultValue = "datacenter1")
          String datacenter,
      @ConfigProperty(name = "openworkflow.cluster.discovery-service", defaultValue = "")
          String discovery,
      @ConfigProperty(name = "openworkflow.cluster.pod-ip", defaultValue = "") String podIp,
      @ConfigProperty(name = "openworkflow.cluster.artery-port") int arteryPort,
      @ConfigProperty(name = "openworkflow.cluster.management-port") int managementPort,
      @ConfigProperty(name = "openworkflow.cluster.required-contact-points") int contacts,
      @ConfigProperty(name = "openworkflow.cluster.role", defaultValue = "workflow-engine")
          String role) {
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

  void close(@Disposes PekkoEngineRuntime runtime) {
    runtime.close();
  }

  @Produces
  @ApplicationScoped
  ExecutionEngineProvider provider(PekkoEngineRuntime runtime) {
    return runtime.provider();
  }

  @Produces
  @ApplicationScoped
  EngineCommandResource commands(ExecutionEngineProvider provider) {
    return new EngineCommandResource(provider);
  }
}
