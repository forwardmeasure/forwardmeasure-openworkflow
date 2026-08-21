/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.forwardmeasure.openworkflow.execution.management.quarkus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProviders;
import com.forwardmeasure.openworkflow.engine.http.HttpExecutionEngineProvider;
import com.forwardmeasure.openworkflow.execution.jaxrs.ExecutionContextProvider;
import com.forwardmeasure.openworkflow.execution.management.AuthzenExecutionAuthorizer;
import com.forwardmeasure.openworkflow.execution.management.ExecutionManagementService;
import com.forwardmeasure.openworkflow.execution.persistence.JpaExecutionPersistenceFactory;
import com.forwardmeasure.openworkflow.execution.query.ExecutionQueryRepository;
import com.forwardmeasure.openworkflow.execution.query.persistence.JpaTenantRoutingExecutionStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.persistence.EntityManager;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Quarkus composition for the query-side store and the actor-context bridge used by {@link
 * QuarkusExecutionResource}. The engine/command-orchestration wiring lives in {@code
 * openworkflow-engine-quarkus-binding} instead - not capability-specific.
 */
@ApplicationScoped
public class OpenWorkflowExecutionManagementQuarkusBinding {

  @Produces
  @ApplicationScoped
  JpaExecutionPersistenceFactory executionPersistence(
      EntityManager entityManager, ObjectMapper objectMapper) {
    return new JpaExecutionPersistenceFactory(entityManager, objectMapper);
  }

  @Produces
  @ApplicationScoped
  ExecutionManagementService executionManagement(
      JpaExecutionPersistenceFactory persistence,
      AuthorizationService authorization,
      ActiveOrganizationProvider organizations,
      ObjectMapper mapper,
      @ConfigProperty(name = "openworkflow.engines.kafka-streams.url") URI kafkaUrl,
      @ConfigProperty(name = "openworkflow.engines.pekko.url") URI pekkoUrl,
      @ConfigProperty(name = "openworkflow.engines.default") String defaultEngine,
      @ConfigProperty(name = "openworkflow.engines.timeout") Duration timeout) {
    HttpClient client = HttpClient.newBuilder().connectTimeout(timeout).build();
    var kafka =
        new HttpExecutionEngineProvider(EngineId.KAFKA_STREAMS, kafkaUrl, client, mapper, timeout);
    var pekko = new HttpExecutionEngineProvider(EngineId.PEKKO, pekkoUrl, client, mapper, timeout);
    EngineId selected = new EngineId(defaultEngine);
    return new ExecutionManagementService(
        persistence,
        new AuthzenExecutionAuthorizer(authorization, ignored -> organizations.current()),
        persistence,
        new ExecutionEngineProviders(List.of(kafka, pekko)),
        ignored -> selected,
        Clock.systemUTC(),
        UUID::randomUUID);
  }

  @Produces
  @ApplicationScoped
  ExecutionQueryRepository executionQueries(
      EntityManager entityManager, ObjectMapper objectMapper) {
    return new JpaTenantRoutingExecutionStore(entityManager, objectMapper);
  }

  @Produces
  @ApplicationScoped
  ExecutionContextProvider executionContext(ActiveOrganizationProvider organizations) {
    return () -> {
      var organization = organizations.current();
      return new com.forwardmeasure.openworkflow.engine.api.TenantActorContext(
          new com.forwardmeasure.openworkflow.engine.api.TenantId(organization.tenantId().value()),
          organization.organizationId(),
          new com.forwardmeasure.openworkflow.engine.api.ActorId(organization.actorId()),
          organization.organizationRoles());
    };
  }
}
