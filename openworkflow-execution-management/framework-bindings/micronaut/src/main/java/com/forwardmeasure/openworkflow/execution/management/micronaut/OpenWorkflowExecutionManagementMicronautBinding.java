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
package com.forwardmeasure.openworkflow.execution.management.micronaut;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProviders;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEventSink;
import com.forwardmeasure.openworkflow.engine.http.HttpExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.http.server.ExecutionEventResource;
import com.forwardmeasure.openworkflow.execution.jaxrs.ExecutionContextProvider;
import com.forwardmeasure.openworkflow.execution.management.AuthzenExecutionAuthorizer;
import com.forwardmeasure.openworkflow.execution.management.ExecutionManagementService;
import com.forwardmeasure.openworkflow.execution.persistence.JpaExecutionPersistenceFactory;
import com.forwardmeasure.openworkflow.execution.query.ExecutionQueryRepository;
import com.forwardmeasure.openworkflow.execution.query.persistence.JpaTenantRoutingExecutionStore;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Micronaut composition for the query-side store and the actor-context bridge used by {@link
 * MicronautExecutionResource}/{@link MicronautExecutionController}. The
 * engine/command-orchestration wiring lives in {@code openworkflow-engine-micronaut-binding}
 * instead - not capability-specific.
 */
@Factory
public class OpenWorkflowExecutionManagementMicronautBinding {

  @Singleton
  ExecutionEventResource executionEventResource(ExecutionEventSink sink) {
    return new ExecutionEventResource(sink);
  }

  @Singleton
  JpaExecutionPersistenceFactory executionPersistence(
      EntityManager entityManager, ObjectMapper objectMapper) {
    return new JpaExecutionPersistenceFactory(entityManager, objectMapper);
  }

  @Singleton
  ExecutionManagementService executionManagement(
      JpaExecutionPersistenceFactory persistence,
      AuthorizationService authorization,
      ActiveOrganizationProvider organizations,
      ObjectMapper mapper,
      @Value("${openworkflow.engines.kafka-streams.url}") URI kafkaUrl,
      @Value("${openworkflow.engines.pekko.url}") URI pekkoUrl,
      @Value("${openworkflow.engines.default}") String defaultEngine,
      @Value("${openworkflow.engines.timeout}") Duration timeout) {
    HttpClient client = HttpClient.newBuilder().connectTimeout(timeout).build();
    var kafka =
        new HttpExecutionEngineProvider(EngineId.KAFKA_STREAMS, kafkaUrl, client, mapper, timeout);
    var pekko = new HttpExecutionEngineProvider(EngineId.PEKKO, pekkoUrl, client, mapper, timeout);
    return new ExecutionManagementService(
        persistence,
        new AuthzenExecutionAuthorizer(authorization, ignored -> organizations.current()),
        persistence,
        new ExecutionEngineProviders(List.of(kafka, pekko)),
        ignored -> new EngineId(defaultEngine),
        Clock.systemUTC(),
        UUID::randomUUID);
  }

  @Singleton
  MicronautExecutionEventSink executionEventSink(
      TenantScope tenants, EntityManagerFactory entityManagerFactory, ObjectMapper objectMapper) {
    return new MicronautExecutionEventSink(tenants, entityManagerFactory, objectMapper);
  }

  @Singleton
  ExecutionQueryRepository executionQueries(
      EntityManager entityManager, ObjectMapper objectMapper) {
    return new JpaTenantRoutingExecutionStore(entityManager, objectMapper);
  }

  @Singleton
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
