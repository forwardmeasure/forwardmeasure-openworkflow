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
package com.forwardmeasure.openworkflow.binding.micronaut;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.adapter.kafka.KafkaOperationAdapterRuntime;
import com.forwardmeasure.openworkflow.adapter.kafka.KafkaProtocolOperationExecutors;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.authorization.authzen.AuthzenAuthorizationFactory;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.management.DefinitionManagementOperations;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.DefinitionManagementResource;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.StudioAuthorizationResource;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProviders;
import com.forwardmeasure.openworkflow.execution.jaxrs.ExecutionContextProvider;
import com.forwardmeasure.openworkflow.execution.management.AuthzenExecutionAuthorizer;
import com.forwardmeasure.openworkflow.execution.management.ExecutionManagementService;
import com.forwardmeasure.openworkflow.execution.persistence.JpaExecutionPersistenceFactory;
import com.forwardmeasure.openworkflow.execution.query.ExecutionQueryRepository;
import com.forwardmeasure.openworkflow.execution.query.persistence.JpaTenantRoutingExecutionStore;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.KafkaStreamsExecutionEngineProvider;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.OksKafkaCommandGateway;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.OksKafkaRuntime;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.OksTopics;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import java.net.URI;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

/** Micronaut composition for the portable definition-management resource. */
@Factory
public class OpenWorkflowMicronautBinding {
  @Singleton
  ObjectMapper objectMapper() {
    return new ObjectMapper();
  }

  @Singleton
  AuthorizationService authorizationService(
      ObjectMapper mapper,
      @Value("${openworkflow.authorization.issuer}") URI issuer,
      @Value("${openworkflow.authorization.client-id}") String clientId,
      @Value("${openworkflow.authorization.client-secret}") String clientSecret,
      @Value("${openworkflow.authorization.request-timeout}") Duration requestTimeout,
      @Value("${openworkflow.authorization.decision-ttl}") Duration decisionTtl,
      @Value("${openworkflow.authorization.maximum-cache-entries}") int cacheEntries,
      @Value("${openworkflow.authorization.policy-version}") String policyVersion) {
    return AuthzenAuthorizationFactory.create(
        mapper,
        issuer,
        clientId,
        clientSecret,
        requestTimeout,
        decisionTtl,
        cacheEntries,
        policyVersion);
  }

  @Singleton
  OpenWorkflowCompiler openWorkflowCompiler() {
    return new OpenWorkflowCompiler();
  }

  @Singleton
  DefinitionManagementResource definitionManagementResource(
      DefinitionManagementOperations operations, ActiveOrganizationProvider organizations) {
    return new DefinitionManagementResource(operations, organizations);
  }

  @Singleton
  StudioAuthorizationResource studioAuthorizationResource(
      AuthorizationService authorization, ActiveOrganizationProvider organizations) {
    return new StudioAuthorizationResource(authorization, organizations);
  }

  @Singleton
  MicronautExecutionEventSink executionEventSink(
      TenantScope tenants, EntityManagerFactory entityManagerFactory, ObjectMapper objectMapper) {
    return new MicronautExecutionEventSink(tenants, entityManagerFactory, objectMapper);
  }

  @Singleton
  @Bean(preDestroy = "close")
  OksKafkaRuntime kafkaRuntime(
      MicronautExecutionEventSink events,
      @Value("${openworkflow.kafka.bootstrap-servers}") String bootstrap,
      @Value("${openworkflow.kafka.application-id}") String applicationId,
      @Value("${openworkflow.kafka.instance-id}") String instanceId,
      @Value("${openworkflow.kafka.topic-prefix}") String topicPrefix,
      @Value("${openworkflow.kafka.state-dir}") String stateDirectory) {
    var runtime =
        new OksKafkaRuntime(
            bootstrap,
            applicationId,
            instanceId,
            Path.of(stateDirectory),
            OksTopics.withPrefix(topicPrefix),
            com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId.parse(
                "did:forwardmeasure:actor:kafka-runtime"),
            events);
    runtime.start();
    return runtime;
  }

  @Singleton
  @Bean(preDestroy = "close")
  OksKafkaCommandGateway kafkaGateway(
      @Value("${openworkflow.kafka.bootstrap-servers}") String bootstrap,
      @Value("${openworkflow.kafka.application-id}") String applicationId,
      @Value("${openworkflow.kafka.instance-id}") String instanceId,
      @Value("${openworkflow.kafka.topic-prefix}") String topicPrefix) {
    return new OksKafkaCommandGateway(
        bootstrap,
        applicationId + '-' + instanceId + "-ingress",
        OksTopics.withPrefix(topicPrefix),
        Clock.systemUTC());
  }

  @Singleton
  @Bean(preDestroy = "close")
  KafkaOperationAdapterRuntime operationAdapters(
      AuthorizationService authorization,
      ObjectMapper objectMapper,
      @Value("${openworkflow.kafka.bootstrap-servers}") String bootstrap,
      @Value("${openworkflow.kafka.instance-id}") String instanceId,
      @Value("${openworkflow.kafka.topic-prefix}") String topicPrefix,
      @Value("${openworkflow.adapters.consumer-group:openworkflow-kafka-operations}")
          String consumerGroup,
      @Value("${openworkflow.adapters.secret-directory:/var/run/secrets/openworkflow}")
          String secretDirectory,
      @Value("${openworkflow.adapters.http-egress-allowlist:}") String egressAllowlist,
      @Value("${openworkflow.operations.protocol.timeout-ms:30000}") long protocolTimeout,
      @Value("${openworkflow.operations.mcp-command-allowlist:}") String mcpCommands,
      @Value("${openworkflow.operations.run.command-allowlist:}") String runCommands,
      @Value("${openworkflow.operations.run.interpreter-allowlist:}") String runInterpreters,
      @Value("${openworkflow.operations.run.image-allowlist:}") String runImages,
      @Value("${openworkflow.operations.run.volume-allowlist:}") String runVolumes,
      @Value("${openworkflow.operations.run.port-allowlist:}") String runPorts,
      @Value("${openworkflow.operations.run.oci-runtime:podman}") String ociRuntime) {
    OksTopics topics = OksTopics.withPrefix(topicPrefix);
    var runtime =
        new KafkaOperationAdapterRuntime(
            bootstrap,
            topics.effects(),
            topics.definitions(),
            topics.commands(),
            topics.deadLetters(),
            consumerGroup,
            instanceId,
            authorization,
            objectMapper,
            secretDirectory,
            egressAllowlist,
            KafkaProtocolOperationExecutors.create(
                authorization,
                objectMapper,
                new KafkaProtocolOperationExecutors.Configuration(
                    protocolTimeout,
                    egressAllowlist,
                    secretDirectory,
                    mcpCommands,
                    runCommands,
                    runInterpreters,
                    runImages,
                    runVolumes,
                    runPorts,
                    ociRuntime)));
    runtime.start();
    return runtime;
  }

  @Singleton
  ExecutionEngineProvider executionEngineProvider(
      OksKafkaRuntime runtime,
      OksKafkaCommandGateway gateway,
      KafkaOperationAdapterRuntime operationAdapters,
      MicronautExecutionEventSink events) {
    runtime.ready();
    operationAdapters.ready();
    return new KafkaStreamsExecutionEngineProvider(gateway, events, Clock.systemUTC(), true);
  }

  @Singleton
  JpaExecutionPersistenceFactory executionPersistence(
      EntityManager entityManager, ObjectMapper objectMapper) {
    return new JpaExecutionPersistenceFactory(entityManager, objectMapper);
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

  @Singleton
  ExecutionManagementService executionManagement(
      JpaExecutionPersistenceFactory persistence,
      ExecutionEngineProvider provider,
      AuthorizationService authorization,
      ActiveOrganizationProvider organizations) {
    return new ExecutionManagementService(
        persistence,
        new AuthzenExecutionAuthorizer(authorization, ignored -> organizations.current()),
        persistence,
        new ExecutionEngineProviders(List.of(provider)),
        ignored -> EngineId.KAFKA_STREAMS,
        Clock.systemUTC(),
        UUID::randomUUID);
  }
}
