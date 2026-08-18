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
package com.forwardmeasure.openworkflow.binding.spring;

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
import com.forwardmeasure.openworkflow.definition.persistence.JpaDefinitionManagementOperations;
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
import jakarta.persistence.EntityManagerFactory;
import java.net.URI;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.oauth2.server.resource.web.authentication.BearerTokenAuthenticationFilter;
import org.springframework.security.web.SecurityFilterChain;

/** Spring composition for the portable definition-management resource. */
@Configuration(proxyBeanMethods = false)
public class OpenWorkflowSpringBinding {
  @Bean
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

  @Bean
  OpenWorkflowCompiler openWorkflowCompiler() {
    return new OpenWorkflowCompiler();
  }

  @Bean
  SecurityFilterChain openWorkflowSecurity(
      HttpSecurity http, SpringTenantScopeFilter tenantScopeFilter) throws Exception {
    return http.csrf(csrf -> csrf.disable())
        .authorizeHttpRequests(requests -> requests.anyRequest().authenticated())
        .oauth2ResourceServer(resourceServer -> resourceServer.jwt(Customizer.withDefaults()))
        .addFilterAfter(tenantScopeFilter, BearerTokenAuthenticationFilter.class)
        .build();
  }

  @Bean
  FilterRegistrationBean<SpringTenantScopeFilter> tenantScopeFilterRegistration(
      SpringTenantScopeFilter filter) {
    var registration = new FilterRegistrationBean<>(filter);
    registration.setEnabled(false);
    return registration;
  }

  @Bean
  DefinitionManagementOperations definitionManagementOperations(
      EntityManagerFactory entityManagerFactory,
      TenantScope tenants,
      AuthorizationService authorization,
      OpenWorkflowCompiler compiler) {
    return new JpaDefinitionManagementOperations(
        SharedEntityManagerCreator.createSharedEntityManager(entityManagerFactory),
        tenants,
        authorization,
        compiler);
  }

  @Bean
  DefinitionManagementResource definitionManagementResource(
      DefinitionManagementOperations operations, ActiveOrganizationProvider organizations) {
    return new DefinitionManagementResource(operations, organizations);
  }

  @Bean
  SpringExecutionEventSink executionEventSink(
      TenantScope tenants, EntityManagerFactory entityManagerFactory, ObjectMapper objectMapper) {
    return new SpringExecutionEventSink(tenants, entityManagerFactory, objectMapper);
  }

  @Bean(initMethod = "start", destroyMethod = "close")
  OksKafkaRuntime kafkaRuntime(
      SpringExecutionEventSink events,
      @Value("${openworkflow.kafka.bootstrap-servers}") String bootstrap,
      @Value("${openworkflow.kafka.application-id}") String applicationId,
      @Value("${openworkflow.kafka.instance-id}") String instanceId,
      @Value("${openworkflow.kafka.topic-prefix}") String topicPrefix,
      @Value("${openworkflow.kafka.state-dir}") String stateDirectory) {
    return new OksKafkaRuntime(
        bootstrap,
        applicationId,
        instanceId,
        Path.of(stateDirectory),
        OksTopics.withPrefix(topicPrefix),
        com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId.parse(
            "did:forwardmeasure:actor:kafka-runtime"),
        events);
  }

  @Bean(destroyMethod = "close")
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

  @Bean(initMethod = "start", destroyMethod = "close")
  KafkaOperationAdapterRuntime operationAdapters(
      AuthorizationService authorization,
      ObjectMapper objectMapper,
      @Value("${openworkflow.kafka.bootstrap-servers}") String bootstrap,
      @Value("${openworkflow.kafka.application-id}") String applicationId,
      @Value("${openworkflow.kafka.instance-id}") String instanceId,
      @Value("${openworkflow.kafka.topic-prefix}") String topicPrefix,
      @Value(
              "${openworkflow.adapters.consumer-group:${openworkflow.kafka.application-id}-operations}")
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
    return new KafkaOperationAdapterRuntime(
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
  }

  @Bean
  ExecutionEngineProvider executionEngineProvider(
      OksKafkaRuntime runtime,
      OksKafkaCommandGateway gateway,
      KafkaOperationAdapterRuntime operationAdapters,
      SpringExecutionEventSink events) {
    runtime.ready();
    operationAdapters.ready();
    return new KafkaStreamsExecutionEngineProvider(gateway, events, Clock.systemUTC(), true);
  }

  @Bean
  JpaExecutionPersistenceFactory executionPersistence(
      EntityManagerFactory entityManagerFactory, ObjectMapper objectMapper) {
    return new JpaExecutionPersistenceFactory(
        SharedEntityManagerCreator.createSharedEntityManager(entityManagerFactory), objectMapper);
  }

  @Bean
  ExecutionQueryRepository executionQueries(
      EntityManagerFactory entityManagerFactory, ObjectMapper objectMapper) {
    return new JpaTenantRoutingExecutionStore(
        SharedEntityManagerCreator.createSharedEntityManager(entityManagerFactory), objectMapper);
  }

  @Bean
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

  @Bean
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

  @Bean
  SpringExecutionResource executionResource(
      ExecutionManagementService management,
      ExecutionQueryRepository queries,
      ExecutionContextProvider contexts,
      ObjectMapper objectMapper) {
    return new SpringExecutionResource(management, queries, contexts, objectMapper);
  }

  @Bean
  ResourceConfig openWorkflowResources(
      DefinitionManagementResource resource,
      SpringExecutionResource executions,
      StudioAuthorizationResource authorizations) {
    return new ResourceConfig().register(resource).register(executions).register(authorizations);
  }

  @Bean
  StudioAuthorizationResource studioAuthorizationResource(
      AuthorizationService authorization, ActiveOrganizationProvider organizations) {
    return new StudioAuthorizationResource(authorization, organizations);
  }
}
