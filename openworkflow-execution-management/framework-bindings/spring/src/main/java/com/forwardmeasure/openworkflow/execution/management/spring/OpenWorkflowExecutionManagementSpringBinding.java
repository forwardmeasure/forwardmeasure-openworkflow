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
package com.forwardmeasure.openworkflow.execution.management.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.execution.jaxrs.ExecutionContextProvider;
import com.forwardmeasure.openworkflow.execution.management.ExecutionManagementService;
import com.forwardmeasure.openworkflow.execution.query.ExecutionQueryRepository;
import com.forwardmeasure.openworkflow.execution.query.persistence.JpaTenantRoutingExecutionStore;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.boot.jersey.autoconfigure.ResourceConfigCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.SharedEntityManagerCreator;

/**
 * Spring composition for the query-side store, the actor-context bridge, and the REST resource
 * itself. The engine/command-orchestration wiring lives in {@code
 * openworkflow-engine-spring-binding} instead - not capability-specific. Registers into the single
 * central Jersey {@code ResourceConfig} via {@link ResourceConfigCustomizer}, same pattern as
 * {@code OpenWorkflowDefinitionManagementSpringBinding}.
 */
@Configuration(proxyBeanMethods = false)
public class OpenWorkflowExecutionManagementSpringBinding {

  @Bean
  SpringExecutionEventSink executionEventSink(
      TenantScope tenants, EntityManagerFactory entityManagerFactory, ObjectMapper objectMapper) {
    return new SpringExecutionEventSink(tenants, entityManagerFactory, objectMapper);
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
  SpringExecutionResource executionResource(
      ExecutionManagementService management,
      ExecutionQueryRepository queries,
      ExecutionContextProvider contexts,
      ObjectMapper objectMapper) {
    return new SpringExecutionResource(management, queries, contexts, objectMapper);
  }

  @Bean
  ResourceConfigCustomizer executionManagementResourceConfigCustomizer(
      SpringExecutionResource executions) {
    return resourceConfig -> resourceConfig.register(executions);
  }
}
