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
package com.forwardmeasure.openworkflow.definition.management.spring;

import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.binding.spring.SpringCorrelationIdProvider;
import com.forwardmeasure.openworkflow.binding.spring.SpringWorkflowTransactionExecutor;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.domain.service.WorkflowGovernanceService;
import com.forwardmeasure.openworkflow.definition.domain.service.WorkflowManagementService;
import com.forwardmeasure.openworkflow.definition.domain.service.jpa.JpaWorkflowGovernanceService;
import com.forwardmeasure.openworkflow.definition.domain.service.jpa.JpaWorkflowManagementService;
import com.forwardmeasure.openworkflow.definition.infrastructure.persistence.WorkflowTransactionExecutor;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.CorrelationIdProvider;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.StudioAuthorizationResource;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.WorkflowDefinitionGovernanceResource;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.WorkflowDefinitionResource;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.WorkflowManagementResource;
import jakarta.persistence.EntityManagerFactory;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.boot.jersey.autoconfigure.ResourceConfigCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Spring composition for the definition-management resources. Registers into the single Jersey
 * {@code ResourceConfig} via {@link ResourceConfigCustomizer} rather than a second {@code
 * ResourceConfig} bean or a parameter on the central module's - this module has no compile
 * dependency on the central {@code openworkflow-spring-binding} module's {@code ResourceConfig}
 * bean, and shouldn't need one; {@code StudioAuthorizationResource} stays produced centrally, it
 * touches no repository/EntityManager.
 */
@Configuration(proxyBeanMethods = false)
public class OpenWorkflowDefinitionManagementSpringBinding {

  @Bean
  OpenWorkflowCompiler openWorkflowCompiler() {
    return new OpenWorkflowCompiler();
  }

  @Bean
  WorkflowTransactionExecutor workflowTransactionExecutor(TransactionTemplate transactions) {
    return new SpringWorkflowTransactionExecutor(transactions);
  }

  @Bean
  CorrelationIdProvider correlationIdProvider(HttpServletRequest request) {
    return new SpringCorrelationIdProvider(request);
  }

  @Bean
  StudioAuthorizationResource studioAuthorizationResource(
      AuthorizationService authorization, ActiveOrganizationProvider organizations) {
    return new StudioAuthorizationResource(authorization, organizations);
  }

  @Bean
  WorkflowManagementService workflowManagementService(
      EntityManagerFactory entityManagerFactory,
      TenantScope tenantScope,
      WorkflowTransactionExecutor transactions,
      AuthorizationService authorization) {
    return new JpaWorkflowManagementService(
        SharedEntityManagerCreator.createSharedEntityManager(entityManagerFactory),
        tenantScope,
        transactions,
        authorization);
  }

  @Bean
  WorkflowGovernanceService workflowGovernanceService(
      EntityManagerFactory entityManagerFactory,
      TenantScope tenantScope,
      WorkflowTransactionExecutor transactions,
      AuthorizationService authorization,
      OpenWorkflowCompiler compiler) {
    return new JpaWorkflowGovernanceService(
        SharedEntityManagerCreator.createSharedEntityManager(entityManagerFactory),
        tenantScope,
        transactions,
        authorization,
        compiler);
  }

  @Bean
  WorkflowManagementResource workflowManagementResource(
      WorkflowManagementService service,
      ActiveOrganizationProvider organizations,
      CorrelationIdProvider correlationIds) {
    return new WorkflowManagementResource(service, organizations, correlationIds);
  }

  @Bean
  WorkflowDefinitionResource workflowDefinitionResource(
      WorkflowGovernanceService service,
      ActiveOrganizationProvider organizations,
      CorrelationIdProvider correlationIds) {
    return new WorkflowDefinitionResource(service, organizations, correlationIds);
  }

  @Bean
  WorkflowDefinitionGovernanceResource workflowDefinitionGovernanceResource(
      WorkflowGovernanceService service,
      ActiveOrganizationProvider organizations,
      CorrelationIdProvider correlationIds) {
    return new WorkflowDefinitionGovernanceResource(service, organizations, correlationIds);
  }

  @Bean
  ResourceConfigCustomizer definitionManagementResourceConfigCustomizer(
      WorkflowManagementResource workflows,
      WorkflowDefinitionResource definitions,
      WorkflowDefinitionGovernanceResource governance,
      StudioAuthorizationResource authorizations) {
    return resourceConfig ->
        resourceConfig
            .register(workflows)
            .register(definitions)
            .register(governance)
            .register(authorizations);
  }
}
