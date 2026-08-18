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
package com.forwardmeasure.openworkflow.execution.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.execution.management.ExecutionRepository;
import com.forwardmeasure.openworkflow.execution.management.ExecutionRepositoryFactory;
import com.forwardmeasure.openworkflow.execution.management.PublishedWorkflowResolver;
import com.forwardmeasure.openworkflow.execution.management.PublishedWorkflowResolverFactory;
import jakarta.persistence.EntityManager;
import java.util.Objects;

/** Creates schema-qualified execution and publication adapters from one shared entity manager. */
public final class JpaExecutionPersistenceFactory
    implements ExecutionRepositoryFactory, PublishedWorkflowResolverFactory {
  private final EntityManager entityManager;
  private final ObjectMapper objectMapper;

  public JpaExecutionPersistenceFactory(EntityManager entityManager, ObjectMapper objectMapper) {
    this.entityManager = Objects.requireNonNull(entityManager, "entityManager");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
  }

  @Override
  public ExecutionRepository forTenant(TenantId tenantId) {
    return new JpaExecutionRepository(jpaTenant(tenantId), entityManager, objectMapper);
  }

  @Override
  public PublishedWorkflowResolver resolverForTenant(TenantId tenantId) {
    return new JpaPublishedWorkflowResolver(jpaTenant(tenantId), entityManager);
  }

  private static com.forwardmeasure.jpa.tenancy.TenantId jpaTenant(TenantId tenantId) {
    return new com.forwardmeasure.jpa.tenancy.TenantId(
        Objects.requireNonNull(tenantId, "tenantId").value());
  }
}
