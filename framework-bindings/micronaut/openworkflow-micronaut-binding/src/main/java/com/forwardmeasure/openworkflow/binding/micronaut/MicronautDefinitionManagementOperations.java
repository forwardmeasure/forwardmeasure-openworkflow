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

import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.management.DefinitionValidation;
import com.forwardmeasure.openworkflow.definition.management.ManagedWorkflowRevision;
import com.forwardmeasure.openworkflow.definition.persistence.JpaDefinitionManagementOperations;
import jakarta.inject.Singleton;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import java.util.List;

/** Micronaut-managed transactional boundary around the portable JPA operations. */
@Singleton
@Transactional
public class MicronautDefinitionManagementOperations extends JpaDefinitionManagementOperations {
  public MicronautDefinitionManagementOperations(
      EntityManager entityManager,
      TenantScope tenantScope,
      AuthorizationService authorization,
      OpenWorkflowCompiler compiler) {
    super(entityManager, tenantScope, authorization, compiler);
  }

  @Override
  @Transactional
  public ManagedWorkflowRevision create(
      ActiveOrganization actor,
      String correlationId,
      String definitionKey,
      String displayName,
      String sourceDocument) {
    return super.create(actor, correlationId, definitionKey, displayName, sourceDocument);
  }

  @Override
  @Transactional
  public DefinitionValidation validate(
      ActiveOrganization actor, String correlationId, String definitionKey, String sourceDocument) {
    return super.validate(actor, correlationId, definitionKey, sourceDocument);
  }

  @Override
  @Transactional
  public ManagedWorkflowRevision revise(
      ActiveOrganization actor,
      String correlationId,
      String definitionKey,
      String displayName,
      String sourceDocument) {
    return super.revise(actor, correlationId, definitionKey, displayName, sourceDocument);
  }

  @Override
  @Transactional
  public ManagedWorkflowRevision submit(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return super.submit(actor, correlationId, definitionKey, revisionNumber);
  }

  @Override
  @Transactional
  public ManagedWorkflowRevision withdraw(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return super.withdraw(actor, correlationId, definitionKey, revisionNumber);
  }

  @Override
  @Transactional
  public ManagedWorkflowRevision approve(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return super.approve(actor, correlationId, definitionKey, revisionNumber);
  }

  @Override
  @Transactional
  public ManagedWorkflowRevision reject(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return super.reject(actor, correlationId, definitionKey, revisionNumber);
  }

  @Override
  @Transactional
  public ManagedWorkflowRevision publish(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return super.publish(actor, correlationId, definitionKey, revisionNumber);
  }

  @Override
  @Transactional
  public ManagedWorkflowRevision deprecate(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return super.deprecate(actor, correlationId, definitionKey, revisionNumber);
  }

  @Override
  @Transactional
  public ManagedWorkflowRevision retrieve(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber) {
    return super.retrieve(actor, correlationId, definitionKey, revisionNumber);
  }

  @Override
  @Transactional
  public List<ManagedWorkflowRevision> list(ActiveOrganization actor, String correlationId) {
    return super.list(actor, correlationId);
  }
}
