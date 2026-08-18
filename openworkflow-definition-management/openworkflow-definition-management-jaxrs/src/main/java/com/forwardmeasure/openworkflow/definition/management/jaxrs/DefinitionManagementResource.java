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
package com.forwardmeasure.openworkflow.definition.management.jaxrs;

import com.forwardmeasure.openworkflow.definition.management.DefinitionManagementOperations;
import com.forwardmeasure.openworkflow.definition.management.ManagedWorkflowRevision;
import com.forwardmeasure.openworkflow.definition.management.api.DefinitionsApi;
import com.forwardmeasure.openworkflow.definition.management.api.model.DefinitionList;
import com.forwardmeasure.openworkflow.definition.management.api.model.DefinitionRevision;
import com.forwardmeasure.openworkflow.definition.management.api.model.DefinitionValidationRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.DefinitionWrite;
import com.forwardmeasure.openworkflow.definition.management.api.model.LifecycleAction;
import com.forwardmeasure.openworkflow.definition.management.api.model.LifecycleState;
import com.forwardmeasure.openworkflow.definition.management.api.model.RevisionWrite;
import jakarta.ws.rs.core.Response;
import java.util.Objects;

/** Portable implementation of the generated transport contract. */
public final class DefinitionManagementResource implements DefinitionsApi {
  private final DefinitionManagementOperations service;
  private final ActiveOrganizationProvider organizations;

  public DefinitionManagementResource(
      DefinitionManagementOperations service, ActiveOrganizationProvider organizations) {
    this.service = Objects.requireNonNull(service, "service");
    this.organizations = Objects.requireNonNull(organizations, "organizations");
  }

  @Override
  public Response createDefinition(String correlationId, DefinitionWrite request) {
    return Response.status(Response.Status.CREATED)
        .entity(
            map(
                service.create(
                    organizations.current(),
                    correlationId,
                    request.getDefinitionKey(),
                    request.getDisplayName(),
                    request.getSourceDocument())))
        .build();
  }

  @Override
  public Response listDefinitions(String correlationId) {
    return Response.ok(
            new DefinitionList(
                service.list(organizations.current(), correlationId).stream()
                    .map(DefinitionManagementResource::map)
                    .toList()))
        .build();
  }

  @Override
  public Response retrieveDefinition(
      String definitionKey, Integer revisionNumber, String correlationId) {
    return Response.ok(
            map(
                service.retrieve(
                    organizations.current(), correlationId, definitionKey, revisionNumber)))
        .build();
  }

  @Override
  public Response reviseDefinition(
      String definitionKey, String correlationId, RevisionWrite request) {
    return Response.status(Response.Status.CREATED)
        .entity(
            map(
                service.revise(
                    organizations.current(),
                    correlationId,
                    definitionKey,
                    request.getDisplayName(),
                    request.getSourceDocument())))
        .build();
  }

  @Override
  public Response transitionDefinition(
      String definitionKey, Integer revisionNumber, String correlationId, LifecycleAction action) {
    ManagedWorkflowRevision revision =
        switch (action) {
          case SUBMIT ->
              service.submit(organizations.current(), correlationId, definitionKey, revisionNumber);
          case WITHDRAW ->
              service.withdraw(
                  organizations.current(), correlationId, definitionKey, revisionNumber);
          case APPROVE ->
              service.approve(
                  organizations.current(), correlationId, definitionKey, revisionNumber);
          case REJECT ->
              service.reject(organizations.current(), correlationId, definitionKey, revisionNumber);
          case PUBLISH ->
              service.publish(
                  organizations.current(), correlationId, definitionKey, revisionNumber);
          case DEPRECATE ->
              service.deprecate(
                  organizations.current(), correlationId, definitionKey, revisionNumber);
        };
    return Response.ok(map(revision)).build();
  }

  @Override
  public Response validateDefinition(String correlationId, DefinitionValidationRequest request) {
    var validation =
        service.validate(
            organizations.current(),
            correlationId,
            request.getDefinitionKey(),
            request.getSourceDocument());
    return Response.ok(
            new com.forwardmeasure.openworkflow.definition.management.api.model
                .DefinitionValidation(
                validation.sourceDigest(),
                validation.resolvedDigest(),
                com.forwardmeasure.openworkflow.definition.management.api.model.DefinitionValidation
                    .SpecificationVersionEnum.fromValue(validation.specificationVersion()),
                validation.compilerProfile()))
        .build();
  }

  private static DefinitionRevision map(ManagedWorkflowRevision revision) {
    return new DefinitionRevision(
        revision.revisionId(),
        revision.definitionKey(),
        revision.displayName(),
        revision.revisionNumber(),
        LifecycleState.fromValue(revision.lifecycleState().name()),
        revision.sourceDocument(),
        revision.resolvedDocument(),
        DefinitionRevision.SpecificationVersionEnum.fromValue(revision.specificationVersion()),
        revision.compilerProfile(),
        revision.sourceDigest(),
        revision.resolvedDigest(),
        revision.authorActorId());
  }
}
