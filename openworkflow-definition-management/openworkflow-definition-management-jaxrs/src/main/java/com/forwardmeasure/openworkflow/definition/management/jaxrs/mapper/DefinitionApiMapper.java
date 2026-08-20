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
package com.forwardmeasure.openworkflow.definition.management.jaxrs.mapper;

import com.forwardmeasure.jpa.core.query.Page;
import com.forwardmeasure.jpa.identity.entity.Actor;
import com.forwardmeasure.openworkflow.definition.management.api.model.Workflow;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinition;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinitionAuthor;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinitionPage;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowDefinitionPublishedBy;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowOwner;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowPage;
import com.forwardmeasure.openworkflow.definition.management.api.model.WorkflowPagePaging;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.List;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

/**
 * Maps {@code -application}'s persistence entities directly to the generated response DTOs — no
 * intermediate canonical type, per the project's canonical-type rule. The only reason this is more
 * than a handful of {@code @Mapping} annotations: the OpenAPI generator doesn't dedupe schemas
 * referenced across files, so {@code ActorReference} became three separately-named but identical
 * classes ({@link WorkflowOwner}, {@link WorkflowDefinitionAuthor}, {@link
 * WorkflowDefinitionPublishedBy}), each needing its own mapping method.
 */
@Mapper
public interface DefinitionApiMapper {
  DefinitionApiMapper INSTANCE = Mappers.getMapper(DefinitionApiMapper.class);

  @Mapping(target = "id", source = "uuid")
  @Mapping(target = "revision", source = "version")
  Workflow toWorkflow(com.forwardmeasure.openworkflow.definition.domain.entity.Workflow workflow);

  @Mapping(target = "id", source = "uuid")
  @Mapping(target = "subject", source = "subjectIdentifier")
  @Mapping(target = "displayName", source = "email")
  WorkflowOwner toWorkflowOwner(Actor actor);

  @Mapping(target = "id", source = "uuid")
  @Mapping(target = "workflowId", source = "workflow.uuid")
  @Mapping(target = "version", source = "documentVersion")
  @Mapping(target = "dsl", source = "specificationVersion")
  @Mapping(target = "status", source = "lifecycleState")
  @Mapping(target = "source", source = "sourceDocument")
  @Mapping(target = "sourceSha256", source = "sourceDigest")
  @Mapping(target = "definitionSha256", source = "resolvedDigest")
  @Mapping(target = "compilerSha256", source = "compilerProfile")
  @Mapping(target = "publishedBy", source = "publication.actor")
  @Mapping(target = "publishedAt", source = "publication.publishedAt")
  @Mapping(target = "deprecatedAt", source = "publication.deprecatedAt")
  @Mapping(target = "revision", source = "version")
  WorkflowDefinition toWorkflowDefinition(
      com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowDefinition definition);

  @Mapping(target = "id", source = "uuid")
  @Mapping(target = "subject", source = "subjectIdentifier")
  @Mapping(target = "displayName", source = "email")
  WorkflowDefinitionAuthor toWorkflowDefinitionAuthor(Actor actor);

  @Mapping(target = "id", source = "uuid")
  @Mapping(target = "subject", source = "subjectIdentifier")
  @Mapping(target = "displayName", source = "email")
  WorkflowDefinitionPublishedBy toWorkflowDefinitionPublishedBy(Actor actor);

  default Date map(OffsetDateTime value) {
    return value == null ? null : Date.from(value.toInstant());
  }

  default WorkflowPage toWorkflowPage(
      Page<com.forwardmeasure.openworkflow.definition.domain.entity.Workflow> page) {
    List<Workflow> data = page.items().stream().map(this::toWorkflow).toList();
    return new WorkflowPage(data, toPaging(page));
  }

  default WorkflowDefinitionPage toWorkflowDefinitionPage(
      Page<com.forwardmeasure.openworkflow.definition.domain.entity.WorkflowDefinition> page) {
    List<WorkflowDefinition> data = page.items().stream().map(this::toWorkflowDefinition).toList();
    return new WorkflowDefinitionPage(data, toPaging(page));
  }

  private WorkflowPagePaging toPaging(Page<?> page) {
    long consumed = (long) page.offset() + page.items().size();
    boolean isLastPage = consumed >= page.totalItems();
    return new WorkflowPagePaging(page.offset(), page.limit(), isLastPage)
        .nextPageStart(isLastPage ? null : (int) consumed)
        .totalCount(page.totalItems());
  }
}
