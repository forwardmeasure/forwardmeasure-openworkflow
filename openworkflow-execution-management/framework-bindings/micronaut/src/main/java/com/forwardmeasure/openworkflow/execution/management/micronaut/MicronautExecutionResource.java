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
import com.forwardmeasure.openworkflow.execution.jaxrs.ExecutionContextProvider;
import com.forwardmeasure.openworkflow.execution.jaxrs.ExecutionResource;
import com.forwardmeasure.openworkflow.execution.management.ExecutionManagementService;
import com.forwardmeasure.openworkflow.execution.query.ExecutionQueryRepository;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import jakarta.inject.Singleton;
import java.util.UUID;

/**
 * Micronaut compile-time discovery edge for the shared portable execution resource. Path is
 * inherited from {@code ExecutionsApi} via {@code micronaut-jaxrs-server} - must not be
 * hand-declared here, matching {@link com.forwardmeasure.openworkflow.definition.management
 * .micronaut.MicronautWorkflowDefinitionResource}'s pattern. Extends {@link ExecutionResource}
 * rather than composing it - path is inherited unchanged; this class adds nothing but the CDI scope
 * Micronaut needs to discover it as a bean at all. Neither tenant-schema binding nor the
 * transaction boundary is this class's concern - both happen in {@code management} and (via {@code
 * TenantScopedExecutionQueryRepository}) in {@code queries} themselves; see this module's binding
 * factory for that wiring.
 */
@Singleton
@Secured(SecurityRule.IS_AUTHENTICATED)
public class MicronautExecutionResource extends ExecutionResource {

  public MicronautExecutionResource(
      ExecutionManagementService management,
      ExecutionQueryRepository queries,
      ExecutionContextProvider contexts,
      ObjectMapper objectMapper) {
    super(management, queries, contexts, objectMapper, UUID::randomUUID);
  }
}
