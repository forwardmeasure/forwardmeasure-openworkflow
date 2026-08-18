/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.binding.micronaut;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionControl;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionStart;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionState;
import com.forwardmeasure.openworkflow.execution.jaxrs.ExecutionContextProvider;
import com.forwardmeasure.openworkflow.execution.jaxrs.ExecutionResource;
import com.forwardmeasure.openworkflow.execution.management.ExecutionManagementService;
import com.forwardmeasure.openworkflow.execution.query.ExecutionQueryRepository;
import jakarta.inject.Singleton;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.core.Response;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/** Micronaut transaction boundary around the portable execution resource. */
@Singleton
public class MicronautExecutionResource {
  private final ExecutionResource delegate;

  MicronautExecutionResource(
      ExecutionManagementService management,
      ExecutionQueryRepository queries,
      ExecutionContextProvider contexts,
      ObjectMapper objectMapper) {
    delegate = new ExecutionResource(management, queries, contexts, objectMapper, UUID::randomUUID);
  }

  @Transactional
  public Response start(String key, String correlation, ExecutionStart request) {
    return delegate.startExecution(key, correlation, request);
  }

  @Transactional
  public Response control(
      UUID id, String operation, String correlation, Long version, ExecutionControl request) {
    return delegate.controlExecution(id, operation, correlation, version, request);
  }

  @Transactional
  public Response get(UUID id) {
    return delegate.getExecution(id);
  }

  @Transactional
  public Response history(UUID id, Long after, Integer limit) {
    return delegate.getExecutionHistory(id, after, limit);
  }

  @Transactional
  public Response list(
      List<ExecutionState> states,
      String engine,
      String correlation,
      Date from,
      Date until,
      String cursor,
      Integer limit) {
    return delegate.listExecutions(states, engine, correlation, from, until, cursor, limit);
  }
}
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
