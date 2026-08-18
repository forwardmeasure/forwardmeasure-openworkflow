/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.binding.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.execution.api.ExecutionsApi;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionControl;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionStart;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionState;
import com.forwardmeasure.openworkflow.execution.jaxrs.ExecutionContextProvider;
import com.forwardmeasure.openworkflow.execution.jaxrs.ExecutionResource;
import com.forwardmeasure.openworkflow.execution.management.ExecutionManagementService;
import com.forwardmeasure.openworkflow.execution.query.ExecutionQueryRepository;
import jakarta.ws.rs.core.Response;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.springframework.transaction.annotation.Transactional;

/** Spring transaction boundary around the portable execution API. */
@Transactional
public class SpringExecutionResource implements ExecutionsApi {
  private final ExecutionResource delegate;

  SpringExecutionResource(
      ExecutionManagementService management,
      ExecutionQueryRepository queries,
      ExecutionContextProvider contexts,
      ObjectMapper objectMapper) {
    delegate = new ExecutionResource(management, queries, contexts, objectMapper, UUID::randomUUID);
  }

  @Override
  public Response startExecution(String key, String correlation, ExecutionStart request) {
    return delegate.startExecution(key, correlation, request);
  }

  @Override
  public Response controlExecution(
      UUID id, String operation, String correlation, Long version, ExecutionControl request) {
    return delegate.controlExecution(id, operation, correlation, version, request);
  }

  @Override
  @Transactional(readOnly = true)
  public Response getExecution(UUID id) {
    return delegate.getExecution(id);
  }

  @Override
  @Transactional(readOnly = true)
  public Response getExecutionHistory(UUID id, Long after, Integer limit) {
    return delegate.getExecutionHistory(id, after, limit);
  }

  @Override
  @Transactional(readOnly = true)
  public Response listExecutions(
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
