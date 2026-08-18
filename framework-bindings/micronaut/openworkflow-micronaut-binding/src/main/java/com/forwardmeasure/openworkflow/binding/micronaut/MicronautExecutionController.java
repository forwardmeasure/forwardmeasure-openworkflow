/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.binding.micronaut;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionControl;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionStart;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionState;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.rules.SecurityRule;
import jakarta.ws.rs.core.Response;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

/** Micronaut routes delegating execution operations to the portable JAX-RS resource. */
@Controller("/api/v1/executions")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class MicronautExecutionController {
  private final MicronautExecutionResource delegate;
  private final MicronautActiveOrganizationProvider organizations;
  private final TenantScope tenants;
  private final ObjectMapper objectMapper;

  public MicronautExecutionController(
      MicronautExecutionResource delegate,
      MicronautActiveOrganizationProvider organizations,
      TenantScope tenants,
      ObjectMapper objectMapper) {
    this.delegate = delegate;
    this.organizations = organizations;
    this.tenants = tenants;
    this.objectMapper = objectMapper;
  }

  @Post
  HttpResponse<Object> start(
      @Header("Idempotency-Key") String key,
      @Header("X-Correlation-ID") String correlation,
      @Body ExecutionStart request,
      Authentication authentication) {
    return inTenant(authentication, () -> delegate.start(key, correlation, request));
  }

  @Post("/{id}/{operation}")
  HttpResponse<Object> control(
      @PathVariable UUID id,
      @PathVariable String operation,
      @Header("X-Correlation-ID") String correlation,
      @Header("If-Match") Long version,
      @Body ExecutionControl request,
      Authentication authentication) {
    return inTenant(
        authentication, () -> delegate.control(id, operation, correlation, version, request));
  }

  @Get("/{id}")
  HttpResponse<Object> get(@PathVariable UUID id, Authentication authentication) {
    return inTenant(authentication, () -> delegate.get(id));
  }

  @Get("/{id}/history")
  HttpResponse<Object> history(
      @PathVariable UUID id,
      @QueryValue(defaultValue = "0") Long after,
      @QueryValue(defaultValue = "100") Integer limit,
      Authentication authentication) {
    return inTenant(authentication, () -> delegate.history(id, after, limit));
  }

  @Get
  HttpResponse<Object> list(
      @QueryValue(defaultValue = "") List<ExecutionState> state,
      @QueryValue(defaultValue = "") String engine,
      @QueryValue(defaultValue = "") String correlation,
      @QueryValue(defaultValue = "") Date from,
      @QueryValue(defaultValue = "") Date until,
      @QueryValue(defaultValue = "") String cursor,
      @QueryValue(defaultValue = "100") Integer limit,
      Authentication authentication) {
    return inTenant(
        authentication,
        () -> delegate.list(state, engine, correlation, from, until, cursor, limit));
  }

  private HttpResponse<Object> inTenant(
      Authentication authentication, Supplier<Response> operation) {
    return organizations.call(
        authentication,
        () ->
            tenants.call(
                TenantSchema.forTenant(organizations.current().tenantId()),
                () -> adapt(operation.get())));
  }

  private HttpResponse<Object> adapt(Response source) {
    MutableHttpResponse<Object> target =
        HttpResponse.status(HttpStatus.valueOf(source.getStatus()));
    source
        .getHeaders()
        .forEach((name, values) -> values.forEach(value -> target.header(name, value.toString())));
    if (!source.hasEntity()) {
      return target;
    }
    try {
      return target
          .contentType(MediaType.APPLICATION_JSON_TYPE)
          .body(objectMapper.writeValueAsString(source.getEntity()));
    } catch (JsonProcessingException failure) {
      throw new IllegalStateException("Unable to serialize execution response", failure);
    }
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
