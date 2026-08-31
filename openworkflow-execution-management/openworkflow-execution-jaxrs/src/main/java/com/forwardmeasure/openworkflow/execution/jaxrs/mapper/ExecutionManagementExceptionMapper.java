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
package com.forwardmeasure.openworkflow.execution.jaxrs.mapper;

import com.forwardmeasure.openworkflow.common.model.Problem;
import com.forwardmeasure.openworkflow.execution.management.ExecutionManagementException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/**
 * Translates every {@link ExecutionManagementException} into a {@link Problem} response. Every
 * resource method just throws; nothing manually builds a {@code Response} for these failures.
 */
@Provider
public final class ExecutionManagementExceptionMapper
    implements ExceptionMapper<ExecutionManagementException> {

  @Override
  public Response toResponse(ExecutionManagementException exception) {
    int status =
        switch (exception.kind()) {
          case NOT_FOUND -> 404;
          case NOT_PUBLISHED -> 404;
          case FORBIDDEN -> 403;
          case STALE_VERSION -> 412;
          case ENGINE_UNAVAILABLE -> 503;
          case UNSUPPORTED_CONSTRUCT -> 422;
        };
    Problem problem =
        new Problem()
            .type("about:blank")
            .title(title(exception.kind()))
            .status(status)
            .detail(exception.getMessage());
    return Response.status(status).type("application/problem+json").entity(problem).build();
  }

  private static String title(ExecutionManagementException.Kind kind) {
    return switch (kind) {
      case NOT_FOUND -> "Not Found";
      case NOT_PUBLISHED -> "Not Found";
      case FORBIDDEN -> "Forbidden";
      case STALE_VERSION -> "Precondition Failed";
      case ENGINE_UNAVAILABLE -> "Service Unavailable";
      case UNSUPPORTED_CONSTRUCT -> "Unprocessable Entity";
    };
  }
}
