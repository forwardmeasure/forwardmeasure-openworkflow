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
import com.forwardmeasure.openworkflow.engine.api.EngineCommandException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/**
 * Translates every {@link EngineCommandException} into a {@link Problem} response. Every resource
 * method just throws; nothing manually builds a {@code Response} for these failures.
 */
@Provider
public final class EngineCommandExceptionMapper implements ExceptionMapper<EngineCommandException> {

  @Override
  public Response toResponse(EngineCommandException exception) {
    int status =
        switch (exception.kind()) {
          case NOT_FOUND -> 404;
          case CONFLICT -> 409;
          case INVALID_STATE -> 409;
          case UNAVAILABLE -> 503;
          case REJECTED -> 409;
          case ENGINE_MISMATCH -> 500;
        };
    Problem problem =
        new Problem()
            .type("about:blank")
            .title(title(exception.kind()))
            .status(status)
            .detail(exception.getMessage());
    return Response.status(status).type("application/problem+json").entity(problem).build();
  }

  private static String title(EngineCommandException.FailureKind kind) {
    return switch (kind) {
      case NOT_FOUND -> "Not Found";
      case CONFLICT -> "Conflict";
      case INVALID_STATE -> "Conflict";
      case UNAVAILABLE -> "Service Unavailable";
      case REJECTED -> "Conflict";
      case ENGINE_MISMATCH -> "Internal Server Error";
    };
  }
}
