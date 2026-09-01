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
package com.forwardmeasure.openworkflow.workflow.runtime.kafka.jaxrs.mapper;

import com.forwardmeasure.openworkflow.common.model.Problem;
import com.forwardmeasure.openworkflow.workflow.runtime.kafka.jaxrs.OksCloudEventIngressException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/**
 * Translates every {@link OksCloudEventIngressException} into a {@link Problem} response - same
 * convention as {@code EngineCommandExceptionMapper}/{@code ExecutionManagementExceptionMapper}:
 * the resource method just throws, nothing manually builds a {@code Response} for these failures.
 */
@Provider
public final class OksCloudEventIngressExceptionMapper
    implements ExceptionMapper<OksCloudEventIngressException> {

  @Override
  public Response toResponse(OksCloudEventIngressException exception) {
    int status =
        switch (exception.kind()) {
          case MALFORMED -> 400;
          case TOO_LARGE -> 413;
        };
    Problem problem =
        new Problem()
            .type("about:blank")
            .title(title(exception.kind()))
            .status(status)
            .detail(exception.getMessage());
    return Response.status(status).type("application/problem+json").entity(problem).build();
  }

  private static String title(OksCloudEventIngressException.Kind kind) {
    return switch (kind) {
      case MALFORMED -> "Bad Request";
      case TOO_LARGE -> "Payload Too Large";
    };
  }
}
