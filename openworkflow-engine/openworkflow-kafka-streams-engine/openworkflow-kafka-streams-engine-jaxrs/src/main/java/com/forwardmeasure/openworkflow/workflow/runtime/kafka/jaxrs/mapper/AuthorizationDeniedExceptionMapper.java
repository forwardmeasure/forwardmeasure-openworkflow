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

import com.forwardmeasure.openworkflow.authorization.AuthorizationDeniedException;
import com.forwardmeasure.openworkflow.common.model.Problem;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/**
 * Translates a denied AuthZEN decision into 403, matching every other operation's error shape.
 * Copied rather than shared from {@code openworkflow-definition-management-jaxrs}'s identical
 * mapper - that module is a different deployable surface and not a dependency this engine's JAX-RS
 * module should acquire just for one small {@code @Provider} class; {@link
 * AuthorizationDeniedException} itself is the shared, engine-neutral type (from {@code
 * openworkflow-authorization-api}, already a direct dependency here).
 */
@Provider
public final class AuthorizationDeniedExceptionMapper
    implements ExceptionMapper<AuthorizationDeniedException> {

  @Override
  public Response toResponse(AuthorizationDeniedException exception) {
    Problem problem =
        new Problem()
            .type("about:blank")
            .title("Forbidden")
            .status(403)
            .detail(exception.getMessage());
    return Response.status(403).type("application/problem+json").entity(problem).build();
  }
}
