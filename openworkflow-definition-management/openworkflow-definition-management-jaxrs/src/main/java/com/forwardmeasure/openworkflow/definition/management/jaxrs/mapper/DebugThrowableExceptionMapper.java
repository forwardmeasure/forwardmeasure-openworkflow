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

import com.forwardmeasure.openworkflow.common.model.Problem;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * TEMPORARY debugging aid - catches whatever DefinitionManagementExceptionMapper and
 * AuthorizationDeniedExceptionMapper don't (every exception type this service doesn't have a
 * specific Problem mapping for yet, e.g. SecurityException from the organization-claim check,
 * AuthorizationUnavailableException from the AuthZEN OAuth call). Without this, those fell through
 * to Quarkus's own generic handler, which in a production build deliberately returns only an opaque
 * error id with no message or stack trace - correct for an untrusted public client, but useless
 * while actively debugging this exact authorization pipeline from the browser. Puts the full
 * exception (class, message, cause chain, stack trace) in Problem.detail using the SAME RFC 9457
 * response shape every other error on this API already uses, so the existing frontend error display
 * picks it up with no separate handling.
 *
 * <p>Revert once the organization-claim authorization work is confirmed working end-to-end - this
 * is more detail than a production error response should return to a browser.
 */
@Provider
public final class DebugThrowableExceptionMapper implements ExceptionMapper<Throwable> {

  @Override
  public Response toResponse(Throwable exception) {
    StringBuilder detail = new StringBuilder();
    Throwable current = exception;
    while (current != null) {
      if (detail.length() > 0) {
        detail.append("\nCaused by: ");
      }
      detail.append(current.getClass().getName()).append(": ").append(current.getMessage());
      current = current.getCause();
    }
    detail.append("\n\n");
    StringWriter stackTrace = new StringWriter();
    exception.printStackTrace(new PrintWriter(stackTrace));
    detail.append(stackTrace);

    Problem problem =
        new Problem()
            .type("about:blank")
            .title("Unhandled Exception (debug mode)")
            .status(500)
            .detail(detail.toString());
    return Response.status(500).type("application/problem+json").entity(problem).build();
  }
}
