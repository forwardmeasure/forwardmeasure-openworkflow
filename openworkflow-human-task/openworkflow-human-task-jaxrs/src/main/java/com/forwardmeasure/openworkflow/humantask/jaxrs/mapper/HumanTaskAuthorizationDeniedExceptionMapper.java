/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.jaxrs.mapper;

import com.forwardmeasure.openworkflow.authorization.AuthorizationDeniedException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
public final class HumanTaskAuthorizationDeniedExceptionMapper
    implements ExceptionMapper<AuthorizationDeniedException> {
  @Override
  public Response toResponse(AuthorizationDeniedException exception) {
    return HumanTaskProblems.response(403, "Forbidden", exception.getMessage());
  }
}
