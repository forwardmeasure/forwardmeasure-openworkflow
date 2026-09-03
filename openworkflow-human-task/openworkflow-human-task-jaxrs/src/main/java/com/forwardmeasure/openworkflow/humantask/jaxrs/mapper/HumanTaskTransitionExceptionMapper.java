/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.jaxrs.mapper;

import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskTransitionException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/** Maps typed state-machine rejections to the reviewed RFC 9457 statuses. */
@Provider
public final class HumanTaskTransitionExceptionMapper
    implements ExceptionMapper<HumanTaskTransitionException> {
  @Override
  public Response toResponse(HumanTaskTransitionException exception) {
    int status =
        switch (exception.failure()) {
          case NOT_FOUND -> 404;
          case REVISION_CONFLICT -> 412;
          case INELIGIBLE_REVIEWER -> 403;
          case VALIDATION, UNKNOWN_ACTION -> 422;
          case ALREADY_EXISTS,
              ILLEGAL_TRANSITION,
              ASSIGNMENT_CONFLICT,
              LEASE_CONFLICT,
              LEASE_EXPIRED,
              STALE_CONTENT ->
              409;
        };
    return HumanTaskProblems.response(status, title(status), exception.getMessage());
  }

  private static String title(int status) {
    return switch (status) {
      case 403 -> "Forbidden";
      case 404 -> "Not Found";
      case 409 -> "Conflict";
      case 412 -> "Precondition Failed";
      case 422 -> "Unprocessable Entity";
      default -> throw new AssertionError(status);
    };
  }
}
