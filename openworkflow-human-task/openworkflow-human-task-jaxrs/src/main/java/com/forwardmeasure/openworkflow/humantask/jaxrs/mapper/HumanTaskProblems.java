/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.jaxrs.mapper;

import com.forwardmeasure.openworkflow.common.model.Problem;
import jakarta.ws.rs.core.Response;

final class HumanTaskProblems {
  private HumanTaskProblems() {}

  static Response response(int status, String title, String detail) {
    return Response.status(status)
        .type("application/problem+json")
        .entity(new Problem().type("about:blank").title(title).status(status).detail(detail))
        .build();
  }
}
