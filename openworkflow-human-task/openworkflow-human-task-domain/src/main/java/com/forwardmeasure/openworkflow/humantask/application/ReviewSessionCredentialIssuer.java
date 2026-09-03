/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskId;

/** Deterministically issues an idempotent review-session identifier and bearer token. */
@FunctionalInterface
public interface ReviewSessionCredentialIssuer {
  Credential issue(HumanTaskId taskId, String commandId, String actorId);

  record Credential(String reviewSessionId, String token) {
    public Credential {
      if (reviewSessionId == null || reviewSessionId.isBlank()) {
        throw new IllegalArgumentException("reviewSessionId must not be blank");
      }
      if (token == null || token.length() < 32) {
        throw new IllegalArgumentException("review token must contain at least 32 characters");
      }
    }
  }
}
