/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.forwardmeasure.jpa.tenancy.TenantId;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class AuthorizationRequestTest {
  private static final ActiveOrganization ACTOR =
      new ActiveOrganization(
          new TenantId(UUID.fromString("01234567-89ab-cdef-0123-456789abcdef")),
          "org-1",
          "reviewer-1",
          Set.of("reviewer"));

  @Test
  void resolvesDecisionActionCodeIntoTheAuthzenWireScope() {
    AuthorizationRequest request =
        new AuthorizationRequest(
            ACTOR,
            AuthorizationResource.humanTask("task-1"),
            AuthorizationAction.HUMAN_TASK_DECIDE,
            "correlation-1",
            Map.of("action_code", "approve-trade"));

    assertEquals("human-task:decide:approve-trade", request.resolvedActionScope());
  }

  @Test
  void rejectsDecisionAuthorizationWithoutAValidActionCode() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new AuthorizationRequest(
                    ACTOR,
                    AuthorizationResource.humanTask("task-1"),
                    AuthorizationAction.HUMAN_TASK_DECIDE,
                    "correlation-1",
                    Map.of())
                .resolvedActionScope());
  }
}
