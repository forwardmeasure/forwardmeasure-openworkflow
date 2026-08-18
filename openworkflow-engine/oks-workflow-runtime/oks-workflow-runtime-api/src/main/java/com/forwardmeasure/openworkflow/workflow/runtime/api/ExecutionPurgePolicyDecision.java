package com.forwardmeasure.openworkflow.workflow.runtime.api;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable retention/legal-hold decision supplied by the trusted ingress policy boundary and
 * revalidated by the durable execution reducer.
 */
public record ExecutionPurgePolicyDecision(
    String decisionId,
    String policyVersion,
    ExecutionKey execution,
    Instant evaluatedAt,
    Instant eligibleAt,
    boolean legalHold,
    String retentionClass) {

  public ExecutionPurgePolicyDecision {
    requireText(decisionId, "decisionId");
    requireText(policyVersion, "policyVersion");
    Objects.requireNonNull(execution, "execution");
    Objects.requireNonNull(evaluatedAt, "evaluatedAt");
    Objects.requireNonNull(eligibleAt, "eligibleAt");
    requireText(retentionClass, "retentionClass");
    if (eligibleAt.isAfter(evaluatedAt)) {
      throw new IllegalArgumentException(
          "A purge decision cannot be eligible after it was evaluated");
    }
  }

  private static void requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
  }
}
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
