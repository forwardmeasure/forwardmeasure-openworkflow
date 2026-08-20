package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;

/** Source, compiled plan and provenance for one admitted immutable version. */
public record WorkflowDefinitionBundle(
    WorkflowDefinitionKey key,
    String source,
    WorkflowPlan plan,
    String compilerSha256,
    String admissionCommandId,
    ActorContext admittedBy,
    Instant admittedAt) {

  public WorkflowDefinitionBundle {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(plan, "plan");
    requireSha256(compilerSha256, "compilerSha256");
    Objects.requireNonNull(admissionCommandId, "admissionCommandId");
    Objects.requireNonNull(admittedBy, "admittedBy");
    Objects.requireNonNull(admittedAt, "admittedAt");
    if (source.isBlank()) {
      throw new IllegalArgumentException("source must not be blank");
    }
    if (admissionCommandId.isBlank()) {
      throw new IllegalArgumentException("admissionCommandId must not be blank");
    }
    if (!key.coordinates().equals(plan.coordinates())) {
      throw new IllegalArgumentException("Definition key does not match compiled coordinates");
    }
    if (!key.tenantId().equals(admittedBy.tenantId())) {
      throw new IllegalArgumentException("Definition and admitting actor tenants must match");
    }
    if (!sha256(source).equals(plan.sourceSha256())) {
      throw new IllegalArgumentException("Definition source does not match compiled source digest");
    }
    if (!compilerSha256.equals(plan.compilerSha256())) {
      throw new IllegalArgumentException(
          "Definition compiler provenance does not match the " + "compiled plan");
    }
  }

  public WorkflowDefinitionReference reference() {
    return new WorkflowDefinitionReference(key, plan.sourceSha256(), plan.definitionSha256());
  }

  private static String sha256(String source) {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256").digest(source.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  private static void requireSha256(String value, String name) {
    Objects.requireNonNull(value, name);
    if (!value.matches("[0-9a-f]{64}")) {
      throw new IllegalArgumentException(name + " must be lowercase SHA-256");
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
