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
package com.forwardmeasure.openworkflow.definition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Objects;

/**
 * Immutable reference to authentication material.
 *
 * <p>The compiled workflow deliberately carries only a tenant-scoped secret name. Credential values
 * are resolved after authorisation at the operation adapter edge and therefore never become
 * workflow state, effects or history.
 */
public record AuthenticationPlan(
    Kind kind,
    String reusableName,
    String secretName,
    JsonNode expressionConfiguration,
    List<String> secretReferences) {

  public enum Kind {
    BASIC,
    BEARER,
    DIGEST,
    OAUTH2,
    OIDC
  }

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public AuthenticationPlan(
      @JsonProperty("kind") Kind kind,
      @JsonProperty("reusableName") String reusableName,
      @JsonProperty("secretName") String secretName,
      @JsonProperty("expressionConfiguration") JsonNode expressionConfiguration,
      @JsonProperty("secretReferences") List<String> secretReferences) {
    Objects.requireNonNull(kind, "kind");
    if (expressionConfiguration != null && expressionConfiguration.isNull()) {
      expressionConfiguration = null;
    }
    if (reusableName != null && reusableName.isBlank()) {
      throw new IllegalArgumentException("reusableName must be null or non-blank");
    }
    if ((secretName == null) == (expressionConfiguration == null)) {
      throw new IllegalArgumentException(
          "Authentication must use exactly one of a secret "
              + "reference or runtime-expression configuration");
    }
    if (secretName != null) {
      requireText(secretName, "secretName");
    }
    if (expressionConfiguration != null) {
      if (!expressionConfiguration.isObject()) {
        throw new IllegalArgumentException(
            "Authentication expression configuration " + "must be an object");
      }
      expressionConfiguration = expressionConfiguration.deepCopy();
    }
    secretReferences =
        secretReferences == null
            ? List.of()
            : secretReferences.stream()
                .map(reference -> requireText(reference, "secret reference"))
                .distinct()
                .sorted()
                .toList();
    if (secretName != null && !secretReferences.isEmpty()) {
      throw new IllegalArgumentException(
          "Secret-backed authentication cannot also declare " + "expression secret references");
    }
    this.kind = kind;
    this.reusableName = reusableName;
    this.secretName = secretName;
    this.expressionConfiguration = expressionConfiguration;
    this.secretReferences = secretReferences;
  }

  public AuthenticationPlan(Kind kind, String reusableName, String secretName) {
    this(kind, reusableName, secretName, null, List.of());
  }

  public AuthenticationPlan(
      Kind kind, String reusableName, String secretName, JsonNode expressionConfiguration) {
    this(kind, reusableName, secretName, expressionConfiguration, List.of());
  }

  public static AuthenticationPlan expressions(
      Kind kind, String reusableName, JsonNode configuration, List<String> secretReferences) {
    return new AuthenticationPlan(kind, reusableName, null, configuration, secretReferences);
  }

  public boolean secretBacked() {
    return secretName != null;
  }

  /** Returns an isolated copy of expression-backed credential metadata. */
  @Override
  public JsonNode expressionConfiguration() {
    return expressionConfiguration == null ? null : expressionConfiguration.deepCopy();
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
