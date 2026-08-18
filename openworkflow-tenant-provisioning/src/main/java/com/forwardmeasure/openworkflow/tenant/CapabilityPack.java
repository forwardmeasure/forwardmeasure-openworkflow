/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.forwardmeasure.openworkflow.tenant;

import java.util.Set;

public record CapabilityPack(
    String id, String version, Set<String> roles, Set<String> workloadIdentities) {
  public static final CapabilityPack OPENWORKFLOW_V1 =
      new CapabilityPack(
          "openworkflow",
          "1",
          Set.of(
              "workflow-author",
              "workflow-approver",
              "workflow-publisher",
              "workflow-execution-controller",
              "workflow-auditor",
              "workflow-administrator"),
          Set.of());

  public static final CapabilityPack ENTITY_INTELLIGENCE_V1 =
      new CapabilityPack(
          "entity-intelligence",
          "1",
          Set.of(
              "entity-intelligence-dossier-analyst",
              "entity-intelligence-entity-analyst",
              "entity-intelligence-evidence-curator",
              "entity-intelligence-information-extraction-operator",
              "entity-intelligence-ingestion-operator",
              "entity-intelligence-investigator",
              "entity-intelligence-resolution-analyst",
              "entity-intelligence-screening-analyst",
              "entity-intelligence-reference-population-submitter",
              "entity-intelligence-reference-population-approver"),
          Set.of("entity-intelligence-workflow-invoker"));

  public CapabilityPack {
    if (id == null || id.isBlank() || version == null || version.isBlank()) {
      throw new IllegalArgumentException("Capability pack identity must not be blank");
    }
    roles = Set.copyOf(roles);
    workloadIdentities = Set.copyOf(workloadIdentities);
    if (roles.contains("workflow-internal") || workloadIdentities.contains("workflow-internal")) {
      throw new IllegalArgumentException("workflow-internal is not a permitted capability role");
    }
  }

  public Set<String> sharedClientRoles() {
    var combined = new java.util.HashSet<>(roles);
    combined.addAll(workloadIdentities);
    return Set.copyOf(combined);
  }

  public static CapabilityPack named(String value) {
    return switch (value.trim().toLowerCase(java.util.Locale.ROOT)) {
      case "openworkflow" -> OPENWORKFLOW_V1;
      case "entity-intelligence" -> ENTITY_INTELLIGENCE_V1;
      default -> throw new IllegalArgumentException("Unknown capability pack: " + value);
    };
  }
}
