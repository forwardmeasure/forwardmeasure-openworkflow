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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Set;

/**
 * The subset of a {@code config/keycloak/*.json} authzen policy document that tenant provisioning
 * needs. Unrecognised fields (scopes, roleGrants, resources, constraints, ...) are ignored here -
 * they belong to the broader authzen policy contract, not to Keycloak Organization bootstrapping.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
record CapabilityPackDocument(
    String packId, String packVersion, List<String> roles, List<String> workloadIdentities) {

  @JsonCreator
  CapabilityPackDocument(
      @JsonProperty("packId") String packId,
      @JsonProperty("packVersion") String packVersion,
      @JsonProperty("roles") List<String> roles,
      @JsonProperty("workloadIdentities") List<String> workloadIdentities) {
    this.packId = packId;
    this.packVersion = packVersion;
    this.roles = roles == null ? List.of() : roles;
    this.workloadIdentities = workloadIdentities == null ? List.of() : workloadIdentities;
  }

  CapabilityPack toCapabilityPack() {
    return new CapabilityPack(
        packId, packVersion, Set.copyOf(roles), Set.copyOf(workloadIdentities));
  }
}
