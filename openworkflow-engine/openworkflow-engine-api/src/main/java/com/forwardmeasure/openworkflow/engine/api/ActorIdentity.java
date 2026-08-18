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
package com.forwardmeasure.openworkflow.engine.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/** Authenticated actor responsible for an externally initiated transition. */
public record ActorIdentity(
    TenantId tenantId,
    String actorDid,
    @JsonInclude(JsonInclude.Include.NON_EMPTY) String organizationId,
    @JsonInclude(JsonInclude.Include.NON_EMPTY) Set<String> organizationRoles,
    @JsonInclude(JsonInclude.Include.NON_EMPTY) String correlationId) {

  public ActorIdentity(TenantId tenantId, String actorDid) {
    this(tenantId, actorDid, null, Set.of(), null);
  }

  public ActorIdentity {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(actorDid, "actorDid");
    if (!actorDid.startsWith("did:") || actorDid.isBlank()) {
      throw new IllegalArgumentException("Actor identity must be a DID");
    }
    organizationRoles =
        organizationRoles == null
            ? Set.of()
            : Collections.unmodifiableSortedSet(new TreeSet<>(organizationRoles));
    if ((organizationId == null) != (correlationId == null)) {
      throw new IllegalArgumentException(
          "Organization and correlation identity coordinates must be supplied together");
    }
    if (organizationId != null && (organizationId.isBlank() || correlationId.isBlank())) {
      throw new IllegalArgumentException("Organization identity coordinates must not be blank");
    }
  }
}
