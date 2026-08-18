package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Authenticated actor and the tenant-scoped authorisation facts used to accept one command. Roles
 * are an audit snapshot, not a substitute for authorisation.
 */
public record ActorContext(
    OksTenantId tenantId,
    ActorId actorId,
    ActorType actorType,
    String displayName,
    String clientId,
    BusinessCorrelationId correlationId,
    Set<String> roles,
    ActorId delegatedBy,
    Instant authenticatedAt,
    @JsonInclude(JsonInclude.Include.NON_NULL) String identityProvider,
    @JsonInclude(JsonInclude.Include.NON_NULL) String subjectIdentifier,
    @JsonInclude(JsonInclude.Include.NON_NULL) String organizationId) {

  public ActorContext {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(actorId, "actorId");
    Objects.requireNonNull(actorType, "actorType");
    Objects.requireNonNull(authenticatedAt, "authenticatedAt");
    roles = roles == null ? Set.of() : Collections.unmodifiableSortedSet(new TreeSet<>(roles));
    if (delegatedBy != null && delegatedBy.equals(actorId)) {
      throw new IllegalArgumentException("An actor cannot delegate to itself");
    }
    if ((identityProvider == null) != (subjectIdentifier == null)) {
      throw new IllegalArgumentException(
          "identityProvider and subjectIdentifier must both " + "be present or both be absent");
    }
    if (identityProvider != null && (identityProvider.isBlank() || subjectIdentifier.isBlank())) {
      throw new IllegalArgumentException("Persisted identity coordinates must not be blank");
    }
    if (organizationId != null && organizationId.isBlank()) {
      throw new IllegalArgumentException("organizationId must not be blank when present");
    }
  }

  /**
   * Backward-compatible constructor for durable records written before Organization propagation.
   */
  public ActorContext(
      OksTenantId tenantId,
      ActorId actorId,
      ActorType actorType,
      String displayName,
      String clientId,
      BusinessCorrelationId correlationId,
      Set<String> roles,
      ActorId delegatedBy,
      Instant authenticatedAt,
      String identityProvider,
      String subjectIdentifier) {
    this(
        tenantId,
        actorId,
        actorType,
        displayName,
        clientId,
        correlationId,
        roles,
        delegatedBy,
        authenticatedAt,
        identityProvider,
        subjectIdentifier,
        null);
  }

  /** Constructor for durable integrations without persisted IdP coordinates. */
  public ActorContext(
      OksTenantId tenantId,
      ActorId actorId,
      ActorType actorType,
      String displayName,
      String clientId,
      BusinessCorrelationId correlationId,
      Set<String> roles,
      ActorId delegatedBy,
      Instant authenticatedAt) {
    this(
        tenantId,
        actorId,
        actorType,
        displayName,
        clientId,
        correlationId,
        roles,
        delegatedBy,
        authenticatedAt,
        null,
        null,
        null);
  }

  /**
   * Backward-compatible constructor for stored records and integrations that predate request
   * correlation. New ingress paths always populate it.
   */
  public ActorContext(
      OksTenantId tenantId,
      ActorId actorId,
      ActorType actorType,
      String displayName,
      String clientId,
      Set<String> roles,
      ActorId delegatedBy,
      Instant authenticatedAt) {
    this(
        tenantId,
        actorId,
        actorType,
        displayName,
        clientId,
        (BusinessCorrelationId) null,
        roles,
        delegatedBy,
        authenticatedAt,
        null,
        null,
        null);
  }

  public ActorContext withCorrelationId(BusinessCorrelationId value) {
    return new ActorContext(
        tenantId,
        actorId,
        actorType,
        displayName,
        clientId,
        value,
        roles,
        delegatedBy,
        authenticatedAt,
        identityProvider,
        subjectIdentifier,
        organizationId);
  }

  /**
   * Maker-checker separation is based on stable identity, never display name, username, client ID
   * or role.
   */
  public boolean sameActor(ActorContext other) {
    return other != null && tenantId.equals(other.tenantId) && actorId.equals(other.actorId);
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
