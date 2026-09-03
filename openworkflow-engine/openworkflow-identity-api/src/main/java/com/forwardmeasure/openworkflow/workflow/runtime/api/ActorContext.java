package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/** Authenticated actor and tenant-scoped authorization facts for one accepted command. */
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
          "identityProvider and subjectIdentifier must both be present or both be absent");
    }
    if (identityProvider != null && (identityProvider.isBlank() || subjectIdentifier.isBlank())) {
      throw new IllegalArgumentException("Persisted identity coordinates must not be blank");
    }
    if (organizationId != null && organizationId.isBlank()) {
      throw new IllegalArgumentException("organizationId must not be blank when present");
    }
  }

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
        null,
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

  /** Maker-checker separation compares stable tenant and actor identity only. */
  public boolean sameActor(ActorContext other) {
    return other != null && tenantId.equals(other.tenantId) && actorId.equals(other.actorId);
  }
}
