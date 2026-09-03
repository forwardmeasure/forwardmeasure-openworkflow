package com.forwardmeasure.openworkflow.workflow.runtime.api;

import java.time.Instant;
import java.util.Set;

/** Actor contexts for transitions initiated by the workflow control plane. */
public final class Actors {
  private Actors() {}

  public static ActorContext system(
      OksTenantId tenantId,
      ActorId registeredSystemActorId,
      String component,
      Instant authenticatedAt) {
    return new ActorContext(
        tenantId,
        registeredSystemActorId,
        ActorType.SYSTEM,
        component,
        null,
        null,
        Set.of(),
        null,
        authenticatedAt);
  }

  public static ActorContext systemCorrelated(
      OksTenantId tenantId,
      ActorId registeredSystemActorId,
      String component,
      BusinessCorrelationId correlationId,
      Instant authenticatedAt) {
    return new ActorContext(
        tenantId,
        registeredSystemActorId,
        ActorType.SYSTEM,
        component,
        null,
        correlationId,
        Set.of(),
        null,
        authenticatedAt);
  }
}
