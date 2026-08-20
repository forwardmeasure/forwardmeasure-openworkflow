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
package com.forwardmeasure.openworkflow.eventing;

import com.forwardmeasure.openworkflow.actor.ScheduleId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.util.Objects;
import java.util.Set;

/** Durable tenant-qualified event-routing target derived from authoritative journals. */
public record CloudEventSubscription(
    TenantId tenantId,
    TargetKind targetKind,
    String targetEntityId,
    String taskPath,
    Set<String> eventTypes,
    long revision,
    boolean active) {
  public enum TargetKind {
    EXECUTION,
    SCHEDULE
  }

  public CloudEventSubscription {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(targetKind, "targetKind");
    Objects.requireNonNull(targetEntityId, "targetEntityId");
    if (targetEntityId.isBlank()) {
      throw new IllegalArgumentException("targetEntityId must not be blank");
    }
    taskPath = taskPath == null ? "" : taskPath;
    eventTypes = eventTypes == null ? Set.of() : Set.copyOf(eventTypes);
    if (revision < 0) throw new IllegalArgumentException("revision must not be negative");
  }

  public static CloudEventSubscription execution(
      ExecutionId executionId,
      String taskPath,
      Set<String> eventTypes,
      long revision,
      boolean active) {
    return new CloudEventSubscription(
        executionId.tenantId(),
        TargetKind.EXECUTION,
        executionId.entityId(),
        taskPath,
        eventTypes,
        revision,
        active);
  }

  public static CloudEventSubscription schedule(
      ScheduleId scheduleId, Set<String> eventTypes, long revision, boolean active) {
    return new CloudEventSubscription(
        scheduleId.tenantId(),
        TargetKind.SCHEDULE,
        scheduleId.entityId(),
        "",
        eventTypes,
        revision,
        active);
  }

  public boolean acceptsType(String eventType) {
    return eventTypes.isEmpty() || eventTypes.contains(eventType);
  }

  public ExecutionId executionId() {
    if (targetKind != TargetKind.EXECUTION) {
      throw new IllegalStateException("Subscription is not an execution target");
    }
    return ExecutionId.fromEntityId(targetEntityId);
  }

  public ScheduleId scheduleId() {
    if (targetKind != TargetKind.SCHEDULE) {
      throw new IllegalStateException("Subscription is not a schedule target");
    }
    return ScheduleId.fromEntityId(targetEntityId);
  }
}
