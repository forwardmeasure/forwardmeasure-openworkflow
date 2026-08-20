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
package com.forwardmeasure.openworkflow.actor;

import com.forwardmeasure.openworkflow.definition.WorkflowCoordinates;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/** Tenant-qualified identity of one admitted workflow schedule. */
public record ScheduleId(TenantId tenantId, WorkflowCoordinates definition) {
  public ScheduleId {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(definition, "definition");
  }

  public String entityId() {
    String raw =
        tenantId.value()
            + "\n"
            + definition.namespace()
            + "\n"
            + definition.name()
            + "\n"
            + definition.version()
            + "\n"
            + definition.dsl();
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
  }

  public static ScheduleId fromEntityId(String entityId) {
    Objects.requireNonNull(entityId, "entityId");
    try {
      String raw = new String(Base64.getUrlDecoder().decode(entityId), StandardCharsets.UTF_8);
      String[] parts = raw.split("\\n", -1);
      if (parts.length != 5) throw new IllegalArgumentException("wrong field count");
      return new ScheduleId(
          TenantId.parse(parts[0]),
          new WorkflowCoordinates(parts[1], parts[2], parts[3], parts[4]));
    } catch (RuntimeException malformed) {
      throw new IllegalArgumentException("Schedule entity ID is malformed", malformed);
    }
  }
}
