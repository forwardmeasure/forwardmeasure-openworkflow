package com.forwardmeasure.openworkflow.workflow.runtime.api;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;

/** Authenticated, tenant-scoped CloudEvent accepted by an ingress adapter. */
public record InboundCloudEvent(
    OksTenantId tenantId, DataReference event, ActorContext acceptedBy, Instant receivedAt) {

  public InboundCloudEvent {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(event, "event");
    Objects.requireNonNull(acceptedBy, "acceptedBy");
    Objects.requireNonNull(receivedAt, "receivedAt");
    if (!tenantId.equals(acceptedBy.tenantId())) {
      throw new IllegalArgumentException("Ingress actor and event tenant must match");
    }
    if (event.storage() != DataReference.Storage.INLINE || !event.inlineValue().isObject()) {
      throw new IllegalArgumentException("Inbound CloudEvent requires an inline object envelope");
    }
    for (String attribute : java.util.List.of("specversion", "id", "source", "type")) {
      if (!event.inlineValue().path(attribute).isTextual()) {
        throw new IllegalArgumentException("CloudEvent attribute " + attribute + " is required");
      }
    }
    if (acceptedBy.correlationId() == null) {
      acceptedBy = acceptedBy.withCorrelationId(eventCorrelation(tenantId, event));
    }
  }

  public String eventKey() {
    return tenantId
        + "\n"
        + event.inlineValue().required("source").textValue()
        + "\n"
        + event.inlineValue().required("id").textValue();
  }

  private static BusinessCorrelationId eventCorrelation(OksTenantId tenantId, DataReference event) {
    String identity =
        tenantId
            + "\n"
            + event.inlineValue().required("source").textValue()
            + "\n"
            + event.inlineValue().required("id").textValue();
    try {
      return new BusinessCorrelationId(
          "cloudevent:sha256:"
              + HexFormat.of()
                  .formatHex(
                      MessageDigest.getInstance("SHA-256")
                          .digest(identity.getBytes(StandardCharsets.UTF_8))));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
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
