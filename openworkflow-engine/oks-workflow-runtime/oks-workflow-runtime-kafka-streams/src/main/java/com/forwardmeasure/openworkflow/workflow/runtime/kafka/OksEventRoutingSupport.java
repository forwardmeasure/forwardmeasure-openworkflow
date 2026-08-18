package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.Actors;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.InboundCloudEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ReceiveEventCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import java.util.Objects;

final class OksEventRoutingSupport {
  private OksEventRoutingSupport() {}

  static String subscriptionId(WorkflowEffect effect) {
    return effect.payload().inlineValue().required("subscriptionId").textValue();
  }

  static String subscriptionKey(WorkflowEffect effect) {
    return subscriptionKey(effect.key().tenantId().toString(), subscriptionId(effect));
  }

  static String subscriptionKey(String tenantId, String subscriptionId) {
    return tenantId.length() + ":" + tenantId + subscriptionId;
  }

  static RoutedCommand route(
      WorkflowEffect subscription,
      InboundCloudEvent inbound,
      ActorId runtimeActorId,
      String runtimeComponent) {
    Objects.requireNonNull(subscription, "subscription");
    Objects.requireNonNull(inbound, "inbound");
    if (!subscription.key().tenantId().equals(inbound.tenantId())
        || inbound.receivedAt().isBefore(subscription.requestedAt())) {
      return null;
    }
    String subscriptionId = subscriptionId(subscription);
    String source = inbound.event().inlineValue().required("source").textValue();
    String eventId = inbound.event().inlineValue().required("id").textValue();
    ExecutionCommand command =
        new ReceiveEventCommand(
            "event:"
                + subscriptionId.length()
                + ":"
                + subscriptionId
                + source.length()
                + ":"
                + source
                + eventId,
            subscription.key(),
            subscriptionId,
            inbound.event(),
            Actors.systemCorrelated(
                inbound.tenantId(),
                runtimeActorId,
                runtimeComponent,
                inbound.acceptedBy().correlationId(),
                inbound.receivedAt()),
            inbound.receivedAt());
    return new RoutedCommand(subscription.key().canonical(), command);
  }

  record RoutedCommand(String key, ExecutionCommand command) {}
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
