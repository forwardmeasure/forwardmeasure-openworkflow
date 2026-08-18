package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.InboundCloudEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffectType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/** Materialises subscriptions and replays already-journalled later events. */
final class OksSubscriptionEffectProcessor
    implements Processor<String, WorkflowEffect, String, ExecutionCommand> {
  private final ActorId runtimeActorId;
  private final String runtimeComponent;
  private ProcessorContext<String, ExecutionCommand> context;
  private KeyValueStore<String, WorkflowEffect> subscriptions;
  private KeyValueStore<String, InboundCloudEvent> events;

  OksSubscriptionEffectProcessor(ActorId runtimeActorId, String runtimeComponent) {
    this.runtimeActorId = runtimeActorId;
    this.runtimeComponent = runtimeComponent;
  }

  @Override
  public void init(ProcessorContext<String, ExecutionCommand> context) {
    this.context = context;
    subscriptions = context.getStateStore(OksStores.EVENT_SUBSCRIPTIONS);
    events = context.getStateStore(OksStores.INBOUND_EVENTS);
  }

  @Override
  public void process(Record<String, WorkflowEffect> record) {
    WorkflowEffect effect = record.value();
    if (effect == null) return;
    String key;
    if (effect.type() == WorkflowEffectType.UPSERT_EVENT_SUBSCRIPTION) {
      key = OksEventRoutingSupport.subscriptionKey(effect);
      subscriptions.put(key, effect);
      try (var values = events.all()) {
        while (values.hasNext()) {
          route(effect, values.next().value, record);
        }
      }
    } else if (effect.type() == WorkflowEffectType.DELETE_EVENT_SUBSCRIPTION) {
      key = OksEventRoutingSupport.subscriptionKey(effect);
      subscriptions.delete(key);
    }
  }

  private void route(
      WorkflowEffect subscription, InboundCloudEvent event, Record<String, WorkflowEffect> source) {
    var routed =
        OksEventRoutingSupport.route(subscription, event, runtimeActorId, runtimeComponent);
    if (routed != null) {
      context.forward(source.withKey(routed.key()).withValue(routed.command()));
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
