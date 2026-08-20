package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.Actors;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.FireTimerCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffectType;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Materialises durable timers and emits idempotent commands when they are due.
 *
 * <p>A one-shot timer is removed from the state store in the same Kafka Streams transaction that
 * emits its fire command. The stable command identity still makes downstream handling idempotent,
 * but the timer source must not amplify one overdue timer into an unbounded command stream while
 * the execution's serialized command lane is busy.
 */
final class OksTimerEffectProcessor
    implements Processor<String, WorkflowEffect, String, ExecutionCommand> {
  private static final Duration SCAN_INTERVAL = Duration.ofMillis(100);

  private final ActorId runtimeActorId;
  private final String runtimeComponent;
  private ProcessorContext<String, ExecutionCommand> context;
  private KeyValueStore<String, WorkflowEffect> timers;

  OksTimerEffectProcessor(ActorId runtimeActorId, String runtimeComponent) {
    this.runtimeActorId = runtimeActorId;
    this.runtimeComponent = runtimeComponent;
  }

  @Override
  public void init(ProcessorContext<String, ExecutionCommand> context) {
    this.context = context;
    timers = context.getStateStore(OksStores.TIMERS);
    context.schedule(SCAN_INTERVAL, PunctuationType.WALL_CLOCK_TIME, this::fireDueTimers);
  }

  @Override
  public void process(Record<String, WorkflowEffect> record) {
    WorkflowEffect effect = record.value();
    if (effect == null) return;
    String timerId = OksTimerEffectOutputProcessor.timerId(effect);
    if (effect.type() == WorkflowEffectType.SCHEDULE_TIMER) {
      timers.put(timerId, effect);
    } else if (effect.type() == WorkflowEffectType.CANCEL_TIMER) {
      timers.delete(timerId);
    }
  }

  private void fireDueTimers(long timestamp) {
    Instant now = Instant.ofEpochMilli(timestamp);
    List<KeyValue<String, WorkflowEffect>> due = new ArrayList<>();
    try (var values = timers.all()) {
      while (values.hasNext()) {
        KeyValue<String, WorkflowEffect> entry = values.next();
        WorkflowEffect effect = entry.value;
        Instant dueAt = Instant.parse(effect.payload().inlineValue().required("dueAt").textValue());
        if (now.isBefore(dueAt)) continue;
        due.add(entry);
      }
    }
    for (KeyValue<String, WorkflowEffect> entry : due) {
      WorkflowEffect effect = entry.value;
      Instant dueAt = Instant.parse(effect.payload().inlineValue().required("dueAt").textValue());
      var actor =
          Actors.systemCorrelated(
              effect.key().tenantId(),
              runtimeActorId,
              runtimeComponent,
              effect.actor().correlationId(),
              dueAt);
      ExecutionCommand command;
      if (OksScheduleSupport.PURPOSE.equals(
          effect.payload().inlineValue().path("purpose").asText())) {
        command = OksScheduleSupport.start(effect, actor);
        timers.delete(entry.key);
        KeyValue<String, WorkflowEffect> next =
            OksScheduleSupport.nextRecurringTimer(effect, now, actor);
        if (next != null) {
          timers.put(next.key, next.value);
        }
      } else {
        command = new FireTimerCommand("timer:" + entry.key, effect.key(), entry.key, actor, dueAt);
        /*
         * State-store mutation and context.forward() are committed by
         * Kafka Streams as one processing transaction.  Removing the
         * timer here therefore cannot lose a successfully emitted
         * command, and prevents every subsequent 100 ms punctuation
         * from publishing the same one-shot timer again.
         */
        timers.delete(entry.key);
      }
      context.forward(new Record<>(effect.key().canonical(), command, timestamp));
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
