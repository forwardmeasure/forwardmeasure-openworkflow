package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.definition.SchedulePlan;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.Actors;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionBundle;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

/** Materialises the first durable interval or cron trigger at admission. */
final class OksDefinitionScheduleProcessor
    implements Processor<String, WorkflowDefinitionBundle, String, WorkflowEffect> {
  private final ActorId runtimeActorId;
  private final String runtimeComponent;
  private ProcessorContext<String, WorkflowEffect> context;

  OksDefinitionScheduleProcessor(ActorId runtimeActorId, String runtimeComponent) {
    this.runtimeActorId = runtimeActorId;
    this.runtimeComponent = runtimeComponent;
  }

  @Override
  public void init(ProcessorContext<String, WorkflowEffect> context) {
    this.context = context;
  }

  @Override
  public void process(Record<String, WorkflowDefinitionBundle> record) {
    WorkflowDefinitionBundle bundle = record.value();
    if (bundle == null || bundle.plan().schedule() == null) return;
    SchedulePlan schedule = bundle.plan().schedule();
    if (schedule.every() != null) {
      String recurrence =
          OksScheduleSupport.durationLiteral(
              schedule.every(),
              JsonNodeFactory.instance.objectNode(),
              bundle.plan().expressions().mode());
      Duration every = OksScheduleSupport.duration(recurrence, bundle.admittedAt());
      forward(
          record,
          bundle,
          OksScheduleSupport.KIND_EVERY,
          bundle.admittedAt().plus(every),
          recurrence);
    }
    if (schedule.cron() != null) {
      forward(
          record,
          bundle,
          OksScheduleSupport.KIND_CRON,
          OksScheduleSupport.nextCron(schedule.cron(), bundle.admittedAt()),
          schedule.cron());
    }
  }

  private void forward(
      Record<String, WorkflowDefinitionBundle> record,
      WorkflowDefinitionBundle bundle,
      String kind,
      Instant dueAt,
      String recurrence) {
    WorkflowEffect effect =
        OksScheduleSupport.timer(
            bundle,
            kind,
            dueAt,
            recurrence,
            JsonNodeFactory.instance.objectNode(),
            Actors.system(
                bundle.key().tenantId(), runtimeActorId, runtimeComponent, bundle.admittedAt()));
    context.forward(new Record<>(effect.key().canonical(), effect, record.timestamp()));
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
