package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffectType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

/** Repartitions timer effects by stable timer identity. */
final class OksTimerEffectOutputProcessor
    extends ContextualProcessor<String, WorkflowEffect, String, WorkflowEffect> {
  @Override
  public void process(Record<String, WorkflowEffect> record) {
    WorkflowEffect effect = record.value();
    if (effect == null
        || (effect.type() != WorkflowEffectType.SCHEDULE_TIMER
            && effect.type() != WorkflowEffectType.CANCEL_TIMER)) {
      return;
    }
    context().forward(record.withKey(timerId(effect)));
  }

  static String timerId(WorkflowEffect effect) {
    return effect.payload().inlineValue().required("timerId").textValue();
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
