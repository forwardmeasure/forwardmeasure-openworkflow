package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.openworkflow.data.DataReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffectType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

/** Projects committed emit effects into the adapter-facing CloudEvents topic. */
final class OksEmittedEventOutputProcessor
    extends ContextualProcessor<String, WorkflowEffect, String, JsonNode> {

  @Override
  public void process(Record<String, WorkflowEffect> record) {
    WorkflowEffect effect = record.value();
    if (effect == null || effect.type() != WorkflowEffectType.EMIT_CLOUD_EVENT) {
      return;
    }
    if (effect.payload().storage() != DataReference.Storage.INLINE) {
      throw new IllegalStateException("CloudEvents outbox requires a materialized envelope");
    }
    JsonNode event = effect.payload().inlineValue();
    String key = event.required("source").textValue() + "\n" + event.required("id").textValue();
    context().forward(record.withKey(key).withValue(event));
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
