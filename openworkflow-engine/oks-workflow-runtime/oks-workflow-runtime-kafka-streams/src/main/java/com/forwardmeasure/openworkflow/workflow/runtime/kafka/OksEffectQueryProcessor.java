package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffectType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/** Materializes committed workflow effects for execution drill-down. */
final class OksEffectQueryProcessor
    extends ContextualProcessor<String, WorkflowEffect, Void, Void> {
  private KeyValueStore<String, WorkflowEffect> store;

  @Override
  public void init(org.apache.kafka.streams.processor.api.ProcessorContext<Void, Void> context) {
    super.init(context);
    store = context.getStateStore(OksStores.EFFECTS);
  }

  @Override
  public void process(Record<String, WorkflowEffect> record) {
    if (record.value() == null) return;
    if (record.value().type() == WorkflowEffectType.PURGE_EXECUTION_PROJECTIONS) {
      deleteExecution(record.value().key().canonical());
    }
    store.put(OksQueryKeys.effect(record.value().key(), record.value().effectId()), record.value());
  }

  private void deleteExecution(String canonicalKey) {
    java.util.ArrayList<String> keys = new java.util.ArrayList<>();
    try (org.apache.kafka.streams.state.KeyValueIterator<String, WorkflowEffect> values =
        store.range(OksQueryKeys.rangeStart(canonicalKey), OksQueryKeys.rangeEnd(canonicalKey))) {
      while (values.hasNext()) keys.add(values.next().key);
    }
    keys.forEach(store::delete);
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
