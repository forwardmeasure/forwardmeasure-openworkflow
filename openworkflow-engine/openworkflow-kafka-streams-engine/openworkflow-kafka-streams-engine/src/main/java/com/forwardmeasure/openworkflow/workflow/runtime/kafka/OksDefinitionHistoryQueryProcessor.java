package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionAdmissionEvent;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/** Materializes immutable definition admission history for audit queries. */
final class OksDefinitionHistoryQueryProcessor
    extends ContextualProcessor<String, WorkflowDefinitionAdmissionEvent, Void, Void> {
  private KeyValueStore<String, WorkflowDefinitionAdmissionEvent> store;

  @Override
  public void init(org.apache.kafka.streams.processor.api.ProcessorContext<Void, Void> context) {
    super.init(context);
    store = context.getStateStore(OksStores.DEFINITION_HISTORY);
  }

  @Override
  public void process(Record<String, WorkflowDefinitionAdmissionEvent> record) {
    if (record.value() == null) return;
    store.put(
        OksQueryKeys.definitionHistory(record.key(), record.value().eventId()), record.value());
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
