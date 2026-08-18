package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionBundle;
import java.util.Objects;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/** Restores the compacted definition bundle topic onto every runtime instance. */
final class OksGlobalDefinitionProcessor
    implements Processor<String, WorkflowDefinitionBundle, Void, Void> {
  private KeyValueStore<String, WorkflowDefinitionBundle> definitions;

  @Override
  public void init(ProcessorContext<Void, Void> context) {
    definitions = Objects.requireNonNull(context, "context").getStateStore(OksStores.DEFINITIONS);
  }

  @Override
  public void process(Record<String, WorkflowDefinitionBundle> record) {
    if (record.key() == null) {
      throw new IllegalArgumentException("Definition bundle record requires a key");
    }
    if (record.value() == null) {
      throw new IllegalArgumentException("Immutable definition bundles cannot be deleted");
    }
    if (!record.value().reference().canonical().equals(record.key())) {
      throw new IllegalArgumentException("Kafka key does not match definition bundle key");
    }
    WorkflowDefinitionBundle existing = definitions.get(record.key());
    if (existing != null && !sameImmutableDefinition(existing, record.value())) {
      throw new IllegalStateException("Immutable definition bundle key has conflicting content");
    }
    if (existing == null) {
      definitions.put(record.key(), record.value());
    }
  }

  private static boolean sameImmutableDefinition(
      WorkflowDefinitionBundle left, WorkflowDefinitionBundle right) {
    return left.key().equals(right.key())
        && left.source().equals(right.source())
        && left.plan().equals(right.plan())
        && left.compilerSha256().equals(right.compilerSha256());
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
