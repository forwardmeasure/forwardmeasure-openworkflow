package com.forwardmeasure.durableprocessing.kafka;

import com.forwardmeasure.durableprocessing.api.DurableCommandOutcome;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

/** Unwraps durable command outcomes for their compacted public status topic. */
final class DurableCommandOutcomeOutputProcessor<C, E, O>
    extends ContextualProcessor<
        String, DurableProcessorOutput<C, E, O>, String, DurableCommandOutcome> {

  @Override
  public void process(Record<String, DurableProcessorOutput<C, E, O>> record) {
    if (record.value() instanceof DurableProcessorOutput.Outcome<C, E, O> outcome) {
      context().forward(record.withValue(outcome.outcome()));
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
