package com.forwardmeasure.durableprocessing.kafka;

import com.forwardmeasure.durableprocessing.api.DurableProcess;
import org.apache.kafka.streams.processor.api.ProcessorContext;

/**
 * Creates one process per Kafka processor. The context permits an adapter to capture read-only
 * local or global reference stores without coupling the generic core to Kafka.
 */
@FunctionalInterface
public interface DurableKafkaProcessFactory<S, C, E, O> {
  DurableProcess<S, C, E, O> create(ProcessorContext<String, ?> context);
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
