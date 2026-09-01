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
package com.forwardmeasure.openworkflow.workflow.runtime.kafka.jaxrs;

/**
 * Response body for one accepted CloudEvent. {@code eventKey} is {@code
 * InboundCloudEvent.eventKey()} - tenant/source/id - the same identity {@code
 * OksInboundEventProcessor} deduplicates on, so a caller can correlate this response with what the
 * runtime eventually journals.
 */
public record CloudEventIngressAcceptedDocument(
    String tenantId, String eventId, String eventSource, String eventKey, String correlationId) {}
