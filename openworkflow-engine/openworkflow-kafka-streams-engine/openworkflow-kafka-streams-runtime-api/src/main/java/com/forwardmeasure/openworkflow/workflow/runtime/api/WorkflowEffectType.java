package com.forwardmeasure.openworkflow.workflow.runtime.api;

/**
 * Durable requests for work outside the deterministic workflow reducer.
 *
 * <p>Effects are committed to Kafka atomically with execution state and history. Adapter topologies
 * may perform the requested work, but must return outcomes as new commands; they never mutate
 * workflow state directly.
 */
public enum WorkflowEffectType {
  EMIT_CLOUD_EVENT(EffectDeliveryGuarantee.KAFKA_EXACTLY_ONCE),
  UPSERT_EVENT_SUBSCRIPTION(EffectDeliveryGuarantee.IDEMPOTENT_MATERIALIZATION),
  DELETE_EVENT_SUBSCRIPTION(EffectDeliveryGuarantee.IDEMPOTENT_MATERIALIZATION),
  UPSERT_ASYNC_API_SUBSCRIPTION(EffectDeliveryGuarantee.IDEMPOTENT_MATERIALIZATION),
  DELETE_ASYNC_API_SUBSCRIPTION(EffectDeliveryGuarantee.IDEMPOTENT_MATERIALIZATION),
  ACK_ASYNC_API_MESSAGE(EffectDeliveryGuarantee.AT_LEAST_ONCE_STABLE_IDENTITY),
  SCHEDULE_TIMER(EffectDeliveryGuarantee.IDEMPOTENT_MATERIALIZATION),
  CANCEL_TIMER(EffectDeliveryGuarantee.IDEMPOTENT_MATERIALIZATION),
  DISPATCH_OPERATION(EffectDeliveryGuarantee.AT_LEAST_ONCE_EXTERNAL_DEDUPLICATION),
  CANCEL_OPERATION(EffectDeliveryGuarantee.AT_LEAST_ONCE_STABLE_IDENTITY),
  CREATE_HUMAN_TASK(EffectDeliveryGuarantee.IDEMPOTENT_MATERIALIZATION),
  EXPIRE_HUMAN_TASK(EffectDeliveryGuarantee.IDEMPOTENT_MATERIALIZATION),
  CANCEL_HUMAN_TASK(EffectDeliveryGuarantee.IDEMPOTENT_MATERIALIZATION),
  COMPUTE_WORKFLOW_TRANSITION(EffectDeliveryGuarantee.AT_LEAST_ONCE_STABLE_IDENTITY),
  PURGE_EXECUTION_PROJECTIONS(EffectDeliveryGuarantee.IDEMPOTENT_MATERIALIZATION);

  private final EffectDeliveryGuarantee deliveryGuarantee;

  WorkflowEffectType(EffectDeliveryGuarantee deliveryGuarantee) {
    this.deliveryGuarantee = deliveryGuarantee;
  }

  public EffectDeliveryGuarantee deliveryGuarantee() {
    return deliveryGuarantee;
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
