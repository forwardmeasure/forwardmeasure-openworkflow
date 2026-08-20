package com.forwardmeasure.openworkflow.workflow.runtime.api;

/**
 * Delivery contract at the boundary where a committed workflow effect is materialised.
 *
 * <p>Kafka exactly-once applies only to records and state inside the Kafka transaction. It never
 * implies exactly-once execution by a remote system.
 */
public enum EffectDeliveryGuarantee {
  /**
   * The effect is projected to another Kafka record in the same exactly-once-v2 processing model.
   */
  KAFKA_EXACTLY_ONCE,

  /**
   * Reapplying the stable effect identity produces the same desired state, such as an upsert,
   * delete, schedule or cancellation marker.
   */
  IDEMPOTENT_MATERIALIZATION,

  /**
   * Delivery can repeat, but every attempt carries the same stable identity and the target
   * operation is expected to be intrinsically idempotent.
   */
  AT_LEAST_ONCE_STABLE_IDENTITY,

  /**
   * Delivery can repeat with the same stable identity and a non-idempotent external target must
   * honour that identity for deduplication.
   */
  AT_LEAST_ONCE_EXTERNAL_DEDUPLICATION
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
