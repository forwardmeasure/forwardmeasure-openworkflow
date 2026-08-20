/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionEventType;
import org.junit.jupiter.api.Test;

class OksKafkaRuntimeMappingTest {
  @Test
  void mapsEveryDurableHistoryTypeSoCanonicalSequencesCannotDevelopGaps() {
    for (ExecutionEventType type : ExecutionEventType.values()) {
      assertNotNull(OksKafkaRuntime.mapping(type), type.name());
    }
    assertEquals(
        ExecutionEvent.EventType.EFFECT_REQUESTED,
        OksKafkaRuntime.mapping(ExecutionEventType.OPERATION_DISPATCHED).type());
    assertEquals(
        ExecutionEvent.EventType.EFFECT_COMPLETED,
        OksKafkaRuntime.mapping(ExecutionEventType.OPERATION_COMPLETED).type());
  }

  @Test
  void mapsRuntimeErrorsToTheCanonicalExecutionErrorShape() {
    var details = JsonNodeFactory.instance.objectNode().put("sourceEventType", "EXECUTION_FAILED");

    var error = OksKafkaRuntime.canonicalError(null, "/do/0/call", details);

    assertEquals("OPENWORKFLOW_RUNTIME_ERROR", error.required("code").textValue());
    assertEquals("Workflow runtime reported an error", error.required("message").textValue());
    assertEquals("/do/0/call", error.required("taskPath").textValue());
    assertEquals(false, error.required("retryable").booleanValue());
    assertEquals("EXECUTION_FAILED", error.at("/details/sourceEventType").textValue());
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
