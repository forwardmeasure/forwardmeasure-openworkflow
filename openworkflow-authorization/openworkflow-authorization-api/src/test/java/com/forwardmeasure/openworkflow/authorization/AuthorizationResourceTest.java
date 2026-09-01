/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.forwardmeasure.openworkflow.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;

class AuthorizationResourceTest {
  @Test
  void eventTargetIdentifiesTheCloudEventIngressEndpointAsAWholeWithNoProperties() {
    AuthorizationResource resource = AuthorizationResource.eventTarget();
    assertEquals("openworkflow-event-target", resource.type());
    assertEquals("event-targets", resource.id());
    assertEquals(Map.of(), resource.properties());
  }

  @Test
  void eventTargetTakesNoInputSoItAlwaysProducesTheSameResource() {
    assertEquals(AuthorizationResource.eventTarget(), AuthorizationResource.eventTarget());
  }

  @Test
  void scheduleIdentifiesOneScheduleByIdConsistentlyWithExecution() {
    AuthorizationResource schedule = AuthorizationResource.schedule("sched-123");
    assertEquals("openworkflow-schedule", schedule.type());
    assertEquals("schedules", schedule.id());
    assertEquals(Map.of("schedule_id", "sched-123"), schedule.properties());

    // Same "<noun>_id" single-property convention as the other per-instance resource factories.
    AuthorizationResource execution = AuthorizationResource.execution("exec-123");
    assertEquals(Map.of("execution_id", "exec-123"), execution.properties());
    AuthorizationResource definition = AuthorizationResource.definition("def-123");
    assertEquals(Map.of("definition_key", "def-123"), definition.properties());
  }

  @Test
  void scheduleRejectsABlankId() {
    assertThrows(IllegalArgumentException.class, () -> AuthorizationResource.schedule(""));
    assertThrows(IllegalArgumentException.class, () -> AuthorizationResource.schedule(null));
  }

  @Test
  void distinctScheduleIdsProduceDistinctResources() {
    assertNotEquals(
        AuthorizationResource.schedule("sched-a"), AuthorizationResource.schedule("sched-b"));
  }
}
