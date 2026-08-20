/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.forwardmeasure.openworkflow.engine.api.ActorId;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommandEnvelope;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.TenantActorContext;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OksKafkaCommandGatewayTest {
  @Test
  void preservesTrustedOrganizationInDurableActorContext() {
    TenantId tenant = new TenantId(UUID.fromString("134b09a7-1c36-4b89-86e7-a28c88bc5cef"));
    ExecutionId execution =
        new ExecutionId(tenant, UUID.fromString("254b09a7-1c36-4b89-86e7-a28c88bc5cef"));
    var envelope =
        new ExecutionCommandEnvelope(
            UUID.fromString("354b09a7-1c36-4b89-86e7-a28c88bc5cef"),
            "correlation-1",
            new TenantActorContext(
                tenant,
                "organization-1",
                new ActorId("actor-1"),
                java.util.Set.of("workflow-execution-controller")),
            EngineId.KAFKA_STREAMS,
            0,
            Instant.parse("2026-08-18T12:00:00Z"),
            new ExecutionCommand.Pause(execution));

    var actor = OksKafkaCommandGateway.actor(envelope);

    assertEquals("organization-1", actor.organizationId());
    assertEquals(java.util.Set.of("workflow-execution-controller"), actor.roles());
    assertEquals("did:forwardmeasure:tenant:" + tenant.value(), actor.tenantId().toString());
    assertEquals("did:forwardmeasure:actor:actor-1", actor.actorId().toString());
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
