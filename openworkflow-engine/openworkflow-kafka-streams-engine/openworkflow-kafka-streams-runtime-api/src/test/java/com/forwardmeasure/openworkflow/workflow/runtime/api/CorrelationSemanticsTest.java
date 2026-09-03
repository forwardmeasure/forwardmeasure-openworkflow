package com.forwardmeasure.openworkflow.workflow.runtime.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.data.DataReferences;
import com.forwardmeasure.openworkflow.definition.WorkflowCoordinates;
import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.Test;

class CorrelationSemanticsTest {
  private static final OksTenantId TENANT = OksTenantId.parse("did:web:ssb.example.test");

  private static final ExecutionKey EXECUTION =
      new ExecutionKey(TENANT, new WorkflowExecutionId("execution-1"));

  private static final Instant NOW = Instant.parse("2026-08-11T12:00:00Z");

  @Test
  void rootExecutionEstablishesItsExecutionIdAsBusinessCorrelation() {
    StartExecutionCommand command =
        new StartExecutionCommand(
            "start-1",
            EXECUTION,
            definition(),
            DataReferences.inline(JsonNodeFactory.instance.objectNode()),
            actor(null),
            NOW);

    assertEquals("execution-1", command.actor().correlationId().value());
  }

  @Test
  void rootExecutionPreservesAnExplicitUpstreamBusinessCorrelation() {
    StartExecutionCommand command =
        new StartExecutionCommand(
            "start-1",
            EXECUTION,
            definition(),
            DataReferences.inline(JsonNodeFactory.instance.objectNode()),
            actor("case-42"),
            NOW);

    assertEquals("case-42", command.actor().correlationId().value());
  }

  @Test
  void controlCommandDefaultsToTheControlledExecution() {
    ControlExecutionCommand command =
        new ControlExecutionCommand(
            "pause-1", EXECUTION, ExecutionControlAction.PAUSE, actor(null), NOW);

    assertEquals("execution-1", command.actor().correlationId().value());
  }

  @Test
  void systemActorFactoriesDistinguishUncorrelatedAndCorrelatedContexts() {
    ActorId runtime = ActorId.parse("did:web:ssb.example.test:actors:oks-workflow-runtime");

    ActorContext uncorrelated = Actors.system(TENANT, runtime, "oks-workflow-runtime", NOW);
    ActorContext correlated =
        Actors.systemCorrelated(
            TENANT,
            runtime,
            "oks-workflow-runtime",
            BusinessCorrelationId.parse("execution-1"),
            NOW);

    assertNull(uncorrelated.correlationId());
    assertEquals("execution-1", correlated.correlationId().value());
  }

  @Test
  void cloudEventDerivesStableBusinessCorrelationFromImmutableIdentity() {
    var envelope =
        JsonNodeFactory.instance
            .objectNode()
            .put("specversion", "1.0")
            .put("id", "event-1")
            .put("source", "https://evidence.example.test")
            .put("type", "com.example.evidence.received");

    InboundCloudEvent first =
        new InboundCloudEvent(TENANT, DataReferences.inline(envelope), actor(null), NOW);
    InboundCloudEvent repeated =
        new InboundCloudEvent(TENANT, DataReferences.inline(envelope), actor(null), NOW);

    assertEquals(first.acceptedBy().correlationId(), repeated.acceptedBy().correlationId());
    assertTrue(
        first.acceptedBy().correlationId().value().matches("cloudevent:sha256:[0-9a-f]{64}"));
    assertNotEquals(
        "00-907a081acf8fcf8133bdfd339bbda408-a17025478087051d-03",
        first.acceptedBy().correlationId().value());
  }

  @Test
  void cloudEventPreservesAnExplicitUpstreamBusinessCorrelation() {
    var envelope =
        JsonNodeFactory.instance
            .objectNode()
            .put("specversion", "1.0")
            .put("id", "event-1")
            .put("source", "https://evidence.example.test")
            .put("type", "com.example.evidence.received");

    InboundCloudEvent event =
        new InboundCloudEvent(
            TENANT, DataReferences.inline(envelope), actor("investigation-17"), NOW);

    assertEquals("investigation-17", event.acceptedBy().correlationId().value());
  }

  private static WorkflowDefinitionReference definition() {
    return new WorkflowDefinitionReference(
        new WorkflowDefinitionKey(
            TENANT, new WorkflowCoordinates("evidence", "extraction", "1.0.0", "1.0.3")),
        "a".repeat(64));
  }

  private static ActorContext actor(String correlationId) {
    return new ActorContext(
        TENANT,
        ActorId.parse("did:web:ssb.example.test:actors:user-1"),
        ActorType.HUMAN,
        "User One",
        "workbench",
        BusinessCorrelationId.parseNullable(correlationId),
        Set.of("workflow-control"),
        null,
        NOW);
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
