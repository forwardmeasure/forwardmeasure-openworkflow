package com.forwardmeasure.openworkflow.workflow.runtime.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.data.DataReferences;
import com.forwardmeasure.openworkflow.data.RuntimeDataLimitException;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource;
import com.forwardmeasure.openworkflow.definition.WorkflowCoordinates;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class WorkflowDefinitionContractTest {
  private static final OksTenantId TENANT = OksTenantId.parse("did:web:tenant.example.com");
  private static final Instant NOW = Instant.parse("2026-07-28T20:00:00Z");
  private static final String SOURCE =
      """
      document:
        dsl: '1.0.3'
        namespace: evidence
        name: extraction
        version: '1.0.0'
      do:
        - initialize:
            set:
              status: ready
      """;

  @Test
  void admittedBundlePinsSourcePlanCompilerAndTenant() {
    var plan = new OpenWorkflowCompiler().compile(SOURCE.getBytes(StandardCharsets.UTF_8));
    var key = new WorkflowDefinitionKey(TENANT, plan.coordinates());
    var bundle =
        new WorkflowDefinitionBundle(
            key, SOURCE, plan, OpenWorkflowCompiler.COMPILER_SHA256, "admit-1", actor(TENANT), NOW);

    assertEquals(key, bundle.reference().key());
    assertEquals(plan.sourceSha256(), bundle.reference().sourceSha256());
  }

  @Test
  void inlineRuntimeDataFailsBeforeItCanExceedTheKafkaEnvelope() {
    var oversized =
        new ObjectMapper()
            .createObjectNode()
            .put("payload", "x".repeat(DataReferences.MAX_INLINE_BYTES));

    RuntimeDataLimitException failure =
        assertThrows(RuntimeDataLimitException.class, () -> DataReferences.inline(oversized));

    assertEquals(DataReferences.MAX_INLINE_BYTES, failure.maximumBytes());
    assertTrue(failure.actualBytes() > DataReferences.MAX_INLINE_BYTES);
  }

  @Test
  void bundleRejectsSourceThatDoesNotMatchItsCompiledPlan() {
    var plan = new OpenWorkflowCompiler().compile(SOURCE.getBytes(StandardCharsets.UTF_8));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new WorkflowDefinitionBundle(
                new WorkflowDefinitionKey(TENANT, plan.coordinates()),
                SOURCE + System.lineSeparator(),
                plan,
                OpenWorkflowCompiler.COMPILER_SHA256,
                "admit-1",
                actor(TENANT),
                NOW));
  }

  @Test
  void bundleRejectsCompilerProvenanceThatDoesNotMatchItsPlan() {
    var plan = new OpenWorkflowCompiler().compile(SOURCE.getBytes(StandardCharsets.UTF_8));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new WorkflowDefinitionBundle(
                new WorkflowDefinitionKey(TENANT, plan.coordinates()),
                SOURCE,
                plan,
                "f".repeat(64),
                "admit-1",
                actor(TENANT),
                NOW));
  }

  @Test
  void executionCannotReferenceAnotherTenantsDefinition() throws Exception {
    OksTenantId other = OksTenantId.parse("did:web:other-tenant.example.com");
    var coordinates = new WorkflowCoordinates("evidence", "extraction", "1", "1.0.3");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StartExecutionCommand(
                "start-1",
                new ExecutionKey(TENANT, new WorkflowExecutionId("run-1")),
                new WorkflowDefinitionReference(
                    new WorkflowDefinitionKey(other, coordinates), "1".repeat(64)),
                DataReferences.inline(new ObjectMapper().readTree("{}")),
                actor(TENANT),
                NOW));
  }

  @Test
  void durableAuditAndEffectRecordsRejectAnotherTenantsActor() throws Exception {
    OksTenantId other = OksTenantId.parse("did:web:other-tenant.example.com");
    ExecutionKey key = new ExecutionKey(TENANT, new WorkflowExecutionId("run-1"));
    ActorContext foreign = actor(other);
    var data = DataReferences.inline(new ObjectMapper().readTree("{}"));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ExecutionHistoryEvent(
                "event-1",
                key,
                0,
                ExecutionEventType.EXECUTION_STARTED,
                "1".repeat(64),
                null,
                null,
                null,
                data,
                data,
                foreign,
                NOW));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new WorkflowEffect(
                "effect-1",
                key,
                WorkflowEffectType.DISPATCH_OPERATION,
                "/do/0/extract",
                data,
                foreign,
                NOW));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new WorkflowDefinitionAdmissionEvent(
                "admission-event-1",
                "admit-1",
                new WorkflowDefinitionKey(
                    TENANT, new WorkflowCoordinates("evidence", "extraction", "1.0.0", "1.0.3")),
                WorkflowDefinitionAdmissionStatus.ADMITTED,
                "1".repeat(64),
                "2".repeat(64),
                "3".repeat(64),
                List.of(),
                foreign,
                NOW));
  }

  @Test
  void canonicalDefinitionKeysAreNotDelimiterAmbiguous() {
    var first = new WorkflowDefinitionKey(TENANT, new WorkflowCoordinates("a", "bc", "1", "1.0.3"));
    var second =
        new WorkflowDefinitionKey(TENANT, new WorkflowCoordinates("ab", "c", "1", "1.0.3"));

    assertNotEquals(first.canonical(), second.canonical());
  }

  @Test
  void publicationEdgeResolvesSchemasBeforeCreatingKafkaCommand() {
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: evidence
          name: external-schema
          version: '1.0.0'
        input:
          schema:
            format: json
            resource:
              endpoint: https://schemas.example.test/input.json
        do:
          - initialize:
              set:
                status: ready
        """;

    AdmitWorkflowDefinitionCommand command =
        WorkflowDefinitionPublication.prepare(
            "publish-1",
            TENANT,
            source,
            actor(TENANT),
            NOW,
            request ->
                ResolvedWorkflowResource.jsonSchema(
                    request.uri(),
                    """
                    {"type":"object","required":["instruction"]}
                    """));

    assertEquals(
        URI.create("https://schemas.example.test/input.json"),
        command.resources().getFirst().uri());
    assertEquals("external-schema", command.key().coordinates().name());
  }

  private static ActorContext actor(OksTenantId tenant) {
    return new ActorContext(
        tenant,
        ActorId.parse("did:web:tenant.example.com:actors:user-1"),
        ActorType.HUMAN,
        "User",
        "client",
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
