package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.Actors;
import com.forwardmeasure.openworkflow.workflow.runtime.api.AdvanceExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.BusinessCorrelationId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ControlExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReferences;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionControlAction;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionEventType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionHistoryEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionPurgePolicyDecision;
import com.forwardmeasure.openworkflow.workflow.runtime.api.FireTimerCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.HumanTaskObservation;
import com.forwardmeasure.openworkflow.workflow.runtime.api.HumanTaskObservationStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ObserveAsyncApiSubscriptionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ObserveHumanTaskCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ObserveOperationCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ObserveWorkflowComputationCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ObserveWorkflowComputationFailureCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OksTenantId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservation;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservationStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.PurgeExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ReapplyExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ReceiveAsyncApiMessageCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ReceiveEventCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.StartExecutionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionAdmissionEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionAdmissionStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionBundle;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionCatalogueEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffectType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowError;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowExecutionId;
import com.forwardmeasure.openworkflow.workflow.runtime.core.ExecutionCursor;
import com.forwardmeasure.openworkflow.workflow.runtime.core.ExecutionPhase;
import com.forwardmeasure.openworkflow.workflow.runtime.core.ExecutionSnapshot;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Golden wire-contract gate for the records retained in Kafka topics and changelogged stores.
 *
 * <p>A changed digest is a deliberate compatibility event. It must be reviewed with a
 * migration/dual-read strategy rather than silently updating this test. Additive fields remain
 * readable across a rolling deployment: absent fields take their record default and unknown future
 * fields are ignored.
 */
final class DurableWireCompatibilityTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Instant NOW = Instant.parse("2026-07-30T12:34:56Z");
  private static final OksTenantId TENANT = OksTenantId.parse("did:web:tenant.example.com");
  private static final ActorContext ACTOR =
      new ActorContext(
          TENANT,
          ActorId.parse("did:web:tenant.example.com:actors:user-17"),
          ActorType.HUMAN,
          "User Seventeen",
          "ssb-public",
          BusinessCorrelationId.parse("correlation-17"),
          Set.of("workflow-start", "evidence-control"),
          ActorId.parse("did:web:tenant.example.com:actors:delegating-service"),
          NOW);
  private static final byte[] SOURCE =
      """
      document:
        dsl: '1.0.3'
        namespace: compatibility
        name: durable-wire
        version: '1.0.0'
      do:
        - initialise:
            set:
              accepted: true
      """
          .getBytes(StandardCharsets.UTF_8);

  @Test
  void pinsAndRoundTripsEveryAuthoritativeDurableRecord() throws Exception {
    Contracts contracts = contracts();

    assertAll(
        "Every authoritative durable record retains its reviewed wire contract",
        () ->
            assertContract(
                contracts.bundle(),
                WorkflowDefinitionBundle.class,
                "6e2f789cfc5e031098676725386dfb98d03468e993c1458030a46e68e2c59777"),
        () ->
            assertContract(
                contracts.catalogue(),
                WorkflowDefinitionCatalogueEvent.class,
                "f68a36e5b97f87ef00ee95379253a24bd5f38fd5cebd5a1ffb2459d76ca0a021"),
        () ->
            assertSnapshotContract(
                contracts.snapshot(),
                "67a863f5989a8b5d82455d62c6e1c567431be50e0628fb907758fc96366677fe"),
        () ->
            assertContract(
                contracts.command(),
                ExecutionCommand.class,
                "804cbbca06c6a690dd074ddcdc163822b9df710fb83c4853ebf7f9bd00b2da0b"),
        () ->
            assertContract(
                contracts.history(),
                ExecutionHistoryEvent.class,
                "c42904bb9d1a77f5a56c55c44054d3b205be457b249bbbb33e6dec4f628e1175"),
        () ->
            assertContract(
                contracts.effect(),
                WorkflowEffect.class,
                "3e96ddb80aa7e41dc9ff7f46098067452327351cc23cb550dc75006d70fba1c5"));
  }

  @Test
  void readsMissingAndUnknownAdditiveFieldsDuringRollingUpgrade() throws Exception {
    StartExecutionCommand expected = contracts().command();
    var serde = new JsonSerde<>(ExecutionCommand.class);
    ObjectNode encoded =
        (ObjectNode) JSON.readTree(serde.serializer().serialize("commands", expected));
    ((ObjectNode) encoded.required("actor")).remove("correlationId");
    encoded.put("futureAdditiveField", "ignored-by-this-reader");
    ((ObjectNode) encoded.required("actor")).put("futureActorField", 42);

    StartExecutionCommand restored =
        (StartExecutionCommand)
            serde.deserializer().deserialize("commands", JSON.writeValueAsBytes(encoded));

    assertEquals(restored.key().executionId().value(), restored.actor().correlationId().value());
    assertEquals(expected.commandId(), restored.commandId());
    assertEquals(expected.key(), restored.key());
    assertEquals(expected.definition(), restored.definition());
    assertEquals(expected.input(), restored.input());
    assertEquals(expected.actor().actorId(), restored.actor().actorId());
    assertEquals(expected.actor().delegatedBy(), restored.actor().delegatedBy());
    assertEquals(expected.actor().roles(), restored.actor().roles());
    assertNull(restored.actor().organizationId());
  }

  @Test
  void preservesOrganizationAndReadsLegacyActorsWithoutIt() throws Exception {
    var serde = new JsonSerde<>(ActorContext.class);
    ActorContext current =
        new ActorContext(
            ACTOR.tenantId(),
            ACTOR.actorId(),
            ACTOR.actorType(),
            ACTOR.displayName(),
            ACTOR.clientId(),
            ACTOR.correlationId(),
            ACTOR.roles(),
            ACTOR.delegatedBy(),
            ACTOR.authenticatedAt(),
            ACTOR.identityProvider(),
            ACTOR.subjectIdentifier(),
            "organization-17");

    byte[] encoded = serde.serializer().serialize("actors", current);
    ActorContext restored = serde.deserializer().deserialize("actors", encoded);
    assertEquals("organization-17", restored.organizationId());
    assertEquals(
        "organization-17",
        restored.withCorrelationId(BusinessCorrelationId.parse("next")).organizationId());

    ObjectNode legacy = (ObjectNode) JSON.readTree(encoded);
    legacy.remove("organizationId");
    ActorContext legacyRestored =
        serde.deserializer().deserialize("actors", JSON.writeValueAsBytes(legacy));
    assertNull(legacyRestored.organizationId());
  }

  @Test
  void executionStateSizeDoesNotGrowWithTheAdmittedDefinition() throws Exception {
    String giantLiteral = "x".repeat(2 * 1024 * 1024);
    byte[] source =
        ("""
        document:
          dsl: '1.0.3'
          namespace: compatibility
          name: large-definition
          version: '1.0.0'
        do:
          - initialise:
              set:
                large: '%s'
        """
                .formatted(giantLiteral))
            .getBytes(StandardCharsets.UTF_8);
    var plan = new OpenWorkflowCompiler().compile(source);
    var reference =
        new WorkflowDefinitionReference(
            new WorkflowDefinitionKey(TENANT, plan.coordinates()),
            plan.sourceSha256(),
            plan.definitionSha256());
    var input = DataReferences.inline(JSON.readTree("{\"instruction\":\"bounded-state\"}"));
    var snapshot =
        new ExecutionSnapshot(
            new ExecutionKey(TENANT, new WorkflowExecutionId("large-definition-run")),
            reference,
            plan,
            ACTOR,
            NOW,
            ExecutionPhase.RUNNING,
            ExecutionCursor.start(input),
            input,
            input,
            input,
            1);

    var serde = new JsonSerde<>(ExecutionSnapshot.class);
    byte[] encoded = serde.serializer().serialize("execution-state", snapshot);
    ExecutionSnapshot restored = serde.deserializer().deserialize("execution-state", encoded);

    assertTrue(
        encoded.length < 16 * 1024,
        "Per-execution state must remain bounded independently "
            + "of definition size; bytes="
            + encoded.length);
    assertNull(restored.plan());
    assertEquals(reference, restored.definition());
    assertEquals(snapshot.key(), restored.key());
  }

  @Test
  void currentReaderAcceptsLegacySnapshotsThatEmbeddedThePlan() throws Exception {
    ExecutionSnapshot expected = contracts().snapshot();
    var serde = new JsonSerde<>(ExecutionSnapshot.class);
    ObjectNode legacy =
        (ObjectNode)
            JSON.readTree(serde.serializer().serialize("legacy-execution-state", expected));
    legacy.set("plan", JSON.valueToTree(expected.plan()));

    ExecutionSnapshot restored =
        serde.deserializer().deserialize("legacy-execution-state", JSON.writeValueAsBytes(legacy));

    assertNull(restored.plan());
    assertEquals(expected.key(), restored.key());
    assertEquals(expected.definition(), restored.definition());
  }

  @Test
  void pinsAndRoundTripsEveryExecutionCommandVariant() throws Exception {
    Contracts contracts = contracts();
    ExecutionKey key = contracts.command().key();
    var system =
        Actors.systemCorrelated(
            TENANT,
            ActorId.parse("did:web:tenant.example.com:actors:oks-workflow-runtime"),
            "oks-workflow-runtime",
            BusinessCorrelationId.parse("correlation-17"),
            NOW);
    var data = contracts.command().input();
    var error =
        new WorkflowError(
            "https://example.test/problems/remote",
            502,
            "operation-17",
            "Remote operation failed",
            "The upstream service rejected the request");
    var cloudEvent =
        DataReferences.inline(
            JSON.readTree(
                """
                {
                  "specversion": "1.0",
                  "id": "event-17",
                  "source": "https://example.test/evidence",
                  "type": "evidence.extracted",
                  "data": {"accepted": true}
                }
                """));
    List<ExecutionCommand> commands =
        List.of(
            contracts.command(),
            new ControlExecutionCommand(
                "pause-wire-contract-run", key, ExecutionControlAction.PAUSE, ACTOR, NOW),
            new AdvanceExecutionCommand("advance-wire-contract-run", key, 7, system, NOW),
            new ReceiveEventCommand(
                "event-wire-contract-run", key, "subscription-17", cloudEvent, system, NOW),
            new ReceiveAsyncApiMessageCommand(
                "async-message-wire-contract-run",
                key,
                "async-subscription-17",
                "evidence.events/2/41",
                data,
                system,
                NOW),
            new ObserveAsyncApiSubscriptionCommand(
                "async-observation-wire-contract-run",
                key,
                "async-subscription-17",
                error,
                system,
                NOW),
            new FireTimerCommand("timer-wire-contract-run", key, "timer-17", system, NOW),
            new ObserveOperationCommand(
                "operation-wire-contract-run",
                key,
                "operation-17",
                new OperationObservation(OperationObservationStatus.SUCCEEDED, data, null, null),
                system,
                NOW),
            new ObserveHumanTaskCommand(
                "human-task-wire-contract-run",
                key,
                "human-task-17",
                new BusinessCorrelationId("human-correlation-17"),
                new HumanTaskObservation(
                    "human-outcome-17", HumanTaskObservationStatus.APPROVED, data, ACTOR, NOW),
                system,
                NOW),
            new ObserveWorkflowComputationCommand(
                "computation-wire-contract-run",
                key,
                "computation-17",
                7,
                sha256(JSON.writeValueAsBytes(JSON.readTree("{\"state\":\"prepared\"}"))),
                JSON.readTree("{\"state\":\"prepared\"}"),
                system,
                NOW),
            new ObserveWorkflowComputationFailureCommand(
                "computation-failure-wire-contract-run",
                key,
                "computation-17",
                7,
                "urn:oks:error:workflow-computation",
                "Computation failed after five attempts",
                system,
                NOW),
            new ReapplyExecutionCommand(
                new ControlExecutionCommand(
                    "queued-pause-wire-contract-run",
                    key,
                    ExecutionControlAction.PAUSE,
                    ACTOR,
                    NOW),
                7),
            new PurgeExecutionCommand(
                "purge-wire-contract-run",
                key,
                new ExecutionPurgePolicyDecision(
                    "purge-decision-17",
                    "retention-policy-1.0.0",
                    key,
                    NOW,
                    NOW.minusSeconds(1),
                    false,
                    "standard"),
                ACTOR,
                NOW,
                7L));
    Map<Class<?>, String> expectedDigests =
        Map.ofEntries(
            Map.entry(
                StartExecutionCommand.class,
                "804cbbca06c6a690dd074ddcdc163822b9df710fb83c4853ebf7f9bd00b2da0b"),
            Map.entry(
                ControlExecutionCommand.class,
                "ebccf7b45664a569e378aaf76802065cc0c72e02f2ae8a70266fe86ed0f8a510"),
            Map.entry(
                AdvanceExecutionCommand.class,
                "b2fab685699df2282fd72db02f351c65492198acc662fa46e7faa965a1e4139b"),
            Map.entry(
                ReceiveEventCommand.class,
                "d183c087c04b910f711b5e0f2d7338e0b6a1596769612addd924bcc1a02a2f20"),
            Map.entry(
                ReceiveAsyncApiMessageCommand.class,
                "5d3e6d87de980a8bcef9110358dbc54616a464c19c11bec1a224ac60e623ec3e"),
            Map.entry(
                ObserveAsyncApiSubscriptionCommand.class,
                "5d7e0571782a61be83759bfca6f019d7c6cc18c193a305cf4fc9f7d5f21e3b5e"),
            Map.entry(
                FireTimerCommand.class,
                "2db360cc40cb60692f67c979ffc930f3758930b3e03d584ea0b7185e2da8626c"),
            Map.entry(
                ObserveOperationCommand.class,
                "1ffb33026b4bfd59dd0bc74bc336181ce665c07151b567c8819af6197809beec"),
            Map.entry(
                ObserveHumanTaskCommand.class,
                "2763a1e5e31df70f78dc9855d5c901c5d2f017aa3b1936fd79324b34d2a6111c"),
            Map.entry(
                ObserveWorkflowComputationCommand.class,
                "a255b4296521e1ba5a54d65c8c48456148a120f86e0fb9624f32e9f587f8ce42"),
            Map.entry(
                ObserveWorkflowComputationFailureCommand.class,
                "c1cd41f9765f00c988d0db07dc287e52b65687cf0707beca4f2514c9aaf6214f"),
            Map.entry(
                ReapplyExecutionCommand.class,
                "0f34505c4e7b84b72ea311a4b9bc8e847fcf696e3a5ba16c561c341683ef4365"),
            Map.entry(
                PurgeExecutionCommand.class,
                "d8c1d872a0e831ac8d2358879e280f36cfb4fada371d8b329275de7483e7069c"));

    assertAll(
        "Every execution-command variant retains its reviewed wire contract",
        commands.stream()
            .map(
                command ->
                    (Executable)
                        () ->
                            assertContract(
                                command,
                                ExecutionCommand.class,
                                expectedDigests.get(command.getClass()))));
  }

  private static <T> void assertContract(T expected, Class<T> type, String expectedSha256) {
    var serde = new JsonSerde<>(type);
    byte[] encoded = serde.serializer().serialize("durable-contract", expected);
    assertEquals(expectedSha256, sha256(encoded), type.getName());
    assertEquals(
        expected, serde.deserializer().deserialize("durable-contract", encoded), type.getName());
  }

  private static void assertSnapshotContract(ExecutionSnapshot expected, String expectedSha256) {
    var serde = new JsonSerde<>(ExecutionSnapshot.class);
    byte[] encoded = serde.serializer().serialize("durable-contract", expected);
    assertEquals(expectedSha256, sha256(encoded), ExecutionSnapshot.class.getName());
    ExecutionSnapshot restored = serde.deserializer().deserialize("durable-contract", encoded);
    assertNull(restored.plan());
    assertEquals(expected.key(), restored.key());
    assertEquals(expected.definition(), restored.definition());
    assertEquals(expected.startedBy(), restored.startedBy());
    assertEquals(expected.startedAt(), restored.startedAt());
    assertEquals(expected.phase(), restored.phase());
    assertEquals(expected.cursor(), restored.cursor());
    assertEquals(expected.initialInput(), restored.initialInput());
    assertEquals(expected.context(), restored.context());
    assertEquals(expected.data(), restored.data());
    assertEquals(expected.nextSequence(), restored.nextSequence());
    assertEquals(expected.failure(), restored.failure());
    assertEquals(expected.laneRootTaskPath(), restored.laneRootTaskPath());
    assertEquals(expected.activeFork(), restored.activeFork());
    assertEquals(expected.forkPositions(), restored.forkPositions());
    assertEquals(expected.pendingInteraction(), restored.pendingInteraction());
    assertEquals(expected.activeTimeouts(), restored.activeTimeouts());
    assertEquals(expected.cancellation(), restored.cancellation());
    assertEquals(expected.pendingComputation(), restored.pendingComputation());
  }

  private static Contracts contracts() throws Exception {
    var plan = new OpenWorkflowCompiler().compile(SOURCE);
    WorkflowDefinitionKey definitionKey = new WorkflowDefinitionKey(TENANT, plan.coordinates());
    WorkflowDefinitionReference definition =
        new WorkflowDefinitionReference(
            definitionKey, plan.sourceSha256(), plan.definitionSha256());
    WorkflowDefinitionBundle bundle =
        new WorkflowDefinitionBundle(
            definitionKey,
            new String(SOURCE, StandardCharsets.UTF_8),
            plan,
            OpenWorkflowCompiler.COMPILER_SHA256,
            "admit-compatible-definition",
            ACTOR,
            NOW);
    WorkflowDefinitionAdmissionEvent admission =
        new WorkflowDefinitionAdmissionEvent(
            definitionKey.canonical() + ":admit-compatible-definition",
            "admit-compatible-definition",
            definitionKey,
            WorkflowDefinitionAdmissionStatus.ADMITTED,
            plan.sourceSha256(),
            plan.definitionSha256(),
            OpenWorkflowCompiler.COMPILER_SHA256,
            List.of(),
            ACTOR,
            NOW);
    WorkflowDefinitionCatalogueEvent catalogue =
        new WorkflowDefinitionCatalogueEvent(admission, bundle);
    ExecutionKey key = new ExecutionKey(TENANT, new WorkflowExecutionId("wire-contract-run"));
    var input =
        DataReferences.inline(
            JSON.readTree(
                "{\"instruction\":\"extract entities\"," + "\"preserveArtifacts\":true}"));
    StartExecutionCommand command =
        new StartExecutionCommand("start-wire-contract-run", key, definition, input, ACTOR, NOW);
    ExecutionSnapshot snapshot =
        new ExecutionSnapshot(
            key,
            definition,
            plan,
            ACTOR,
            NOW,
            ExecutionPhase.RUNNING,
            ExecutionCursor.start(input),
            input,
            input,
            input,
            1);
    ExecutionHistoryEvent history =
        new ExecutionHistoryEvent(
            "wire-contract-run:0",
            key,
            0,
            ExecutionEventType.EXECUTION_STARTED,
            plan.definitionSha256(),
            null,
            null,
            null,
            input,
            input,
            ACTOR,
            NOW);
    WorkflowEffect effect =
        new WorkflowEffect(
            "wire-contract-run:effect:0",
            key,
            WorkflowEffectType.DISPATCH_OPERATION,
            "/do/0/initialise",
            input,
            ACTOR,
            NOW);
    return new Contracts(bundle, catalogue, snapshot, command, history, effect);
  }

  private static String sha256(byte[] value) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
    } catch (Exception impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  private record Contracts(
      WorkflowDefinitionBundle bundle,
      WorkflowDefinitionCatalogueEvent catalogue,
      ExecutionSnapshot snapshot,
      StartExecutionCommand command,
      ExecutionHistoryEvent history,
      WorkflowEffect effect) {}
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
