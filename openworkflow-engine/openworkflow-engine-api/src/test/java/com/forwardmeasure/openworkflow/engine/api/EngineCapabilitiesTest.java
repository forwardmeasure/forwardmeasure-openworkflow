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
package com.forwardmeasure.openworkflow.engine.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.openworkflow.definition.CallPlan;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.RunPlan;
import com.forwardmeasure.openworkflow.definition.WorkflowPlan;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Codifies the gap audit (forwardmeasure-openworkflow/docs/engine-construct-gap-audit.md) as
 * assertions - if a future engine change adds or removes support for a construct without updating
 * {@link EngineCapabilities}, one of these should fail.
 */
class EngineCapabilitiesTest {

  @Test
  void pekkoDeclaresEveryConstructSupportedExceptHumanTask() {
    for (CallPlan.Kind kind : CallPlan.Kind.values()) {
      boolean expectedSupported = kind != CallPlan.Kind.HUMAN_TASK;
      assertEquals(expectedSupported, EngineCapabilities.PEKKO.supports(kind), kind.toString());
    }
    for (RunPlan.Kind kind : RunPlan.Kind.values()) {
      assertTrue(EngineCapabilities.PEKKO.supports(kind), kind.toString());
    }
  }

  @Test
  void kafkaStreamsDeclaresEveryConstructSupportedExceptHumanTask() {
    for (CallPlan.Kind kind : CallPlan.Kind.values()) {
      boolean expectedSupported = kind != CallPlan.Kind.HUMAN_TASK;
      assertEquals(
          expectedSupported, EngineCapabilities.KAFKA_STREAMS.supports(kind), kind.toString());
    }
    for (RunPlan.Kind kind : RunPlan.Kind.values()) {
      assertTrue(EngineCapabilities.KAFKA_STREAMS.supports(kind), kind.toString());
    }
  }

  @Test
  void forEngineResolvesTheKnownConstantsAndRejectsUnknownIds() {
    assertEquals(EngineCapabilities.PEKKO, EngineCapabilities.forEngine(EngineId.PEKKO));
    assertEquals(
        EngineCapabilities.KAFKA_STREAMS, EngineCapabilities.forEngine(EngineId.KAFKA_STREAMS));
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> EngineCapabilities.forEngine(new EngineId("some-future-engine")));
  }

  @Test
  void findsAHumanTaskCallNestedInsideADoBlockOnBothEngines() {
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: wp4
                  name: nested-human-task
                  version: '1.0.0'
                do:
                  - wrapper:
                      do:
                        - approve:
                            call: com.forwardmeasure.openworkflow.human-task
                            with:
                              title: Review extracted evidence
                              presentation:
                                kind: RAW_JSON
                              approvals:
                                stages:
                                  - level: 1
                                    name: First Review
                                    requiredApprovals: 1
                                    candidateRoles: [evidence-reviewer]
                """
                    .getBytes(StandardCharsets.UTF_8));

    Optional<com.forwardmeasure.openworkflow.definition.PlanStep> pekkoFound =
        EngineCapabilities.PEKKO.findUnsupportedStep(plan);
    Optional<com.forwardmeasure.openworkflow.definition.PlanStep> kafkaFound =
        EngineCapabilities.KAFKA_STREAMS.findUnsupportedStep(plan);

    assertTrue(pekkoFound.isPresent());
    assertEquals(CallPlan.Kind.HUMAN_TASK, pekkoFound.orElseThrow().callPlan().kind());
    assertTrue(kafkaFound.isPresent());
    assertEquals(CallPlan.Kind.HUMAN_TASK, kafkaFound.orElseThrow().callPlan().kind());
  }

  @Test
  void aSubworkflowRunIsAcceptedByBothEnginesNowThatKafkaStreamsSupportsIt() {
    var child =
        new com.forwardmeasure.openworkflow.definition.ResolvedSubflow(
            new com.forwardmeasure.openworkflow.definition.WorkflowCoordinates(
                "evidence", "child", "2.0.0", "1.0.3"),
            "a".repeat(64),
            "b".repeat(64));
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: has-subworkflow
                  version: '1.0.0'
                do:
                  - child:
                      run:
                        workflow:
                          namespace: evidence
                          name: child
                          version: '2.0.0'
                          input:
                            evidenceId: '${ .evidenceId }'
                """
                    .getBytes(StandardCharsets.UTF_8),
                java.util.List.of(),
                (namespace, name, version) ->
                    "evidence".equals(namespace) && "child".equals(name) && "2.0.0".equals(version)
                        ? Optional.of(child)
                        : Optional.empty());

    assertTrue(EngineCapabilities.PEKKO.findUnsupportedStep(plan).isEmpty());
    assertTrue(EngineCapabilities.KAFKA_STREAMS.findUnsupportedStep(plan).isEmpty());
  }

  @Test
  void aPlanUsingOnlyOrdinaryConstructsIsAcceptedByBothEngines() {
    WorkflowPlan plan =
        new OpenWorkflowCompiler()
            .compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: wp4
                  name: plain
                  version: '1.0.0'
                do:
                  - initialize:
                      set:
                        ready: true
                """
                    .getBytes(StandardCharsets.UTF_8));

    assertFalse(EngineCapabilities.PEKKO.findUnsupportedStep(plan).isPresent());
    assertFalse(EngineCapabilities.KAFKA_STREAMS.findUnsupportedStep(plan).isPresent());
  }
}
