package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.openworkflow.definition.WorkflowCoordinates;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorType;
import com.forwardmeasure.openworkflow.workflow.runtime.api.AdmitWorkflowDefinitionCommand;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OksTenantId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionAdmissionEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionAdmissionStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionKey;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Processor-level proof that a compilable definition whose compiled bundle would exceed the
 * provisioned definition-topic ceiling is rejected before any bundle reaches Kafka state or output
 * topics.
 */
class DefinitionRecordLimitTest {
  private static final OksTenantId TENANT = OksTenantId.parse("did:web:tenant.example.test");
  private static final WorkflowDefinitionKey KEY =
      new WorkflowDefinitionKey(
          TENANT, new WorkflowCoordinates("record-limits", "oversized-bundle", "1.0.0", "1.0.3"));
  private static final Instant NOW = Instant.parse("2026-07-31T00:00:00Z");

  @TempDir Path stateDirectory;

  @Test
  void rejectsCompiledBundleBeforeDefinitionTopicsOrStores() {
    OksTopics topics = OksTopics.withPrefix("test.oks.definition-record-limit");
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "definition-record-limit-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.toString());
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: record-limits
          name: oversized-bundle
          version: '1.0.0'
          summary: |
            %s
        do:
          - finish:
              set:
                status: complete
        """
            .formatted("x".repeat(2_900_000));
    var admission =
        new AdmitWorkflowDefinitionCommand("admit-oversized-bundle", KEY, source, actor(), NOW);

    try (var driver =
        new TopologyTestDriver(
            new OksTopology(ActorId.parse(TENANT + ":actors:runtime"), "definition-limit-test")
                .build(topics),
            properties)) {
      var commands =
          driver.createInputTopic(
              topics.definitionCommands(),
              new StringSerializer(),
              new JsonSerde<>(AdmitWorkflowDefinitionCommand.class).serializer());
      var decisions =
          driver.createOutputTopic(
              topics.definitionHistory(),
              new StringDeserializer(),
              new JsonSerde<>(WorkflowDefinitionAdmissionEvent.class).deserializer());
      var definitions =
          driver.createOutputTopic(
              topics.definitions(),
              new StringDeserializer(),
              new JsonSerde<>(
                      com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionBundle
                          .class)
                  .deserializer());

      commands.pipeInput(KEY.canonical(), admission);

      WorkflowDefinitionAdmissionEvent rejected = decisions.readValue();
      assertEquals(WorkflowDefinitionAdmissionStatus.REJECTED, rejected.status());
      assertTrue(
          rejected.issues().stream()
              .anyMatch(
                  issue ->
                      issue.contains("Workflow definition record is")
                          && issue.contains("maximum is")),
          rejected.issues().toString());
      assertTrue(
          definitions.isEmpty(),
          "An oversized immutable bundle must never reach " + "the definitions topic");
    }
  }

  private static ActorContext actor() {
    return new ActorContext(
        TENANT,
        ActorId.parse(TENANT + ":actors:publisher"),
        ActorType.HUMAN,
        "Publisher",
        "definition-test",
        Set.of("workflow-publish"),
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
