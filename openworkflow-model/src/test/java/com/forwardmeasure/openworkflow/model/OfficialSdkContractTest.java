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
package com.forwardmeasure.openworkflow.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.serverlessworkflow.api.WorkflowFormat;
import io.serverlessworkflow.api.WorkflowReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

class OfficialSdkContractTest {
  private static final String EXPECTED_DSL =
      System.getProperty("openworkflow.specification.version");
  private static final String EXPECTED_SCHEMA_SHA256 =
      System.getProperty("serverlessworkflow.schema.sha256");

  @Test
  void sdkReaderAcceptsThePinnedOpenWorkflowDsl() throws IOException {
    var workflow =
        WorkflowReader.readWorkflowFromString(
            """
            document:
              dsl: '1.0.3'
              namespace: forwardmeasure
              name: sdk-contract
              version: '1.0.0'
            do:
              - initialize:
                  set:
                    accepted: true
            """,
            WorkflowFormat.YAML);

    assertEquals(EXPECTED_DSL, workflow.getDocument().getDsl());
    assertEquals("forwardmeasure", workflow.getDocument().getNamespace());
    assertEquals(1, workflow.getDo().size());
  }

  @Test
  void sdkReaderRejectsSchemaInvalidDefinitions() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WorkflowReader.readWorkflowFromString(
                """
                document:
                  dsl: '1.0.3'
                do: []
                """,
                WorkflowFormat.YAML));
  }

  @Test
  void packagedSchemaMatchesTheRecordedOpenWorkflow103Digest()
      throws IOException, NoSuchAlgorithmException {
    byte[] schema;
    try (InputStream input =
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("schema/workflow.yaml")) {
      assertTrue(input != null, "SDK must package its normative schema");
      schema = input.readAllBytes();
    }

    String schemaText = new String(schema, StandardCharsets.UTF_8);
    assertTrue(
        schemaText.startsWith(
            "$id: https://open-workflow-specification.org/schemas/1.0.3/workflow.yaml"));
    assertEquals(EXPECTED_SCHEMA_SHA256, HexFormat.of().formatHex(sha256().digest(schema)));
  }

  @Test
  void sdkReaderExercisesTheCompleteOfficialV103ExampleCorpus() throws Exception {
    List<Path> examples = fixtures("examples");
    assertEquals(66, examples.size(), "the complete v1.0.3 example corpus must remain pinned");
    var rejected = new ArrayList<String>();
    for (Path example : examples) {
      try {
        WorkflowReader.readWorkflowFromString(Files.readString(example), WorkflowFormat.YAML);
      } catch (IllegalArgumentException failure) {
        rejected.add(example.getFileName().toString());
      }
    }
    // This upstream example uses the pre-1.0 arguments/stdin shape and is rejected by both the
    // tag's normative schema and the pinned SDK. Keep the discrepancy explicit and deterministic.
    assertEquals(List.of("run-script-with-stdin-and-arguments.yaml"), rejected);
  }

  @Test
  void sdkReaderRejectsEveryOfficialV103InvalidFixture() throws Exception {
    List<Path> invalid = fixtures("invalid");
    assertEquals(3, invalid.size(), "the complete v1.0.3 invalid corpus must remain pinned");
    for (Path fixture : invalid) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              WorkflowReader.readWorkflowFromString(Files.readString(fixture), WorkflowFormat.YAML),
          fixture.toString());
    }
  }

  @Test
  void officialCorpusMatchesItsRecordedDigests() throws Exception {
    Path root = resourcePath("official-v1.0.3");
    for (String line : Files.readAllLines(root.resolve("SHA256SUMS"))) {
      String[] parts = line.split("  ", 2);
      assertEquals(
          parts[0],
          HexFormat.of().formatHex(sha256().digest(Files.readAllBytes(root.resolve(parts[1])))),
          parts[1]);
    }
  }

  private static List<Path> fixtures(String directory) throws Exception {
    try (var paths = Files.list(resourcePath("official-v1.0.3/" + directory))) {
      return paths
          .filter(path -> path.getFileName().toString().endsWith(".yaml"))
          .sorted()
          .toList();
    }
  }

  private static Path resourcePath(String name) throws Exception {
    var resource = OfficialSdkContractTest.class.getClassLoader().getResource(name);
    if (resource == null) {
      throw new IllegalStateException("Missing test resource " + name);
    }
    return Path.of(resource.toURI());
  }

  private static MessageDigest sha256() throws NoSuchAlgorithmException {
    return MessageDigest.getInstance("SHA-256");
  }
}
