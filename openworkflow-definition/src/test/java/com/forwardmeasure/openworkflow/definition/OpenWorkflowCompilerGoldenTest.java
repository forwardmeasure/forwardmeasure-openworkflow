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
package com.forwardmeasure.openworkflow.definition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class OpenWorkflowCompilerGoldenTest {
  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
  private final OpenWorkflowCompiler compiler = new OpenWorkflowCompiler();

  @Test
  void compiledPortableShapeMatchesTheReviewedGolden() throws IOException {
    WorkflowPlan plan = compiler.compile(resource("compiler-golden/basic.workflow.yaml"));
    var actual = YAML.createObjectNode();
    actual.set("coordinates", YAML.valueToTree(plan.coordinates()));
    actual.set("expressions", YAML.valueToTree(plan.expressions()));
    var steps = actual.putArray("steps");
    for (PlanStep step : plan.steps()) {
      var item = steps.addObject();
      item.put("name", step.name());
      item.put("path", step.path());
      item.put("kind", step.kind().name());
    }

    assertEquals(YAML.readTree(resource("compiler-golden/basic.plan.yaml")), actual);
  }

  @Test
  void duplicateYamlKeysAreRejectedBeforeCompilation() {
    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compiler.compile(
                    """
                    document:
                      dsl: '1.0.3'
                      namespace: duplicate
                      name: first
                      name: second
                      version: '1.0.0'
                    do:
                      - initialize:
                          set:
                            ready: true
                    """
                        .getBytes(StandardCharsets.UTF_8)));

    assertTrue(failure.getMessage().contains("Duplicate field 'name'"));
  }

  @Test
  void compiledPlanDoesNotExposeMutableJsonTrees() throws IOException {
    WorkflowPlan plan = compiler.compile(resource("compiler-golden/basic.workflow.yaml"));

    ((ObjectNode) plan.definition()).put("tampered", true);
    ((ObjectNode) plan.steps().getFirst().definition()).put("tampered", true);
    ((ObjectNode) plan.steps().getFirst().configuration()).put("tampered", true);

    assertFalse(plan.definition().has("tampered"));
    assertFalse(plan.steps().getFirst().definition().has("tampered"));
    assertFalse(plan.steps().getFirst().configuration().has("tampered"));
  }

  private static byte[] resource(String name) throws IOException {
    try (var input =
        OpenWorkflowCompilerGoldenTest.class.getClassLoader().getResourceAsStream(name)) {
      if (input == null) {
        throw new IOException("Missing test resource " + name);
      }
      return input.readAllBytes();
    }
  }
}
