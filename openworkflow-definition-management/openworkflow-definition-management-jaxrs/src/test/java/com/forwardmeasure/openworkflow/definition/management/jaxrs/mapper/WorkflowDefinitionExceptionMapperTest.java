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
package com.forwardmeasure.openworkflow.definition.management.jaxrs.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.forwardmeasure.openworkflow.common.model.Problem;
import com.forwardmeasure.openworkflow.common.model.Violation;
import com.forwardmeasure.openworkflow.definition.WorkflowDefinitionException;
import jakarta.ws.rs.core.Response;
import java.util.List;
import org.junit.jupiter.api.Test;

class WorkflowDefinitionExceptionMapperTest {
  private final WorkflowDefinitionExceptionMapper mapper = new WorkflowDefinitionExceptionMapper();

  @Test
  void mapsToAnUnprocessableEntityProblemResponse() {
    try (Response response =
        mapper.toResponse(new WorkflowDefinitionException(List.of("/do/0/greet not valid")))) {
      assertEquals(422, response.getStatus());
      assertEquals("application/problem+json", response.getMediaType().toString());
      Problem problem = (Problem) response.getEntity();
      assertEquals("Unprocessable Entity", problem.getTitle());
      assertEquals(422, problem.getStatus());
    }
  }

  @Test
  void splitsALeadingJsonPointerIntoTheViolationField() {
    try (Response response =
        mapper.toResponse(
            new WorkflowDefinitionException(
                List.of("/do/2/task3 [required] required property 'call' not found")))) {
      Problem problem = (Problem) response.getEntity();
      List<Violation> violations = problem.getViolations();
      assertEquals(1, violations.size());
      assertEquals("/do/2/task3", violations.get(0).getField());
      assertEquals("[required] required property 'call' not found", violations.get(0).getMessage());
    }
  }

  @Test
  void fallsBackToTheWholeStringWhenThereIsNoLeadingPointer() {
    // WorkflowContractAnalyzer's schema-compatibility findings don't lead with a JSON Pointer.
    try (Response response =
        mapper.toResponse(
            new WorkflowDefinitionException(
                List.of("Schema compatibility task fetchPet output is not compatible with ...")))) {
      Problem problem = (Problem) response.getEntity();
      List<Violation> violations = problem.getViolations();
      assertEquals(1, violations.size());
      assertNull(violations.get(0).getField());
      assertEquals(
          "Schema compatibility task fetchPet output is not compatible with ...",
          violations.get(0).getMessage());
    }
  }

  @Test
  void preservesEveryViolationAndTheJoinedDetailMessage() {
    List<String> violations =
        List.of("/do/0/a not valid", "/do/1/b not valid", "Schema compatibility c/d mismatch");
    try (Response response = mapper.toResponse(new WorkflowDefinitionException(violations))) {
      Problem problem = (Problem) response.getEntity();
      assertEquals(3, problem.getViolations().size());
      assertEquals(String.join(System.lineSeparator(), violations), problem.getDetail());
    }
  }
}
