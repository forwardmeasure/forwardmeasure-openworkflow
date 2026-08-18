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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

final class WorkflowContractAnalyzerTest {

  @Test
  void rejectsAnIncompatibleSequentialTaskEdge() {
    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compile(
                    workflow(
                        """
                        - produce:
                            set:
                              customerId: one
                            output:
                              schema:
                                document:
                                  type: object
                                  required: [customerId]
                                  properties:
                                    customerId: {type: string}
                        - consume:
                            input:
                              schema:
                                document:
                                  type: object
                                  required: [customerId]
                                  properties:
                                    customerId: {type: integer}
                            set:
                              accepted: true
                        """)));

    assertTrue(
        failure
            .getMessage()
            .contains("/do/0/produce/output/schema -> " + "/do/1/consume/input/schema"));
    assertTrue(failure.getMessage().contains("INCOMPATIBLE"));
    assertTrue(failure.getMessage().contains("producer type string is not accepted"));
  }

  @Test
  void acceptsAProducerThatIsNarrowerThanItsConsumer() {
    WorkflowPlan plan =
        assertDoesNotThrow(
            () ->
                compile(
                    workflow(
                        """
                        - produce:
                            set:
                              customerId: one
                              jurisdiction: US
                            output:
                              schema:
                                document:
                                  type: object
                                  additionalProperties: false
                                  required: [customerId, jurisdiction]
                                  properties:
                                    customerId: {type: string, minLength: 3}
                                    jurisdiction: {const: US}
                        - consume:
                            input:
                              schema:
                                document:
                                  type: object
                                  additionalProperties: true
                                  required: [customerId]
                                  properties:
                                    customerId: {type: string, minLength: 1}
                            set:
                              accepted: true
                        """)));

    WorkflowContractAnalysis analysis =
        new WorkflowContractAnalyzer(plan.resources()).analyze(plan);

    assertTrue(analysis.proven());
    assertEquals(1, analysis.findings().size());
    assertEquals(SchemaCompatibilityStatus.COMPATIBLE, analysis.findings().getFirst().status());
  }

  @Test
  void compatibilityIsSubsumptionRatherThanSchemaEquality() {
    WorkflowPlan plan =
        assertDoesNotThrow(
            () ->
                compile(
                    workflow(
                        """
                        - produce:
                            set:
                              count: 3
                              state: complete
                            output:
                              schema:
                                document:
                                  type: object
                                  additionalProperties: false
                                  required: [count, state]
                                  properties:
                                    count:
                                      type: integer
                                      minimum: 1
                                      maximum: 10
                                    state:
                                      enum: [complete]
                        - consume:
                            input:
                              schema:
                                document:
                                  type: object
                                  additionalProperties: true
                                  required: [count]
                                  properties:
                                    count:
                                      type: number
                                      minimum: 0
                                      maximum: 100
                                    state:
                                      enum: [pending, complete, failed]
                            set:
                              accepted: true
                        """)));

    WorkflowContractAnalysis analysis =
        new WorkflowContractAnalyzer(plan.resources()).analyze(plan);

    assertTrue(analysis.proven());
    assertEquals(1, analysis.findings().size());
    assertTrue(
        !plan.steps()
            .getFirst()
            .dataFlow()
            .outputSchema()
            .document()
            .equals(plan.steps().get(1).dataFlow().inputSchema().document()));
  }

  @Test
  void provesCompatibilityThroughLocalSchemaReferences() {
    assertDoesNotThrow(
        () ->
            compile(
                workflow(
                    """
                    - produce:
                        set: '${ "customer-123" }'
                        output:
                          schema:
                            document:
                              $id: https://schemas.example.test/producer.json
                              $defs:
                                identifier:
                                  type: string
                                  minLength: 3
                              $ref: '#/$defs/identifier'
                    - consume:
                        input:
                          schema:
                            document:
                              type: string
                              minLength: 1
                        set:
                          accepted: true
                    """)));
  }

  @Test
  void doesNotIgnoreAssertionSiblingsOfAReference() {
    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compile(
                    workflow(
                        """
                        - produce:
                            set: '${ "customer-123" }'
                            output:
                              schema:
                                document:
                                  $id: https://schemas.example.test/producer-with-sibling.json
                                  $defs:
                                    identifier: {type: string}
                                  $ref: '#/$defs/identifier'
                                  maxLength: 12
                        - consume:
                            input:
                              schema:
                                document: {type: string}
                            set:
                              accepted: true
                        """)));

    assertTrue(failure.getMessage().contains("UNPROVEN"));
    assertTrue(failure.getMessage().contains("assertion siblings next to $ref"));
  }

  @Test
  void checksNamedControlFlowTargetsRatherThanYamlAdjacency() {
    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compile(
                    workflow(
                        """
                        - route:
                            set:
                              value: 7
                            output:
                              schema:
                                document: {type: integer}
                            then: target
                        - bypassed:
                            set:
                              bypassed: true
                        - target:
                            input:
                              schema:
                                document: {type: string}
                            set:
                              consumed: true
                        """)));

    assertTrue(
        failure
            .getMessage()
            .contains("/do/0/route/output/schema -> " + "/do/2/target/input/schema"));
  }

  @Test
  void checksWorkflowInputAndTerminalOutputBoundaries() {
    WorkflowDefinitionException inputFailure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compile(
                    """
                    document:
                      dsl: '1.0.3'
                      namespace: contracts
                      name: input-boundary
                      version: '1.0.0'
                    input:
                      schema:
                        document: {type: string}
                    do:
                      - consume:
                          input:
                            schema:
                              document: {type: integer}
                          set:
                            complete: true
                    """));
    assertTrue(inputFailure.getMessage().contains("/input/schema -> /do/0/consume/input/schema"));

    WorkflowDefinitionException outputFailure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compile(
                    """
                    document:
                      dsl: '1.0.3'
                      namespace: contracts
                      name: output-boundary
                      version: '1.0.0'
                    do:
                      - produce:
                          set:
                            complete: true
                          output:
                            schema:
                              document: {type: string}
                    output:
                      schema:
                        document: {type: integer}
                    """));
    assertTrue(
        outputFailure.getMessage().contains("/do/0/produce/output/schema -> /output/schema"));
  }

  @Test
  void permitsAnExplicitWorkflowOutputTransformationBetweenSchemas() {
    assertDoesNotThrow(
        () ->
            compile(
                """
                document:
                  dsl: '1.0.3'
                  namespace: contracts
                  name: transformed-output-boundary
                  version: '1.0.0'
                do:
                  - produce:
                      set:
                        result: complete
                      output:
                        schema:
                          document:
                            type: object
                            required: [result]
                output:
                  as: '${ {answer: .result} }'
                  schema:
                    document:
                      type: object
                      required: [answer]
                """));
  }

  @Test
  void failsClosedWhenSchemaInclusionCannotBeProven() {
    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compile(
                    workflow(
                        """
                        - produce:
                            set: '${ "ABC-123" }'
                            output:
                              schema:
                                document:
                                  type: string
                                  pattern: '^[A-Z]+-[0-9]+$'
                        - consume:
                            input:
                              schema:
                                document:
                                  type: string
                                  pattern: '^[A-Z]{3}-[0-9]{3}$'
                            set:
                              accepted: true
                        """)));

    assertTrue(failure.getMessage().contains("UNPROVEN"));
    assertTrue(failure.getMessage().contains("cannot prove pattern constraint inclusion"));
  }

  @Test
  void preservesStandardsComplianceForAnUntypedEdge() {
    assertDoesNotThrow(
        () ->
            compile(
                workflow(
                    """
                    - produce:
                        set:
                          anything: true
                    - consume:
                        input:
                          schema:
                            document: {type: string}
                        set:
                          accepted: true
                    """)));
  }

  @Test
  void checksConditionalPassThroughAsASeparateProducerContract() {
    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compile(
                    workflow(
                        """
                        - maybeTransform:
                            if: '${ .enabled }'
                            input:
                              schema:
                                document:
                                  type: object
                                  required: [enabled]
                            set: '${ "transformed" }'
                            output:
                              schema:
                                document: {type: string}
                        - consume:
                            input:
                              schema:
                                document: {type: string}
                            set:
                              accepted: true
                        """)));

    assertTrue(
        failure
            .getMessage()
            .contains("/do/0/maybeTransform/input/schema -> " + "/do/1/consume/input/schema"));
  }

  @Test
  void checksNestedDoTaskEdges() {
    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compile(
                    workflow(
                        """
                        - composite:
                            do:
                              - produce:
                                  set: '${ 1 }'
                                  output:
                                    schema:
                                      document: {type: integer}
                              - consume:
                                  input:
                                    schema:
                                      document: {type: boolean}
                                  set: '${ true }'
                        """)));

    assertTrue(
        failure
            .getMessage()
            .contains(
                "/do/0/composite/do/0/produce/output/schema -> "
                    + "/do/0/composite/do/1/consume/input/schema"),
        failure.getMessage());
  }

  @Test
  void checksEveryForkBranchEntryContract() {
    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compile(
                    workflow(
                        """
                        - parallel:
                            input:
                              schema:
                                document: {type: string}
                            fork:
                              branches:
                                - numericBranch:
                                    input:
                                      schema:
                                        document: {type: integer}
                                    set: '${ 1 }'
                                - textBranch:
                                    input:
                                      schema:
                                        document: {type: string}
                                    set: '${ . }'
                        """)));

    assertTrue(
        failure
            .getMessage()
            .contains(
                "/do/0/parallel/input/schema -> "
                    + "/do/0/parallel/fork/branches/0/0/"
                    + "numericBranch/input/schema"),
        failure.getMessage());
  }

  @Test
  void checksLoopBodyEdges() {
    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compile(
                    workflow(
                        """
                        - loop:
                            for:
                              in:
                                - {value: 1}
                                - {value: 2}
                            do:
                              - produce:
                                  set: '${ 1 }'
                                  output:
                                    schema:
                                      document: {type: integer}
                              - consume:
                                  input:
                                    schema:
                                      document: {type: string}
                                  set: '${ . }'
                        """)));

    assertTrue(
        failure
            .getMessage()
            .contains(
                "/do/0/loop/do/0/produce/output/schema -> "
                    + "/do/0/loop/do/1/consume/input/schema"),
        failure.getMessage());
  }

  @Test
  void checksLoopDeclaredBoundaryEdges() {
    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compile(
                    workflow(
                        """
                        - loop:
                            input:
                              schema:
                                document: {type: string}
                            for:
                              in:
                                - {value: 1}
                                - {value: 2}
                            do:
                              - consume:
                                  input:
                                    schema:
                                      document: {type: integer}
                                  set: '${ 1 }'
                        """)));

    assertTrue(
        failure
            .getMessage()
            .contains("/do/0/loop/input/schema -> " + "/do/0/loop/do/0/consume/input/schema"),
        failure.getMessage());
  }

  @Test
  void checksTryBodyEdges() {
    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compile(
                    workflow(
                        """
                        - guarded:
                            try:
                              - produce:
                                  set: '${ 1 }'
                                  output:
                                    schema:
                                      document: {type: integer}
                              - consume:
                                  input:
                                    schema:
                                      document: {type: string}
                                  set: '${ . }'
                            catch:
                              errors:
                                with:
                                  type: '*'
                        """)));

    assertTrue(
        failure
            .getMessage()
            .contains(
                "/do/0/guarded/try/0/produce/output/schema -> "
                    + "/do/0/guarded/try/1/consume/input/schema"),
        failure.getMessage());
  }

  private static WorkflowPlan compile(String source) {
    return new OpenWorkflowCompiler().compile(source.getBytes(StandardCharsets.UTF_8));
  }

  private static String workflow(String tasks) {
    return """
    document:
      dsl: '1.0.3'
      namespace: contracts
      name: edge-analysis
      version: '1.0.0'
    do:
    """
        + tasks;
  }
}
