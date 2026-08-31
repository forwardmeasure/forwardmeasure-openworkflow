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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OpenWorkflowCompilerTest {
  private final OpenWorkflowCompiler compiler = new OpenWorkflowCompiler();

  @Test
  void compilerProfileHasAStableContentDigest() {
    assertTrue(OpenWorkflowCompiler.COMPILER_SHA256.matches("[0-9a-f]{64}"));
    assertEquals(
        "e0cfa77227a9b537be7519041683508151e21dc80aa991dfbf6362dfc36b2331",
        OpenWorkflowCompiler.SCHEMA_SHA256);
  }

  @Test
  void immutableDefinitionDigestPinsTheCompilerProfile() {
    byte[] source =
        yaml(
            """
            document:
              dsl: '1.0.3'
              namespace: evidence
              name: compiler-pinned
              version: '1.0.0'
            do:
              - initialize:
                  set:
                    ready: true
            """);

    WorkflowPlan first = compiler.compile(source);
    WorkflowPlan second = compiler.compile(source);

    assertNotEquals(first.sourceSha256(), first.definitionSha256());
    assertEquals(first.definitionSha256(), second.definitionSha256());
  }

  @Test
  void compilesReusableExtensionsIntoTypedDurableMiddleware() {
    WorkflowPlan plan =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: extension-middleware
                  version: '1.0.0'
                use:
                  extensions:
                    - audit:
                        extend: all
                        when: '${ $task.name == "initialize" }'
                        before:
                          - record-start:
                              set:
                                stage: before
                        after:
                          - record-end:
                              set:
                                stage: after
                do:
                  - initialize:
                      set:
                        ready: true
                """));

    PlanStep wrapper = plan.steps().getFirst();
    assertEquals(PlanStepKind.EXTENSION, wrapper.kind());
    assertEquals("initialize", wrapper.name());
    assertEquals(1, wrapper.extensionPlan().applications().size());
    assertEquals(PlanStepKind.SET, wrapper.extensionPlan().target().kind());
    assertEquals(
        List.of("record-start", "initialize", "record-end"),
        wrapper.children().stream().map(PlanStep::name).toList());
  }

  @Test
  void compilesNestedDoAndLiteralSetFromNormativeSource() {
    WorkflowPlan plan =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: extract
                  version: '1.0.0'
                do:
                  - prepare:
                      do:
                        - initialize:
                            set:
                              status: ready
                """));

    assertEquals("evidence", plan.coordinates().namespace());
    assertEquals(PlanStepKind.DO, plan.steps().getFirst().kind());
    assertEquals(PlanStepKind.SET, plan.steps().getFirst().children().getFirst().kind());
    assertEquals(
        "/do/0/prepare/do/0/initialize", plan.steps().getFirst().children().getFirst().path());
  }

  @Test
  void compilesGovernedHumanWorkThroughTheCustomFunctionExtensionPoint() {
    WorkflowPlan plan =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: governed-review
                  version: '1.0.0'
                do:
                  - approve:
                      call: com.forwardmeasure.oks.human-task
                      with:
                        title: Review extracted evidence
                        input: '${ . }'
                        presentation:
                          kind: RAW_JSON
                        approvals:
                          makerChecker: true
                          distinctApprovers: true
                          stages:
                            - level: 1
                              name: First Review
                              requiredApprovals: 1
                              candidateRoles: [evidence-reviewer]
                        dueAfter: PT4H
                """));

    PlanStep step = plan.steps().getFirst();
    assertEquals(PlanStepKind.CALL, step.kind());
    assertEquals(CallPlan.Kind.HUMAN_TASK, step.callPlan().kind());
    assertTrue(step.children().isEmpty());
    assertEquals(
        "Review extracted evidence", step.callPlan().arguments().required("title").textValue());
  }

  @Test
  void compilesOneCorrelatedAsyncApiWorkerLifecycle() {
    URI documentUri = URI.create("https://contracts.test/workers.asyncapi.yaml");
    ResolvedWorkflowResource resource =
        ResolvedWorkflowResource.of(
            documentUri,
            "application/yaml",
            """
            asyncapi: 3.0.0
            info:
              title: Workers
              version: 1.0.0
            servers:
              broker:
                host: kafka.test:9092
                protocol: kafka
            channels:
              commands:
                address: workers.commands
              events:
                address: workers.events
            operations:
              submit:
                action: send
                channel:
                  $ref: '#/channels/commands'
              observe:
                action: receive
                channel:
                  $ref: '#/channels/events'
              cancel:
                action: send
                channel:
                  $ref: '#/channels/commands'
            """);
    WorkflowPlan plan =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: workers
                  name: correlated
                  version: '1.0.0'
                do:
                  - execute:
                      call: com.forwardmeasure.oks.correlated-worker
                      with:
                        document:
                          endpoint:
                            uri: https://contracts.test/workers.asyncapi.yaml
                        command:
                          operation: submit
                          message:
                            payload:
                              request: '${ . }'
                        events:
                          operation: observe
                          subscription:
                            consume:
                              until: '${ .payload.status == "SUCCEEDED" }'
                              for: PT30M
                        cancellation:
                          operation: cancel
                          message:
                            payload: {}
                """),
            List.of(resource));

    CallPlan call = plan.steps().getFirst().callPlan();
    assertEquals(CallPlan.Kind.CORRELATED_WORKER, call.kind());
    assertEquals(WorkflowResourceKind.ASYNC_API_DOCUMENT, call.resource().kind());
    assertTrue(call.asyncApiSubscription() != null);
    assertTrue(call.asyncApiSubscription().consumption().duration() != null);
  }

  @Test
  void resolverDiscoversTheCorrelatedWorkerAsyncApiDocument() {
    byte[] source =
        yaml(
            """
            document:
              dsl: '1.0.3'
              namespace: workers
              name: resolved-correlated-worker
              version: '1.0.0'
            do:
              - execute:
                  call: com.forwardmeasure.oks.correlated-worker
                  with:
                    document:
                      endpoint:
                        uri: https://contracts.test/workers.asyncapi.yaml
                    command:
                      operation: submit
                      message:
                        payload: {}
                    events:
                      operation: observe
                      subscription:
                        consume:
                          until: '${ .payload.status == "SUCCEEDED" }'
                          for: PT30M
                    cancellation:
                      operation: cancel
                      message:
                        payload: {}
            """);

    var resources =
        new WorkflowResourceResolver()
            .resolve(
                source,
                request ->
                    ResolvedWorkflowResource.of(
                        request.uri(),
                        "application/yaml",
                        """
                        asyncapi: 3.0.0
                        info:
                          title: Workers
                          version: 1.0.0
                        servers:
                          kafka:
                            host: kafka.test:9092
                            protocol: kafka
                        channels:
                          commands:
                            address: workers.commands
                          events:
                            address: workers.events
                        operations:
                          submit:
                            action: send
                            channel:
                              $ref: '#/channels/commands'
                          observe:
                            action: receive
                            channel:
                              $ref: '#/channels/events'
                          cancel:
                            action: send
                            channel:
                              $ref: '#/channels/commands'
                        """));

    assertEquals(1, resources.size());
    assertEquals(
        URI.create("https://contracts.test/workers.asyncapi.yaml"), resources.getFirst().uri());
  }

  @Test
  void schemaViolationsNeverProduceAPlan() {
    var failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compiler.compile(
                    yaml(
                        """
                        document:
                          dsl: '1.0.3'
                          namespace: evidence
                          name: extract
                        do: []
                        """)));

    assertTrue(failure.violations().stream().anyMatch(value -> value.contains("version")));
  }

  @Test
  void rejectsWorkflowSourceLargerThanTheAdmissionLimit() {
    byte[] oversized = new byte[OpenWorkflowCompiler.MAX_SOURCE_BYTES + 1];

    WorkflowDefinitionException failure =
        assertThrows(WorkflowDefinitionException.class, () -> compiler.compile(oversized));

    assertEquals(
        List.of("Workflow source exceeds " + OpenWorkflowCompiler.MAX_SOURCE_BYTES + " bytes"),
        failure.violations());
  }

  @Test
  void acceptsValidWorkflowSourceAboveJacksonsDefaultStringLimit() {
    String source =
        """
        document:
          dsl: '1.0.3'
          namespace: admission-limits
          name: large-valid-source
          version: '1.0.0'
          summary: |
            %s
        do:
          - finish:
              set:
                status: complete
        """
            .formatted("x".repeat(3_200_000));

    WorkflowPlan plan = compiler.compile(yaml(source));

    assertEquals("large-valid-source", plan.coordinates().name());
  }

  @Test
  void rejectsAnExternalResourceLargerThanThePerResourceLimit() {
    String oversized = "x".repeat(ResolvedWorkflowResource.MAX_CONTENT_BYTES + 1);

    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ResolvedWorkflowResource.of(
                    URI.create("https://contracts.test/oversized.json"),
                    "application/json",
                    oversized));

    assertEquals(
        "Workflow resource exceeds " + ResolvedWorkflowResource.MAX_CONTENT_BYTES + " bytes",
        failure.getMessage());
  }

  @Test
  void rejectsMoreExternalResourcesThanTheAdmissionLimit() {
    List<ResolvedWorkflowResource> resources = new ArrayList<>();
    for (int index = 0; index <= WorkflowResourceResolver.MAX_RESOURCES; index++) {
      resources.add(
          ResolvedWorkflowResource.json(
              URI.create("https://contracts.test/" + index + ".json"), "{}"));
    }

    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () -> compiler.compile(minimalWorkflow(), resources));

    assertEquals(
        List.of("/: resolved workflow resources exceed " + WorkflowResourceResolver.MAX_RESOURCES),
        failure.violations());
  }

  @Test
  void rejectsExternalResourcesWhoseCombinedSizeExceedsTheAdmissionLimit() {
    int resourceBytes = ResolvedWorkflowResource.MAX_CONTENT_BYTES;
    String content = "x".repeat(resourceBytes);
    List<ResolvedWorkflowResource> resources = new ArrayList<>();
    for (int index = 0; index < 5; index++) {
      resources.add(
          ResolvedWorkflowResource.of(
              URI.create("https://contracts.test/large-" + index),
              "application/octet-stream",
              content));
    }

    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () -> compiler.compile(minimalWorkflow(), resources));

    assertEquals(
        List.of(
            "/: resolved workflow resources exceed "
                + WorkflowResourceResolver.MAX_TOTAL_BYTES
                + " bytes"),
        failure.violations());
  }

  @Test
  void compilesJqExpressionsAndCommonTaskFlow() {
    WorkflowPlan plan =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: extract
                  version: '1.0.0'
                input:
                  from: '${ {instruction: .instruction, enabled: .enabled} }'
                do:
                  - initialize:
                      if: '${ .enabled }'
                      input:
                        from: '${ .instruction }'
                      set:
                        copied: '${ . }'
                      output:
                        as:
                          result: '${ .copied }'
                      export:
                        as: '${ $context + {last: .result} }'
                      then: end
                output:
                  as: '${ {result: .result} }'
                """));

    PlanStep step = plan.steps().getFirst();
    assertEquals("${ .enabled }", step.dataFlow().condition());
    assertEquals("${ .instruction }", step.dataFlow().inputFrom().textValue());
    assertEquals("end", step.dataFlow().thenDirective());
    assertEquals("${ {result: .result} }", plan.dataFlow().outputAs().textValue());
  }

  @Test
  void compilesOrderedSwitchCasesAndTheirScopedTargets() {
    WorkflowPlan plan =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: classify
                  version: '1.0.0'
                do:
                  - choose:
                      switch:
                        - fallback:
                            then: other
                        - red:
                            when: '${ .color == "red" }'
                            then: selected
                      then: end
                  - selected:
                      set:
                        result: selected
                  - other:
                      set:
                        result: other
                """));

    PlanStep step = plan.steps().getFirst();
    assertEquals(PlanStepKind.SWITCH, step.kind());
    assertEquals(2, step.switchCases().size());
    assertTrue(step.switchCases().getFirst().defaultCase());
    assertEquals("${ .color == \"red\" }", step.switchCases().get(1).condition());
    assertEquals("selected", step.switchCases().get(1).thenDirective());
    assertEquals("end", step.dataFlow().thenDirective());
  }

  @Test
  void rejectsAmbiguousOrOutOfScopeSwitchFlow() {
    var duplicateDefault =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compiler.compile(
                    yaml(
                        """
                        document:
                          dsl: '1.0.3'
                          namespace: evidence
                          name: invalid-switch
                          version: '1.0.0'
                        do:
                          - choose:
                              switch:
                                - first:
                                    then: end
                                - second:
                                    then: end
                        """)));
    assertTrue(duplicateDefault.getMessage().contains("only one default case"));

    var unknownTarget =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compiler.compile(
                    yaml(
                        """
                        document:
                          dsl: '1.0.3'
                          namespace: evidence
                          name: invalid-switch
                          version: '1.0.0'
                        do:
                          - choose:
                              switch:
                                - red:
                                    when: '${ .color == "red" }'
                                    then: outside-this-scope
                        """)));
    assertTrue(unknownTarget.getMessage().contains("does not exist in the same scope"));

    WorkflowPlan bareCondition =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: standards-expression-switch
                  version: '1.0.0'
                do:
                  - choose:
                      switch:
                        - red:
                            when: .color == "red"
                            then: end
                """));
    assertEquals(
        "${ .color == \"red\" }",
        bareCondition.steps().getFirst().switchCases().getFirst().condition());
  }

  @Test
  void compilesForExpressionsInlineCollectionsAndNestedTasks() {
    WorkflowPlan expressionPlan =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: iterate
                  version: '1.0.0'
                do:
                  - loopColors:
                      while: '${ .continue }'
                      for:
                        each: color
                        in: '${ .colors }'
                        at: position
                      do:
                        - record:
                            set:
                              value: '${ $color }'
                              position: '${ $position }'
                """));

    PlanStep loop = expressionPlan.steps().getFirst();
    assertEquals(PlanStepKind.FOR, loop.kind());
    assertEquals("color", loop.forPlan().itemVariable());
    assertEquals("position", loop.forPlan().indexVariable());
    assertEquals("${ .colors }", loop.forPlan().collection().textValue());
    assertEquals("${ .continue }", loop.forPlan().whileCondition());
    assertEquals("/do/0/loopColors/do/0/record", loop.children().getFirst().path());

    WorkflowPlan inlinePlan =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: inline-iterate
                  version: '1.0.0'
                do:
                  - loopColors:
                      for:
                        in:
                          - name: red
                          - name: green
                      do:
                        - record:
                            set:
                              value: '${ $item.name }'
                              position: '${ $index }'
                """));
    assertTrue(inlinePlan.steps().getFirst().forPlan().collection().isArray());
    assertEquals("item", inlinePlan.steps().getFirst().forPlan().itemVariable());
    assertEquals("index", inlinePlan.steps().getFirst().forPlan().indexVariable());
  }

  @Test
  void rejectsInvalidForVariableBindingsAndNormalizesExpressions() {
    var sameVariable =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compiler.compile(
                    yaml(
                        """
                        document:
                          dsl: '1.0.3'
                          namespace: evidence
                          name: invalid-for
                          version: '1.0.0'
                        do:
                          - loop:
                              for:
                                each: value
                                in: '${ .values }'
                                at: value
                              do:
                                - record:
                                    set:
                                      value: '${ $value }'
                        """)));
    assertTrue(sameVariable.getMessage().contains("must name different variables"));

    var reservedVariable =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compiler.compile(
                    yaml(
                        """
                        document:
                          dsl: '1.0.3'
                          namespace: evidence
                          name: invalid-for
                          version: '1.0.0'
                        do:
                          - loop:
                              for:
                                each: context
                                in: '${ .values }'
                              do:
                                - record:
                                    set:
                                      value: '${ $context }'
                        """)));
    assertTrue(reservedVariable.getMessage().contains("reserved by Open Workflow"));

    WorkflowPlan bareWhile =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: standards-expression-for
                  version: '1.0.0'
                do:
                  - loop:
                      for:
                        in: '${ .values }'
                      while: .continue
                      do:
                        - record:
                            set:
                              value: '${ $item }'
                """));
    assertEquals("${ .continue }", bareWhile.steps().getFirst().forPlan().whileCondition());
  }

  @Test
  void compilesForkBranchesInDeclarationOrderAndCompeteMode() {
    WorkflowPlan plan =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: parallel-extract
                  version: '1.0.0'
                do:
                  - parallel:
                      fork:
                        compete: true
                        branches:
                          - people:
                              set:
                                kind: people
                          - organisations:
                              do:
                                - extract:
                                    set:
                                      kind: organisations
                """));

    PlanStep fork = plan.steps().getFirst();
    assertEquals(PlanStepKind.FORK, fork.kind());
    assertTrue(fork.forkPlan().compete());
    assertEquals(
        java.util.List.of("people", "organisations"),
        fork.children().stream().map(PlanStep::name).toList());
    assertEquals(
        "/do/0/parallel/fork/branches/1/0/organisations/do/0/extract",
        fork.children().get(1).children().getFirst().path());
  }

  @Test
  void compilesCloudEventsEmitPropertiesAsDefinitionOwnedTemplates() {
    WorkflowPlan plan =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: emit-result
                  version: '1.0.0'
                do:
                  - publish:
                      emit:
                        event:
                          with:
                            source: https://evidence.example.test
                            type: com.forwardmeasure.evidence.extracted.v1
                            data:
                              evidenceId: '${ .evidenceId }'
                """));

    PlanStep emit = plan.steps().getFirst();
    assertEquals(PlanStepKind.EMIT, emit.kind());
    assertEquals(
        "com.forwardmeasure.evidence.extracted.v1",
        emit.configuration().required("type").textValue());
    assertEquals(
        "${ .evidenceId }",
        emit.configuration().required("data").required("evidenceId").textValue());
  }

  @Test
  void explicitExpressionFieldsNormalizeBareJq() {
    WorkflowPlan plan =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: extract
                  version: '1.0.0'
                do:
                  - initialize:
                      if: .enabled
                      set:
                        status: ready
                """));
    assertEquals("${ .enabled }", plan.steps().getFirst().dataFlow().condition());
  }

  @Test
  void compilesAndEnforcesInlineJsonSchemas() throws Exception {
    WorkflowPlan plan =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: extract
                  version: '1.0.0'
                input:
                  schema:
                    format: json
                    document:
                      type: object
                      required: [instruction]
                      properties:
                        instruction:
                          type: string
                do:
                  - initialize:
                      set:
                        status: ready
                """));

    assertEquals("/input/schema", plan.dataFlow().inputSchema().definitionPath());
    assertTrue(!plan.dataFlow().inputSchema().external());
    var validator = new DataSchemaValidator(plan.resources());
    validator.validate(
        plan.dataFlow().inputSchema(),
        new ObjectMapper().readTree("{\"instruction\":\"extract\"}"));
    assertThrows(
        DataSchemaValidationException.class,
        () -> validator.validate(plan.dataFlow().inputSchema(), new ObjectMapper().readTree("{}")));

    var malformed =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compiler.compile(
                    yaml(
                        """
                        document:
                          dsl: '1.0.3'
                          namespace: evidence
                          name: malformed-schema
                          version: '1.0.0'
                        input:
                          schema:
                            format: json
                            document:
                              type: definitely-not-a-json-schema-type
                        do:
                          - initialize:
                              set:
                                status: ready
                        """)));
    assertTrue(malformed.getMessage().contains("invalid JSON Schema"));
  }

  @Test
  void schemaValidationIgnoresPinnedNonSchemaProtocolResources() throws Exception {
    ResolvedWorkflowResource proto =
        ResolvedWorkflowResource.of(
            URI.create("https://contracts.test/extractor.proto"),
            "text/x-protobuf",
            """
            syntax = "proto3";
            message Evidence { string id = 1; }
            """);
    JsonNode schemaDocument =
        new ObjectMapper()
            .readTree(
                """
                {"type":"object","required":["evidenceId"]}
                """);
    ResolvedDataSchema schema =
        new ResolvedDataSchema("/input/schema", "json", null, "a".repeat(64), schemaDocument);

    new DataSchemaValidator(List.of(proto))
        .validate(schema, new ObjectMapper().readTree("{\"evidenceId\":\"E-1\"}"));
  }

  @Test
  void resolvesAndPinsTransitiveExternalSchemaGraph() throws Exception {
    byte[] source =
        yaml(
            """
            document:
              dsl: '1.0.3'
              namespace: evidence
              name: extract
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
            """);
    Map<URI, String> documents =
        Map.of(
            URI.create("https://schemas.example.test/input.json"),
            """
            {
              "$schema": "https://json-schema.org/draft/2020-12/schema",
              "$ref": "types/instruction.json"
            }
            """,
            URI.create("https://schemas.example.test/types/instruction.json"),
            """
            {
              "type": "object",
              "required": ["instruction"],
              "properties": {"instruction": {"type": "string"}}
            }
            """);
    var resources =
        new WorkflowResourceResolver()
            .resolve(
                source,
                request ->
                    ResolvedWorkflowResource.jsonSchema(
                        request.uri(), documents.get(request.uri())));

    WorkflowPlan plan = compiler.compile(source, resources);

    assertEquals(2, plan.resources().size());
    assertEquals(
        URI.create("https://schemas.example.test/input.json"),
        plan.dataFlow().inputSchema().resourceUri());
    var validator = new DataSchemaValidator(plan.resources());
    validator.validate(
        plan.dataFlow().inputSchema(),
        new ObjectMapper().readTree("{\"instruction\":\"extract\"}"));
    assertThrows(
        DataSchemaValidationException.class,
        () -> validator.validate(plan.dataFlow().inputSchema(), new ObjectMapper().readTree("{}")));
    var missing = assertThrows(WorkflowDefinitionException.class, () -> compiler.compile(source));
    assertTrue(missing.getMessage().contains("was not resolved before publication"));
  }

  @Test
  void externalSchemaEndpointMaySelectAJsonPointerFragment() throws Exception {
    byte[] source =
        yaml(
            """
            document:
              dsl: '1.0.3'
              namespace: evidence
              name: fragment-schema
              version: '1.0.0'
            input:
              schema:
                format: json
                resource:
                  endpoint: https://schemas.example.test/bundle.json#/$defs/input
            do:
              - initialize:
                  set:
                    status: ready
            """);
    String bundle =
        """
        {
          "$defs": {
            "input": {
              "type": "object",
              "required": ["instruction"]
            }
          }
        }
        """;
    var resources =
        new WorkflowResourceResolver()
            .resolve(source, request -> ResolvedWorkflowResource.jsonSchema(request.uri(), bundle));

    WorkflowPlan plan = compiler.compile(source, resources);
    assertEquals(
        "#/$defs/input",
        plan.dataFlow()
            .inputSchema()
            .resourceUri()
            .getRawFragment()
            .transform(value -> "#" + value));
    var validator = new DataSchemaValidator(resources);
    assertThrows(
        DataSchemaValidationException.class,
        () -> validator.validate(plan.dataFlow().inputSchema(), new ObjectMapper().readTree("{}")));
  }

  @Test
  void keepsCanonicalSchemaIdsLocalToAnImmutableRetrievalResource() throws Exception {
    byte[] source =
        yaml(
            """
            document:
              dsl: '1.0.3'
              namespace: evidence
              name: internal-schema-endpoint
              version: '1.0.0'
            input:
              schema:
                format: json
                resource:
                  endpoint: http://contracts:8080/contracts/input.json
            do:
              - initialize:
                  set:
                    status: ready
            """);
    URI retrievalUri = URI.create("http://contracts:8080/contracts/input.json");
    String schema =
        """
        {
          "$id": "https://schemas.example.test/input.json",
          "type": "object",
          "properties": {
            "instruction": {"$ref": "#/$defs/instruction"}
          },
          "$defs": {
            "instruction": {"type": "string"}
          }
        }
        """;

    var resources =
        new WorkflowResourceResolver()
            .resolve(source, request -> ResolvedWorkflowResource.jsonSchema(request.uri(), schema));

    assertEquals(
        List.of(retrievalUri), resources.stream().map(ResolvedWorkflowResource::uri).toList());
    WorkflowPlan plan = compiler.compile(source, resources);
    assertEquals(retrievalUri, plan.dataFlow().inputSchema().resourceUri());
  }

  @Test
  void resolvesExternalReferencesFromAnInlineSchema() throws Exception {
    byte[] source =
        yaml(
            """
            document:
              dsl: '1.0.3'
              namespace: evidence
              name: inline-reference
              version: '1.0.0'
            input:
              schema:
                format: json
                document:
                  $id: https://schemas.example.test/workflow-input.json
                  $ref: common/instruction.json
            do:
              - initialize:
                  set:
                    status: ready
            """);
    URI referenced = URI.create("https://schemas.example.test/common/instruction.json");
    var resources =
        new WorkflowResourceResolver()
            .resolve(
                source,
                request ->
                    ResolvedWorkflowResource.jsonSchema(
                        request.uri(),
                        """
                        {"type":"object","required":["instruction"]}
                        """));

    assertEquals(referenced, resources.getFirst().uri());
    WorkflowPlan plan = compiler.compile(source, resources);
    assertThrows(
        DataSchemaValidationException.class,
        () ->
            new DataSchemaValidator(resources)
                .validate(plan.dataFlow().inputSchema(), new ObjectMapper().readTree("{}")));
  }

  @Test
  void compilesNormativeListenStrategiesCorrelationsAndForeach() {
    WorkflowPlan plan =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: listen
                  version: '1.0.0'
                do:
                  - collect:
                      listen:
                        to:
                          any:
                            - with:
                                type: evidence.received.v1
                              correlate:
                                caseId:
                                  from: .caseId
                          until: ( . | length ) >= 2
                        read: envelope
                      foreach:
                        item: event
                        at: eventIndex
                        do:
                          - retain:
                              set:
                                retained: ${ $event }
                """));

    PlanStep step = plan.steps().getFirst();
    assertEquals(PlanStepKind.LISTEN, step.kind());
    assertEquals(EventConsumptionPlan.Mode.ANY, step.listenPlan().consumption().mode());
    assertEquals("( . | length ) >= 2", step.listenPlan().consumption().untilCondition());
    assertEquals(
        "caseId",
        step.listenPlan().consumption().filters().getFirst().correlations().getFirst().name());
    assertEquals(EventReadMode.ENVELOPE, step.listenPlan().readAs());
    assertEquals("event", step.listenPlan().itemVariable());
    assertEquals(PlanStepKind.SET, step.children().getFirst().kind());
  }

  @Test
  void compilesEveryNormativeWaitDurationForm() {
    WorkflowPlan plan =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: waits
                  version: '1.0.0'
                do:
                  - literal:
                      wait: PT30S
                  - inline:
                      wait:
                        minutes: 1
                        milliseconds: 250
                  - expression:
                      wait: '${ .delay }'
                """));

    assertEquals(
        List.of(DurationPlan.Kind.LITERAL, DurationPlan.Kind.INLINE, DurationPlan.Kind.EXPRESSION),
        plan.steps().stream()
            .map(PlanStep::waitPlan)
            .map(WaitPlan::duration)
            .map(DurationPlan::kind)
            .toList());
    assertTrue(plan.steps().stream().allMatch(step -> step.kind() == PlanStepKind.WAIT));
  }

  @Test
  void retainsDocumentMetadataAndCompilesTimeoutsAndSchedules() {
    WorkflowPlan plan =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: governed
                  version: '1.0.0'
                  title: Governed Evidence Extraction
                  summary: Extracts evidence under a bounded SLA.
                  tags:
                    domain: evidence
                    owner: investigations
                  metadata:
                    classification: restricted
                use:
                  timeouts:
                    short:
                      after:
                        seconds: 5
                timeout:
                  after: PT1H
                schedule:
                  every: PT15M
                  cron: '0 0 * * *'
                  after:
                    minutes: 1
                  on:
                    one:
                      with:
                        type: evidence.ready.v1
                  read: envelope
                do:
                  - extract:
                      timeout: short
                      set:
                        status: complete
                """));

    assertEquals("Governed Evidence Extraction", plan.metadata().title());
    assertEquals("investigations", plan.metadata().tags().required("owner").textValue());
    assertEquals(DurationPlan.Kind.LITERAL, plan.timeout().after().kind());
    assertEquals("short", plan.steps().getFirst().timeout().reusableName());
    assertEquals(DurationPlan.Kind.INLINE, plan.steps().getFirst().timeout().after().kind());
    assertEquals("0 0 * * *", plan.schedule().cron());
    assertEquals(EventConsumptionPlan.Mode.ONE, plan.schedule().on().mode());
    assertEquals(EventReadMode.ENVELOPE, plan.schedule().readAs());
  }

  @Test
  void defaultsAnEventScheduleToReadingCloudEventData() {
    WorkflowPlan plan =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: scheduled-data
                  version: '1.0.0'
                schedule:
                  on:
                    one:
                      with:
                        type: evidence.ready.v1
                do:
                  - complete:
                      set:
                        accepted: true
                """));

    assertEquals(EventReadMode.DATA, plan.schedule().readAs());
  }

  @Test
  void rejectsScheduleReadWithoutAnEventTrigger() {
    assertThrows(
        WorkflowDefinitionException.class,
        () ->
            compiler.compile(
                yaml(
                    """
                    document:
                      dsl: '1.0.3'
                      namespace: evidence
                      name: invalid-scheduled-read
                      version: '1.0.0'
                    schedule:
                      every: PT1M
                      read: raw
                    do:
                      - complete:
                          set:
                            accepted: true
                    """)));
  }

  @Test
  void rejectsAnUnknownReusableTimeoutAtPublication() {
    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compiler.compile(
                    yaml(
                        """
                        document:
                          dsl: '1.0.3'
                          namespace: evidence
                          name: missing-timeout
                          version: '1.0.0'
                        do:
                          - extract:
                              timeout: absent
                              set:
                                status: complete
                        """)));

    assertTrue(failure.getMessage().contains("use.timeouts"));
  }

  @Test
  void compilesEveryStandardCallVariantIntoATypedImmutablePlan() {
    byte[] source =
        yaml(
            """
            document:
              dsl: '1.0.3'
              namespace: evidence
              name: calls
              version: '1.0.0'
            use:
              functions:
                enrich-evidence:
                  set:
                    evidenceId: '${ .evidenceId }'
                    enriched: true
            do:
              - publish:
                  call: asyncapi
                  with:
                    document:
                      endpoint: https://contracts.test/events.yaml
                    operation: publishEvidence
                    message:
                      payload:
                        evidenceId: '${ .evidenceId }'
              - classify:
                  call: grpc
                  with:
                    proto:
                      endpoint: https://contracts.test/classifier.proto
                    service:
                      name: evidence.Classifier
                      host: classifier.test
                      port: 443
                    method: Classify
                    arguments:
                      evidenceId: '${ .evidenceId }'
              - fetch:
                  call: http
                  with:
                    method: GET
                    endpoint: https://evidence.test/{evidenceId}
                    query:
                      tenant: '${ $context.tenant }'
                    output: response
              - index:
                  call: openapi
                  with:
                    document:
                      endpoint: https://contracts.test/openapi.yaml
                    operationId: indexEvidence
                    parameters:
                      evidenceId: '${ .evidenceId }'
              - delegate:
                  call: a2a
                  with:
                    agentCard:
                      endpoint: https://agent.test/.well-known/agent-card.json
                    method: message/send
                    parameters:
                      message:
                        messageId: '${ $task.reference }'
              - tool:
                  call: mcp
                  with:
                    method: tools/call
                    parameters:
                      name: extract
                    transport:
                      http:
                        endpoint: https://mcp.test
                    client:
                      name: oks
                      version: 1.0.0
              - enrich:
                  call: enrich-evidence
                  with:
                    evidenceId: '${ .evidenceId }'
            """);
    var resources =
        new WorkflowResourceResolver()
            .resolve(
                source,
                request ->
                    ResolvedWorkflowResource.of(
                        request.uri(),
                        request.kind() == WorkflowResourceKind.GRPC_PROTO
                            ? "text/x-protobuf"
                            : "application/yaml",
                        request.kind() == WorkflowResourceKind.GRPC_PROTO
                            ? """
                            syntax = "proto3";
                            package evidence;
                            service Classifier {
                              rpc Classify (Evidence) returns (Evidence);
                            }
                            message Evidence {}
                            """
                            : request.kind() == WorkflowResourceKind.OPEN_API_DOCUMENT
                                ? """
                                openapi: 3.1.0
                                info: {title: Evidence, version: 1.0.0}
                                servers: [{url: https://evidence.test}]
                                paths:
                                  /evidence/{evidenceId}:
                                    post:
                                      operationId: indexEvidence
                                      responses:
                                        '200': {description: indexed}
                                """
                                : request.kind() == WorkflowResourceKind.ASYNC_API_DOCUMENT
                                    ? """
                                    asyncapi: 3.0.0
                                    info: {title: Evidence, version: 1.0.0}
                                    channels:
                                      evidence: {address: evidence}
                                    operations:
                                      publishEvidence:
                                        action: send
                                        channel: {$ref: '#/channels/evidence'}
                                    """
                                    : "{}"));
    WorkflowPlan plan = compiler.compile(source, resources);

    assertEquals(
        List.of(
            CallPlan.Kind.ASYNC_API,
            CallPlan.Kind.GRPC,
            CallPlan.Kind.HTTP,
            CallPlan.Kind.OPEN_API,
            CallPlan.Kind.A2A,
            CallPlan.Kind.MCP,
            CallPlan.Kind.FUNCTION),
        plan.steps().stream().map(PlanStep::callPlan).map(CallPlan::kind).toList());
    assertTrue(plan.steps().stream().allMatch(step -> step.kind() == PlanStepKind.CALL));
    assertEquals("enrich-evidence", plan.steps().getLast().callPlan().functionName());
    assertEquals(
        "${ .evidenceId }",
        plan.steps().getLast().callPlan().arguments().required("evidenceId").textValue());
    assertEquals(PlanStepKind.SET, plan.steps().getLast().children().getFirst().kind());
    assertEquals(4, plan.resources().size());
    assertEquals(
        WorkflowResourceKind.OPEN_API_DOCUMENT, plan.steps().get(3).callPlan().resource().kind());
    assertEquals(
        plan.resources().stream()
            .filter(resource -> resource.uri().toString().endsWith("openapi.yaml"))
            .findFirst()
            .orElseThrow()
            .sha256(),
        plan.steps().get(3).callPlan().resource().sha256());
  }

  @Test
  void compilesEveryRunVariantAndPinsExternalScriptSource() {
    byte[] source =
        yaml(
            """
            document:
              dsl: '1.0.3'
              namespace: evidence
              name: runs
              version: '1.0.0'
            do:
              - container:
                  run:
                    await: true
                    return: all
                    container:
                      image: busybox:1.36
                      command: echo
                      arguments: [hello]
              - inline-script:
                  run:
                    return: stdout
                    script:
                      language: python
                      code: print("hello")
              - external-script:
                  run:
                    script:
                      language: python
                      source:
                        endpoint: https://contracts.test/extract.py
              - shell:
                  run:
                    await: false
                    return: none
                    shell:
                      command: printf
                      arguments: [hello]
              - child:
                  run:
                    workflow:
                      namespace: evidence
                      name: child
                      version: '2.0.0'
                      input:
                        evidenceId: '${ .evidenceId }'
            """);
    List<ResolvedWorkflowResource> resources =
        new WorkflowResourceResolver()
            .resolve(
                source,
                request ->
                    ResolvedWorkflowResource.of(
                        request.uri(), "text/x-python", "print('external')"));

    ResolvedSubflow child =
        new ResolvedSubflow(
            new WorkflowCoordinates("evidence", "child", "2.0.0", "1.0.3"),
            "a".repeat(64),
            "b".repeat(64));
    WorkflowPlan plan =
        compiler.compile(
            source,
            resources,
            (namespace, name, version) ->
                "evidence".equals(namespace) && "child".equals(name) && "2.0.0".equals(version)
                    ? java.util.Optional.of(child)
                    : java.util.Optional.empty());

    assertEquals(
        List.of(
            RunPlan.Kind.CONTAINER,
            RunPlan.Kind.SCRIPT,
            RunPlan.Kind.SCRIPT,
            RunPlan.Kind.SHELL,
            RunPlan.Kind.WORKFLOW),
        plan.steps().stream().map(PlanStep::runPlan).map(RunPlan::kind).toList());
    assertTrue(plan.steps().getFirst().runPlan().await());
    assertEquals(RunPlan.ReturnMode.ALL, plan.steps().getFirst().runPlan().returnMode());
    assertFalse(plan.steps().get(3).runPlan().await());
    assertEquals(
        WorkflowResourceKind.SCRIPT_SOURCE, plan.steps().get(2).runPlan().resource().kind());
    assertEquals(resources.getFirst().sha256(), plan.steps().get(2).runPlan().resource().sha256());
    assertEquals(child, plan.steps().getLast().runPlan().subflow());
    assertNotEquals(
        plan.sourceSha256(),
        plan.definitionSha256(),
        "The child definition digest must influence its parent");
  }

  @Test
  void rejectsRecursiveReusableFunctionsBeforeAdmission() {
    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compiler.compile(
                    yaml(
                        """
                        document:
                          dsl: '1.0.3'
                          namespace: evidence
                          name: recursive-functions
                          version: '1.0.0'
                        use:
                          functions:
                            first:
                              call: second
                            second:
                              call: first
                        do:
                          - invoke:
                              call: first
                        """)));

    assertTrue(failure.getMessage().contains("reusable function cycle detected"));
  }

  @Test
  void resolvesReusableErrorsAndRetryPoliciesIntoImmutableFaultPlans() {
    WorkflowPlan plan =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: durable-faults
                  version: '1.0.0'
                use:
                  errors:
                    unavailable:
                      type: https://example.com/errors/unavailable
                      status: 503
                      title: Temporarily unavailable
                      detail: '${ "attempt for \\(.caseId) failed" }'
                  retries:
                    standard:
                      when: $error.status == 503
                      delay:
                        seconds: 2
                      backoff:
                        exponential: {}
                      limit:
                        attempt:
                          count: 4
                      jitter:
                        from:
                          milliseconds: 10
                        to:
                          milliseconds: 50
                do:
                  - guarded:
                      try:
                        - reject:
                            raise:
                              error: unavailable
                      catch:
                        errors:
                          with:
                            status: 503
                        as: problem
                        when: $problem.status == 503
                        retry: standard
                        do:
                          - record:
                              set:
                                caught: '${ $problem.title }'
                        then: end
                """));

    PlanStep guarded = plan.steps().getFirst();
    assertEquals(PlanStepKind.TRY, guarded.kind());
    assertEquals(PlanStepKind.RAISE, guarded.tryPlan().steps().getFirst().kind());
    ErrorPlan error = guarded.tryPlan().steps().getFirst().raisePlan().error();
    assertEquals(503, error.status());
    assertEquals("https://example.com/errors/unavailable", error.type().textValue());
    CatchPlan caught = guarded.tryPlan().catchPlan();
    assertEquals("problem", caught.as());
    assertEquals(503, caught.errors().status());
    assertEquals(RetryPlan.Backoff.EXPONENTIAL, caught.retry().backoff());
    assertEquals(4, caught.retry().attemptCount());
    assertEquals(PlanStepKind.SET, caught.steps().getFirst().kind());
    assertEquals("end", caught.thenDirective());
  }

  @Test
  void rejectsAnUnknownReusableErrorBeforeAdmission() {
    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compiler.compile(
                    yaml(
                        """
                        document:
                          dsl: '1.0.3'
                          namespace: evidence
                          name: invalid-fault
                          version: '1.0.0'
                        do:
                          - reject:
                              raise:
                                error: absent
                        """)));

    assertTrue(failure.violations().stream().anyMatch(value -> value.contains("use.errors")));
  }

  @Test
  void rejectsACatchFlowTargetOutsideItsTaskScope() {
    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compiler.compile(
                    yaml(
                        """
                        document:
                          dsl: '1.0.3'
                          namespace: evidence
                          name: invalid-catch-target
                          version: '1.0.0'
                        do:
                          - guarded:
                              try:
                                - reject:
                                    raise:
                                      error:
                                        type: https://example.com/error
                                        status: 500
                              catch:
                                then: outside-this-scope
                        """)));

    assertTrue(failure.getMessage().contains("does not exist in the same scope"));
  }

  @Test
  void compilesAuthenticationAsAnOpaqueDeclaredSecretReference() {
    WorkflowPlan plan =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: secured-call
                  version: '1.0.0'
                use:
                  secrets:
                    - evidence-api-token
                  authentications:
                    evidence-api:
                      bearer:
                        use: evidence-api-token
                do:
                  - invoke:
                      call: http
                      with:
                        method: GET
                        endpoint:
                          uri: https://evidence.example.test/items
                          authentication:
                            use: evidence-api
                """));

    CallPlan call = plan.steps().getFirst().callPlan();
    assertEquals(AuthenticationPlan.Kind.BEARER, call.authentication().kind());
    assertEquals("evidence-api", call.authentication().reusableName());
    assertEquals("evidence-api-token", call.authentication().secretName());
    assertFalse(call.arguments().required("endpoint").has("authentication"));
    assertFalse(call.arguments().toString().contains("evidence-api-token"));
  }

  @Test
  void readsAuthenticationPlansWrittenBeforeSecretReferencePinning() throws Exception {
    AuthenticationPlan restored =
        new ObjectMapper()
            .readValue(
                """
                {
                  "kind": "BEARER",
                  "reusableName": "evidence-api",
                  "secretName": "evidence-api-token",
                  "expressionConfiguration": null
                }
                """,
                AuthenticationPlan.class);

    assertEquals(AuthenticationPlan.Kind.BEARER, restored.kind());
    assertEquals("evidence-api-token", restored.secretName());
    assertEquals(List.of(), restored.secretReferences());
  }

  @Test
  void rejectsInlineCredentialsFromTheSecureAdmissionProfile() {
    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compiler.compile(
                    yaml(
                        """
                        document:
                          dsl: '1.0.3'
                          namespace: evidence
                          name: unsafe-call
                          version: '1.0.0'
                        do:
                          - invoke:
                              call: http
                              with:
                                method: GET
                                endpoint:
                                  uri: https://evidence.example.test/items
                                  authentication:
                                    basic:
                                      username: leaked-user
                                      password: leaked-password
                        """)));

    assertTrue(failure.getMessage().contains("inline authentication credentials"));
  }

  @Test
  void rejectsAuthenticationUsingAnUndeclaredSecret() {
    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compiler.compile(
                    yaml(
                        """
                        document:
                          dsl: '1.0.3'
                          namespace: evidence
                          name: undeclared-secret
                          version: '1.0.0'
                        use:
                          authentications:
                            evidence-api:
                              bearer:
                                use: missing-secret
                        do:
                          - invoke:
                              call: openapi
                              with:
                                document:
                                  endpoint: https://evidence.example.test/openapi.yaml
                                operationId: listEvidence
                                authentication:
                                  use: evidence-api
                        """)));

    assertTrue(failure.getMessage().contains("undeclared secret 'missing-secret'"));
  }

  @Test
  void admitsOnlyDeclaredMcpStdioEnvironmentSecretReferences() {
    WorkflowPlan plan =
        compiler.compile(
            yaml(
                """
                document:
                  dsl: '1.0.3'
                  namespace: evidence
                  name: secured-mcp-stdio
                  version: '1.0.0'
                use:
                  secrets:
                    - evidence-mcp-environment
                do:
                  - invoke:
                      call: mcp
                      with:
                        method: tools/list
                        transport:
                          stdio:
                            command: /opt/mcp/bin/server
                          options:
                            environmentSecret: evidence-mcp-environment
                """));

    assertEquals(
        "evidence-mcp-environment",
        plan.steps()
            .getFirst()
            .callPlan()
            .arguments()
            .at("/transport/options/environmentSecret")
            .textValue());

    WorkflowDefinitionException undeclared =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compiler.compile(
                    yaml(
                        """
                        document:
                          dsl: '1.0.3'
                          namespace: evidence
                          name: unsecured-mcp-stdio
                          version: '1.0.0'
                        do:
                          - invoke:
                              call: mcp
                              with:
                                method: tools/list
                                transport:
                                  stdio:
                                    command: /opt/mcp/bin/server
                                  options:
                                    environmentSecret: missing-secret
                        """)));
    assertTrue(undeclared.getMessage().contains("undeclared secret 'missing-secret'"));
  }

  @Test
  void resolvesAndCompilesCataloguedFunctionsImmutably() {
    byte[] source =
        yaml(
            """
            document:
              dsl: '1.0.3'
              namespace: evidence
              name: catalogued-function
              version: '1.0.0'
            use:
              catalogs:
                evidence:
                  endpoint:
                    uri: https://catalog.example.test/
            do:
              - normalize:
                  call: normalize:1.2.3@evidence
                  with:
                    value: '${ .name }'
            """);
    URI functionUri =
        URI.create("https://catalog.example.test/functions/" + "normalize/1.2.3/function.yaml");
    String function =
        """
        input:
          schema:
            document:
              type: object
              required: [value]
        run:
          shell:
            command: normalize
            arguments:
              - '${ .value }'
        output:
          as: '${ .stdout }'
        """;
    List<ResolvedWorkflowResource> resources =
        new WorkflowResourceResolver()
            .resolve(
                source,
                request -> {
                  assertEquals(WorkflowResourceKind.FUNCTION_DEFINITION, request.kind());
                  assertEquals(functionUri, request.uri());
                  return ResolvedWorkflowResource.of(request.uri(), "application/yaml", function);
                });

    WorkflowPlan plan = compiler.compile(source, resources);
    PlanStep invocation = plan.steps().getFirst();

    assertEquals(WorkflowResourceKind.FUNCTION_DEFINITION, invocation.callPlan().resource().kind());
    assertEquals(functionUri, invocation.callPlan().resource().uri());
    assertEquals(PlanStepKind.RUN, invocation.children().getFirst().kind());
    assertEquals(resources.getFirst().sha256(), invocation.callPlan().resource().sha256());
  }

  @Test
  void callOperationArgumentsCannotMutateAfterCompilation() {
    var source = new ObjectMapper().createObjectNode().put("value", 7);
    var operation = new CallPlan(CallPlan.Kind.FUNCTION, "normalize", null, source);

    source.put("value", 99);
    JsonNode exposed = operation.arguments();
    ((com.fasterxml.jackson.databind.node.ObjectNode) exposed).put("value", 42);

    assertEquals(7, operation.arguments().required("value").intValue());
  }

  @Test
  void resolvesAndPinsLiteralEventDataSchemasAtPublication() {
    byte[] source =
        yaml(
            """
            document:
              dsl: '1.0.3'
              namespace: evidence
              name: schema-governed-event
              version: '1.0.0'
            schedule:
              on:
                one:
                  with:
                    type: evidence.ready.v1
                    dataschema: https://schemas.test/evidence-ready.json
            do:
              - complete:
                  set:
                    accepted: true
            """);
    URI schemaUri = URI.create("https://schemas.test/evidence-ready.json");
    List<ResolvedWorkflowResource> resources =
        new WorkflowResourceResolver()
            .resolve(
                source,
                request -> {
                  assertEquals(schemaUri, request.uri());
                  assertEquals(WorkflowResourceKind.DATA_SCHEMA, request.kind());
                  return ResolvedWorkflowResource.jsonSchema(
                      schemaUri,
                      """
                      {
                        "$schema":"https://json-schema.org/draft/2020-12/schema",
                        "type":"object",
                        "required":["evidenceId"],
                        "properties":{"evidenceId":{"type":"string"}},
                        "additionalProperties":false
                      }
                      """);
                });

    WorkflowPlan plan = compiler.compile(source, resources);
    ResolvedDataSchema compiled = plan.schedule().on().filters().getFirst().dataSchema();

    assertEquals(schemaUri, compiled.resourceUri());
    assertEquals(resources.getFirst().sha256(), compiled.sha256());
    assertEquals(
        "string",
        compiled
            .document()
            .required("properties")
            .required("evidenceId")
            .required("type")
            .textValue());
  }

  @Test
  void rejectsAnUnresolvedLiteralEventDataSchema() {
    WorkflowDefinitionException failure =
        assertThrows(
            WorkflowDefinitionException.class,
            () ->
                compiler.compile(
                    yaml(
                        """
                        document:
                          dsl: '1.0.3'
                          namespace: evidence
                          name: unresolved-event-schema
                          version: '1.0.0'
                        schedule:
                          on:
                            one:
                              with:
                                type: evidence.ready.v1
                                dataschema: https://schemas.test/missing.json
                        do:
                          - complete:
                              set:
                                accepted: true
                        """)));

    assertTrue(failure.getMessage().contains("was not resolved before publication"));
  }

  @Test
  void resolvesProtocolDocumentsTransitivelyBeforeAdmission() {
    byte[] source =
        yaml(
            """
            document:
              dsl: '1.0.3'
              namespace: evidence
              name: transitive-protocol-resources
              version: '1.0.0'
            do:
              - invoke-http:
                  call: openapi
                  with:
                    document:
                      endpoint: https://contracts.test/openapi/root.yaml
                    operationId: extract
              - receive:
                  call: asyncapi
                  with:
                    document:
                      endpoint: https://contracts.test/asyncapi/root.yaml
                    operation: evidenceReceived
                    subscription:
                      consume:
                        amount: 1
              - invoke-grpc:
                  call: grpc
                  with:
                    proto:
                      endpoint: https://contracts.test/proto/service.proto
                    service:
                      name: Evidence
                      host: evidence.test
                    method: Extract
            """);
    Map<URI, String> documents =
        Map.of(
            URI.create("https://contracts.test/openapi/root.yaml"),
            """
            openapi: 3.1.0
            paths:
              /evidence:
                post:
                  operationId: extract
                  requestBody:
                    $ref: ./components.yaml#/components/requestBodies/Extract
            """,
            URI.create("https://contracts.test/openapi/components.yaml"),
            """
            components:
              requestBodies:
                Extract:
                  content:
                    application/json:
                      schema:
                        type: object
            """,
            URI.create("https://contracts.test/asyncapi/root.yaml"),
            """
            asyncapi: 3.0.0
            operations:
              evidenceReceived:
                action: receive
                channel:
                  $ref: ./channels.yaml#/channels/evidence
            """,
            URI.create("https://contracts.test/asyncapi/channels.yaml"),
            """
            channels:
              evidence:
                address: evidence.received
            """,
            URI.create("https://contracts.test/proto/service.proto"),
            """
            syntax = "proto3";
            import "types/evidence.proto";
            service Evidence {
              rpc Extract (EvidenceRequest) returns (EvidenceReply);
            }
            """,
            URI.create("https://contracts.test/proto/types/evidence.proto"),
            """
            syntax = "proto3";
            import public "common.proto";
            message EvidenceRequest { string id = 1; }
            message EvidenceReply { string result = 1; }
            """,
            URI.create("https://contracts.test/proto/types/common.proto"),
            """
            syntax = "proto3";
            message TraceContext { string trace_id = 1; }
            """);

    List<ResolvedWorkflowResource> resources =
        new WorkflowResourceResolver()
            .resolve(
                source,
                request -> {
                  String content = documents.get(request.uri());
                  if (content == null) {
                    throw new IllegalArgumentException("Unexpected resource " + request.uri());
                  }
                  String mediaType =
                      request.kind() == WorkflowResourceKind.GRPC_PROTO
                          ? "text/x-protobuf"
                          : "application/yaml";
                  return ResolvedWorkflowResource.of(request.uri(), mediaType, content);
                });

    assertEquals(7, resources.size());
    assertEquals(
        List.of(
            "https://contracts.test/openapi/root.yaml",
            "https://contracts.test/asyncapi/root.yaml",
            "https://contracts.test/proto/service.proto",
            "https://contracts.test/openapi/components.yaml",
            "https://contracts.test/asyncapi/channels.yaml",
            "https://contracts.test/proto/types/evidence.proto",
            "https://contracts.test/proto/types/common.proto"),
        resources.stream().map(resource -> resource.uri().toString()).toList());

    /*
     * Resolution alone is not sufficient: admission performs an
     * independent reachability check so an arbitrary extra resource cannot
     * be smuggled into the immutable definition bundle. This compilation
     * proves that the two valid API reference graphs and the complete proto
     * import graph are all recognized as reachable.
     */
    WorkflowPlan plan = compiler.compile(source, resources);
    assertEquals(7, plan.resources().size());
    assertEquals(
        resources.stream().map(ResolvedWorkflowResource::sha256).toList(),
        plan.resources().stream().map(ResolvedWorkflowResource::sha256).toList());
  }

  @Test
  void rejectsOpenApiOperationAbsentFromThePinnedDocument() {
    byte[] source =
        yaml(
            """
            document:
              dsl: '1.0.3'
              namespace: evidence
              name: missing-openapi-operation
              version: '1.0.0'
            do:
              - invoke:
                  call: openapi
                  with:
                    document:
                      endpoint: https://contracts.test/openapi.yaml
                    operationId: absent
            """);
    var resources =
        new WorkflowResourceResolver()
            .resolve(
                source,
                request ->
                    ResolvedWorkflowResource.of(
                        request.uri(),
                        "application/yaml",
                        """
                        openapi: 3.1.0
                        info: {title: Evidence, version: 1.0.0}
                        servers: [{url: https://api.test}]
                        paths:
                          /evidence:
                            get:
                              operationId: listEvidence
                              responses:
                                '200': {description: ok}
                        """));
    WorkflowDefinitionException failure =
        assertThrows(WorkflowDefinitionException.class, () -> compiler.compile(source, resources));
    assertTrue(
        failure
            .getMessage()
            .contains("does not identify an operation in the pinned OpenAPI document"));
  }

  @Test
  void rejectsAsyncApiOperationAbsentFromThePinnedDocument() {
    byte[] source =
        yaml(
            """
            document:
              dsl: '1.0.3'
              namespace: evidence
              name: missing-asyncapi-operation
              version: '1.0.0'
            do:
              - invoke:
                  call: asyncapi
                  with:
                    document:
                      endpoint: https://contracts.test/events.yaml
                    operation: absent
                    message:
                      payload: {value: evidence}
            """);
    var resources =
        new WorkflowResourceResolver()
            .resolve(
                source,
                request ->
                    ResolvedWorkflowResource.of(
                        request.uri(),
                        "application/yaml",
                        """
                        asyncapi: 3.0.0
                        info: {title: Evidence, version: 1.0.0}
                        channels:
                          evidence: {address: evidence}
                        operations:
                          publishEvidence:
                            action: send
                            channel: {$ref: '#/channels/evidence'}
                        """));

    WorkflowDefinitionException failure =
        assertThrows(WorkflowDefinitionException.class, () -> compiler.compile(source, resources));

    assertTrue(
        failure
            .getMessage()
            .contains("must uniquely identify an operation in the pinned AsyncAPI document"));
  }

  @Test
  void rejectsGrpcMethodAbsentFromThePinnedProto() {
    byte[] source =
        yaml(
            """
            document:
              dsl: '1.0.3'
              namespace: evidence
              name: missing-grpc-method
              version: '1.0.0'
            do:
              - invoke:
                  call: grpc
                  with:
                    proto:
                      endpoint: https://contracts.test/evidence.proto
                    service:
                      name: evidence.Classifier
                      host: classifier.test
                    method: Missing
            """);
    var resources =
        new WorkflowResourceResolver()
            .resolve(
                source,
                request ->
                    ResolvedWorkflowResource.of(
                        request.uri(),
                        "text/x-protobuf",
                        """
                        syntax = "proto3";
                        package evidence;
                        service Classifier {
                          rpc Classify (Evidence) returns (Evidence);
                        }
                        message Evidence {}
                        """));

    WorkflowDefinitionException failure =
        assertThrows(WorkflowDefinitionException.class, () -> compiler.compile(source, resources));

    assertTrue(
        failure.getMessage().contains("does not identify an RPC on service evidence.Classifier"));
  }

  private static byte[] yaml(String source) {
    return source.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] minimalWorkflow() {
    return yaml(
        """
        document:
          dsl: '1.0.3'
          namespace: test
          name: resource-limits
          version: '1.0.0'
        do:
          - initialize:
              set:
                ready: true
        """);
  }
}
