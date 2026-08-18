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
package com.forwardmeasure.openworkflow.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashSet;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JqRuntimeExpressionEvaluatorTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final JqRuntimeExpressionEvaluator evaluator = new JqRuntimeExpressionEvaluator();

  @Test
  void strictModeEvaluatesOnlyDefinitionExpressions() throws Exception {
    JsonNode input = JSON.readTree("{\"person\":{\"name\":\"Ada\"}," + "\"payload\":\"${ now }\"}");
    JsonNode template =
        JSON.readTree(
            "{\"name\":\"${ .person.name }\"," + "\"literal\":\"ordinary text\"," + "\"count\":2}");

    JsonNode result =
        evaluator.evaluateTemplate(
            template, input, RuntimeExpressionArguments.empty(), ExpressionMode.STRICT);

    assertEquals("Ada", result.required("name").textValue());
    assertEquals("ordinary text", result.required("literal").textValue());
    assertEquals(2, result.required("count").intValue());
    assertEquals(
        "${ now }",
        input.required("payload").textValue(),
        "Expression-looking runtime input must remain ordinary data");
  }

  @Test
  void exposesStandardArgumentsAndJqTruthiness() throws Exception {
    JsonNode context = JSON.readTree("{\"approved\":true}");
    JsonNode input = JSON.readTree("{\"amount\":25}");
    RuntimeExpressionArguments arguments =
        new RuntimeExpressionArguments(context, input, null, null, null, null, null, null);

    assertTrue(
        evaluator.evaluateCondition(
            "${ $context.approved and .amount > 20 }", input, arguments, ExpressionMode.STRICT));
    assertFalse(evaluator.evaluateCondition("${ null }", input, arguments, ExpressionMode.STRICT));
  }

  @Test
  void looseModeEvaluatesValidStringsAndPreservesInvalidStrings() throws Exception {
    JsonNode input = JSON.readTree("{\"name\":\"Ada\"}");
    JsonNode template = JSON.readTree("{\"evaluated\":\".name\",\"literal\":\"ordinary text\"}");

    JsonNode result =
        evaluator.evaluateTemplate(
            template, input, RuntimeExpressionArguments.empty(), ExpressionMode.LOOSE);

    assertEquals("Ada", result.required("evaluated").textValue());
    assertEquals("ordinary text", result.required("literal").textValue());
  }

  @Test
  void rejectsInvalidStrictAndMultiValueExpressions() throws Exception {
    JsonNode input = JSON.readTree("[1,2]");

    assertThrows(
        RuntimeExpressionException.class,
        () ->
            evaluator.evaluateExpression(
                ".[]", input, RuntimeExpressionArguments.empty(), ExpressionMode.STRICT));
    assertThrows(
        RuntimeExpressionException.class,
        () ->
            evaluator.evaluateExpression(
                "${ .[] }", input, RuntimeExpressionArguments.empty(), ExpressionMode.STRICT));
  }

  @Test
  void wallClockFunctionIsUnavailableInsideDurableEvaluation() throws Exception {
    JsonNode input = JSON.readTree("{}");

    assertThrows(
        RuntimeExpressionException.class,
        () ->
            evaluator.evaluateExpression(
                "${ now }", input, RuntimeExpressionArguments.empty(), ExpressionMode.STRICT));
    assertThrows(
        RuntimeExpressionException.class,
        () ->
            evaluator.evaluateExpression(
                "${ debug_scope }",
                input,
                RuntimeExpressionArguments.empty(),
                ExpressionMode.STRICT));
  }

  @Test
  void builtinInventoryFailsClosedWhenALibraryUpgradeAddsAFunction() {
    var approved = JqRuntimeExpressionEvaluator.approvedBuiltinInventory();
    assertFalse(approved.contains("now/0"));
    assertFalse(approved.contains("debug_scope/0"));

    var changed = new HashSet<>(approved);
    changed.add("now/0");
    changed.add("debug_scope/0");
    changed.add("read_environment/0");
    assertThrows(
        IllegalStateException.class,
        () -> JqRuntimeExpressionEvaluator.validateBuiltinInventory(changed));
  }

  @Test
  void exposesDefinitionNamedIterationVariablesWithoutMutatingInput() throws Exception {
    JsonNode input = JSON.readTree("{\"processed\":[]}");
    RuntimeExpressionArguments arguments =
        new RuntimeExpressionArguments(
            null,
            input,
            null,
            null,
            null,
            null,
            null,
            null,
            Map.of(
                "color", JSON.readTree("\"green\""),
                "position", JSON.readTree("1")));

    JsonNode result =
        evaluator.evaluateExpression(
            "${ {value: $color, index: $position, input: .} }",
            input,
            arguments,
            ExpressionMode.STRICT);

    assertEquals("green", result.required("value").textValue());
    assertEquals(1, result.required("index").intValue());
    assertEquals(input, result.required("input"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RuntimeExpressionArguments(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                Map.of("context", JSON.readTree("{}"))));
  }

  @Test
  void identifiesConstantTemplatesWithoutEvaluatingRuntimeData() throws Exception {
    JsonNode strictConstant =
        JSON.readTree(
            """
            {
              "method": "POST",
              "endpoint": "https://extractor.example/v1/extract",
              "options": {"limit": 10}
            }
            """);
    JsonNode strictExpression =
        JSON.readTree(
            """
            {"body": "${ .document }"}
            """);
    JsonNode looseExpression =
        JSON.readTree(
            """
            {"body": ".document"}
            """);

    assertFalse(evaluator.requiresEvaluation(strictConstant, ExpressionMode.STRICT));
    assertTrue(evaluator.requiresEvaluation(strictExpression, ExpressionMode.STRICT));
    assertTrue(evaluator.requiresEvaluation(looseExpression, ExpressionMode.LOOSE));
  }
}
