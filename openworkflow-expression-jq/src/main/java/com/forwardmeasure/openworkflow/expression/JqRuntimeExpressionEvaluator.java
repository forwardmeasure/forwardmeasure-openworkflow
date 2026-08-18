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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import net.thisptr.jackson.jq.BuiltinFunctionLoader;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Versions;
import net.thisptr.jackson.jq.exception.JsonQueryException;

/**
 * Deterministic implementation of the mandatory Open Workflow jq expression language.
 *
 * <p>Only expressions originating in a workflow definition are passed to this class. Runtime input
 * is always data and is never recursively interpreted as jq. Definition objects and arrays are
 * templates: recognised expression strings are evaluated while ordinary JSON values remain literal.
 *
 * <p>Wall-clock and diagnostic functions are deliberately omitted from the scope. Durable reducers
 * must receive time and external facts as explicit command data rather than reading process-local
 * state.
 */
public final class JqRuntimeExpressionEvaluator {
  private static final Pattern STRICT_EXPRESSION = Pattern.compile("^\\s*\\$\\{([\\s\\S]+)}\\s*$");
  private static final Set<String> DENIED_FUNCTIONS = Set.of("now/0", "debug_scope/0");

  /**
   * Reviewed jackson-jq 1.6.2/JQ 1.7 inventory. An upgrade that adds a builtin fails closed until
   * the new function is explicitly reviewed.
   */
  private static final Set<String> EXPECTED_BUILTIN_INVENTORY =
      Arrays.stream(
              """
              @base64/0 @base64d/0 @csv/0 @html/0 @json/0
              @sh/0 @text/0 @tsv/0 @uri/0 _flatten/1
              _match_impl/3 _modify/2 _nwise/1 _nwise/2
              _sub_impl/3 acos/0 add/0 all/0 all/1 all/2 any/0
              any/1 any/2 arrays/0 ascii_downcase/0
              ascii_upcase/0 asin/0 atan/0 atan2/2 booleans/0
              builtins/0 capture/1 capture/2 cbrt/0 ceil/0
              combinations/0 combinations/1 contains/1 cos/0
              cosh/0 debug_scope/0 del/1 delpaths/1 empty/0
              endswith/1 error/0 error/1 exp/0 exp10/0 exp2/0
              explode/0 expm1/0 finites/0 first/0 first/1
              flatten/0 flatten/1 floor/0 from_entries/0
              fromdateiso8601/0 fromjson/0 getpath/1 group_by/1
              gsub/2 gsub/3 has/1 implode/0 in/1 index/1
              indices/1 infinite/0 inside/1 isfinite/0
              isinfinite/0 isnan/0 isnormal/0 iterables/0 join/1
              keys/0 keys_unsorted/0 last/0 last/1 leaf_paths/0
              length/0 limit/2 log/0 log10/0 log1p/0 log2/0
              ltrimstr/1 map/1 map_values/1 match/1 match/2
              max/0 max_by/1 min/0 min_by/1 nan/0 normals/0
              not/0 now/0 nth/1 nth/2 nulls/0 numbers/0
              objects/0 path/1 paths/0 paths/1 pick/1 pow/2
              range/1 range/2 range/3 recurse/0 recurse/1
              recurse/2 recurse_down/0 reverse/0 rindex/1
              round/0 rtrimstr/1 scalars/0 scan/1 scan/2
              select/1 setpath/2 sin/0 sinh/0 sort/0 sort_by/1
              split/1 split/2 splits/1 splits/2 sqrt/0
              startswith/1 strings/0 sub/2 sub/3 tan/0 tanh/0
              test/1 test/2 to_entries/0 todateiso8601/0
              tojson/0 tonumber/0 tostring/0 transpose/0 type/0
              unique/0 unique_by/1 until/2 utf8bytelength/0
              values/0 walk/1 while/2 with_entries/1
              """
                  .strip()
                  .split("\\s+"))
          .collect(Collectors.toUnmodifiableSet());

  public JsonNode evaluateTemplate(
      JsonNode template,
      JsonNode evaluatedOn,
      RuntimeExpressionArguments arguments,
      ExpressionMode mode) {
    Objects.requireNonNull(template, "template");
    Objects.requireNonNull(evaluatedOn, "evaluatedOn");
    Objects.requireNonNull(arguments, "arguments");
    Objects.requireNonNull(mode, "mode");
    return evaluateTemplateValue(template, evaluatedOn, arguments, mode);
  }

  public JsonNode evaluateExpression(
      String expression,
      JsonNode evaluatedOn,
      RuntimeExpressionArguments arguments,
      ExpressionMode mode) {
    Objects.requireNonNull(expression, "expression");
    Objects.requireNonNull(evaluatedOn, "evaluatedOn");
    Objects.requireNonNull(arguments, "arguments");
    Objects.requireNonNull(mode, "mode");
    Matcher strict = STRICT_EXPRESSION.matcher(expression);
    if (strict.matches()) {
      return evaluateJq(strict.group(1), evaluatedOn, arguments);
    }
    if (mode == ExpressionMode.STRICT) {
      throw new RuntimeExpressionException("Strict runtime expressions must be enclosed in ${ }");
    }
    try {
      return evaluateJq(expression, evaluatedOn, arguments);
    } catch (RuntimeExpressionException failure) {
      return Scope.newEmptyScope().getObjectMapper().getNodeFactory().textNode(expression);
    }
  }

  public boolean evaluateCondition(
      String expression,
      JsonNode evaluatedOn,
      RuntimeExpressionArguments arguments,
      ExpressionMode mode) {
    JsonNode result = evaluateExpression(expression, evaluatedOn, arguments, mode);
    return !result.isNull() && !(result.isBoolean() && !result.booleanValue());
  }

  public void validateTemplate(JsonNode template, ExpressionMode mode) {
    Objects.requireNonNull(template, "template");
    Objects.requireNonNull(mode, "mode");
    validateTemplateValue(template, mode);
  }

  public void validateExpression(String expression, ExpressionMode mode) {
    Objects.requireNonNull(expression, "expression");
    Objects.requireNonNull(mode, "mode");
    Matcher strict = STRICT_EXPRESSION.matcher(expression);
    if (strict.matches()) {
      compile(strict.group(1));
      return;
    }
    if (mode == ExpressionMode.STRICT) {
      throw new RuntimeExpressionException("Strict runtime expressions must be enclosed in ${ }");
    }
    try {
      compile(expression);
    } catch (RuntimeExpressionException ignoredLiteral) {
      // Loose mode preserves a string when it cannot be evaluated.
    }
  }

  /**
   * Reports whether evaluating a definition-owned template needs runtime data. Constant templates
   * can be copied without materialising an artifact-backed workflow value.
   */
  public boolean requiresEvaluation(JsonNode template, ExpressionMode mode) {
    if (template == null) {
      return false;
    }
    Objects.requireNonNull(mode, "mode");
    if (template.isContainerNode()) {
      for (JsonNode value : template) {
        if (requiresEvaluation(value, mode)) {
          return true;
        }
      }
      return false;
    }
    if (!template.isTextual()) {
      return false;
    }
    String value = template.textValue();
    if (STRICT_EXPRESSION.matcher(value).matches()) {
      return true;
    }
    if (mode == ExpressionMode.STRICT) {
      return false;
    }
    try {
      compile(value);
      return true;
    } catch (RuntimeExpressionException literal) {
      return false;
    }
  }

  private JsonNode evaluateTemplateValue(
      JsonNode template,
      JsonNode evaluatedOn,
      RuntimeExpressionArguments arguments,
      ExpressionMode mode) {
    if (template.isObject()) {
      ObjectNode result = JsonNodeFactory.instance.objectNode();
      template
          .properties()
          .forEach(
              field ->
                  result.set(
                      field.getKey(),
                      evaluateTemplateValue(field.getValue(), evaluatedOn, arguments, mode)));
      return result;
    }
    if (template.isArray()) {
      ArrayNode result = JsonNodeFactory.instance.arrayNode();
      template.forEach(
          value -> result.add(evaluateTemplateValue(value, evaluatedOn, arguments, mode)));
      return result;
    }
    if (!template.isTextual()) {
      return template.deepCopy();
    }
    String value = template.textValue();
    Matcher strict = STRICT_EXPRESSION.matcher(value);
    if (strict.matches()) {
      return evaluateJq(strict.group(1), evaluatedOn, arguments);
    }
    if (mode == ExpressionMode.STRICT) {
      return template.deepCopy();
    }
    try {
      return evaluateJq(value, evaluatedOn, arguments);
    } catch (RuntimeExpressionException ignoredLiteral) {
      return template.deepCopy();
    }
  }

  private void validateTemplateValue(JsonNode template, ExpressionMode mode) {
    if (template.isContainerNode()) {
      template.forEach(value -> validateTemplateValue(value, mode));
      return;
    }
    if (!template.isTextual()) {
      return;
    }
    String value = template.textValue();
    Matcher strict = STRICT_EXPRESSION.matcher(value);
    if (strict.matches()) {
      compile(strict.group(1));
    } else if (mode == ExpressionMode.LOOSE) {
      try {
        compile(value);
      } catch (RuntimeExpressionException ignoredLiteral) {
        // Loose mode preserves a string when it cannot be evaluated.
      }
    }
  }

  private JsonNode evaluateJq(
      String expression, JsonNode evaluatedOn, RuntimeExpressionArguments arguments) {
    JsonQuery query = compile(expression);
    Scope scope = deterministicScope(arguments);
    List<JsonNode> output = new ArrayList<>();
    try {
      query.apply(scope, evaluatedOn, value -> output.add(value.deepCopy()));
    } catch (JsonQueryException failure) {
      throw new RuntimeExpressionException(
          "jq evaluation failed: " + failure.getMessage(), failure);
    }
    if (output.size() != 1) {
      throw new RuntimeExpressionException(
          "A runtime expression must produce exactly one value; produced " + output.size());
    }
    return output.getFirst();
  }

  private static JsonQuery compile(String expression) {
    if (expression.isBlank()) {
      throw new RuntimeExpressionException("A runtime expression must not be blank");
    }
    try {
      return JsonQuery.compile(expression, Versions.JQ_1_7);
    } catch (JsonQueryException failure) {
      throw new RuntimeExpressionException(
          "jq compilation failed: " + failure.getMessage(), failure);
    }
  }

  private static Scope deterministicScope(RuntimeExpressionArguments arguments) {
    Scope loaded = Scope.newEmptyScope();
    BuiltinFunctionLoader.getInstance().loadFunctions(Versions.JQ_1_7, loaded);
    validateBuiltinInventory(loaded.getLocalFunctions().keySet());
    Scope result = Scope.newEmptyScope();
    for (Map.Entry<String, net.thisptr.jackson.jq.Function> function :
        loaded.getLocalFunctions().entrySet()) {
      if (!DENIED_FUNCTIONS.contains(function.getKey())) {
        result.addFunction(function.getKey(), function.getValue());
      }
    }
    result.setValue("context", arguments.context());
    result.setValue("input", arguments.input());
    result.setValue("output", arguments.output());
    result.setValue("secrets", arguments.secrets());
    result.setValue("authorization", arguments.authorization());
    result.setValue("task", arguments.task());
    result.setValue("workflow", arguments.workflow());
    result.setValue("runtime", arguments.runtime());
    arguments.variables().forEach(result::setValue);
    return result;
  }

  static void validateBuiltinInventory(Set<String> actual) {
    Objects.requireNonNull(actual, "actual");
    if (EXPECTED_BUILTIN_INVENTORY.equals(actual)) {
      return;
    }
    Set<String> added = new TreeSet<>(actual);
    added.removeAll(EXPECTED_BUILTIN_INVENTORY);
    Set<String> removed = new TreeSet<>(EXPECTED_BUILTIN_INVENTORY);
    removed.removeAll(actual);
    throw new IllegalStateException(
        "Unreviewed jackson-jq builtin inventory; added=" + added + ", removed=" + removed);
  }

  static Set<String> approvedBuiltinInventory() {
    Set<String> approved = new TreeSet<>(EXPECTED_BUILTIN_INVENTORY);
    approved.removeAll(DENIED_FUNCTIONS);
    return Set.copyOf(approved);
  }
}
