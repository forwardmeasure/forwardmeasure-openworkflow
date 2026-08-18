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

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Conservative JSON Schema producer-to-consumer compatibility checker.
 *
 * <p>Compatibility means that every JSON value permitted by the producer schema is also permitted
 * by the consumer schema. The checker proves the commonly used structural subset and reports {@link
 * SchemaCompatibilityStatus#UNPROVEN} for constructs for which it cannot make a sound inclusion
 * decision. It never treats an unsupported construct as compatible.
 */
public final class JsonSchemaCompatibilityChecker {
  private static final Set<String> PROOF_BREAKING_KEYWORDS =
      Set.of(
          "allOf",
          "anyOf",
          "oneOf",
          "not",
          "if",
          "then",
          "else",
          "dependentSchemas",
          "dependentRequired",
          "patternProperties",
          "propertyNames",
          "unevaluatedProperties",
          "unevaluatedItems",
          "contains",
          "minContains",
          "maxContains",
          "prefixItems",
          "contentSchema",
          "$dynamicRef",
          "$recursiveRef");

  private static final Set<String> JSON_TYPES =
      Set.of("null", "boolean", "object", "array", "number", "integer", "string");

  private final DataSchemaValidator validator;

  public JsonSchemaCompatibilityChecker(List<ResolvedWorkflowResource> resources) {
    validator = new DataSchemaValidator(Objects.requireNonNull(resources, "resources"));
  }

  public Result compare(ResolvedDataSchema producer, ResolvedDataSchema consumer) {
    Objects.requireNonNull(producer, "producer");
    Objects.requireNonNull(consumer, "consumer");
    if (producer.format().equals(consumer.format())
        && producer.sha256().equals(consumer.sha256())) {
      return Result.compatible("schemas are digest-identical");
    }
    return compare(
        producer, consumer, producer.document(), consumer.document(), "$", new HashSet<>());
  }

  private Result compare(
      ResolvedDataSchema producerSchema,
      ResolvedDataSchema consumerSchema,
      JsonNode producer,
      JsonNode consumer,
      String path,
      Set<NodePair> visited) {
    if (producer.equals(consumer)) {
      return Result.compatible(path + " is structurally identical");
    }
    if (consumer.isBoolean()) {
      return consumer.booleanValue()
          ? Result.compatible(path + " consumer accepts every value")
          : producer.isBoolean() && !producer.booleanValue()
              ? Result.compatible(path + " producer emits no values")
              : Result.incompatible(path + " consumer rejects every value");
    }
    if (producer.isBoolean()) {
      return producer.booleanValue()
          ? Result.incompatible(path + " producer accepts every value but consumer does not")
          : Result.compatible(path + " producer emits no values");
    }
    if (!producer.isObject() || !consumer.isObject()) {
      return Result.unproven(path + " uses a legacy tuple-schema form");
    }

    NodePair pair = new NodePair(producer, consumer);
    if (!visited.add(pair)) {
      return Result.compatible(path + " recursive schemas already compared");
    }

    if (hasReferenceAssertionSiblings(producer) || hasReferenceAssertionSiblings(consumer)) {
      return Result.unproven(path + " has assertion siblings next to $ref");
    }
    JsonNode resolvedProducer = resolveLocalReference(producerSchema.document(), producer);
    JsonNode resolvedConsumer = resolveLocalReference(consumerSchema.document(), consumer);
    if (resolvedProducer == null || resolvedConsumer == null) {
      return Result.unproven(path + " contains a non-local or unresolved schema reference");
    }
    if (resolvedProducer != producer || resolvedConsumer != consumer) {
      return compare(
          producerSchema, consumerSchema, resolvedProducer, resolvedConsumer, path, visited);
    }

    Set<String> unsupported = unsupportedKeywords(producer, consumer);
    if (!unsupported.isEmpty()) {
      return Result.unproven(
          path + " uses unsupported inclusion keyword(s) " + String.join(", ", unsupported));
    }

    Result finite = compareFiniteProducer(consumerSchema, producer, consumer, path);
    if (finite != null) {
      return finite;
    }

    Set<String> producerTypes = types(producer);
    Set<String> consumerTypes = types(consumer);
    Result typeResult = compareTypes(producerTypes, consumerTypes, path);
    if (typeResult.status() != SchemaCompatibilityStatus.COMPATIBLE) {
      return typeResult;
    }

    if (producerTypes.size() != 1 || consumerTypes.size() != 1) {
      return constraintsAbsent(producer, consumer)
          ? Result.compatible(path + " type domain is compatible")
          : Result.unproven(path + " combines constraints with a union or implicit type");
    }

    String producerType = producerTypes.iterator().next();
    String consumerType = consumerTypes.iterator().next();
    if ("integer".equals(producerType) && "number".equals(consumerType)) {
      return compareNumbers(producer, consumer, path);
    }
    if (!producerType.equals(consumerType)) {
      return Result.incompatible(
          path
              + " producer type "
              + producerType
              + " is not accepted by consumer type "
              + consumerType);
    }
    return switch (producerType) {
      case "object" ->
          compareObjects(producerSchema, consumerSchema, producer, consumer, path, visited);
      case "array" ->
          compareArrays(producerSchema, consumerSchema, producer, consumer, path, visited);
      case "string" -> compareStrings(producer, consumer, path);
      case "number", "integer" -> compareNumbers(producer, consumer, path);
      default -> Result.compatible(path + " primitive type is compatible");
    };
  }

  private Result compareFiniteProducer(
      ResolvedDataSchema consumerSchema, JsonNode producer, JsonNode consumer, String path) {
    List<JsonNode> values = finiteValues(producer);
    if (values == null) {
      if (consumer.has("const") || consumer.has("enum")) {
        return Result.unproven(path + " consumer has a finite value set but producer does not");
      }
      return null;
    }
    for (JsonNode value : values) {
      try {
        validator.validate(schemaAt(consumerSchema, consumer, path), value);
      } catch (DataSchemaValidationException failure) {
        return Result.incompatible(path + " producer permits " + value + " which consumer rejects");
      } catch (RuntimeException failure) {
        return Result.unproven(
            path
                + " finite values could not be checked against "
                + "the consumer schema: "
                + failure.getMessage());
      }
    }
    return Result.compatible(path + " every finite producer value is accepted");
  }

  private static List<JsonNode> finiteValues(JsonNode schema) {
    if (schema.has("const")) {
      return List.of(schema.required("const"));
    }
    if (schema.path("enum").isArray()) {
      List<JsonNode> values = new ArrayList<>();
      schema.required("enum").forEach(values::add);
      return List.copyOf(values);
    }
    return null;
  }

  private static ResolvedDataSchema schemaAt(
      ResolvedDataSchema owner, JsonNode schema, String path) {
    return new ResolvedDataSchema(
        owner.definitionPath() + path, owner.format(), null, sha256(schema.toString()), schema);
  }

  private Result compareObjects(
      ResolvedDataSchema producerSchema,
      ResolvedDataSchema consumerSchema,
      JsonNode producer,
      JsonNode consumer,
      String path,
      Set<NodePair> visited) {
    Set<String> producerRequired = textSet(producer.path("required"));
    Set<String> consumerRequired = textSet(consumer.path("required"));
    if (!producerRequired.containsAll(consumerRequired)) {
      Set<String> missing = new LinkedHashSet<>(consumerRequired);
      missing.removeAll(producerRequired);
      return Result.incompatible(
          path + " producer does not require consumer field(s) " + String.join(", ", missing));
    }

    Result cardinality = compareBounds(producer, consumer, "minProperties", "maxProperties", path);
    if (cardinality.status() != SchemaCompatibilityStatus.COMPATIBLE) {
      return cardinality;
    }

    JsonNode producerProperties = producer.path("properties");
    JsonNode consumerProperties = consumer.path("properties");
    Result aggregate = Result.compatible(path + " object fields are compatible");

    Iterator<String> consumerNames = consumerProperties.fieldNames();
    while (consumerNames.hasNext()) {
      String name = consumerNames.next();
      JsonNode producerProperty = producerProperties.get(name);
      if (producerProperty == null) {
        producerProperty = additionalSchema(producer);
        if (producerProperty == null) {
          continue;
        }
      }
      Result property =
          compare(
              producerSchema,
              consumerSchema,
              producerProperty,
              consumerProperties.required(name),
              path + "/properties/" + escape(name),
              visited);
      aggregate = aggregate.merge(property);
      if (aggregate.status() == SchemaCompatibilityStatus.INCOMPATIBLE) {
        return aggregate;
      }
    }

    Iterator<String> producerNames = producerProperties.fieldNames();
    while (producerNames.hasNext()) {
      String name = producerNames.next();
      if (consumerProperties.has(name)) {
        continue;
      }
      JsonNode consumerAdditional = additionalSchema(consumer);
      if (consumerAdditional == null) {
        return Result.incompatible(
            path + " producer permits property '" + name + "' which consumer forbids");
      }
      Result property =
          compare(
              producerSchema,
              consumerSchema,
              producerProperties.required(name),
              consumerAdditional,
              path + "/properties/" + escape(name),
              visited);
      aggregate = aggregate.merge(property);
      if (aggregate.status() == SchemaCompatibilityStatus.INCOMPATIBLE) {
        return aggregate;
      }
    }

    JsonNode producerAdditional = additionalSchema(producer);
    JsonNode consumerAdditional = additionalSchema(consumer);
    if (producerAdditional != null) {
      if (consumerAdditional == null) {
        return Result.incompatible(
            path + " producer permits additional properties but consumer forbids them");
      }
      aggregate =
          aggregate.merge(
              compare(
                  producerSchema,
                  consumerSchema,
                  producerAdditional,
                  consumerAdditional,
                  path + "/additionalProperties",
                  visited));
    }
    return aggregate;
  }

  private Result compareArrays(
      ResolvedDataSchema producerSchema,
      ResolvedDataSchema consumerSchema,
      JsonNode producer,
      JsonNode consumer,
      String path,
      Set<NodePair> visited) {
    Result bounds = compareBounds(producer, consumer, "minItems", "maxItems", path);
    if (bounds.status() != SchemaCompatibilityStatus.COMPATIBLE) {
      return bounds;
    }
    if (consumer.path("uniqueItems").asBoolean(false)
        && !producer.path("uniqueItems").asBoolean(false)) {
      return Result.incompatible(path + " consumer requires unique items but producer does not");
    }
    JsonNode producerItems = producer.get("items");
    JsonNode consumerItems = consumer.get("items");
    if (consumerItems == null || consumerItems.isBoolean() && consumerItems.booleanValue()) {
      return Result.compatible(path + " array items are accepted");
    }
    if (producerItems == null) {
      return Result.incompatible(
          path + " producer permits arbitrary items but consumer constrains them");
    }
    return compare(
        producerSchema, consumerSchema, producerItems, consumerItems, path + "/items", visited);
  }

  private static Result compareStrings(JsonNode producer, JsonNode consumer, String path) {
    Result bounds = compareBounds(producer, consumer, "minLength", "maxLength", path);
    if (bounds.status() != SchemaCompatibilityStatus.COMPATIBLE) {
      return bounds;
    }
    for (String keyword : List.of("pattern", "format")) {
      if (consumer.has(keyword)
          && (!producer.has(keyword)
              || !producer.required(keyword).equals(consumer.required(keyword)))) {
        return Result.unproven(path + " cannot prove " + keyword + " constraint inclusion");
      }
    }
    return Result.compatible(path + " string constraints are compatible");
  }

  private static Result compareNumbers(JsonNode producer, JsonNode consumer, String path) {
    Bound producerMinimum = minimum(producer);
    Bound consumerMinimum = minimum(consumer);
    if (consumerMinimum != null
        && (producerMinimum == null || producerMinimum.weakerMinimumThan(consumerMinimum))) {
      return Result.incompatible(path + " producer minimum is weaker than consumer minimum");
    }
    Bound producerMaximum = maximum(producer);
    Bound consumerMaximum = maximum(consumer);
    if (consumerMaximum != null
        && (producerMaximum == null || producerMaximum.weakerMaximumThan(consumerMaximum))) {
      return Result.incompatible(path + " producer maximum is weaker than consumer maximum");
    }
    if (consumer.has("multipleOf")) {
      if (!producer.has("multipleOf")) {
        return Result.incompatible(path + " consumer requires multipleOf but producer does not");
      }
      BigDecimal producerMultiple = producer.required("multipleOf").decimalValue();
      BigDecimal consumerMultiple = consumer.required("multipleOf").decimalValue();
      if (producerMultiple.remainder(consumerMultiple).compareTo(BigDecimal.ZERO) != 0) {
        return Result.incompatible(
            path + " producer multipleOf is not a multiple of the consumer constraint");
      }
    }
    return Result.compatible(path + " numeric constraints are compatible");
  }

  private static Result compareBounds(
      JsonNode producer,
      JsonNode consumer,
      String minimumKeyword,
      String maximumKeyword,
      String path) {
    if (consumer.has(minimumKeyword)
        && (!producer.has(minimumKeyword)
            || producer.required(minimumKeyword).longValue()
                < consumer.required(minimumKeyword).longValue())) {
      return Result.incompatible(
          path + " producer " + minimumKeyword + " is weaker than consumer " + minimumKeyword);
    }
    if (consumer.has(maximumKeyword)
        && (!producer.has(maximumKeyword)
            || producer.required(maximumKeyword).longValue()
                > consumer.required(maximumKeyword).longValue())) {
      return Result.incompatible(
          path + " producer " + maximumKeyword + " is weaker than consumer " + maximumKeyword);
    }
    return Result.compatible(path + " cardinality bounds are compatible");
  }

  private static Result compareTypes(Set<String> producer, Set<String> consumer, String path) {
    for (String type : producer) {
      if (!consumer.contains(type) && !("integer".equals(type) && consumer.contains("number"))) {
        return Result.incompatible(
            path + " producer type " + type + " is not accepted by consumer types " + consumer);
      }
    }
    return Result.compatible(path + " types are compatible");
  }

  private static Set<String> types(JsonNode schema) {
    JsonNode type = schema.get("type");
    if (type == null) {
      return JSON_TYPES;
    }
    if (type.isTextual()) {
      return Set.of(type.textValue());
    }
    Set<String> result = new LinkedHashSet<>();
    type.forEach(item -> result.add(item.textValue()));
    return Set.copyOf(result);
  }

  private static boolean constraintsAbsent(JsonNode producer, JsonNode consumer) {
    Set<String> ignored =
        Set.of(
            "$schema",
            "$id",
            "$anchor",
            "$defs",
            "definitions",
            "title",
            "description",
            "default",
            "examples",
            "type");
    return fields(producer).stream().allMatch(ignored::contains)
        && fields(consumer).stream().allMatch(ignored::contains);
  }

  private static Set<String> fields(JsonNode node) {
    Set<String> result = new LinkedHashSet<>();
    node.fieldNames().forEachRemaining(result::add);
    return result;
  }

  private static Set<String> unsupportedKeywords(JsonNode producer, JsonNode consumer) {
    Set<String> result = new LinkedHashSet<>();
    PROOF_BREAKING_KEYWORDS.forEach(
        keyword -> {
          if (producer.has(keyword) || consumer.has(keyword)) {
            result.add(keyword);
          }
        });
    return result;
  }

  private static JsonNode resolveLocalReference(JsonNode root, JsonNode schema) {
    JsonNode reference = schema.get("$ref");
    if (reference == null) {
      return schema;
    }
    String value = reference.textValue();
    if (value == null || !value.startsWith("#")) {
      return null;
    }
    String pointer = value.substring(1);
    JsonNode resolved = pointer.isEmpty() ? root : root.at(pointer);
    return resolved.isMissingNode() ? null : resolved;
  }

  private static boolean hasReferenceAssertionSiblings(JsonNode schema) {
    if (!schema.has("$ref")) {
      return false;
    }
    Set<String> annotations =
        Set.of(
            "$ref",
            "$schema",
            "$id",
            "$anchor",
            "$defs",
            "definitions",
            "$comment",
            "title",
            "description",
            "default",
            "examples",
            "deprecated",
            "readOnly",
            "writeOnly");
    /*
     * Draft 2019-09 and 2020-12 apply assertion siblings next to a $ref,
     * while older drafts ignore them. Resolving only the target would
     * therefore be an unsound compatibility proof.
     */
    return !fields(schema).stream().allMatch(annotations::contains);
  }

  private static JsonNode additionalSchema(JsonNode schema) {
    JsonNode additional = schema.get("additionalProperties");
    if (additional == null || additional.isBoolean() && additional.booleanValue()) {
      return com.fasterxml.jackson.databind.node.BooleanNode.TRUE;
    }
    if (additional.isBoolean() && !additional.booleanValue()) {
      return null;
    }
    return additional;
  }

  private static Set<String> textSet(JsonNode array) {
    if (!array.isArray()) {
      return Set.of();
    }
    Set<String> result = new LinkedHashSet<>();
    array.forEach(item -> result.add(item.textValue()));
    return Set.copyOf(result);
  }

  private static Bound minimum(JsonNode schema) {
    if (schema.has("exclusiveMinimum")) {
      return new Bound(schema.required("exclusiveMinimum").decimalValue(), true);
    }
    return schema.has("minimum")
        ? new Bound(schema.required("minimum").decimalValue(), false)
        : null;
  }

  private static Bound maximum(JsonNode schema) {
    if (schema.has("exclusiveMaximum")) {
      return new Bound(schema.required("exclusiveMaximum").decimalValue(), true);
    }
    return schema.has("maximum")
        ? new Bound(schema.required("maximum").decimalValue(), false)
        : null;
  }

  private static String escape(String value) {
    return value.replace("~", "~0").replace("/", "~1");
  }

  private static String sha256(String value) {
    try {
      return java.util.HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  public record Result(SchemaCompatibilityStatus status, String reason) {

    public Result {
      Objects.requireNonNull(status, "status");
      Objects.requireNonNull(reason, "reason");
    }

    static Result compatible(String reason) {
      return new Result(SchemaCompatibilityStatus.COMPATIBLE, reason);
    }

    static Result incompatible(String reason) {
      return new Result(SchemaCompatibilityStatus.INCOMPATIBLE, reason);
    }

    static Result unproven(String reason) {
      return new Result(SchemaCompatibilityStatus.UNPROVEN, reason);
    }

    Result merge(Result other) {
      if (status == SchemaCompatibilityStatus.INCOMPATIBLE) {
        return this;
      }
      if (other.status == SchemaCompatibilityStatus.INCOMPATIBLE) {
        return other;
      }
      if (status == SchemaCompatibilityStatus.UNPROVEN) {
        return this;
      }
      return other.status == SchemaCompatibilityStatus.UNPROVEN ? other : this;
    }
  }

  private record NodePair(JsonNode producer, JsonNode consumer) {}

  private record Bound(BigDecimal value, boolean exclusive) {
    boolean weakerMinimumThan(Bound consumer) {
      int comparison = value.compareTo(consumer.value);
      return comparison < 0 || comparison == 0 && !exclusive && consumer.exclusive;
    }

    boolean weakerMaximumThan(Bound consumer) {
      int comparison = value.compareTo(consumer.value);
      return comparison > 0 || comparison == 0 && !exclusive && consumer.exclusive;
    }
  }
}
