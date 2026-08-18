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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.SchemaLocation;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Deterministic JSON Schema validation backed only by schema bytes embedded in the immutable
 * workflow plan.
 */
public final class DataSchemaValidator {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final Map<String, String> resources;
  private final Map<String, com.networknt.schema.Schema> compiled = new ConcurrentHashMap<>();

  public DataSchemaValidator(List<ResolvedWorkflowResource> resources) {
    Map<String, String> content = new LinkedHashMap<>();
    for (ResolvedWorkflowResource resource : Objects.requireNonNull(resources, "resources")) {
      /*
       * A workflow bundle also pins protocol documents, proto files and
       * scripts. Only structured JSON/YAML resources belong in the
       * JSON Schema registry. Treating every immutable resource as a
       * schema made otherwise valid gRPC workflows fail at execution
       * start when protoc source was parsed as YAML.
       */
      if (!structuredDocument(resource.mediaType())) {
        continue;
      }
      final String normalized;
      try {
        normalized = JSON.writeValueAsString(WorkflowResourceResolver.parseSchema(resource));
      } catch (com.fasterxml.jackson.core.JsonProcessingException failure) {
        throw new IllegalArgumentException(
            "Cannot normalize schema resource " + resource.uri(), failure);
      }
      String prior = content.put(resource.uri().toString(), normalized);
      if (prior != null) {
        throw new IllegalArgumentException("Duplicate schema resource " + resource.uri());
      }
    }
    this.resources = Map.copyOf(content);
  }

  private static boolean structuredDocument(String mediaType) {
    String normalized = mediaType.toLowerCase(java.util.Locale.ROOT).split(";", 2)[0].trim();
    return normalized.equals("application/json")
        || normalized.endsWith("+json")
        || normalized.equals("application/yaml")
        || normalized.equals("application/x-yaml")
        || normalized.equals("text/yaml")
        || normalized.equals("text/x-yaml");
  }

  public void validate(ResolvedDataSchema schema, JsonNode value) {
    if (schema == null) {
      return;
    }
    Objects.requireNonNull(value, "value");
    var executable = compile(schema);
    var errors =
        executable.validate(
            value,
            context -> context.executionConfig(config -> config.formatAssertionsEnabled(true)));
    if (!errors.isEmpty()) {
      List<SchemaViolation> violations =
          errors.stream()
              .map(
                  error ->
                      new SchemaViolation(
                          error.getInstanceLocation().toString(),
                          error.getSchemaLocation().toString(),
                          error.getKeyword(),
                          error.getMessage()))
              .toList();
      throw new DataSchemaValidationException(schema, value, violations);
    }
  }

  /** Compiles the schema and eagerly reports malformed schema documents. */
  public com.networknt.schema.Schema compile(ResolvedDataSchema schema) {
    Objects.requireNonNull(schema, "schema");
    return compiled.computeIfAbsent(
        schema.format()
            + ":"
            + schema.sha256()
            + ":"
            + (schema.resourceUri() == null ? "inline" : schema.resourceUri()),
        ignored -> compileUncached(schema));
  }

  private com.networknt.schema.Schema compileUncached(ResolvedDataSchema schema) {
    SchemaLocation location =
        SchemaLocation.of(
            schema.external()
                ? schema.resourceUri().toString()
                : "urn:oks:inline-schema:" + schema.sha256());
    try {
      SpecificationVersion version = specificationVersion(schema.format());
      SchemaRegistry registry =
          SchemaRegistry.withDefaultDialect(
              version,
              builder ->
                  builder
                      .resourceLoaders(loaders -> loaders.resources(resources))
                      .schemaLoader(loader -> loader.fetchRemoteResources(false)));
      String dialect = schema.document().path("$schema").asText(version.getDialectId());
      var schemaErrors = registry.getSchema(SchemaLocation.of(dialect)).validate(schema.document());
      if (!schemaErrors.isEmpty()) {
        throw new IllegalArgumentException(
            "Schema document does not satisfy "
                + dialect
                + ": "
                + schemaErrors.iterator().next().getMessage());
      }
      return schema.external()
          ? registry.getSchema(location)
          : registry.getSchema(location, schema.document());
    } catch (RuntimeException failure) {
      throw new IllegalStateException(
          "Pinned schema cannot be compiled at " + schema.definitionPath(), failure);
    }
  }

  private static SpecificationVersion specificationVersion(String format) {
    return switch (format) {
      case "json", "json:2020-12", "json:draft-2020-12" -> SpecificationVersion.DRAFT_2020_12;
      case "json:2019-09", "json:draft-2019-09" -> SpecificationVersion.DRAFT_2019_09;
      case "json:7", "json:07", "json:draft-07" -> SpecificationVersion.DRAFT_7;
      case "json:6", "json:06", "json:draft-06" -> SpecificationVersion.DRAFT_6;
      case "json:4", "json:04", "json:draft-04" -> SpecificationVersion.DRAFT_4;
      default ->
          throw new IllegalArgumentException("Unsupported JSON Schema format version " + format);
    };
  }
}
