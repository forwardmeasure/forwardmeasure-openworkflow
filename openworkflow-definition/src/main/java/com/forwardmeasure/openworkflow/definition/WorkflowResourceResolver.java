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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.yaml.snakeyaml.LoaderOptions;

/**
 * Resolves every external resource required by a workflow before publication.
 *
 * <p>Top-level resources are discovered from Open Workflow input/output/export schema declarations.
 * Transitive {@code $ref} and {@code $dynamicRef} resources are resolved relative to the containing
 * schema. Call documents are discovered for AsyncAPI, gRPC, OpenAPI and A2A calls. The returned
 * list is suitable for embedding once in an immutable admission command.
 */
public final class WorkflowResourceResolver {
  public static final int MAX_RESOURCES = 256;
  public static final long MAX_TOTAL_BYTES = 20L * 1024 * 1024;
  private static final ObjectMapper YAML =
      new ObjectMapper(yamlFactory()).enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Pattern PROTO_IMPORT =
      Pattern.compile("(?m)^\\s*import\\s+(?:(?:public|weak)\\s+)?" + "\"([^\"]+)\"\\s*;");

  private static YAMLFactory yamlFactory() {
    LoaderOptions loaderOptions = new LoaderOptions();
    loaderOptions.setCodePointLimit(OpenWorkflowCompiler.MAX_SOURCE_BYTES);
    return YAMLFactory.builder()
        .loaderOptions(loaderOptions)
        .streamReadConstraints(
            StreamReadConstraints.builder()
                .maxDocumentLength(OpenWorkflowCompiler.MAX_SOURCE_BYTES)
                .maxStringLength(OpenWorkflowCompiler.MAX_SOURCE_BYTES)
                .build())
        .build();
  }

  public List<ResolvedWorkflowResource> resolve(
      byte[] workflowSource, WorkflowResourceLoader loader) {
    Objects.requireNonNull(workflowSource, "workflowSource");
    Objects.requireNonNull(loader, "loader");
    final JsonNode root;
    try {
      root = YAML.readTree(workflowSource);
    } catch (IOException failure) {
      throw new WorkflowDefinitionException(
          List.of("Source is not valid YAML: " + failure.getMessage()));
    }

    ArrayDeque<WorkflowResourceRequest> pending = new ArrayDeque<>();
    discoverResourceDeclarations(root, pending);
    JsonNode catalogs = root.path("use").path("catalogs");
    discoverCatalogFunctions(root, catalogs, pending);
    Map<URI, ResolvedWorkflowResource> resolved = new LinkedHashMap<>();
    Set<ResourceExpansion> expandedResources = new HashSet<>();
    long totalBytes = 0;
    while (!pending.isEmpty()) {
      WorkflowResourceRequest request = pending.removeFirst();
      ResolvedWorkflowResource existing = resolved.get(request.uri());
      if (existing != null) {
        expandResource(existing, request.kind(), catalogs, pending, expandedResources);
        continue;
      }
      if (resolved.size() >= MAX_RESOURCES) {
        throw new WorkflowDefinitionException(
            List.of("External workflow resources exceed " + MAX_RESOURCES + " resources"));
      }
      final ResolvedWorkflowResource resource;
      try {
        resource =
            Objects.requireNonNull(
                loader.load(request),
                "Workflow resource loader returned null for " + request.uri());
      } catch (RuntimeException failure) {
        throw new WorkflowDefinitionException(
            List.of(
                "Unable to resolve external workflow resource "
                    + request.uri()
                    + ": "
                    + rootMessage(failure)));
      }
      if (!request.uri().equals(resource.uri())) {
        throw new WorkflowDefinitionException(
            List.of(
                "Workflow resource loader returned "
                    + resource.uri()
                    + " for requested "
                    + request.uri()));
      }
      totalBytes += resource.content().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
      if (totalBytes > MAX_TOTAL_BYTES) {
        throw new WorkflowDefinitionException(
            List.of("External workflow resources exceed " + MAX_TOTAL_BYTES + " bytes"));
      }
      resolved.put(resource.uri(), resource);
      expandResource(resource, request.kind(), catalogs, pending, expandedResources);
    }
    return List.copyOf(resolved.values());
  }

  private static void expandResource(
      ResolvedWorkflowResource resource,
      WorkflowResourceKind kind,
      JsonNode catalogs,
      ArrayDeque<WorkflowResourceRequest> pending,
      Set<ResourceExpansion> expandedResources) {
    if (!expandedResources.add(new ResourceExpansion(resource.uri(), kind))) {
      return;
    }
    switch (kind) {
      case DATA_SCHEMA ->
          discoverReferences(
              parseSchema(resource), resource.uri(), WorkflowResourceKind.DATA_SCHEMA, pending);
      case OPEN_API_DOCUMENT, ASYNC_API_DOCUMENT ->
          discoverReferences(
              parseDocument(resource, "API document"), resource.uri(), kind, pending);
      case GRPC_PROTO -> discoverProtoImports(resource, pending);
      case FUNCTION_DEFINITION -> {
        JsonNode function = parseDocument(resource, "catalog function");
        discoverResourceDeclarations(function, pending);
        discoverCatalogFunctions(function, catalogs, pending);
      }
      case A2A_AGENT_CARD, SCRIPT_SOURCE -> {
        // These resource types do not have transitive references.
      }
    }
  }

  private static void discoverCatalogFunctions(
      JsonNode node, JsonNode catalogs, ArrayDeque<WorkflowResourceRequest> pending) {
    if (node == null) return;
    if (node.isObject()) {
      JsonNode call = node.get("call");
      if (call != null && call.isTextual()) {
        CatalogFunctionReference.resolve(call.textValue(), catalogs)
            .ifPresent(
                reference ->
                    pending.add(
                        new WorkflowResourceRequest(
                            reference.uri(),
                            reference.name(),
                            reference.endpoint(),
                            WorkflowResourceKind.FUNCTION_DEFINITION)));
      }
      node.properties()
          .forEach(entry -> discoverCatalogFunctions(entry.getValue(), catalogs, pending));
    } else if (node.isArray()) {
      node.forEach(child -> discoverCatalogFunctions(child, catalogs, pending));
    }
  }

  private static void discoverResourceDeclarations(
      JsonNode node, ArrayDeque<WorkflowResourceRequest> pending) {
    if (node == null) return;
    if (node.isObject()) {
      for (String sectionName : List.of("input", "output", "export")) {
        JsonNode section = node.get(sectionName);
        JsonNode schema = section == null ? null : section.get("schema");
        if (schema != null && schema.isObject() && schema.has("resource")) {
          JsonNode resource = schema.required("resource");
          JsonNode endpoint = resource.required("endpoint");
          URI uri = literalEndpoint(endpoint);
          pending.add(
              new WorkflowResourceRequest(
                  withoutFragment(uri),
                  resource.path("name").asText(null),
                  endpoint,
                  WorkflowResourceKind.DATA_SCHEMA));
        } else if (schema != null && schema.isObject() && schema.has("document")) {
          JsonNode document = schema.required("document");
          URI base = URI.create("urn:oks:inline-schema:publication");
          JsonNode id = document.get("$id");
          if (id != null && id.isTextual()) {
            try {
              URI declared = URI.create(id.textValue());
              if (declared.isAbsolute()) {
                base = declared;
              }
            } catch (IllegalArgumentException ignored) {
              // Compilation reports the malformed JSON Schema.
            }
          }
          discoverReferences(document, base, WorkflowResourceKind.DATA_SCHEMA, pending);
        }
      }
      discoverEventDataSchemas(node, pending);
      discoverCallResource(node, pending);
      discoverRunResource(node, pending);
      node.properties()
          .forEach(
              entry -> {
                if (!"schema".equals(entry.getKey())) {
                  discoverResourceDeclarations(entry.getValue(), pending);
                }
              });
    } else if (node.isArray()) {
      node.forEach(child -> discoverResourceDeclarations(child, pending));
    }
  }

  private static void discoverEventDataSchemas(
      JsonNode node, ArrayDeque<WorkflowResourceRequest> pending) {
    for (String strategy : List.of("one", "all", "any")) {
      JsonNode filters = node.get(strategy);
      if (filters == null) {
        continue;
      }
      if (filters.isObject()) {
        discoverEventDataSchema(filters, pending);
      } else if (filters.isArray()) {
        filters.forEach(filter -> discoverEventDataSchema(filter, pending));
      }
    }
  }

  private static void discoverEventDataSchema(
      JsonNode filter, ArrayDeque<WorkflowResourceRequest> pending) {
    JsonNode declared = filter.path("with").get("dataschema");
    if (declared == null || !declared.isTextual()) {
      return;
    }
    String value = declared.textValue();
    if (value.contains("${") || value.contains("{")) {
      return;
    }
    URI uri = literalEndpoint(declared);
    pending.add(
        new WorkflowResourceRequest(
            withoutFragment(uri), null, declared, WorkflowResourceKind.DATA_SCHEMA));
  }

  private static void discoverCallResource(
      JsonNode node, ArrayDeque<WorkflowResourceRequest> pending) {
    JsonNode call = node.get("call");
    JsonNode arguments = node.get("with");
    if (call == null || !call.isTextual() || arguments == null || !arguments.isObject()) {
      return;
    }
    String field;
    WorkflowResourceKind kind;
    switch (call.textValue()) {
      case "asyncapi", CallPlan.CORRELATED_WORKER_FUNCTION -> {
        field = "document";
        kind = WorkflowResourceKind.ASYNC_API_DOCUMENT;
      }
      case "grpc" -> {
        field = "proto";
        kind = WorkflowResourceKind.GRPC_PROTO;
      }
      case "openapi" -> {
        field = "document";
        kind = WorkflowResourceKind.OPEN_API_DOCUMENT;
      }
      case "a2a" -> {
        field = "agentCard";
        kind = WorkflowResourceKind.A2A_AGENT_CARD;
      }
      default -> {
        return;
      }
    }
    JsonNode resource = arguments.get(field);
    if (resource == null) {
      return;
    }
    JsonNode endpoint = resource.required("endpoint");
    URI uri = literalEndpoint(endpoint);
    pending.add(
        new WorkflowResourceRequest(
            withoutFragment(uri), resource.path("name").asText(null), endpoint, kind));
  }

  private static void discoverRunResource(
      JsonNode node, ArrayDeque<WorkflowResourceRequest> pending) {
    JsonNode run = node.get("run");
    if (run == null || !run.isObject()) {
      return;
    }
    JsonNode script = run.get("script");
    if (script == null || !script.isObject() || !script.has("source")) {
      return;
    }
    JsonNode resource = script.required("source");
    JsonNode endpoint = resource.required("endpoint");
    URI uri = literalEndpoint(endpoint);
    pending.add(
        new WorkflowResourceRequest(
            withoutFragment(uri),
            resource.path("name").asText(null),
            endpoint,
            WorkflowResourceKind.SCRIPT_SOURCE));
  }

  private static void discoverReferences(
      JsonNode node,
      URI base,
      WorkflowResourceKind kind,
      ArrayDeque<WorkflowResourceRequest> pending) {
    if (node.isObject()) {
      URI effectiveBase = base;
      JsonNode id = node.get("$id");
      if (id != null && id.isTextual()) {
        effectiveBase = base.resolve(id.textValue());
      }
      for (String field : List.of("$ref", "$dynamicRef")) {
        JsonNode reference = node.get(field);
        if (reference != null && reference.isTextual()) {
          URI resolved = effectiveBase.resolve(reference.textValue());
          URI resourceUri = withoutFragment(resolved);
          if (!resourceUri.equals(withoutFragment(base))) {
            pending.add(new WorkflowResourceRequest(resourceUri, null, null, kind));
          }
        }
      }
      URI childBase = effectiveBase;
      node.properties()
          .forEach(entry -> discoverReferences(entry.getValue(), childBase, kind, pending));
    } else if (node.isArray()) {
      node.forEach(child -> discoverReferences(child, base, kind, pending));
    }
  }

  private static void discoverProtoImports(
      ResolvedWorkflowResource resource, ArrayDeque<WorkflowResourceRequest> pending) {
    Matcher imports = PROTO_IMPORT.matcher(resource.content());
    while (imports.find()) {
      String imported = imports.group(1);
      if (imported.startsWith("google/protobuf/")) {
        // Standard well-known types are supplied by protoc itself.
        continue;
      }
      URI importedUri = resource.uri().resolve(imported).normalize();
      if (!importedUri.isAbsolute() || importedUri.getFragment() != null) {
        throw new WorkflowDefinitionException(
            List.of(
                "Proto import does not resolve to an absolute "
                    + "fragment-free URI from "
                    + resource.uri()
                    + ": "
                    + imported));
      }
      pending.add(
          new WorkflowResourceRequest(importedUri, null, null, WorkflowResourceKind.GRPC_PROTO));
    }
  }

  static URI literalEndpoint(JsonNode endpoint) {
    JsonNode uriNode = endpoint.isObject() ? endpoint.get("uri") : endpoint;
    if (uriNode == null || !uriNode.isTextual()) {
      throw new WorkflowDefinitionException(
          List.of("External resource endpoint must contain a literal URI"));
    }
    String value = uriNode.textValue();
    if (value.contains("${") || value.contains("{")) {
      throw new WorkflowDefinitionException(
          List.of(
              "External resource endpoint must be immutable at publication "
                  + "time; runtime expressions and URI templates are "
                  + "not supported"));
    }
    try {
      URI uri = URI.create(value).normalize();
      if (!uri.isAbsolute()) {
        throw new IllegalArgumentException();
      }
      return uri;
    } catch (IllegalArgumentException failure) {
      throw new WorkflowDefinitionException(
          List.of("External resource endpoint is not an absolute URI: " + value));
    }
  }

  static JsonNode parseSchema(ResolvedWorkflowResource resource) {
    JsonNode document = parseDocument(resource, "schema");
    if (!document.isObject() && !document.isBoolean()) {
      throw new WorkflowDefinitionException(
          List.of(
              "Schema resource "
                  + resource.uri()
                  + " must contain a JSON Schema object or boolean"));
    }
    return document;
  }

  static JsonNode parseDocument(ResolvedWorkflowResource resource, String description) {
    try {
      String mediaType = resource.mediaType().toLowerCase(java.util.Locale.ROOT);
      ObjectMapper mapper = mediaType.contains("yaml") || mediaType.contains("yml") ? YAML : JSON;
      JsonNode document = mapper.readTree(resource.content());
      if (document == null) {
        throw new WorkflowDefinitionException(
            List.of(description + " resource " + resource.uri() + " is empty"));
      }
      return document.deepCopy();
    } catch (IOException failure) {
      throw new WorkflowDefinitionException(
          List.of(
              description
                  + " resource "
                  + resource.uri()
                  + " is not valid JSON/YAML: "
                  + failure.getMessage()));
    }
  }

  private static URI withoutFragment(URI uri) {
    if (uri.getFragment() == null) return uri.normalize();
    return URI.create(uri.toString().substring(0, uri.toString().indexOf('#'))).normalize();
  }

  private static String rootMessage(Throwable failure) {
    Throwable current = failure;
    while (current.getCause() != null) {
      current = current.getCause();
    }
    return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
  }

  private record ResourceExpansion(URI uri, WorkflowResourceKind kind) {}
}
