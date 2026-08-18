package com.forwardmeasure.openworkflow.adapter.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceKind;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorContext;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ExecutionKey;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffect;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowEffectType;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Validated adapter-facing projection of one committed operation effect. */
public record OperationRequest(
    String operationId,
    String operationKind,
    String definitionReference,
    JsonNode descriptor,
    ResolvedWorkflowResource resource,
    ActorContext requestedBy,
    String effectId,
    Instant requestedAt,
    @JsonIgnore ResolvedAuthentication authentication,
    @JsonIgnore Map<String, ResolvedSecret> secrets,
    @JsonIgnore List<ResolvedWorkflowResource> resources) {

  public OperationRequest {
    requireText(operationId, "operationId");
    requireText(operationKind, "operationKind");
    requireText(definitionReference, "definitionReference");
    Objects.requireNonNull(descriptor, "descriptor");
    Objects.requireNonNull(requestedBy, "requestedBy");
    requireText(effectId, "effectId");
    Objects.requireNonNull(requestedAt, "requestedAt");
    secrets = Map.copyOf(Objects.requireNonNull(secrets, "secrets"));
    secrets.forEach(
        (name, secret) -> {
          requireText(name, "secret name");
          Objects.requireNonNull(secret, "resolved secret");
          if (!name.equals(secret.name())) {
            throw new IllegalArgumentException("Resolved secret map key does not match its name");
          }
        });
    resources = List.copyOf(Objects.requireNonNull(resources, "resources"));
    if (!descriptor.isObject()) {
      throw new IllegalArgumentException("Operation descriptor must be an object");
    }
    descriptor = descriptor.deepCopy();
    if (resource != null) {
      WorkflowResourceReference expected = resourceReference(descriptor);
      if (expected == null
          || !expected.uri().equals(resource.uri())
          || !expected.sha256().equals(resource.sha256())) {
        throw new IllegalArgumentException("Resolved operation resource does not match descriptor");
      }
      if (resources.stream()
          .noneMatch(
              candidate ->
                  candidate.uri().equals(resource.uri())
                      && candidate.sha256().equals(resource.sha256()))) {
        throw new IllegalArgumentException(
            "Operation resource graph does not contain " + "the primary resource");
      }
    } else if (!resources.isEmpty()) {
      throw new IllegalArgumentException("Operation resource graph requires a primary resource");
    }
  }

  public OperationRequest(
      String operationId,
      String operationKind,
      String definitionReference,
      JsonNode descriptor,
      ResolvedWorkflowResource resource,
      ActorContext requestedBy,
      String effectId,
      Instant requestedAt,
      ResolvedAuthentication authentication,
      List<ResolvedWorkflowResource> resources) {
    this(
        operationId,
        operationKind,
        definitionReference,
        descriptor,
        resource,
        requestedBy,
        effectId,
        requestedAt,
        authentication,
        Map.of(),
        resources);
  }

  public OperationRequest(
      String operationId,
      String operationKind,
      String definitionReference,
      JsonNode descriptor,
      ResolvedWorkflowResource resource,
      ActorContext requestedBy,
      String effectId,
      Instant requestedAt,
      ResolvedAuthentication authentication) {
    this(
        operationId,
        operationKind,
        definitionReference,
        descriptor,
        resource,
        requestedBy,
        effectId,
        requestedAt,
        authentication,
        Map.of(),
        resource == null ? List.of() : List.of(resource));
  }

  public OperationRequest(
      String operationId,
      String operationKind,
      String definitionReference,
      JsonNode descriptor,
      ResolvedWorkflowResource resource,
      ActorContext requestedBy,
      String effectId,
      Instant requestedAt) {
    this(
        operationId,
        operationKind,
        definitionReference,
        descriptor,
        resource,
        requestedBy,
        effectId,
        requestedAt,
        null);
  }

  public OperationRequest(
      String operationId,
      String operationKind,
      String definitionReference,
      JsonNode descriptor,
      ResolvedWorkflowResource resource,
      ActorContext requestedBy) {
    this(
        operationId,
        operationKind,
        definitionReference,
        descriptor,
        resource,
        requestedBy,
        operationId,
        requestedBy.authenticatedAt(),
        null);
  }

  public static OperationRequest from(WorkflowEffect effect) {
    Objects.requireNonNull(effect, "effect");
    if (effect.type() != WorkflowEffectType.DISPATCH_OPERATION
        && effect.type() != WorkflowEffectType.CANCEL_OPERATION) {
      throw new IllegalArgumentException("Effect is not an external operation");
    }
    JsonNode descriptor = effect.payload().inlineValue();
    String executionKey = descriptor.required("executionKey").textValue();
    if (!effect.key().canonical().equals(executionKey)) {
      throw new IllegalArgumentException(
          "Operation descriptor execution key does not match " + "the committed effect");
    }
    String taskPath = descriptor.required("taskPath").textValue();
    if (!effect.taskPath().equals(taskPath)) {
      throw new IllegalArgumentException(
          "Operation descriptor task path does not match " + "the committed effect");
    }
    return new OperationRequest(
        descriptor.required("operationId").textValue(),
        descriptor.required("operationKind").textValue(),
        descriptor.required("definitionReference").textValue(),
        descriptor,
        null,
        effect.actor(),
        effect.effectId(),
        effect.requestedAt(),
        null,
        Map.of(),
        List.of());
  }

  public OperationRequest resolveResource(OperationResourceResolver resolver) {
    Objects.requireNonNull(resolver, "resolver");
    WorkflowResourceReference reference = resourceReference(descriptor);
    if (reference == null) {
      return this;
    }
    ResolvedWorkflowResource primary =
        Objects.requireNonNull(
            resolver.resolve(definitionReference, reference),
            "Operation resource resolver returned null");
    List<ResolvedWorkflowResource> graph =
        List.copyOf(
            Objects.requireNonNull(
                resolver.resolveAll(definitionReference, reference),
                "Operation resource resolver returned null graph"));
    return new OperationRequest(
        operationId,
        operationKind,
        definitionReference,
        descriptor,
        primary,
        requestedBy,
        effectId,
        requestedAt,
        authentication,
        secrets,
        graph);
  }

  public OperationRequest withAuthentication(ResolvedAuthentication resolved) {
    if (authentication != null) {
      throw new IllegalStateException("Operation request is already authenticated");
    }
    return new OperationRequest(
        operationId,
        operationKind,
        definitionReference,
        descriptor,
        resource,
        requestedBy,
        effectId,
        requestedAt,
        Objects.requireNonNull(resolved, "resolved"),
        secrets,
        resources);
  }

  public OperationRequest withSecrets(Map<String, ResolvedSecret> resolved) {
    Objects.requireNonNull(resolved, "resolved");
    if (!secrets.isEmpty()) {
      throw new IllegalStateException("Operation request already has resolved secrets");
    }
    return new OperationRequest(
        operationId,
        operationKind,
        definitionReference,
        descriptor,
        resource,
        requestedBy,
        effectId,
        requestedAt,
        authentication,
        resolved,
        resources);
  }

  @JsonIgnore
  public Optional<ResolvedSecret> secret(String name) {
    return Optional.ofNullable(secrets.get(name));
  }

  /**
   * Returns the tenant-qualified workflow aggregate addressed by this operation. Kafka uses the
   * canonical aggregate key; external protocols can use its nested run identifier because tenant
   * identity is propagated separately.
   */
  @JsonIgnore
  public ExecutionKey executionKey() {
    return ExecutionKey.parse(descriptor.required("executionKey").textValue());
  }

  private static WorkflowResourceReference resourceReference(JsonNode descriptor) {
    if (!descriptor.has("resourceUri")) {
      return null;
    }
    return new WorkflowResourceReference(
        WorkflowResourceKind.valueOf(descriptor.required("resourceKind").textValue()),
        java.net.URI.create(descriptor.required("resourceUri").textValue()),
        descriptor.required("resourceSha256").textValue());
  }

  private static void requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
  }
}
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
