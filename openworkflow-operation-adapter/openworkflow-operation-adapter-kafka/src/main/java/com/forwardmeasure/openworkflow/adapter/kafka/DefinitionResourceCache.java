package com.forwardmeasure.openworkflow.adapter.kafka;

import com.forwardmeasure.openworkflow.adapter.api.OperationResourceResolver;
import com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OksTenantId;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionBundle;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

/** In-memory projection of the compacted immutable definition-bundle topic. */
final class DefinitionResourceCache implements OperationResourceResolver {
  private final Map<String, WorkflowDefinitionBundle> definitions = new ConcurrentHashMap<>();
  private final Object changed = new Object();

  void put(WorkflowDefinitionBundle bundle) {
    Objects.requireNonNull(bundle, "bundle");
    definitions.put(bundle.reference().canonical(), bundle);
    signal();
  }

  void remove(String definitionReference) {
    definitions.remove(definitionReference);
    signal();
  }

  /**
   * Waits for the local compacted-topic projection to observe a definition.
   *
   * <p>Definition admission and execution effects use different Kafka topics. Their consumers
   * therefore cannot rely on cross-topic arrival order, even though the runtime cannot create an
   * effect without first resolving the admitted definition. This wait closes that projection race
   * without accepting an unpinned or mismatched resource.
   */
  boolean awaitDefinition(
      String definitionReference, Duration timeout, BooleanSupplier keepWaiting) {
    Objects.requireNonNull(definitionReference, "definitionReference");
    Objects.requireNonNull(timeout, "timeout");
    Objects.requireNonNull(keepWaiting, "keepWaiting");
    long deadline = System.nanoTime() + timeout.toNanos();
    synchronized (changed) {
      while (!definitions.containsKey(definitionReference) && keepWaiting.getAsBoolean()) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) return false;
        try {
          changed.wait(Math.min(Duration.ofNanos(remaining).toMillis() + 1, 250));
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
      return definitions.containsKey(definitionReference);
    }
  }

  /**
   * Validates the immutable definition identity before an adapter is allowed to perform external
   * I/O.
   *
   * <p>The definitions topic is the adapter dispatcher's admission boundary. A descriptor cannot
   * select a definition admitted for another tenant, and it cannot substitute a digest while
   * retaining a valid catalogue key.
   */
  void validateDefinition(
      String definitionReference, OksTenantId tenantId, String definitionSha256) {
    Objects.requireNonNull(definitionReference, "definitionReference");
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(definitionSha256, "definitionSha256");
    WorkflowDefinitionBundle bundle = definitions.get(definitionReference);
    if (bundle == null) {
      throw new IllegalStateException("Definition bundle is not available: " + definitionReference);
    }
    if (!bundle.key().tenantId().equals(tenantId)) {
      throw new IllegalArgumentException(
          "Operation tenant does not match the admitted " + "definition tenant");
    }
    if (!bundle.reference().canonical().equals(definitionReference)) {
      throw new IllegalArgumentException(
          "Definition reference does not match the admitted " + "definition bundle");
    }
    if (!bundle.plan().definitionSha256().equals(definitionSha256)) {
      throw new IllegalArgumentException(
          "Operation definition digest does not match the " + "admitted definition bundle");
    }
  }

  void signal() {
    synchronized (changed) {
      changed.notifyAll();
    }
  }

  @Override
  public ResolvedWorkflowResource resolve(
      String definitionReference, WorkflowResourceReference reference) {
    WorkflowDefinitionBundle bundle = definitions.get(definitionReference);
    if (bundle == null) {
      throw new IllegalStateException("Definition bundle is not available: " + definitionReference);
    }
    return bundle.plan().resources().stream()
        .filter(
            resource ->
                resource.uri().equals(reference.uri())
                    && resource.sha256().equals(reference.sha256()))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Definition bundle does not contain resource "
                        + reference.uri()
                        + " at "
                        + reference.sha256()));
  }

  @Override
  public List<ResolvedWorkflowResource> resolveAll(
      String definitionReference, WorkflowResourceReference primary) {
    WorkflowDefinitionBundle bundle = definitions.get(definitionReference);
    if (bundle == null) {
      throw new IllegalStateException("Definition bundle is not available: " + definitionReference);
    }
    // Resolve first so a forged primary reference cannot expose another
    // definition's resource graph.
    resolve(definitionReference, primary);
    return bundle.plan().resources();
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
