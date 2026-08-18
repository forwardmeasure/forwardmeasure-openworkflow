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
package com.forwardmeasure.openworkflow.definition.management;

import com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

public record ManagedWorkflowRevision(
    UUID revisionId,
    String definitionKey,
    String displayName,
    int revisionNumber,
    WorkflowLifecycleState lifecycleState,
    String sourceDocument,
    String resolvedDocument,
    List<ResolvedWorkflowResource> resolvedResources,
    String specificationVersion,
    String compilerProfile,
    String sourceDigest,
    String resolvedDigest,
    String authorActorId) {
  private static final Pattern SHA_256 = Pattern.compile("[0-9a-f]{64}");

  public ManagedWorkflowRevision {
    Objects.requireNonNull(revisionId, "revisionId");
    definitionKey = text(definitionKey, "definitionKey");
    displayName = text(displayName, "displayName");
    if (revisionNumber < 1) {
      throw new IllegalArgumentException("revisionNumber must be positive");
    }
    Objects.requireNonNull(lifecycleState, "lifecycleState");
    sourceDocument = text(sourceDocument, "sourceDocument");
    resolvedDocument = text(resolvedDocument, "resolvedDocument");
    resolvedResources = resolvedResources == null ? List.of() : List.copyOf(resolvedResources);
    specificationVersion = text(specificationVersion, "specificationVersion");
    compilerProfile = text(compilerProfile, "compilerProfile");
    sourceDigest = digest(sourceDigest, "sourceDigest");
    resolvedDigest = digest(resolvedDigest, "resolvedDigest");
    authorActorId = text(authorActorId, "authorActorId");
  }

  public ManagedWorkflowRevision transitionTo(WorkflowLifecycleState target) {
    return new ManagedWorkflowRevision(
        revisionId,
        definitionKey,
        displayName,
        revisionNumber,
        target,
        sourceDocument,
        resolvedDocument,
        resolvedResources,
        specificationVersion,
        compilerProfile,
        sourceDigest,
        resolvedDigest,
        authorActorId);
  }

  /** Compatibility constructor for definitions that do not reference external resources. */
  public ManagedWorkflowRevision(
      UUID revisionId,
      String definitionKey,
      String displayName,
      int revisionNumber,
      WorkflowLifecycleState lifecycleState,
      String sourceDocument,
      String resolvedDocument,
      String specificationVersion,
      String compilerProfile,
      String sourceDigest,
      String resolvedDigest,
      String authorActorId) {
    this(
        revisionId,
        definitionKey,
        displayName,
        revisionNumber,
        lifecycleState,
        sourceDocument,
        resolvedDocument,
        List.of(),
        specificationVersion,
        compilerProfile,
        sourceDigest,
        resolvedDigest,
        authorActorId);
  }

  private static String digest(String value, String name) {
    if (!SHA_256.matcher(text(value, name)).matches()) {
      throw new IllegalArgumentException(name + " must be a lowercase SHA-256 digest");
    }
    return value;
  }

  private static String text(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
