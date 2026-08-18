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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Exact external resource bytes resolved before a definition is admitted.
 *
 * <p>The content and digest are retained in the immutable definition bundle, so compilation,
 * execution and restoration never depend on a mutable remote endpoint.
 */
public record ResolvedWorkflowResource(URI uri, String mediaType, String content, String sha256) {
  public static final int MAX_CONTENT_BYTES = 5 * 1024 * 1024;

  public ResolvedWorkflowResource {
    Objects.requireNonNull(uri, "uri");
    uri = uri.normalize();
    mediaType = requireText(mediaType, "mediaType");
    content = requireText(content, "content");
    requireSha256(sha256, "sha256");
    if (!uri.isAbsolute() || uri.getFragment() != null) {
      throw new IllegalArgumentException(
          "Workflow resource URI must be absolute and fragment-free");
    }
    byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    if (bytes.length > MAX_CONTENT_BYTES) {
      throw new IllegalArgumentException(
          "Workflow resource exceeds " + MAX_CONTENT_BYTES + " bytes");
    }
    if (!sha256(bytes).equals(sha256)) {
      throw new IllegalArgumentException("Workflow resource content does not match its SHA-256");
    }
  }

  public static ResolvedWorkflowResource json(URI uri, String content) {
    return of(uri, "application/json", content);
  }

  public static ResolvedWorkflowResource jsonSchema(URI uri, String content) {
    return of(uri, "application/schema+json", content);
  }

  public static ResolvedWorkflowResource of(URI uri, String mediaType, String content) {
    byte[] bytes = Objects.requireNonNull(content, "content").getBytes(StandardCharsets.UTF_8);
    return new ResolvedWorkflowResource(uri, mediaType, content, sha256(bytes));
  }

  public WorkflowResourceReference reference(WorkflowResourceKind kind) {
    return new WorkflowResourceReference(kind, uri, sha256);
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static void requireSha256(String value, String name) {
    Objects.requireNonNull(value, name);
    if (!value.matches("[0-9a-f]{64}")) {
      throw new IllegalArgumentException(name + " must be lowercase SHA-256");
    }
  }

  static String sha256(byte[] value) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }
}
