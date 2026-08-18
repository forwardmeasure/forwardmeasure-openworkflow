package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.util.Objects;

/**
 * Exact task input or output. Values are either bounded inline JSON or immutable content-addressed
 * artifacts.
 */
public record DataReference(
    Storage storage,
    JsonNode inlineValue,
    URI artifactUri,
    String mediaType,
    long sizeBytes,
    String sha256) {

  public DataReference {
    Objects.requireNonNull(storage, "storage");
    /*
     * Jackson represents an explicit JSON null in a JsonNode-typed record
     * component as NullNode. For ARTIFACT the field is a structural
     * placeholder, not an inline JSON value, so normalize it before
     * enforcing the mutually-exclusive representation. INLINE still
     * supports JSON null as legitimate workflow data.
     */
    if (storage == Storage.ARTIFACT && inlineValue != null && inlineValue.isNull()) {
      inlineValue = null;
    }
    Objects.requireNonNull(mediaType, "mediaType");
    Objects.requireNonNull(sha256, "sha256");
    if (sizeBytes < 0) {
      throw new IllegalArgumentException("sizeBytes must not be negative");
    }
    if (storage == Storage.INLINE && inlineValue == null) {
      throw new IllegalArgumentException("Inline data requires a value");
    }
    if (storage == Storage.INLINE && artifactUri != null) {
      throw new IllegalArgumentException("Inline data cannot have an artifact URI");
    }
    if (storage == Storage.ARTIFACT && artifactUri == null) {
      throw new IllegalArgumentException("Artifact data requires an immutable URI");
    }
    if (storage == Storage.ARTIFACT && inlineValue != null) {
      throw new IllegalArgumentException("Artifact data cannot also contain inline data");
    }
  }

  public enum Storage {
    INLINE,
    ARTIFACT
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
