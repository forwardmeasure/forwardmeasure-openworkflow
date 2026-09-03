package com.forwardmeasure.openworkflow.data;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.util.Objects;

/** Exact bounded inline value or immutable content-addressed artifact. */
public record DataReference(
    Storage storage,
    JsonNode inlineValue,
    URI artifactUri,
    String mediaType,
    long sizeBytes,
    String sha256) {

  public DataReference {
    Objects.requireNonNull(storage, "storage");
    if (storage == Storage.ARTIFACT && inlineValue != null && inlineValue.isNull()) {
      inlineValue = null;
    }
    Objects.requireNonNull(mediaType, "mediaType");
    Objects.requireNonNull(sha256, "sha256");
    if (sizeBytes < 0) throw new IllegalArgumentException("sizeBytes must not be negative");
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

  @Override
  public JsonNode inlineValue() {
    return inlineValue == null ? null : inlineValue.deepCopy();
  }

  public enum Storage {
    INLINE,
    ARTIFACT
  }
}
