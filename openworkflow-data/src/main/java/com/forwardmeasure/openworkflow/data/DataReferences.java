package com.forwardmeasure.openworkflow.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/** Creates immutable, content-addressed portable data references. */
public final class DataReferences {
  public static final int MAX_INLINE_BYTES = 32 * 1024;
  private static final ObjectMapper CANONICAL =
      new ObjectMapper().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

  private DataReferences() {}

  public static DataReference inline(JsonNode value) {
    Objects.requireNonNull(value, "value");
    final byte[] bytes;
    try {
      bytes = CANONICAL.writeValueAsBytes(value);
    } catch (JsonProcessingException failure) {
      throw new IllegalArgumentException("Runtime data is not serializable JSON", failure);
    }
    if (bytes.length > MAX_INLINE_BYTES) {
      throw new RuntimeDataLimitException(bytes.length, MAX_INLINE_BYTES);
    }
    return new DataReference(
        DataReference.Storage.INLINE,
        value.deepCopy(),
        null,
        "application/json",
        bytes.length,
        sha256(bytes));
  }

  private static String sha256(byte[] value) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }
}
