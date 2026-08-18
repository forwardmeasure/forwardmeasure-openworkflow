package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.util.Locale;
import java.util.Objects;

/**
 * Stable JSON projection used when one durable descriptor must carry another data reference without
 * materialising its content.
 */
public final class DataReferenceJson {

  private DataReferenceJson() {}

  public static ObjectNode encode(DataReference reference) {
    Objects.requireNonNull(reference, "reference");
    ObjectNode encoded = JsonNodeFactory.instance.objectNode();
    encoded.put("storage", reference.storage().name());
    if (reference.storage() == DataReference.Storage.INLINE) {
      encoded.set("inlineValue", reference.inlineValue().deepCopy());
    } else {
      encoded.put("artifactUri", reference.artifactUri().toString());
    }
    encoded.put("mediaType", reference.mediaType());
    encoded.put("sizeBytes", reference.sizeBytes());
    encoded.put("sha256", reference.sha256());
    return encoded;
  }

  public static DataReference decode(JsonNode encoded) {
    Objects.requireNonNull(encoded, "encoded");
    if (!encoded.isObject()) {
      throw new IllegalArgumentException("Data reference JSON must be an object");
    }
    DataReference.Storage storage;
    try {
      storage =
          DataReference.Storage.valueOf(requiredText(encoded, "storage").toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException invalidStorage) {
      throw new IllegalArgumentException(
          "Data reference JSON has an invalid storage value", invalidStorage);
    }
    JsonNode inline =
        storage == DataReference.Storage.INLINE ? encoded.required("inlineValue").deepCopy() : null;
    URI artifact =
        storage == DataReference.Storage.ARTIFACT
            ? URI.create(requiredText(encoded, "artifactUri"))
            : null;
    JsonNode size = encoded.required("sizeBytes");
    if (!size.canConvertToLong() || size.longValue() < 0) {
      throw new IllegalArgumentException("Data reference JSON has an invalid sizeBytes");
    }
    return new DataReference(
        storage,
        inline,
        artifact,
        requiredText(encoded, "mediaType"),
        size.longValue(),
        requiredText(encoded, "sha256"));
  }

  private static String requiredText(JsonNode value, String field) {
    JsonNode candidate = value.required(field);
    if (!candidate.isTextual() || candidate.textValue().isBlank()) {
      throw new IllegalArgumentException("Data reference JSON requires non-blank " + field);
    }
    return candidate.textValue();
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
