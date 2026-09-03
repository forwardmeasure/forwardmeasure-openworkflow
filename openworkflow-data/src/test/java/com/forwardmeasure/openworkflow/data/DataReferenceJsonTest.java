/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.net.URI;
import org.junit.jupiter.api.Test;

class DataReferenceJsonTest {

  @Test
  void roundTripsInlineAndArtifactReferencesExactly() {
    DataReference inline =
        DataReferences.inline(
            JsonNodeFactory.instance.objectNode().put("documentId", "evidence-1"));
    DataReference artifact =
        new DataReference(
            DataReference.Storage.ARTIFACT,
            null,
            URI.create("urn:forwardmeasure:openworkflow:data:artifact-1"),
            "application/json",
            40960,
            "a".repeat(64));

    assertEquals(inline, DataReferenceJson.decode(DataReferenceJson.encode(inline)));
    assertEquals(artifact, DataReferenceJson.decode(DataReferenceJson.encode(artifact)));
  }

  @Test
  void rejectsIncompleteOrUnknownReferenceProjections() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DataReferenceJson.decode(
                JsonNodeFactory.instance.objectNode().put("storage", "DATABASE")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DataReferenceJson.decode(
                JsonNodeFactory.instance
                    .objectNode()
                    .put("storage", "ARTIFACT")
                    .put("mediaType", "application/json")
                    .put("sizeBytes", 1)
                    .put("sha256", "a".repeat(64))));
  }

  @Test
  void artifactReferenceRoundTripsThroughTheDurableJacksonShape() throws Exception {
    DataReference artifact =
        new DataReference(
            DataReference.Storage.ARTIFACT,
            null,
            URI.create("urn:forwardmeasure:openworkflow:data:artifact-2"),
            "application/json",
            40960,
            "c".repeat(64));
    ObjectMapper json = new ObjectMapper();

    assertEquals(artifact, json.readValue(json.writeValueAsBytes(artifact), DataReference.class));
  }
}
