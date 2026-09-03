package com.forwardmeasure.openworkflow.workflow.runtime.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.data.DataReference;
import com.forwardmeasure.openworkflow.data.DataReferenceJson;
import com.forwardmeasure.openworkflow.data.DataReferences;
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
            URI.create("urn:oks:workflow-data:" + "10000000-0000-0000-0000-000000000001"),
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
            URI.create("urn:oks:workflow-data:" + "10000000-0000-0000-0000-000000000003"),
            "application/json",
            40960,
            "c".repeat(64));
    ObjectMapper json = new ObjectMapper();

    assertEquals(artifact, json.readValue(json.writeValueAsBytes(artifact), DataReference.class));
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
