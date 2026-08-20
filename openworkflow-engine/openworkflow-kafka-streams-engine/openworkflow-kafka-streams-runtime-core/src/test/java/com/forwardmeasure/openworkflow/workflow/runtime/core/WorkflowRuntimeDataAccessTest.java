package com.forwardmeasure.openworkflow.workflow.runtime.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReference;
import com.forwardmeasure.openworkflow.workflow.runtime.api.DataReferences;
import com.forwardmeasure.openworkflow.workflow.runtime.api.RuntimeDataLimitException;
import java.net.URI;
import org.junit.jupiter.api.Test;

class WorkflowRuntimeDataAccessTest {

  private final WorkflowRuntimeDataAccess access = WorkflowRuntimeDataAccess.inlineOnly();

  @Test
  void resolvesInlineDataWithoutSharingMutableJsonState() {
    ObjectNode original = JsonNodeFactory.instance.objectNode().put("documentId", "evidence-123");
    DataReference reference = DataReferences.inline(original);

    ObjectNode resolved = (ObjectNode) access.resolve(reference);
    resolved.put("documentId", "changed");

    assertNotSame(reference.inlineValue(), resolved);
    assertEquals("evidence-123", reference.inlineValue().path("documentId").textValue());
  }

  @Test
  void rejectsArtifactResolutionAndPreservesTheExactCutpoint() {
    DataReference artifact =
        new DataReference(
            DataReference.Storage.ARTIFACT,
            null,
            URI.create("urn:oks:workflow-data:sha256:" + "0123456789abcdef".repeat(4)),
            "application/json",
            4194304,
            "0123456789abcdef".repeat(4));

    WorkflowDataMaterializationRequiredException failure =
        assertThrows(
            WorkflowDataMaterializationRequiredException.class, () -> access.resolve(artifact));

    assertSame(artifact, failure.reference());
  }

  @Test
  void generatedReferencesRetainTheKafkaInlineLimit() {
    String oversized = "x".repeat(DataReferences.MAX_INLINE_BYTES + 1);

    assertThrows(
        RuntimeDataLimitException.class,
        () -> access.reference(JsonNodeFactory.instance.textNode(oversized)));
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
