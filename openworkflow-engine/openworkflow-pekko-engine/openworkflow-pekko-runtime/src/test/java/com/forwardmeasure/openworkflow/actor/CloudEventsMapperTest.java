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
package com.forwardmeasure.openworkflow.actor;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent;
import java.net.URI;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class CloudEventsMapperTest {
  private final ObjectMapper json = new ObjectMapper();
  private final CloudEventsMapper mapper = new CloudEventsMapper(json);

  @Test
  void roundTripsTheDurableEnvelopeThroughTheOfficialSdk() {
    var data = JsonNodeFactory.instance.objectNode().put("caseId", "case-7").put("value", 42);
    var original =
        new WorkflowCloudEvent(
            "1.0",
            "event-7",
            URI.create("https://events.forwardmeasure.com/workflows"),
            "com.forwardmeasure.workflow.result.v1",
            "case-7",
            Instant.parse("2026-08-15T12:00:00Z"),
            "application/json",
            data,
            Map.of(
                "tenant", JsonNodeFactory.instance.textNode("tenant-7"),
                "dataschema",
                    JsonNodeFactory.instance.textNode(
                        "https://schemas.forwardmeasure.com/result-v1.json"),
                "attempt", JsonNodeFactory.instance.numberNode(2L),
                "accepted", JsonNodeFactory.instance.booleanNode(true)));

    assertEquals(original, mapper.fromSdk(mapper.toSdk(original)));
  }

  @Test
  void preservesNonJsonPayloadBytes() throws Exception {
    byte[] bytes = new byte[] {0, 1, 2, 3, (byte) 255};
    var original =
        new WorkflowCloudEvent(
            "1.0",
            "binary-1",
            URI.create("urn:forwardmeasure:test"),
            "com.forwardmeasure.binary.v1",
            null,
            null,
            "application/octet-stream",
            JsonNodeFactory.instance.binaryNode(bytes),
            Map.of());

    var roundTripped = mapper.fromSdk(mapper.toSdk(original));
    assertEquals(original, roundTripped);
    assertArrayEquals(bytes, roundTripped.data().binaryValue());
  }
}
