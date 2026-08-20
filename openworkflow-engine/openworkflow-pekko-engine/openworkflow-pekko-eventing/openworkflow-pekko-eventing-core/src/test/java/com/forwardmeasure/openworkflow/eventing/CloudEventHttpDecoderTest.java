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
package com.forwardmeasure.openworkflow.eventing;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class CloudEventHttpDecoderTest {
  private final CloudEventHttpDecoder decoder = new CloudEventHttpDecoder(new ObjectMapper());

  @Test
  void decodesStructuredCloudEvent() {
    byte[] body =
        """
        {"specversion":"1.0","id":"structured-1","source":"urn:test",
         "type":"example.structured.v1","subject":"case-7",
         "datacontenttype":"application/json","tenant":"tenant-7",
         "data":{"accepted":true}}
        """
            .getBytes(StandardCharsets.UTF_8);

    var event = decoder.decode("application/cloudevents+json; charset=utf-8", Map.of(), body);

    assertEquals("structured-1", event.id());
    assertEquals("case-7", event.subject());
    assertEquals(true, event.data().required("accepted").booleanValue());
    assertEquals("tenant-7", event.extensions().get("tenant").textValue());
  }

  @Test
  void decodesBinaryCloudEventWithoutChangingPayloadBytes() throws Exception {
    byte[] body = new byte[] {0, 1, 2, 3, (byte) 255};
    var event =
        decoder.decode(
            "application/octet-stream",
            Map.of(
                "CE-SpecVersion", List.of("1.0"),
                "ce-id", List.of("binary-1"),
                "ce-source", List.of("urn:test:binary"),
                "ce-type", List.of("example.binary.v1"),
                "ce-time", List.of("2026-08-15T12:00:00Z"),
                "ce-tenant", List.of("tenant-7")),
            body);

    assertEquals("binary-1", event.id());
    assertEquals(Instant.parse("2026-08-15T12:00:00Z"), event.time());
    assertEquals("tenant-7", event.extensions().get("tenant").textValue());
    assertArrayEquals(body, event.data().binaryValue());
  }
}
