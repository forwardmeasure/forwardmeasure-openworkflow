package com.forwardmeasure.openworkflow.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.time.Duration;
import org.junit.jupiter.api.Test;

final class OperationTimeoutsTest {
  @Test
  void resolvesIsoAndObjectDurationsUnderTheOperatorMaximum() {
    assertEquals(
        Duration.ofSeconds(3),
        OperationTimeouts.configuredOrMaximum(
            JsonNodeFactory.instance.textNode("PT3S"), Duration.ofSeconds(30)));
    var object = JsonNodeFactory.instance.objectNode().put("seconds", 2).put("milliseconds", 250);
    assertEquals(
        Duration.ofMillis(2250),
        OperationTimeouts.configuredOrMaximum(object, Duration.ofSeconds(30)));
    assertEquals(
        Duration.ofSeconds(30),
        OperationTimeouts.configuredOrMaximum(
            JsonNodeFactory.instance.textNode("PT2M"), Duration.ofSeconds(30)));
    assertEquals(
        Duration.ofSeconds(30),
        OperationTimeouts.configuredOrMaximum(
            JsonNodeFactory.instance.textNode("P1M"), Duration.ofSeconds(30)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            OperationTimeouts.configuredOrMaximum(
                JsonNodeFactory.instance.objectNode(), Duration.ofSeconds(30)));
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
