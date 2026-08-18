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
package com.forwardmeasure.openworkflow.definition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class Iso8601DurationTest {

  @Test
  void appliesMonthsAndYearsAsCalendarAmounts() {
    Instant januaryEnd = Instant.parse("2024-01-31T12:00:00Z");

    assertEquals(Instant.parse("2024-02-29T12:00:00Z"), Iso8601Duration.addTo(januaryEnd, "P1M"));
    assertEquals(Instant.parse("2025-01-31T12:00:00Z"), Iso8601Duration.addTo(januaryEnd, "P1Y"));
  }

  @Test
  void supportsEveryLiteralComponentAndFractions() {
    Instant anchor = Instant.parse("2026-01-01T00:00:00Z");

    assertEquals(
        Instant.parse("2026-01-17T14:30:00.500Z"),
        Iso8601Duration.addTo(anchor, "P2W2DT14H30M0.5S"));
    assertEquals(Duration.ofMinutes(90), Iso8601Duration.between(anchor, "PT1.5H"));
  }

  @Test
  void rejectsEmptyAndMalformedDurations() {
    assertThrows(IllegalArgumentException.class, () -> Iso8601Duration.validate("P"));
    assertThrows(IllegalArgumentException.class, () -> Iso8601Duration.validate("1 hour"));
  }
}
