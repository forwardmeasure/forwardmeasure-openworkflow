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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Open Workflow's complete ISO 8601 duration literal.
 *
 * <p>{@link Duration#parse(CharSequence)} cannot represent calendar years or months. Open Workflow
 * 1.0.3 explicitly admits both, so deadlines and recurring schedules must apply the amount to a
 * concrete UTC instant rather than silently coercing it to a fixed number of seconds.
 */
public final class Iso8601Duration {
  private static final Pattern LITERAL =
      Pattern.compile(
          "^P"
              + "(?:(\\d+(?:\\.\\d+)?)Y)?"
              + "(?:(\\d+(?:\\.\\d+)?)M)?"
              + "(?:(\\d+(?:\\.\\d+)?)W)?"
              + "(?:(\\d+(?:\\.\\d+)?)D)?"
              + "(?:T"
              + "(?:(\\d+(?:\\.\\d+)?)H)?"
              + "(?:(\\d+(?:\\.\\d+)?)M)?"
              + "(?:(\\d+(?:\\.\\d+)?)S)?"
              + ")?$");
  private static final BigDecimal SECONDS_PER_HOUR = BigDecimal.valueOf(3_600);
  private static final BigDecimal SECONDS_PER_MINUTE = BigDecimal.valueOf(60);
  private static final BigDecimal NANOS_PER_SECOND = BigDecimal.valueOf(1_000_000_000L);

  private Iso8601Duration() {}

  /** Applies a standards-valid duration to a UTC instant. */
  public static Instant addTo(Instant anchor, String literal) {
    Objects.requireNonNull(anchor, "anchor");
    Matcher matcher = matcher(literal);
    ZonedDateTime value = ZonedDateTime.ofInstant(anchor, ZoneOffset.UTC);
    value = addCalendar(value, component(matcher, 1), ChronoUnit.YEARS);
    value = addCalendar(value, component(matcher, 2), ChronoUnit.MONTHS);
    value = addCalendar(value, component(matcher, 3), ChronoUnit.WEEKS);
    value = addCalendar(value, component(matcher, 4), ChronoUnit.DAYS);
    BigDecimal seconds =
        component(matcher, 5)
            .multiply(SECONDS_PER_HOUR)
            .add(component(matcher, 6).multiply(SECONDS_PER_MINUTE))
            .add(component(matcher, 7));
    return value.toInstant().plusNanos(toNanos(seconds));
  }

  /**
   * Resolves the amount relative to an anchor. The anchor matters for years and months (for
   * example, one month after 31 January).
   */
  public static Duration between(Instant anchor, String literal) {
    return Duration.between(anchor, addTo(anchor, literal));
  }

  /** Validates the literal without selecting an execution-time anchor. */
  public static void validate(String literal) {
    matcher(literal);
  }

  private static Matcher matcher(String literal) {
    Objects.requireNonNull(literal, "literal");
    Matcher matcher = LITERAL.matcher(literal);
    if (!matcher.matches()
        || java.util.stream.IntStream.rangeClosed(1, 7)
            .allMatch(index -> matcher.group(index) == null)) {
      throw new IllegalArgumentException("Invalid ISO 8601 duration: " + literal);
    }
    return matcher;
  }

  private static ZonedDateTime addCalendar(
      ZonedDateTime current, BigDecimal amount, ChronoUnit unit) {
    BigInteger whole = amount.toBigInteger();
    BigDecimal fraction = amount.subtract(new BigDecimal(whole));
    ZonedDateTime result = current.plus(whole.longValueExact(), unit);
    if (fraction.signum() == 0) {
      return result;
    }
    Duration oneUnit = Duration.between(result.toInstant(), result.plus(1, unit).toInstant());
    BigDecimal fractionalNanos = BigDecimal.valueOf(oneUnit.toNanos()).multiply(fraction);
    return result.plusNanos(fractionalNanos.setScale(0, RoundingMode.HALF_UP).longValueExact());
  }

  private static long toNanos(BigDecimal seconds) {
    return seconds.multiply(NANOS_PER_SECOND).setScale(0, RoundingMode.HALF_UP).longValueExact();
  }

  private static BigDecimal component(Matcher matcher, int group) {
    String value = matcher.group(group);
    return value == null ? BigDecimal.ZERO : new BigDecimal(value);
  }
}
