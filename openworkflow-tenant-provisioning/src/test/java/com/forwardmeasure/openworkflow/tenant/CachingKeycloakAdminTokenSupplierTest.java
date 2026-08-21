/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.forwardmeasure.openworkflow.tenant;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CachingKeycloakAdminTokenSupplierTest {
  @Test
  void reusesTheCachedTokenUntilItIsCloseToExpiry() {
    MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
    AtomicInteger fetchCount = new AtomicInteger();
    CachingKeycloakAdminTokenSupplier supplier =
        new CachingKeycloakAdminTokenSupplier(
            () -> {
              fetchCount.incrementAndGet();
              return new CachingKeycloakAdminTokenSupplier.TokenResponse(
                  "token-" + fetchCount.get(), Duration.ofMinutes(5));
            },
            clock);

    assertEquals("token-1", supplier.bearerToken());
    assertEquals("token-1", supplier.bearerToken());
    clock.advance(Duration.ofMinutes(4));
    assertEquals("token-1", supplier.bearerToken());
    assertEquals(1, fetchCount.get());
  }

  @Test
  void refetchesOnceTheCachedTokenEntersTheRefreshSafetyMargin() {
    MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
    AtomicInteger fetchCount = new AtomicInteger();
    CachingKeycloakAdminTokenSupplier supplier =
        new CachingKeycloakAdminTokenSupplier(
            () -> {
              fetchCount.incrementAndGet();
              return new CachingKeycloakAdminTokenSupplier.TokenResponse(
                  "token-" + fetchCount.get(), Duration.ofMinutes(5));
            },
            clock);

    assertEquals("token-1", supplier.bearerToken());
    clock.advance(Duration.ofMinutes(4).plusSeconds(31));
    assertEquals("token-2", supplier.bearerToken());
    assertEquals(2, fetchCount.get());
  }

  @Test
  void aTokenWithNoUsableLifetimeIsFetchedOnEveryCall() {
    MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
    AtomicInteger fetchCount = new AtomicInteger();
    CachingKeycloakAdminTokenSupplier supplier =
        new CachingKeycloakAdminTokenSupplier(
            () -> {
              fetchCount.incrementAndGet();
              return new CachingKeycloakAdminTokenSupplier.TokenResponse(
                  "token-" + fetchCount.get(), Duration.ZERO);
            },
            clock);

    supplier.bearerToken();
    supplier.bearerToken();
    supplier.bearerToken();
    assertEquals(3, fetchCount.get());
  }

  private static final class MutableClock extends Clock {
    private Instant now;

    MutableClock(Instant now) {
      this.now = now;
    }

    void advance(Duration duration) {
      now = now.plus(duration);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Instant instant() {
      return now;
    }
  }
}
