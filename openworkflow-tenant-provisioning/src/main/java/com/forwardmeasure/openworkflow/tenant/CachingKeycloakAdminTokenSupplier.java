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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Caches the Keycloak admin bearer token for its lifetime instead of re-authenticating via password
 * grant on every Admin REST call. A tenant/capability-pack reconciliation loop can make many dozens
 * of Admin REST calls per run; without caching, each one triggers a fresh login against the master
 * realm's token endpoint, which is wasteful and risks tripping Keycloak's brute-force protection on
 * the admin account.
 */
final class CachingKeycloakAdminTokenSupplier implements KeycloakAdminTokenSupplier {
  private static final Duration REFRESH_SAFETY_MARGIN = Duration.ofSeconds(30);

  private final Supplier<TokenResponse> tokenFetcher;
  private final Clock clock;
  private volatile CachedToken cached;

  CachingKeycloakAdminTokenSupplier(Supplier<TokenResponse> tokenFetcher) {
    this(tokenFetcher, Clock.systemUTC());
  }

  CachingKeycloakAdminTokenSupplier(Supplier<TokenResponse> tokenFetcher, Clock clock) {
    this.tokenFetcher = Objects.requireNonNull(tokenFetcher, "tokenFetcher");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  @Override
  public String bearerToken() {
    CachedToken token = cached;
    if (token != null && clock.instant().isBefore(token.expiresAt())) {
      return token.value();
    }
    synchronized (this) {
      token = cached;
      if (token != null && clock.instant().isBefore(token.expiresAt())) {
        return token.value();
      }
      TokenResponse response = tokenFetcher.get();
      Instant expiresAt = clock.instant().plus(response.expiresIn()).minus(REFRESH_SAFETY_MARGIN);
      token = new CachedToken(response.accessToken(), expiresAt);
      cached = token;
      return token.value();
    }
  }

  /**
   * @param expiresIn how long the token is valid for, per Keycloak's {@code expires_in}. Pass
   *     {@link Duration#ZERO} (or anything not comfortably larger than the refresh safety margin)
   *     if the token endpoint didn't report a usable lifetime - the token is still returned, it
   *     just won't be cached across calls.
   */
  record TokenResponse(String accessToken, Duration expiresIn) {
    TokenResponse {
      Objects.requireNonNull(accessToken, "accessToken");
      Objects.requireNonNull(expiresIn, "expiresIn");
    }
  }

  private record CachedToken(String value, Instant expiresAt) {}
}
