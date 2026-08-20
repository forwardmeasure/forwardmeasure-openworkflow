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

import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;

public record KeycloakAdminConfiguration(
    URI serverUri, String realm, String sharedClientId, Duration requestTimeout) {
  private static final Pattern SEGMENT = Pattern.compile("[A-Za-z0-9._-]+");

  public KeycloakAdminConfiguration {
    Objects.requireNonNull(serverUri, "serverUri");
    validateSegment(realm, "realm");
    validateSegment(sharedClientId, "sharedClientId");
    Objects.requireNonNull(requestTimeout, "requestTimeout");
    if (requestTimeout.isZero() || requestTimeout.isNegative()) {
      throw new IllegalArgumentException("requestTimeout must be positive");
    }
  }

  static String validateSegment(String value, String name) {
    if (value == null || !SEGMENT.matcher(value).matches()) {
      throw new IllegalArgumentException(name + " contains invalid URI path characters");
    }
    return value;
  }
}
