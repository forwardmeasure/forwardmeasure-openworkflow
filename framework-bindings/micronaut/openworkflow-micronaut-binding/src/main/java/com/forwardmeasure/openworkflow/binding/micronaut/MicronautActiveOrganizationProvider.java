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
package com.forwardmeasure.openworkflow.binding.micronaut;

import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.KeycloakOrganizationClaims;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.ActiveOrganizationProvider;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.utils.SecurityService;
import jakarta.inject.Singleton;
import java.util.Objects;
import java.util.function.Supplier;

/** Reads only the nested active-Organization claims from Micronaut's verified JWT. */
@Singleton
public class MicronautActiveOrganizationProvider implements ActiveOrganizationProvider {
  private final SecurityService security;
  private final ThreadLocal<Authentication> scopedAuthentication = new ThreadLocal<>();

  public MicronautActiveOrganizationProvider(SecurityService security) {
    this.security = security;
  }

  @Override
  public ActiveOrganization current() {
    var authentication = scopedAuthentication.get();
    if (authentication == null) {
      authentication =
          security
              .getAuthentication()
              .orElseThrow(() -> new SecurityException("An authenticated JWT is required"));
    }
    return KeycloakOrganizationClaims.extract(
        authentication.getAttributes(), "forwardmeasure-openworkflow");
  }

  <T> T call(Authentication authentication, Supplier<T> operation) {
    Objects.requireNonNull(authentication, "authentication");
    Objects.requireNonNull(operation, "operation");
    if (scopedAuthentication.get() != null) {
      throw new IllegalStateException("Micronaut authentication scope is already active");
    }
    scopedAuthentication.set(authentication);
    try {
      return operation.get();
    } finally {
      scopedAuthentication.remove();
    }
  }
}
