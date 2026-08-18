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
package com.forwardmeasure.openworkflow.authorization.authzen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;

/** Creates the fail-closed Keycloak AuthZEN adapter from one YAML-bindable runtime setting set. */
public final class AuthzenAuthorizationFactory {
  private AuthzenAuthorizationFactory() {}

  public static AuthorizationService create(
      ObjectMapper mapper,
      URI issuer,
      String clientId,
      String clientSecret,
      Duration requestTimeout,
      Duration decisionTtl,
      int maximumCacheEntries,
      String policyVersion) {
    HttpClient client = HttpClient.newBuilder().connectTimeout(requestTimeout).build();
    String base = issuer.toString().replaceAll("/+$", "");
    var tokens =
        new OAuthClientCredentialsTokenSupplier(
            client,
            mapper,
            URI.create(base + "/protocol/openid-connect/token"),
            clientId,
            clientSecret,
            requestTimeout);
    return new AuthzenAuthorizationService(
        client,
        mapper,
        tokens,
        new AuthzenConfiguration(
            URI.create(base + "/authzen/access/v1/evaluation"),
            URI.create(base + "/authzen/access/v1/evaluations"),
            requestTimeout,
            decisionTtl,
            maximumCacheEntries,
            policyVersion));
  }
}
