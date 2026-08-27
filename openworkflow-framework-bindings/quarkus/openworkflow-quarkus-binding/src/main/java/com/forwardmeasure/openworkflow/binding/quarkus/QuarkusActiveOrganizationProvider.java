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
package com.forwardmeasure.openworkflow.binding.quarkus;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.authorization.KeycloakOrganizationClaims;
import jakarta.enterprise.context.RequestScoped;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.jwt.JsonWebToken;

/** Reads only the nested active-Organization claims from Quarkus's verified JWT. */
@RequestScoped
public class QuarkusActiveOrganizationProvider implements ActiveOrganizationProvider {
  private final JsonWebToken token;
  private final ObjectMapper mapper;
  private final String clientId;

  // Deliberately NOT openworkflow.authorization.client-id - that property is
  // this service's OWN identity for its outbound AuthZEN OAuth
  // client-credentials call (OpenWorkflowQuarkusBinding), set to
  // "openworkflow". This is a different thing: which client's roles to read
  // out of the INCOMING browser-issued JWT's organization claim. Those
  // roles are mapped onto "forwardmeasure-public" (confirmed live -
  // GET .../organizations/{org}/groups/{group} shows
  // "clientRoles":{"forwardmeasure-public":[...]}), the public client
  // Studio/Dashboard actually log in through. Conflating the two here was a
  // real mistake in an earlier pass at this fix - caught before it shipped.
  public QuarkusActiveOrganizationProvider(
      JsonWebToken token,
      ObjectMapper mapper,
      @ConfigProperty(name = "openworkflow.authorization.organization-client-id")
          String organizationClientId) {
    this.token = token;
    this.mapper = mapper;
    this.clientId = organizationClientId;
  }

  @Override
  public ActiveOrganization current() {
    try {
      String[] segments = token.getRawToken().split("\\.");
      if (segments.length != 3) {
        throw new SecurityException("Verified JWT has an invalid compact representation");
      }
      byte[] payload = Base64.getUrlDecoder().decode(segments[1]);
      Map<String, Object> claims = mapper.readValue(payload, new TypeReference<>() {});
      return KeycloakOrganizationClaims.extract(claims, clientId);
    } catch (IllegalArgumentException | IOException exception) {
      throw new SecurityException("Verified JWT claims could not be decoded", exception);
    }
  }
}
