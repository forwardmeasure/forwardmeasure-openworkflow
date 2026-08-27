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
package com.forwardmeasure.openworkflow.binding.spring;

import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.authorization.KeycloakOrganizationClaims;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;

/** Reads only the nested active-Organization claims from Spring Security's verified JWT. */
@Component
public class SpringActiveOrganizationProvider implements ActiveOrganizationProvider {
  private final String clientId;

  // Deliberately NOT openworkflow.authorization.client-id - that property is
  // this service's OWN identity for its outbound AuthZEN OAuth
  // client-credentials call (OpenWorkflowSpringBinding), set to
  // "openworkflow". This is a different thing: which client's roles to read
  // out of the INCOMING browser-issued JWT's organization claim. Those
  // roles are mapped onto "forwardmeasure-public" (confirmed live -
  // GET .../organizations/{org}/groups/{group} shows
  // "clientRoles":{"forwardmeasure-public":[...]}), the public client
  // Studio/Dashboard actually log in through.
  public SpringActiveOrganizationProvider(
      @Value("${openworkflow.authorization.organization-client-id}") String organizationClientId) {
    this.clientId = organizationClientId;
  }

  @Override
  public ActiveOrganization current() {
    var authentication = SecurityContextHolder.getContext().getAuthentication();
    if (!(authentication instanceof JwtAuthenticationToken token)
        || !authentication.isAuthenticated()) {
      throw new SecurityException("An authenticated JWT is required");
    }
    return KeycloakOrganizationClaims.extract(token.getToken().getClaims(), clientId);
  }
}
