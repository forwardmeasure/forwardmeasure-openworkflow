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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.authorization.authzen.AuthzenAuthorizationFactory;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.StudioAuthorizationResource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.net.URI;
import java.time.Duration;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** Quarkus CDI composition for the portable definition-management resource. */
@ApplicationScoped
public class OpenWorkflowQuarkusBinding {
  @Produces
  @ApplicationScoped
  AuthorizationService authorizationService(
      ObjectMapper mapper,
      @ConfigProperty(name = "openworkflow.authorization.issuer") URI issuer,
      @ConfigProperty(name = "openworkflow.authorization.client-id") String clientId,
      @ConfigProperty(name = "openworkflow.authorization.client-secret") String clientSecret,
      @ConfigProperty(name = "openworkflow.authorization.request-timeout") Duration requestTimeout,
      @ConfigProperty(name = "openworkflow.authorization.decision-ttl") Duration decisionTtl,
      @ConfigProperty(name = "openworkflow.authorization.maximum-cache-entries") int cacheEntries,
      @ConfigProperty(name = "openworkflow.authorization.policy-version") String policyVersion) {
    return AuthzenAuthorizationFactory.create(
        mapper,
        issuer,
        clientId,
        clientSecret,
        requestTimeout,
        decisionTtl,
        cacheEntries,
        policyVersion);
  }

  @Produces
  @ApplicationScoped
  OpenWorkflowCompiler openWorkflowCompiler() {
    return new OpenWorkflowCompiler();
  }

  @Produces
  @ApplicationScoped
  StudioAuthorizationResource studioAuthorizationResource(
      AuthorizationService authorization, ActiveOrganizationProvider organizations) {
    return new StudioAuthorizationResource(authorization, organizations);
  }
}
