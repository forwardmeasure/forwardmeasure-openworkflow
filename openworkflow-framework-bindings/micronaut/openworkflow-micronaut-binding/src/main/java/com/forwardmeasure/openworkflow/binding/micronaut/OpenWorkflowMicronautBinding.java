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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.authorization.authzen.AuthzenAuthorizationFactory;
import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.net.URI;
import java.time.Duration;

/** Micronaut composition for the portable definition-management resource. */
@Factory
public class OpenWorkflowMicronautBinding {
  @Singleton
  ObjectMapper objectMapper() {
    return new ObjectMapper();
  }

  @Singleton
  AuthorizationService authorizationService(
      ObjectMapper mapper,
      @Value("${openworkflow.authorization.issuer}") URI issuer,
      @Value("${openworkflow.authorization.client-id}") String clientId,
      @Value("${openworkflow.authorization.client-secret}") String clientSecret,
      @Value("${openworkflow.authorization.request-timeout}") Duration requestTimeout,
      @Value("${openworkflow.authorization.decision-ttl}") Duration decisionTtl,
      @Value("${openworkflow.authorization.maximum-cache-entries}") int cacheEntries,
      @Value("${openworkflow.authorization.policy-version}") String policyVersion) {
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

  @Singleton
  OpenWorkflowCompiler openWorkflowCompiler() {
    return new OpenWorkflowCompiler();
  }
}
