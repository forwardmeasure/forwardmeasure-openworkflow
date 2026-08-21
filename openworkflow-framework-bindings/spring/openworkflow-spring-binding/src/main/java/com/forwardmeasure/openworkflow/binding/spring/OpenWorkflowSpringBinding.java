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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.authorization.authzen.AuthzenAuthorizationFactory;
import java.net.URI;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.oauth2.server.resource.web.authentication.BearerTokenAuthenticationFilter;
import org.springframework.security.web.SecurityFilterChain;

/** Spring composition for the portable definition-management resource. */
@Configuration(proxyBeanMethods = false)
public class OpenWorkflowSpringBinding {
  @Bean
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

  @Bean
  SecurityFilterChain openWorkflowSecurity(
      HttpSecurity http, SpringTenantScopeFilter tenantScopeFilter) throws Exception {
    return http.csrf(csrf -> csrf.disable())
        .authorizeHttpRequests(requests -> requests.anyRequest().authenticated())
        .oauth2ResourceServer(resourceServer -> resourceServer.jwt(Customizer.withDefaults()))
        .addFilterAfter(tenantScopeFilter, BearerTokenAuthenticationFilter.class)
        .build();
  }

  @Bean
  FilterRegistrationBean<SpringTenantScopeFilter> tenantScopeFilterRegistration(
      SpringTenantScopeFilter filter) {
    var registration = new FilterRegistrationBean<>(filter);
    registration.setEnabled(false);
    return registration;
  }
}
