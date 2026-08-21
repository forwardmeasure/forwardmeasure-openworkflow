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
package com.forwardmeasure.openworkflow.definition.management.jaxrs;

import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.authorization.AuthorizationAction;
import com.forwardmeasure.openworkflow.authorization.AuthorizationDecision;
import com.forwardmeasure.openworkflow.authorization.AuthorizationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationResource;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.definition.management.api.AuthorizationApi;
import com.forwardmeasure.openworkflow.definition.management.api.model.BatchAuthorizationRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.BatchAuthorizationResponse;
import jakarta.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * AuthZEN batch facade used only for Studio UI affordances (e.g. greying out a button); every other
 * operation in this capability independently and authoritatively re-checks authorization
 * server-side regardless of what this endpoint returns.
 */
public class StudioAuthorizationResource implements AuthorizationApi {
  private final AuthorizationService authorization;
  private final ActiveOrganizationProvider organizations;

  public StudioAuthorizationResource(
      AuthorizationService authorization, ActiveOrganizationProvider organizations) {
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.organizations = Objects.requireNonNull(organizations, "organizations");
  }

  @Override
  public Response batchEvaluateAuthorizations(
      String xCorrelationID, BatchAuthorizationRequest batchAuthorizationRequest) {
    var active = organizations.current();
    List<String> actions = batchAuthorizationRequest.getActions();
    Map<String, Object> properties =
        batchAuthorizationRequest.getProperties() == null
            ? Map.of()
            : batchAuthorizationRequest.getProperties();
    AuthorizationResource resource =
        new AuthorizationResource(
            batchAuthorizationRequest.getResourceType(),
            batchAuthorizationRequest.getResourceId(),
            properties);
    List<AuthorizationDecision> decisions =
        authorization.evaluateBatch(
            actions.stream()
                .map(
                    scope ->
                        new AuthorizationRequest(
                            active,
                            resource,
                            action(scope),
                            xCorrelationID,
                            Map.of("studio", true)))
                .toList());
    BatchAuthorizationResponse response = new BatchAuthorizationResponse();
    response.setDecisions(
        IntStream.range(0, actions.size())
            .boxed()
            .collect(
                Collectors.toUnmodifiableMap(
                    actions::get, index -> decisions.get(index).permitted())));
    return Response.ok(response).build();
  }

  private static AuthorizationAction action(String scope) {
    return Arrays.stream(AuthorizationAction.values())
        .filter(candidate -> candidate.scope().equals(scope))
        .findFirst()
        .orElseThrow(
            () -> new IllegalArgumentException("Unsupported authorization action: " + scope));
  }
}
