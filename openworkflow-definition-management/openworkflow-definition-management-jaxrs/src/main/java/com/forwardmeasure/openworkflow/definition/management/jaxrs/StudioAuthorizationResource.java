/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information.
 */
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

import com.forwardmeasure.openworkflow.authorization.AuthorizationAction;
import com.forwardmeasure.openworkflow.authorization.AuthorizationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationResource;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * AuthZEN batch facade used only for Studio affordances; command authorization remains
 * authoritative.
 */
@Path("/api/v1/authorizations")
public final class StudioAuthorizationResource {
  private final AuthorizationService authorization;
  private final ActiveOrganizationProvider organizations;

  public StudioAuthorizationResource(
      AuthorizationService authorization, ActiveOrganizationProvider organizations) {
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.organizations = Objects.requireNonNull(organizations, "organizations");
  }

  @POST
  public Response evaluate(
      @HeaderParam("X-Correlation-ID") String correlationId, BatchRequest request) {
    var active = organizations.current();
    var decisions =
        authorization.evaluateBatch(
            request.actions().stream()
                .map(
                    scope ->
                        new AuthorizationRequest(
                            active,
                            new AuthorizationResource(
                                request.resourceType(), request.resourceId(), request.properties()),
                            action(scope),
                            correlationId,
                            Map.of("studio", true)))
                .toList());
    return Response.ok(
            new BatchResponse(
                java.util.stream.IntStream.range(0, request.actions().size())
                    .boxed()
                    .collect(
                        java.util.stream.Collectors.toUnmodifiableMap(
                            index -> request.actions().get(index),
                            index -> decisions.get(index).permitted()))))
        .build();
  }

  private static AuthorizationAction action(String scope) {
    return java.util.Arrays.stream(AuthorizationAction.values())
        .filter(candidate -> candidate.scope().equals(scope))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Unsupported authorization action"));
  }

  public record BatchRequest(
      String resourceType,
      String resourceId,
      Map<String, Object> properties,
      List<String> actions) {
    public BatchRequest {
      properties = Map.copyOf(properties == null ? Map.of() : properties);
      actions = List.copyOf(actions);
    }
  }

  public record BatchResponse(Map<String, Boolean> decisions) {}
}
