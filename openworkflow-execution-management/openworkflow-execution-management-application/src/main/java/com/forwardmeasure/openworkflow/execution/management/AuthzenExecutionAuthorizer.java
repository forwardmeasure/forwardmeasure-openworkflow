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
package com.forwardmeasure.openworkflow.execution.management;

import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.AuthorizationAction;
import com.forwardmeasure.openworkflow.authorization.AuthorizationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationResource;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.engine.api.TenantActorContext;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/** AuthZEN-backed execution authorization using trusted request identity only. */
public final class AuthzenExecutionAuthorizer implements ExecutionAuthorizer {
  private final AuthorizationService authorization;
  private final Function<TenantActorContext, ActiveOrganization> organizations;

  public AuthzenExecutionAuthorizer(
      AuthorizationService authorization,
      Function<TenantActorContext, ActiveOrganization> organizations) {
    this.authorization = Objects.requireNonNull(authorization, "authorization");
    this.organizations = Objects.requireNonNull(organizations, "organizations");
  }

  @Override
  public void authorize(
      TenantActorContext context, Action action, String resourceId, String correlationId) {
    authorization.requireAuthorized(
        new AuthorizationRequest(
            organizations.apply(context),
            AuthorizationResource.execution(resourceId),
            switch (action) {
              case START -> AuthorizationAction.EXECUTION_START;
              case PAUSE -> AuthorizationAction.EXECUTION_PAUSE;
              case RESUME -> AuthorizationAction.EXECUTION_RESUME;
              case CANCEL -> AuthorizationAction.EXECUTION_CANCEL;
            },
            correlationId,
            Map.of("active_organization_id", context.organizationId())));
  }
}
