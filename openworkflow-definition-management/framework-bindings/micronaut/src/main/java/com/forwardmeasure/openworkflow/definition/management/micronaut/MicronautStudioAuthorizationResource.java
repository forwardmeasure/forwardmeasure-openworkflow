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
package com.forwardmeasure.openworkflow.definition.management.micronaut;

import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.definition.management.jaxrs.StudioAuthorizationResource;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Micronaut compile-time discovery edge for the framework-neutral resource. HTTP metadata is
 * inherited from the generated {@code AuthorizationApi} interface via {@code
 * micronaut-jaxrs-server}. No repository/EntityManager use, so unlike the other three resources
 * this needs no capability-specific service bean - just the already-central {@code
 * AuthorizationService}/{@code ActiveOrganizationProvider}.
 */
@Singleton
public final class MicronautStudioAuthorizationResource extends StudioAuthorizationResource {

  @Inject
  public MicronautStudioAuthorizationResource(
      AuthorizationService authorization, ActiveOrganizationProvider organizations) {
    super(authorization, organizations);
  }
}
