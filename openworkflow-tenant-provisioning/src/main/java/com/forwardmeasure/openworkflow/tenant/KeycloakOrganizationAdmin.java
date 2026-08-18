/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.forwardmeasure.openworkflow.tenant;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Minimal administration port; implementations must target one configured realm and shared client.
 */
public interface KeycloakOrganizationAdmin {
  Set<String> sharedClientRoles();

  void createSharedClientRole(String role);

  Optional<OrganizationState> organizationByAlias(String alias);

  OrganizationState createOrganization(String name, String alias, Map<String, String> attributes);

  void updateOrganization(OrganizationState organization);

  Set<String> organizationRoleGroups(String organizationId);

  void createOrganizationRoleGroup(String organizationId, String role);
}
