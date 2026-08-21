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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.forwardmeasure.jpa.tenancy.TenantId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TenantOrganizationReconcilerTest {
  private static final CapabilityPack TEST_PACK =
      new CapabilityPack("test-pack", "1", Set.of("test-role-a", "test-role-b"), Set.of());

  @Test
  void reconciliationIsIdempotentAndNeverAssignsMemberRoles() {
    FakeAdmin admin = new FakeAdmin();
    TenantOrganizationReconciler reconciler = new TenantOrganizationReconciler(admin);
    TenantProvisioningRequest request =
        new TenantProvisioningRequest(new TenantId(UUID.randomUUID()), "Acme", "acme");

    OrganizationState first = reconciler.reconcileCapabilityPack(request, TEST_PACK);
    OrganizationState second = reconciler.reconcileCapabilityPack(request, TEST_PACK);

    assertEquals(first, second);
    assertEquals(TEST_PACK.roles(), admin.roles);
    assertEquals(TEST_PACK.roles(), admin.groups.get(first.id()));
    assertEquals(
        "did:forwardmeasure:tenant:" + request.tenantId(),
        first.attributes().get(TenantOrganizationReconciler.TENANT_DID_ATTRIBUTE));
    assertEquals(1, admin.createCount);
    assertEquals(0, admin.memberRoleAssignmentCount);
  }

  @Test
  void aliasCannotBeReboundToAnotherTenant() {
    FakeAdmin admin = new FakeAdmin();
    TenantOrganizationReconciler reconciler = new TenantOrganizationReconciler(admin);
    reconciler.reconcileCapabilityPack(
        new TenantProvisioningRequest(new TenantId(UUID.randomUUID()), "Acme", "acme"), TEST_PACK);
    assertThrows(
        IllegalStateException.class,
        () ->
            reconciler.reconcileCapabilityPack(
                new TenantProvisioningRequest(new TenantId(UUID.randomUUID()), "Other", "acme"),
                TEST_PACK));
  }

  private static final class FakeAdmin implements KeycloakOrganizationAdmin {
    private final Set<String> roles = new HashSet<>();
    private final Map<String, OrganizationState> organizations = new HashMap<>();
    private final Map<String, Set<String>> groups = new HashMap<>();
    private int createCount;
    private int memberRoleAssignmentCount;

    @Override
    public Set<String> sharedClientRoles() {
      return Set.copyOf(roles);
    }

    @Override
    public void createSharedClientRole(String role) {
      roles.add(role);
    }

    @Override
    public Optional<OrganizationState> organizationByAlias(String alias) {
      return Optional.ofNullable(organizations.get(alias));
    }

    @Override
    public OrganizationState createOrganization(
        String name, String alias, Map<String, String> attributes) {
      createCount++;
      OrganizationState state =
          new OrganizationState("org-" + createCount, name, alias, true, attributes);
      organizations.put(alias, state);
      return state;
    }

    @Override
    public void updateOrganization(OrganizationState organization) {
      organizations.put(organization.alias(), organization);
    }

    @Override
    public Set<String> organizationRoleGroups(String organizationId) {
      return Set.copyOf(groups.getOrDefault(organizationId, Set.of()));
    }

    @Override
    public void createOrganizationRoleGroup(String organizationId, String role) {
      groups.computeIfAbsent(organizationId, ignored -> new HashSet<>()).add(role);
    }
  }
}
