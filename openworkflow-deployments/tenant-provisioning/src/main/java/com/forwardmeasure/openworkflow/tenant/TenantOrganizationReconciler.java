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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Idempotently reconciles shared roles and tenant Organizations without assigning member roles. */
public final class TenantOrganizationReconciler {
  public static final String TENANT_ID_ATTRIBUTE = "forwardmeasure.tenant-id";
  public static final String TENANT_DID_ATTRIBUTE = "forwardmeasure.tenant-did";
  public static final String CAPABILITY_PACK_ATTRIBUTE_PREFIX = "forwardmeasure.capability-pack.";

  private final KeycloakOrganizationAdmin admin;

  public TenantOrganizationReconciler(KeycloakOrganizationAdmin admin) {
    this.admin = Objects.requireNonNull(admin, "admin");
  }

  public OrganizationState provisionOrganization(TenantProvisioningRequest request) {
    Objects.requireNonNull(request, "request");
    Map<String, String> desiredAttributes =
        Map.of(
            TENANT_ID_ATTRIBUTE,
            request.tenantId().toString(),
            TENANT_DID_ATTRIBUTE,
            "did:forwardmeasure:tenant:" + request.tenantId());
    return admin
        .organizationByAlias(request.alias())
        .map(existing -> reconcileExisting(request, existing, desiredAttributes))
        .orElseGet(
            () ->
                admin.createOrganization(
                    request.displayName(), request.alias(), desiredAttributes));
  }

  public OrganizationState reconcileCapabilityPack(
      TenantProvisioningRequest request, CapabilityPack pack) {
    Objects.requireNonNull(pack, "pack");
    OrganizationState organization = provisionOrganization(request);
    Set<String> existingRoles = admin.sharedClientRoles();
    pack.sharedClientRoles().stream()
        .sorted()
        .filter(role -> !existingRoles.contains(role))
        .forEach(admin::createSharedClientRole);

    Set<String> existingGroups =
        admin.organizationRoleGroups(organization.id(), organization.alias());
    pack.roles().stream()
        .sorted()
        .filter(role -> !existingGroups.contains(role))
        .forEach(
            role ->
                admin.createOrganizationRoleGroup(organization.id(), organization.alias(), role));

    Map<String, String> attributes = new LinkedHashMap<>(organization.attributes());
    attributes.put(CAPABILITY_PACK_ATTRIBUTE_PREFIX + pack.id(), pack.version());
    OrganizationState desired =
        new OrganizationState(
            organization.id(),
            organization.name(),
            organization.alias(),
            organization.enabled(),
            Map.copyOf(attributes));
    if (!desired.equals(organization)) {
      admin.updateOrganization(desired);
    }
    return desired;
  }

  private OrganizationState reconcileExisting(
      TenantProvisioningRequest request,
      OrganizationState existing,
      Map<String, String> desiredAttributes) {
    String assignedTenant = existing.attributes().get(TENANT_ID_ATTRIBUTE);
    if (assignedTenant != null && !assignedTenant.equals(request.tenantId().toString())) {
      throw new IllegalStateException("Organization alias is already bound to another tenant");
    }
    Map<String, String> attributes = new LinkedHashMap<>(existing.attributes());
    attributes.putAll(desiredAttributes);
    OrganizationState desired =
        new OrganizationState(
            existing.id(), existing.name(), existing.alias(), true, Map.copyOf(attributes));
    if (!desired.equals(existing)) {
      admin.updateOrganization(desired);
    }
    return desired;
  }
}
