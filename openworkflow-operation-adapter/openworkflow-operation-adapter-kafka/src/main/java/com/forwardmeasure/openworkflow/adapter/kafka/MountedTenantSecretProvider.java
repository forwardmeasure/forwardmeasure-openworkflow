/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.adapter.kafka;

import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/** Reads Kubernetes-style mounted secrets from {@code root/tenant-uuid/secret-name}. */
public final class MountedTenantSecretProvider implements TenantSecretProvider {
  private final Path root;

  public MountedTenantSecretProvider(Path root) {
    this.root = Objects.requireNonNull(root, "root").toAbsolutePath().normalize();
  }

  @Override
  public char[] resolve(TenantId tenantId, String secretName) {
    String tenant = Objects.requireNonNull(tenantId, "tenantId").value().toString();
    if (!tenant.matches("[A-Za-z0-9._:%-]+")
        || secretName == null
        || !secretName.matches("[A-Za-z0-9._-]+")) {
      throw new SecurityException("Invalid tenant or secret path component");
    }
    Path target = root.resolve(tenant).resolve(secretName).normalize();
    if (!target.startsWith(root)) throw new SecurityException("Secret path escaped its root");
    try {
      return Files.readString(target, StandardCharsets.UTF_8).stripTrailing().toCharArray();
    } catch (Exception failure) {
      throw new IllegalStateException("Tenant secret is unavailable: " + secretName, failure);
    }
  }
}
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
