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
package com.forwardmeasure.openworkflow.actor;

import java.util.Objects;

/**
 * The shared, schema-less Postgres connection every tenant's persistence plugin is built from -
 * {@link TenantPersistencePlugins} overlays a tenant's own schema on top of this at plugin-config
 * construction time. {@code baseUrl} must not carry a {@code currentSchema}/schema qualifier of its
 * own; tenant isolation comes entirely from the per-tenant plugin config, not from this connection.
 */
public record PostgresConnectionSettings(String baseUrl, String username, String password) {
  public PostgresConnectionSettings {
    Objects.requireNonNull(baseUrl, "baseUrl");
    Objects.requireNonNull(username, "username");
    Objects.requireNonNull(password, "password");
  }
}
