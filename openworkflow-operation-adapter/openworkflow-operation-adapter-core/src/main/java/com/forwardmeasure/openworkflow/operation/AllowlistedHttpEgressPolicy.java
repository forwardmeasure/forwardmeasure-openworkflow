package com.forwardmeasure.openworkflow.operation;

import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Exact-host tenant allowlist; an empty/missing tenant policy denies all egress. */
public final class AllowlistedHttpEgressPolicy implements HttpEgressPolicy {
  private final Map<TenantId, Set<String>> hosts;

  public AllowlistedHttpEgressPolicy(Map<TenantId, Set<String>> hosts) {
    this.hosts =
        Objects.requireNonNull(hosts, "hosts").entrySet().stream()
            .collect(
                java.util.stream.Collectors.toUnmodifiableMap(
                    Map.Entry::getKey,
                    entry ->
                        entry.getValue().stream()
                            .map(value -> value.toLowerCase(Locale.ROOT))
                            .collect(java.util.stream.Collectors.toUnmodifiableSet())));
  }

  @Override
  public void authorize(TenantId tenantId, URI destination) {
    String host = destination.getHost();
    if (host == null
        || !hosts.getOrDefault(tenantId, Set.of()).contains(host.toLowerCase(Locale.ROOT))) {
      throw new SecurityException(
          "HTTP egress is not allowed for tenant " + tenantId.value() + " to " + destination);
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
