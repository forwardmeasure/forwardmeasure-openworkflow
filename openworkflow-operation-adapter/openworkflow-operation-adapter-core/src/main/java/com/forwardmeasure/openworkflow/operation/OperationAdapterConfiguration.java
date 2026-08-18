package com.forwardmeasure.openworkflow.operation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Shared host-neutral construction of the secured HTTP operation edge. */
public final class OperationAdapterConfiguration {
  private OperationAdapterConfiguration() {}

  public static HttpOperationExecutor executor(
      ObjectMapper json, long timeoutMillis, String egressAllowlist, String secretDirectory) {
    SecretProvider secrets = secretProvider(secretDirectory);
    return new JdkHttpOperationExecutor(
        json, Duration.ofMillis(timeoutMillis), secrets, egressPolicy(egressAllowlist));
  }

  public static SecretProvider secretProvider(String secretDirectory) {
    return secretDirectory == null || secretDirectory.isBlank()
        ? SecretProvider.rejecting()
        : new DirectorySecretProvider(Path.of(secretDirectory));
  }

  public static HttpEgressPolicy egressPolicy(String configured) {
    return new AllowlistedHttpEgressPolicy(parseAllowlist(configured));
  }

  /** Parses {@code tenant=host|host;tenant2=host}; absence means deny all. */
  public static Map<TenantId, Set<String>> parseAllowlist(String configured) {
    var result = new LinkedHashMap<TenantId, Set<String>>();
    if (configured == null || configured.isBlank()) return Map.of();
    for (String tenantEntry : configured.split(";")) {
      String[] pair = tenantEntry.strip().split("=", 2);
      if (pair.length != 2 || pair[0].isBlank()) {
        throw new IllegalArgumentException("Invalid tenant HTTP egress allowlist");
      }
      var hosts = new LinkedHashSet<String>();
      for (String host : pair[1].split("\\|")) {
        if (!host.isBlank()) hosts.add(host.strip());
      }
      if (hosts.isEmpty())
        throw new IllegalArgumentException("Tenant HTTP egress allowlist cannot be empty");
      result.put(new TenantId(pair[0].strip()), Set.copyOf(hosts));
    }
    return Map.copyOf(result);
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
