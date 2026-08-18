package com.forwardmeasure.openworkflow.operation.runner;

import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Host-neutral parser for deny-by-default, tenant-qualified runner policy. */
public final class RunPolicyConfiguration {
  private RunPolicyConfiguration() {}

  public static RunExecutionPolicy policy(String commands, String interpreters, String images) {
    return policy(commands, interpreters, images, "", "");
  }

  public static RunExecutionPolicy policy(
      String commands, String interpreters, String images, String volumes, String ports) {
    return RunExecutionPolicy.configured(
        sets(commands, "command"),
        interpreters(interpreters),
        sets(images, "image"),
        sets(volumes, "volume"),
        sets(ports, "port"));
  }

  /** Parses {@code tenant=value|value;tenant2=value}; blank denies all. */
  static Map<TenantId, Set<String>> sets(String configured, String kind) {
    Map<TenantId, Set<String>> result = new LinkedHashMap<>();
    if (configured == null || configured.isBlank()) return Map.of();
    for (String tenantEntry : configured.split(";")) {
      String[] pair = pair(tenantEntry, kind);
      Set<String> values = new LinkedHashSet<>();
      for (String value : pair[1].split("\\|")) {
        if (!value.isBlank()) values.add(value.strip());
      }
      if (values.isEmpty())
        throw new IllegalArgumentException("Tenant run " + kind + " allowlist cannot be empty");
      result.put(new TenantId(pair[0]), Set.copyOf(values));
    }
    return Map.copyOf(result);
  }

  /** Parses {@code tenant=language:executable|language:executable}. */
  static Map<TenantId, Map<String, String>> interpreters(String configured) {
    Map<TenantId, Map<String, String>> result = new LinkedHashMap<>();
    if (configured == null || configured.isBlank()) return Map.of();
    for (String tenantEntry : configured.split(";")) {
      String[] tenant = pair(tenantEntry, "interpreter");
      Map<String, String> languages = new LinkedHashMap<>();
      for (String entry : tenant[1].split("\\|")) {
        String[] mapping = entry.strip().split(":", 2);
        if (mapping.length != 2 || mapping[0].isBlank() || mapping[1].isBlank())
          throw new IllegalArgumentException("Invalid tenant run interpreter mapping");
        languages.put(mapping[0].strip().toLowerCase(java.util.Locale.ROOT), mapping[1].strip());
      }
      if (languages.isEmpty())
        throw new IllegalArgumentException("Tenant run interpreter allowlist cannot be empty");
      result.put(new TenantId(tenant[0]), Map.copyOf(languages));
    }
    return Map.copyOf(result);
  }

  private static String[] pair(String entry, String kind) {
    String[] pair = entry.strip().split("=", 2);
    if (pair.length != 2 || pair[0].isBlank())
      throw new IllegalArgumentException("Invalid tenant run " + kind + " allowlist");
    pair[0] = pair[0].strip();
    return pair;
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
