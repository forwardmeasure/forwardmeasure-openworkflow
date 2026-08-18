package com.forwardmeasure.openworkflow.operation.agent;

import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Tenant-aware admission boundary for commands used by the MCP stdio transport. */
@FunctionalInterface
public interface McpStdioCommandPolicy {
  void authorize(TenantId tenantId, String command);

  static McpStdioCommandPolicy rejecting() {
    return (tenant, command) -> {
      throw new SecurityException("MCP stdio command policy is not configured");
    };
  }

  static McpStdioCommandPolicy allowlisted(Set<String> commands) {
    Objects.requireNonNull(commands, "commands");
    Set<String> allowed =
        commands.stream()
            .map(command -> requireText(command, "command"))
            .collect(java.util.stream.Collectors.toUnmodifiableSet());
    return (tenant, command) -> {
      Objects.requireNonNull(tenant, "tenant");
      if (!allowed.contains(command))
        throw new SecurityException(
            "MCP stdio command is not allowed for tenant " + tenant.value());
    };
  }

  static McpStdioCommandPolicy tenantAllowlisted(Map<TenantId, Set<String>> commands) {
    Objects.requireNonNull(commands, "commands");
    Map<TenantId, Set<String>> allowed =
        commands.entrySet().stream()
            .collect(
                java.util.stream.Collectors.toUnmodifiableMap(
                    entry -> Objects.requireNonNull(entry.getKey(), "tenant"),
                    entry ->
                        entry.getValue().stream()
                            .map(command -> requireText(command, "command"))
                            .collect(java.util.stream.Collectors.toUnmodifiableSet())));
    return (tenant, command) -> {
      Set<String> tenantCommands = allowed.get(Objects.requireNonNull(tenant, "tenant"));
      if (tenantCommands == null || !tenantCommands.contains(command)) {
        throw new SecurityException(
            "MCP stdio command is not allowed for tenant " + tenant.value());
      }
    };
  }

  /** Parses {@code tenant=command|command;tenant2=command}; blank denies all. */
  static McpStdioCommandPolicy configured(String configured) {
    if (configured == null || configured.isBlank()) return tenantAllowlisted(Map.of());
    Map<TenantId, Set<String>> result = new LinkedHashMap<>();
    for (String tenantEntry : configured.split(";")) {
      String[] pair = tenantEntry.strip().split("=", 2);
      if (pair.length != 2 || pair[0].isBlank())
        throw new IllegalArgumentException("Invalid tenant MCP stdio command allowlist");
      Set<String> tenantCommands = new LinkedHashSet<>();
      for (String command : pair[1].split("\\|")) {
        if (!command.isBlank()) tenantCommands.add(command.strip());
      }
      if (tenantCommands.isEmpty())
        throw new IllegalArgumentException("Tenant MCP stdio command allowlist cannot be empty");
      result.put(new TenantId(pair[0].strip()), Set.copyOf(tenantCommands));
    }
    return tenantAllowlisted(result);
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
    return value;
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
