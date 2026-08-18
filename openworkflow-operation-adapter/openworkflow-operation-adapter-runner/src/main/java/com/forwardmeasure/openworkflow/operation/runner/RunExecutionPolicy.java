package com.forwardmeasure.openworkflow.operation.runner;

import com.forwardmeasure.openworkflow.engine.api.TenantId;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Tenant-qualified allowlists for local process and script execution. */
public interface RunExecutionPolicy {
  void authorizeCommand(TenantId tenantId, String command);

  String interpreter(TenantId tenantId, String language);

  void authorizeImage(TenantId tenantId, String image);

  void authorizeVolume(TenantId tenantId, String source, String target);

  void authorizePort(TenantId tenantId, String host, String container);

  static RunExecutionPolicy rejecting() {
    return configured(Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
  }

  static RunExecutionPolicy configured(
      Map<TenantId, Set<String>> commands,
      Map<TenantId, Map<String, String>> interpreters,
      Map<TenantId, Set<String>> images) {
    return configured(commands, interpreters, images, Map.of(), Map.of());
  }

  static RunExecutionPolicy configured(
      Map<TenantId, Set<String>> commands,
      Map<TenantId, Map<String, String>> interpreters,
      Map<TenantId, Set<String>> images,
      Map<TenantId, Set<String>> volumes,
      Map<TenantId, Set<String>> ports) {
    Map<TenantId, Set<String>> commandCopy = immutableSets(commands);
    Map<TenantId, Set<String>> imageCopy = immutableSets(images);
    Map<TenantId, Set<String>> volumeCopy = immutableSets(volumes);
    Map<TenantId, Set<String>> portCopy = immutableSets(ports);
    Map<TenantId, Map<String, String>> interpreterCopy =
        interpreters.entrySet().stream()
            .collect(
                java.util.stream.Collectors.toUnmodifiableMap(
                    entry -> Objects.requireNonNull(entry.getKey(), "tenant"),
                    entry -> Map.copyOf(entry.getValue())));
    return new RunExecutionPolicy() {
      @Override
      public void authorizeCommand(TenantId tenant, String command) {
        requireAllowed(commandCopy, tenant, command, "command");
      }

      @Override
      public String interpreter(TenantId tenant, String language) {
        String executable =
            interpreterCopy
                .getOrDefault(tenant, Map.of())
                .get(language.toLowerCase(java.util.Locale.ROOT));
        if (executable == null)
          throw new SecurityException(
              "Script language is not allowed for tenant " + tenant.value());
        return executable;
      }

      @Override
      public void authorizeImage(TenantId tenant, String image) {
        requireAllowed(imageCopy, tenant, image, "container image");
      }

      @Override
      public void authorizeVolume(TenantId tenant, String source, String target) {
        requireAllowed(volumeCopy, tenant, source + ">" + target, "container volume");
      }

      @Override
      public void authorizePort(TenantId tenant, String host, String container) {
        requireAllowed(portCopy, tenant, host + ">" + container, "container port");
      }
    };
  }

  private static Map<TenantId, Set<String>> immutableSets(Map<TenantId, Set<String>> values) {
    return values.entrySet().stream()
        .collect(
            java.util.stream.Collectors.toUnmodifiableMap(
                entry -> Objects.requireNonNull(entry.getKey(), "tenant"),
                entry -> Set.copyOf(entry.getValue())));
  }

  private static void requireAllowed(
      Map<TenantId, Set<String>> configured, TenantId tenant, String value, String kind) {
    Objects.requireNonNull(tenant, "tenant");
    Objects.requireNonNull(value, kind);
    if (!configured.getOrDefault(tenant, Set.of()).contains(value)) {
      throw new SecurityException("Run " + kind + " is not allowed for tenant " + tenant.value());
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
