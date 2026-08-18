/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.adapter.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.adapter.api.OperationDataReferenceFactory;
import com.forwardmeasure.openworkflow.adapter.http.HttpCallAdapter;
import com.forwardmeasure.openworkflow.adapter.http.HttpEndpointPolicy;
import com.forwardmeasure.openworkflow.adapter.openapi.OpenApiCallAdapter;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor;
import com.forwardmeasure.openworkflow.workflow.runtime.api.ActorId;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.CommonClientConfigs;

/** Host-neutral lifecycle for committed Kafka effects and the production HTTP/OpenAPI adapters. */
public final class KafkaOperationAdapterRuntime implements AutoCloseable {
  private static final String TENANT_PREFIX = "did:forwardmeasure:tenant:";

  private final KafkaOperationAdapterDispatcher dispatcher;
  private final List<AutoCloseable> adapters;
  private final AtomicBoolean started = new AtomicBoolean();

  public KafkaOperationAdapterRuntime(
      String bootstrapServers,
      String effectsTopic,
      String definitionsTopic,
      String commandsTopic,
      String deadLettersTopic,
      String consumerGroup,
      String instanceId,
      AuthorizationService authorization,
      ObjectMapper json,
      String secretDirectory,
      String egressAllowlist) {
    this(
        bootstrapServers,
        effectsTopic,
        definitionsTopic,
        commandsTopic,
        deadLettersTopic,
        consumerGroup,
        instanceId,
        authorization,
        json,
        secretDirectory,
        egressAllowlist,
        null);
  }

  public KafkaOperationAdapterRuntime(
      String bootstrapServers,
      String effectsTopic,
      String definitionsTopic,
      String commandsTopic,
      String deadLettersTopic,
      String consumerGroup,
      String instanceId,
      AuthorizationService authorization,
      ObjectMapper json,
      String secretDirectory,
      String egressAllowlist,
      ProtocolOperationExecutor protocolExecutor) {
    Objects.requireNonNull(json, "json");
    OperationDataReferenceFactory dataReferences = OperationDataReferenceFactory.boundedInline();
    TenantSecretProvider mountedSecrets =
        secretDirectory == null || secretDirectory.isBlank()
            ? (tenant, name) -> {
              throw new IllegalStateException("No tenant secret directory is configured");
            }
            : new MountedTenantSecretProvider(Path.of(secretDirectory));
    var security =
        new AuthzenOperationSecurityResolver(
            authorization, new MountedOperationCredentialResolver(mountedSecrets, dataReferences));
    Map<TenantId, java.util.Set<String>> allowedHosts = parseAllowlist(egressAllowlist);
    HttpEndpointPolicy endpoints =
        (request, method, endpoint) -> {
          TenantId tenant = tenant(request.requestedBy().tenantId().toString());
          if (!java.util.Set.of("http", "https").contains(endpoint.getScheme())
              || endpoint.getHost() == null
              || endpoint.getUserInfo() != null
              || endpoint.getFragment() != null
              || !allowedHosts
                  .getOrDefault(tenant, java.util.Set.of())
                  .contains(endpoint.getHost().toLowerCase(java.util.Locale.ROOT))) {
            throw new SecurityException("HTTP endpoint is not permitted for tenant " + tenant);
          }
        };
    var http =
        new HttpCallAdapter(
            HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(Duration.ofSeconds(30))
                .build(),
            json,
            dataReferences,
            endpoints);
    var openApi = new OpenApiCallAdapter(http, dataReferences);
    ProtocolOperationAdapter protocol =
        protocolExecutor == null
            ? null
            : new ProtocolOperationAdapter(json, dataReferences, protocolExecutor);
    this.adapters = protocol == null ? List.of(openApi, http) : List.of(openApi, http, protocol);
    Properties kafka = new Properties();
    kafka.put(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
        requireText(bootstrapServers, "bootstrapServers"));
    this.dispatcher =
        new KafkaOperationAdapterDispatcher(
            kafka,
            requireText(effectsTopic, "effectsTopic"),
            requireText(definitionsTopic, "definitionsTopic"),
            requireText(commandsTopic, "commandsTopic"),
            requireText(deadLettersTopic, "deadLettersTopic"),
            requireText(consumerGroup, "consumerGroup"),
            requireText(instanceId, "instanceId"),
            List.copyOf(
                this.adapters.stream()
                    .map(com.forwardmeasure.openworkflow.adapter.api.OperationAdapter.class::cast)
                    .toList()),
            security,
            ActorId.parse("did:forwardmeasure:actor:operation-adapter"),
            "openworkflow-operation-adapter");
  }

  public void start() {
    if (!started.compareAndSet(false, true)) return;
    try {
      dispatcher.start();
    } catch (RuntimeException | Error failure) {
      started.set(false);
      dispatcher.close();
      throw failure;
    }
  }

  public boolean ready() {
    return started.get() && dispatcher.running();
  }

  public Throwable failure() {
    return dispatcher.failure();
  }

  @Override
  public void close() {
    if (!started.getAndSet(false)) return;
    dispatcher.close();
    for (AutoCloseable adapter : adapters) {
      try {
        adapter.close();
      } catch (Exception failure) {
        // Adapters are idempotently closed; continue so every transport releases resources.
      }
    }
  }

  private static TenantId tenant(String did) {
    if (!did.startsWith(TENANT_PREFIX)) {
      throw new SecurityException("Operation tenant is not a ForwardMeasure tenant DID");
    }
    return new TenantId(UUID.fromString(did.substring(TENANT_PREFIX.length())));
  }

  private static Map<TenantId, java.util.Set<String>> parseAllowlist(String configured) {
    if (configured == null || configured.isBlank()) return Map.of();
    Map<TenantId, java.util.Set<String>> result = new LinkedHashMap<>();
    for (String entry : configured.split(";")) {
      String[] pair = entry.strip().split("=", 2);
      if (pair.length != 2 || pair[0].isBlank()) {
        throw new IllegalArgumentException("Invalid tenant HTTP egress allowlist");
      }
      java.util.Set<String> hosts = new java.util.LinkedHashSet<>();
      for (String host : pair[1].split("\\|")) {
        if (!host.isBlank()) hosts.add(host.strip().toLowerCase(java.util.Locale.ROOT));
      }
      if (hosts.isEmpty()) {
        throw new IllegalArgumentException("Tenant HTTP egress allowlist cannot be empty");
      }
      result.put(new TenantId(UUID.fromString(pair[0].strip())), java.util.Set.copyOf(hosts));
    }
    return Map.copyOf(result);
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " is required");
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
