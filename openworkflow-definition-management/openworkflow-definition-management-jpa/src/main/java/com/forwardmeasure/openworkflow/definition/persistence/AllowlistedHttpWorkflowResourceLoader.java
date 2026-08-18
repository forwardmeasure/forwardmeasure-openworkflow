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
package com.forwardmeasure.openworkflow.definition.persistence;

import com.forwardmeasure.openworkflow.definition.ResolvedWorkflowResource;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceLoader;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceRequest;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Publication-only HTTP loader with exact-host egress policy, bounded bodies and no redirects. */
public final class AllowlistedHttpWorkflowResourceLoader implements WorkflowResourceLoader {
  public static final String ALLOWED_HOSTS_ENV = "OPENWORKFLOW_DEFINITION_RESOURCE_ALLOWED_HOSTS";
  private static final Duration TIMEOUT = Duration.ofSeconds(20);

  private final Set<String> allowedHosts;
  private final HttpClient client;

  public AllowlistedHttpWorkflowResourceLoader(Set<String> allowedHosts) {
    this.allowedHosts =
        Objects.requireNonNull(allowedHosts, "allowedHosts").stream()
            .map(host -> host.toLowerCase(Locale.ROOT))
            .collect(Collectors.toUnmodifiableSet());
    this.client =
        HttpClient.newBuilder()
            .connectTimeout(TIMEOUT)
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
  }

  public static AllowlistedHttpWorkflowResourceLoader fromEnvironment() {
    String configured = System.getenv().getOrDefault(ALLOWED_HOSTS_ENV, "");
    Set<String> hosts =
        Arrays.stream(configured.split(","))
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .collect(Collectors.toUnmodifiableSet());
    return new AllowlistedHttpWorkflowResourceLoader(hosts);
  }

  @Override
  public ResolvedWorkflowResource load(WorkflowResourceRequest request) {
    URI uri = Objects.requireNonNull(request, "request").uri();
    String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
    String host = uri.getHost() == null ? "" : uri.getHost().toLowerCase(Locale.ROOT);
    if (!(scheme.equals("http") || scheme.equals("https")) || !allowedHosts.contains(host)) {
      throw new IllegalArgumentException("Workflow resource endpoint is not allowlisted: " + uri);
    }
    HttpRequest httpRequest =
        HttpRequest.newBuilder(uri).timeout(TIMEOUT).header("Accept", "*/*").GET().build();
    try {
      HttpResponse<java.io.InputStream> response =
          client.send(httpRequest, HttpResponse.BodyHandlers.ofInputStream());
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        response.body().close();
        throw new IllegalArgumentException(
            "Workflow resource endpoint returned HTTP " + response.statusCode() + ": " + uri);
      }
      byte[] bytes;
      try (var body = response.body()) {
        bytes = body.readNBytes(ResolvedWorkflowResource.MAX_CONTENT_BYTES + 1);
      }
      if (bytes.length > ResolvedWorkflowResource.MAX_CONTENT_BYTES) {
        throw new IllegalArgumentException("Workflow resource exceeds the publication size limit");
      }
      String mediaType =
          response.headers().firstValue("Content-Type").orElse("application/octet-stream");
      int separator = mediaType.indexOf(';');
      if (separator >= 0) mediaType = mediaType.substring(0, separator).trim();
      return ResolvedWorkflowResource.of(uri, mediaType, new String(bytes, StandardCharsets.UTF_8));
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalArgumentException(
          "Workflow resource retrieval was interrupted", interrupted);
    } catch (IOException failure) {
      throw new IllegalArgumentException("Workflow resource retrieval failed: " + uri, failure);
    }
  }
}
