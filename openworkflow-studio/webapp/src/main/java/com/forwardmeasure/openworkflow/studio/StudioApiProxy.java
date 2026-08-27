package com.forwardmeasure.openworkflow.studio;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Framework-neutral, bounded same-origin bridge from Studio's /api path to the runtime. */
public final class StudioApiProxy {
  // Every header the real API contract declares (common-definitions.yaml's If-Match/
  // X-Correlation-ID, execution-management.openapi.yaml's Idempotency-Key) must be
  // listed here - Studio's frontend already sends each one correctly; an allowlist
  // that falls behind the contract silently drops it before the backend ever sees it.
  private static final Set<String> REQUEST_HEADERS =
      Set.of(
          "authorization",
          "content-type",
          "accept",
          "x-request-id",
          "x-correlation-id",
          "if-match",
          "idempotency-key");
  private static final Set<String> RESPONSE_HEADERS =
      Set.of(
          "content-type", "cache-control", "etag", "location", "x-request-id", "x-correlation-id");

  private final URI defaultUpstream;
  private final URI executionUpstream;
  private final HttpClient client;

  /**
   * Two upstreams, not one: definition-management and execution-management are separate deployables
   * with disjoint API surfaces (only execution-management serves {@code v1/executions/*}), so a
   * single upstream can only ever reach one of them - every execution call silently 404'd against
   * the other service until this split existed.
   */
  public StudioApiProxy(String defaultUpstream, String executionUpstream) {
    this.defaultUpstream = validateUpstream(defaultUpstream);
    this.executionUpstream = validateUpstream(executionUpstream);
    this.client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  }

  private static URI validateUpstream(String upstream) {
    URI parsed = URI.create(upstream.endsWith("/") ? upstream : upstream + "/");
    if (!Set.of("http", "https").contains(parsed.getScheme())) {
      throw new IllegalArgumentException("Studio API upstream must use http or https");
    }
    return parsed;
  }

  public ProxyResponse forward(
      String method, String pathAndQuery, Map<String, List<String>> headers, byte[] body)
      throws IOException, InterruptedException {
    String relative = pathAndQuery.startsWith("/") ? pathAndQuery.substring(1) : pathAndQuery;
    URI upstream = relative.startsWith("v1/executions") ? executionUpstream : defaultUpstream;
    var target = upstream.resolve(relative);
    var request =
        HttpRequest.newBuilder(target)
            .timeout(Duration.ofSeconds(30))
            .method(
                method,
                body.length == 0
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofByteArray(body));
    headers.forEach(
        (name, values) -> {
          if (REQUEST_HEADERS.contains(name.toLowerCase()))
            values.forEach(value -> request.header(name, value));
        });
    var response = client.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
    var responseHeaders =
        response.headers().map().entrySet().stream()
            .filter(entry -> RESPONSE_HEADERS.contains(entry.getKey().toLowerCase()))
            .collect(
                java.util.stream.Collectors.toUnmodifiableMap(
                    Map.Entry::getKey, Map.Entry::getValue));
    return new ProxyResponse(response.statusCode(), responseHeaders, response.body());
  }

  public record ProxyResponse(int status, Map<String, List<String>> headers, byte[] body) {}
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
