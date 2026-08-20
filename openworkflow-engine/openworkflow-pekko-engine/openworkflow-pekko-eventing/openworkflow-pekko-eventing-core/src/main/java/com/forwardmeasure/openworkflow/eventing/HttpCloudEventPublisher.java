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
package com.forwardmeasure.openworkflow.eventing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.actor.CloudEventsMapper;
import com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** CloudEvents HTTP structured-content publisher using the official Java SDK format. */
public final class HttpCloudEventPublisher implements CloudEventPublisher {
  private final URI endpoint;
  private final HttpClient client;
  private final CloudEventsMapper mapper;
  private final Duration timeout;

  public HttpCloudEventPublisher(
      URI endpoint, HttpClient client, ObjectMapper json, Duration timeout) {
    this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
    this.client = Objects.requireNonNull(client, "client");
    this.mapper = new CloudEventsMapper(Objects.requireNonNull(json, "json"));
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    if (timeout.isZero() || timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must be positive");
    }
  }

  public HttpCloudEventPublisher(URI endpoint, ObjectMapper json, Duration timeout) {
    this(endpoint, HttpClient.newHttpClient(), json, timeout);
  }

  @Override
  public CompletionStage<Void> publish(String operationId, WorkflowCloudEvent event) {
    Objects.requireNonNull(operationId, "operationId");
    Objects.requireNonNull(event, "event");
    var format = EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
    if (format == null) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("CloudEvents JSON event format is unavailable"));
    }
    byte[] body = format.serialize(mapper.toSdk(event));
    HttpRequest request =
        HttpRequest.newBuilder(endpoint)
            .timeout(timeout)
            .header("Content-Type", JsonFormat.CONTENT_TYPE)
            .header("Idempotency-Key", operationId)
            .POST(HttpRequest.BodyPublishers.ofByteArray(body))
            .build();
    return client
        .sendAsync(request, HttpResponse.BodyHandlers.discarding())
        .thenCompose(
            response ->
                response.statusCode() >= 200 && response.statusCode() < 300
                    ? CompletableFuture.completedFuture(null)
                    : CompletableFuture.failedFuture(
                        new IllegalStateException(
                            "CloudEvent endpoint returned HTTP " + response.statusCode())));
  }
}
