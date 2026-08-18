package com.forwardmeasure.openworkflow.operation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.HttpOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.pekko.Done;

/** AsyncAPI HTTP publish driver backed by the secured HTTP edge. */
public final class AsyncApiHttpOperationExecutor implements ProtocolOperationExecutor {
  private final HttpOperationExecutor http;
  private final Clock clock;
  private final ProtocolOperationExecutor subscriptions;

  public AsyncApiHttpOperationExecutor(HttpOperationExecutor http) {
    this(http, Clock.systemUTC(), null);
  }

  public AsyncApiHttpOperationExecutor(
      HttpOperationExecutor http,
      Duration subscriptionTimeout,
      HttpEgressPolicy egress,
      SecretProvider secrets) {
    this(
        http,
        Clock.systemUTC(),
        new AsyncApiSseOperationExecutor(subscriptionTimeout, egress, secrets));
  }

  AsyncApiHttpOperationExecutor(HttpOperationExecutor http, Clock clock) {
    this(http, clock, null);
  }

  AsyncApiHttpOperationExecutor(
      HttpOperationExecutor http, Clock clock, ProtocolOperationExecutor subscriptions) {
    this.http = Objects.requireNonNull(http, "http");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.subscriptions = subscriptions;
  }

  @Override
  public CompletionStage<Done> execute(
      ExecutionId executionId, ProtocolOperationDescriptor operation, ObservationSink sink) {
    if (operation.kind() != ProtocolOperationDescriptor.Kind.ASYNC_API
        || !(operation.protocol().equals("http")
            || operation.protocol().equals("https")
            || operation.protocol().equals("mercure"))) {
      return CompletableFuture.failedFuture(
          new IllegalArgumentException("AsyncAPI HTTP driver received an incompatible operation"));
    }
    if (operation.mode() == ProtocolOperationDescriptor.Mode.SUBSCRIBE) {
      return subscriptions == null
          ? CompletableFuture.failedFuture(
              new IllegalStateException("AsyncAPI HTTP subscription transport is not configured"))
          : subscriptions.execute(executionId, operation, sink);
    }
    JsonNode message = operation.request();
    var headers = new LinkedHashMap<String, String>();
    if (message.path("headers").isObject()) {
      message
          .path("headers")
          .properties()
          .forEach(
              entry ->
                  headers.put(
                      entry.getKey(),
                      entry.getValue().isValueNode()
                          ? entry.getValue().asText()
                          : entry.getValue().toString()));
    }
    JsonNode payload =
        message.has("payload") ? message.get("payload") : JsonNodeFactory.instance.nullNode();
    var request =
        new HttpOperationDescriptor(
            operation.operationId(),
            HttpOperationDescriptor.Kind.HTTP,
            "POST",
            operation.endpoint(),
            headers,
            payload,
            HttpOperationDescriptor.Output.CONTENT,
            false,
            null,
            null,
            operation.authentication(),
            operation.authenticationContext());
    return http.execute(executionId, request)
        .thenCompose(
            result ->
                sink.observe(
                    "response",
                    result.error() == null ? result.output() : result.error(),
                    result.error() != null,
                    true,
                    clock.instant()))
        .thenApply(ignored -> Done.getInstance());
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
