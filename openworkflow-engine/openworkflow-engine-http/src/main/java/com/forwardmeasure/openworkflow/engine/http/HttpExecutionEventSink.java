/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.engine.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEventSink;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Sends engine facts to execution management's canonical projection ingress. */
public final class HttpExecutionEventSink implements ExecutionEventSink {
  private final URI endpoint;
  private final HttpClient client;
  private final ObjectMapper mapper;
  private final Duration timeout;

  public HttpExecutionEventSink(
      URI endpoint, HttpClient client, ObjectMapper mapper, Duration timeout) {
    this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
    this.client = Objects.requireNonNull(client, "client");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
  }

  @Override
  public CompletionStage<Void> project(ExecutionEvent event) {
    return send(event, false);
  }

  @Override
  public CompletionStage<Void> projectNext(ExecutionEvent event) {
    return send(event, true);
  }

  private CompletionStage<Void> send(ExecutionEvent event, boolean next) {
    try {
      HttpRequest request =
          HttpRequest.newBuilder(endpoint.resolve("events?next=" + next))
              .timeout(timeout)
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(event)))
              .build();
      return client
          .sendAsync(request, HttpResponse.BodyHandlers.discarding())
          .thenCompose(
              response ->
                  response.statusCode() / 100 == 2
                      ? CompletableFuture.completedFuture(null)
                      : CompletableFuture.failedFuture(
                          new IllegalStateException(
                              "execution event ingress returned HTTP " + response.statusCode())));
    } catch (java.io.IOException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }
}
