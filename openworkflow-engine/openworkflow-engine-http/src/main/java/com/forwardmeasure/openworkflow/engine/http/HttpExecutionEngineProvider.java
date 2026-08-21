/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. */
package com.forwardmeasure.openworkflow.engine.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.engine.api.CommandAcknowledgement;
import com.forwardmeasure.openworkflow.engine.api.EngineCommandException;
import com.forwardmeasure.openworkflow.engine.api.EngineHealth;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommandEnvelope;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Engine provider that dispatches to an independently scaled engine workload. */
public final class HttpExecutionEngineProvider implements ExecutionEngineProvider {
  private final EngineId engineId;
  private final URI endpoint;
  private final HttpClient client;
  private final ObjectMapper mapper;
  private final Duration timeout;

  public HttpExecutionEngineProvider(
      EngineId engineId, URI endpoint, HttpClient client, ObjectMapper mapper, Duration timeout) {
    this.engineId = Objects.requireNonNull(engineId, "engineId");
    this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
    this.client = Objects.requireNonNull(client, "client");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
  }

  @Override
  public EngineId engineId() {
    return engineId;
  }

  @Override
  public CompletionStage<CommandAcknowledgement> submit(ExecutionCommandEnvelope envelope) {
    if (!engineId.equals(envelope.selectedEngine())) {
      return CompletableFuture.failedFuture(
          new EngineCommandException(
              EngineCommandException.FailureKind.ENGINE_MISMATCH, "execution is pinned elsewhere"));
    }
    try {
      HttpRequest request =
          HttpRequest.newBuilder(endpoint.resolve("commands"))
              .timeout(timeout)
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(envelope)))
              .build();
      return client
          .sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
          .thenApply(
              response -> {
                if (response.statusCode() / 100 != 2) {
                  throw new EngineCommandException(
                      EngineCommandException.FailureKind.UNAVAILABLE,
                      "engine returned HTTP " + response.statusCode());
                }
                try {
                  return mapper.readValue(response.body(), CommandAcknowledgement.class);
                } catch (java.io.IOException failure) {
                  throw new EngineCommandException(
                      EngineCommandException.FailureKind.UNAVAILABLE,
                      "invalid engine acknowledgement: " + failure.getMessage());
                }
              });
    } catch (java.io.IOException failure) {
      return CompletableFuture.failedFuture(
          new EngineCommandException(
              EngineCommandException.FailureKind.UNAVAILABLE, failure.getMessage()));
    }
  }

  @Override
  public EngineHealth health() {
    try {
      HttpRequest request =
          HttpRequest.newBuilder(endpoint.resolve("health")).timeout(timeout).GET().build();
      HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
      if (response.statusCode() / 100 != 2) {
        throw new EngineCommandException(
            EngineCommandException.FailureKind.UNAVAILABLE, "engine health check failed");
      }
      return mapper.readValue(response.body(), EngineHealth.class);
    } catch (java.io.IOException failure) {
      throw new EngineCommandException(
          EngineCommandException.FailureKind.UNAVAILABLE, failure.getMessage());
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new EngineCommandException(
          EngineCommandException.FailureKind.UNAVAILABLE, "engine health check interrupted");
    }
  }
}
