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
package com.forwardmeasure.openworkflow.adapter.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.adapter.api.OperationAdapter;
import com.forwardmeasure.openworkflow.adapter.api.OperationDataReferenceFactory;
import com.forwardmeasure.openworkflow.adapter.api.OperationProgressSink;
import com.forwardmeasure.openworkflow.adapter.api.OperationRequest;
import com.forwardmeasure.openworkflow.engine.api.ActorIdentity;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.TenantId;
import com.forwardmeasure.openworkflow.operation.ProtocolOperationExecutor;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservation;
import com.forwardmeasure.openworkflow.workflow.runtime.api.OperationObservationStatus;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowError;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** Adapts the common durable protocol transport contract to the Kafka operation outbox. */
public final class ProtocolOperationAdapter implements OperationAdapter, AutoCloseable {
  private static final String TENANT_PREFIX = "did:forwardmeasure:tenant:";

  private final ObjectMapper json;
  private final OperationDataReferenceFactory dataReferences;
  private final ProtocolOperationExecutor executor;
  private final Map<String, Active> active = new ConcurrentHashMap<>();

  public ProtocolOperationAdapter(
      ObjectMapper json,
      OperationDataReferenceFactory dataReferences,
      ProtocolOperationExecutor executor) {
    this.json = Objects.requireNonNull(json, "json");
    this.dataReferences = Objects.requireNonNull(dataReferences, "dataReferences");
    this.executor = Objects.requireNonNull(executor, "executor");
  }

  @Override
  public boolean supports(OperationRequest request) {
    return request.descriptor().has("protocolOperation");
  }

  @Override
  public CompletionStage<OperationObservation> execute(
      OperationRequest request, OperationProgressSink progress) {
    Objects.requireNonNull(progress, "progress");
    ProtocolOperationDescriptor descriptor = descriptor(request);
    Active operation = new Active(request);
    Active previous = active.putIfAbsent(request.operationId(), operation);
    if (previous != null) return previous.terminal;
    executor
        .execute(
            executionId(request),
            descriptor,
            (observationId, value, failed, terminal, observedAt) -> {
              if (operation.cancelled.get()) {
                return CompletableFuture.completedFuture(
                    com.forwardmeasure.openworkflow.actor.ProtocolTransport.ObservationDisposition
                        .STOP);
              }
              if (!terminal) {
                progress.publish(
                    new OperationObservation(
                        OperationObservationStatus.PROGRESS,
                        null,
                        null,
                        dataReferences.captureProgress(request, value)));
                return CompletableFuture.completedFuture(
                    com.forwardmeasure.openworkflow.actor.ProtocolTransport.ObservationDisposition
                        .CONTINUE);
              }
              operation.terminal.complete(terminal(request, value, failed));
              return CompletableFuture.completedFuture(
                  com.forwardmeasure.openworkflow.actor.ProtocolTransport.ObservationDisposition
                      .STOP);
            })
        .whenComplete(
            (done, failure) -> {
              if (failure != null) operation.terminal.completeExceptionally(failure);
              else if (!operation.terminal.isDone())
                operation.terminal.completeExceptionally(
                    new IllegalStateException(
                        "Protocol transport ended without a terminal observation"));
            });
    operation.terminal.whenComplete(
        (ignored, failure) -> active.remove(request.operationId(), operation));
    return operation.terminal;
  }

  @Override
  public CompletionStage<Void> cancel(OperationRequest request) {
    Active operation = active.get(request.operationId());
    if (operation != null && operation.cancelled.compareAndSet(false, true)) {
      operation.terminal.complete(cancelled(request));
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void close() {
    active.values().forEach(operation -> operation.terminal.complete(cancelled(operation.request)));
    active.clear();
  }

  private ProtocolOperationDescriptor descriptor(OperationRequest request) {
    try {
      ProtocolOperationDescriptor durable =
          json.treeToValue(
              request.descriptor().required("protocolOperation"),
              ProtocolOperationDescriptor.class);
      var actor = request.requestedBy();
      String organization = actor.organizationId();
      String correlation = actor.correlationId() == null ? null : actor.correlationId().value();
      ActorIdentity identity =
          organization == null || correlation == null
              ? new ActorIdentity(tenant(actor.tenantId().toString()), actor.actorId().toString())
              : new ActorIdentity(
                  tenant(actor.tenantId().toString()),
                  actor.actorId().toString(),
                  organization,
                  actor.roles(),
                  correlation);
      return durable.requestedBy(identity);
    } catch (com.fasterxml.jackson.core.JsonProcessingException failure) {
      throw new IllegalArgumentException("Invalid durable protocol operation", failure);
    }
  }

  private static ExecutionId executionId(OperationRequest request) {
    String value = request.executionKey().executionId().value();
    UUID execution;
    try {
      execution = UUID.fromString(value);
    } catch (IllegalArgumentException nonUuid) {
      execution = UUID.nameUUIDFromBytes(value.getBytes(StandardCharsets.UTF_8));
    }
    return new ExecutionId(tenant(request.requestedBy().tenantId().toString()), execution);
  }

  private OperationObservation terminal(
      OperationRequest request, com.fasterxml.jackson.databind.JsonNode value, boolean failed) {
    if (!failed) {
      return new OperationObservation(
          OperationObservationStatus.SUCCEEDED, dataReferences.capture(request, value), null, null);
    }
    return new OperationObservation(
        OperationObservationStatus.FAILED,
        null,
        new WorkflowError(
            "urn:forwardmeasure:openworkflow:protocol-operation-failed",
            502,
            request.operationId(),
            "Protocol operation failed",
            value.toString()),
        null);
  }

  private static OperationObservation cancelled(OperationRequest request) {
    return new OperationObservation(
        OperationObservationStatus.CANCELLED,
        null,
        new WorkflowError(
            "urn:forwardmeasure:openworkflow:protocol-operation-cancelled",
            499,
            request.operationId(),
            "Protocol operation cancelled",
            "The workflow cancelled the in-flight protocol operation"),
        null);
  }

  private static TenantId tenant(String did) {
    if (!did.startsWith(TENANT_PREFIX)) {
      throw new SecurityException("Operation tenant is not a ForwardMeasure tenant DID");
    }
    return new TenantId(UUID.fromString(did.substring(TENANT_PREFIX.length())));
  }

  private static final class Active {
    private final OperationRequest request;
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final CompletableFuture<OperationObservation> terminal = new CompletableFuture<>();

    private Active(OperationRequest request) {
      this.request = request;
    }
  }
}
