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
package com.forwardmeasure.openworkflow.actor;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.openworkflow.engine.api.EventConsumptionWindow;
import com.forwardmeasure.openworkflow.engine.api.ProtocolOperationDescriptor;
import com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Durable emit/listen state owned by one task frame. */
public record EventExecutionFrame(
    Kind kind,
    String operationId,
    WorkflowCloudEvent emitted,
    List<WorkflowCloudEvent> accepted,
    Map<String, JsonNode> correlations,
    java.util.Set<Integer> matchedFilters,
    EventConsumptionWindow untilWindow,
    @JsonInclude(JsonInclude.Include.NON_EMPTY) List<JsonNode> protocolItems,
    @JsonInclude(JsonInclude.Include.NON_NULL) ProtocolOperationDescriptor protocolOperation,
    @JsonInclude(JsonInclude.Include.NON_NULL) ProtocolOperationDescriptor commandOperation,
    @JsonInclude(JsonInclude.Include.NON_NULL) ProtocolOperationDescriptor eventsOperation,
    @JsonInclude(JsonInclude.Include.NON_NULL) ProtocolOperationDescriptor cancellationOperation,
    boolean commandPublished) {
  public EventExecutionFrame(
      Kind kind,
      String operationId,
      WorkflowCloudEvent emitted,
      List<WorkflowCloudEvent> accepted,
      Map<String, JsonNode> correlations,
      java.util.Set<Integer> matchedFilters) {
    this(kind, operationId, emitted, accepted, correlations, matchedFilters, null, List.of(), null);
  }

  public EventExecutionFrame(
      Kind kind,
      String operationId,
      WorkflowCloudEvent emitted,
      List<WorkflowCloudEvent> accepted,
      Map<String, JsonNode> correlations,
      java.util.Set<Integer> matchedFilters,
      EventConsumptionWindow untilWindow) {
    this(
        kind,
        operationId,
        emitted,
        accepted,
        correlations,
        matchedFilters,
        untilWindow,
        List.of(),
        null);
  }

  /** Backward-compatible canonical shape for every frame kind but {@code CORRELATED_WORKER}. */
  public EventExecutionFrame(
      Kind kind,
      String operationId,
      WorkflowCloudEvent emitted,
      List<WorkflowCloudEvent> accepted,
      Map<String, JsonNode> correlations,
      java.util.Set<Integer> matchedFilters,
      EventConsumptionWindow untilWindow,
      List<JsonNode> protocolItems,
      ProtocolOperationDescriptor protocolOperation) {
    this(
        kind,
        operationId,
        emitted,
        accepted,
        correlations,
        matchedFilters,
        untilWindow,
        protocolItems,
        protocolOperation,
        null,
        null,
        null,
        false);
  }

  public enum Kind {
    EMIT,
    LISTEN,
    SUBWORKFLOW,
    HTTP_CALL,
    PROTOCOL_CALL,
    CORRELATED_WORKER
  }

  public EventExecutionFrame {
    Objects.requireNonNull(kind, "kind");
    Objects.requireNonNull(operationId, "operationId");
    if (operationId.isBlank()) {
      throw new IllegalArgumentException("operationId must not be blank");
    }
    accepted = accepted == null ? List.of() : List.copyOf(accepted);
    correlations =
        correlations == null
            ? Map.of()
            : correlations.entrySet().stream()
                .collect(
                    java.util.stream.Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        entry -> Objects.requireNonNull(entry.getValue()).deepCopy()));
    matchedFilters =
        matchedFilters == null ? java.util.Set.of() : java.util.Set.copyOf(matchedFilters);
    protocolItems =
        protocolItems == null
            ? List.of()
            : protocolItems.stream()
                .map(value -> Objects.requireNonNull(value, "protocol item").<JsonNode>deepCopy())
                .toList();
    if (kind == Kind.EMIT && (emitted == null || !accepted.isEmpty())) {
      throw new IllegalArgumentException("Emit frame requires one outbound event");
    }
    if (kind != Kind.EMIT && emitted != null) {
      throw new IllegalArgumentException("Only an emit frame can contain an emitted event");
    }
    if ((kind == Kind.SUBWORKFLOW || kind == Kind.HTTP_CALL)
        && (!accepted.isEmpty()
            || !correlations.isEmpty()
            || !matchedFilters.isEmpty()
            || untilWindow != null
            || !protocolItems.isEmpty())) {
      throw new IllegalArgumentException(
          "Subworkflow frame cannot contain event-consumption state");
    }
    if (kind != Kind.PROTOCOL_CALL && !protocolItems.isEmpty()) {
      throw new IllegalArgumentException("Only a protocol call can contain stream items");
    }
    if ((kind == Kind.PROTOCOL_CALL) != (protocolOperation != null)) {
      // Null remains readable for histories written before descriptors
      // were retained in the durable frame.
      if (kind != Kind.PROTOCOL_CALL || protocolOperation != null) {
        throw new IllegalArgumentException("Only a protocol frame can retain a protocol operation");
      }
    }
    if ((kind == Kind.CORRELATED_WORKER) != (commandOperation != null || eventsOperation != null)) {
      throw new IllegalArgumentException(
          "Only a correlated-worker frame can retain command/events operations");
    }
    if (kind == Kind.CORRELATED_WORKER && (commandOperation == null || eventsOperation == null)) {
      throw new IllegalArgumentException(
          "A correlated-worker frame requires its command and events operations");
    }
    if (kind != Kind.CORRELATED_WORKER && (cancellationOperation != null || commandPublished)) {
      throw new IllegalArgumentException(
          "Only a correlated-worker frame can retain cancellation state");
    }
  }

  public static EventExecutionFrame emit(String operationId, WorkflowCloudEvent event) {
    return new EventExecutionFrame(
        Kind.EMIT,
        operationId,
        Objects.requireNonNull(event),
        List.of(),
        Map.of(),
        java.util.Set.of(),
        null,
        List.of(),
        null);
  }

  public static EventExecutionFrame listen(String operationId) {
    return new EventExecutionFrame(
        Kind.LISTEN, operationId, null, List.of(), Map.of(), java.util.Set.of());
  }

  public static EventExecutionFrame subworkflow(String operationId) {
    return new EventExecutionFrame(
        Kind.SUBWORKFLOW,
        operationId,
        null,
        List.of(),
        Map.of(),
        java.util.Set.of(),
        null,
        List.of(),
        null);
  }

  public static EventExecutionFrame httpCall(String operationId) {
    return new EventExecutionFrame(
        Kind.HTTP_CALL,
        operationId,
        null,
        List.of(),
        Map.of(),
        java.util.Set.of(),
        null,
        List.of(),
        null);
  }

  public static EventExecutionFrame protocolCall(String operationId) {
    return new EventExecutionFrame(
        Kind.PROTOCOL_CALL,
        operationId,
        null,
        List.of(),
        Map.of(),
        java.util.Set.of(),
        null,
        List.of(),
        null);
  }

  public static EventExecutionFrame protocolCall(ProtocolOperationDescriptor operation) {
    Objects.requireNonNull(operation, "operation");
    return new EventExecutionFrame(
        Kind.PROTOCOL_CALL,
        operation.operationId(),
        null,
        List.of(),
        Map.of(),
        java.util.Set.of(),
        null,
        List.of(),
        operation);
  }

  /**
   * A {@code correlated-worker} call's durable waiting frame, carrying its command (PUBLISH),
   * events (SUBSCRIBE), and optional cancellation (PUBLISH) operations - one lifecycle spanning up
   * to three correlated external operations instead of the single operation every other protocol
   * call owns.
   */
  public static EventExecutionFrame correlatedWorker(
      String lifecycleId,
      ProtocolOperationDescriptor commandOperation,
      ProtocolOperationDescriptor eventsOperation,
      ProtocolOperationDescriptor cancellationOperation) {
    return new EventExecutionFrame(
        Kind.CORRELATED_WORKER,
        lifecycleId,
        null,
        List.of(),
        Map.of(),
        java.util.Set.of(),
        null,
        List.of(),
        null,
        Objects.requireNonNull(commandOperation, "commandOperation"),
        Objects.requireNonNull(eventsOperation, "eventsOperation"),
        cancellationOperation,
        false);
  }

  public EventExecutionFrame withCommandPublished() {
    if (kind != Kind.CORRELATED_WORKER) {
      throw new IllegalStateException("Frame is not a correlated-worker call");
    }
    return new EventExecutionFrame(
        kind,
        operationId,
        null,
        accepted,
        correlations,
        matchedFilters,
        untilWindow,
        protocolItems,
        protocolOperation,
        commandOperation,
        eventsOperation,
        cancellationOperation,
        true);
  }

  public EventExecutionFrame acceptProtocolItem(JsonNode item) {
    if (kind != Kind.PROTOCOL_CALL) {
      throw new IllegalStateException("Frame is not a protocol call");
    }
    var items = new java.util.ArrayList<>(protocolItems);
    items.add(Objects.requireNonNull(item, "item").deepCopy());
    return new EventExecutionFrame(
        kind,
        operationId,
        null,
        accepted,
        correlations,
        matchedFilters,
        untilWindow,
        items,
        protocolOperation);
  }

  @Override
  public List<JsonNode> protocolItems() {
    return protocolItems.stream().map(value -> value.<JsonNode>deepCopy()).toList();
  }

  public EventExecutionFrame accept(
      WorkflowCloudEvent event, Map<String, JsonNode> nextCorrelations, int matchedFilter) {
    if (kind != Kind.LISTEN) throw new IllegalStateException("Frame is not listening");
    var events = new java.util.ArrayList<>(accepted);
    events.add(Objects.requireNonNull(event));
    var filters = new java.util.LinkedHashSet<>(matchedFilters);
    filters.add(matchedFilter);
    return new EventExecutionFrame(
        kind,
        operationId,
        null,
        events,
        nextCorrelations,
        filters,
        untilWindow,
        protocolItems,
        protocolOperation);
  }

  public EventExecutionFrame withUntil(EventConsumptionWindow nextUntil) {
    if (kind != Kind.LISTEN) throw new IllegalStateException("Frame is not listening");
    return new EventExecutionFrame(
        kind,
        operationId,
        null,
        accepted,
        correlations,
        matchedFilters,
        Objects.requireNonNull(nextUntil, "nextUntil"),
        protocolItems,
        protocolOperation);
  }
}
