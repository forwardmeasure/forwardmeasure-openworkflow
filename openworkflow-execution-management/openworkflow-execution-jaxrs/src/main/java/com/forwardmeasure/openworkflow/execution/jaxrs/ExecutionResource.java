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
package com.forwardmeasure.openworkflow.execution.jaxrs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.forwardmeasure.openworkflow.engine.api.EngineId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionProjection;
import com.forwardmeasure.openworkflow.execution.api.ExecutionsApi;
import com.forwardmeasure.openworkflow.execution.api.model.Execution;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionControl;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionHistoryEntry;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionPage;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionStart;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionState;
import com.forwardmeasure.openworkflow.execution.management.CanonicalExecution;
import com.forwardmeasure.openworkflow.execution.management.ExecutionAdmissionRequest;
import com.forwardmeasure.openworkflow.execution.management.ExecutionManagementService;
import com.forwardmeasure.openworkflow.execution.query.ExecutionQueryRepository;
import com.forwardmeasure.openworkflow.execution.query.ExecutionSearch;
import jakarta.ws.rs.core.Response;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

/** Portable implementation shared unchanged by Quarkus, Spring, and Micronaut. */
public class ExecutionResource implements ExecutionsApi {
  private final ExecutionManagementService management;
  private final ExecutionQueryRepository queries;
  private final ExecutionContextProvider contexts;
  private final ObjectMapper objectMapper;
  private final Supplier<UUID> commandIds;

  public ExecutionResource(
      ExecutionManagementService management,
      ExecutionQueryRepository queries,
      ExecutionContextProvider contexts,
      ObjectMapper objectMapper,
      Supplier<UUID> commandIds) {
    this.management = Objects.requireNonNull(management, "management");
    this.queries = Objects.requireNonNull(queries, "queries");
    this.contexts = Objects.requireNonNull(contexts, "contexts");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    this.commandIds = Objects.requireNonNull(commandIds, "commandIds");
  }

  @Override
  public Response startExecution(
      String idempotencyKey, String correlationId, ExecutionStart request) {
    var context = contexts.current();
    CanonicalExecution execution =
        management
            .start(
                new ExecutionAdmissionRequest(
                    context,
                    request.getRevisionId(),
                    commandIds.get(),
                    idempotencyKey,
                    correlationId,
                    objectMapper.valueToTree(request.getInput())))
            .toCompletableFuture()
            .join();
    return Response.accepted(map(execution)).build();
  }

  @Override
  public Response controlExecution(
      UUID executionId,
      String operation,
      String correlationId,
      Long expectedVersion,
      ExecutionControl control) {
    var context = contexts.current();
    var id = new ExecutionId(context.tenantId(), executionId);
    CanonicalExecution execution =
        switch (operation) {
          case "pause" ->
              management
                  .pause(context, id, commandIds.get(), expectedVersion, correlationId)
                  .toCompletableFuture()
                  .join();
          case "resume" ->
              management
                  .resume(context, id, commandIds.get(), expectedVersion, correlationId)
                  .toCompletableFuture()
                  .join();
          case "cancel" ->
              management
                  .cancel(
                      context,
                      id,
                      commandIds.get(),
                      expectedVersion,
                      correlationId,
                      control == null || control.getReason() == null
                          ? "cancelled by actor"
                          : control.getReason())
                  .toCompletableFuture()
                  .join();
          default -> throw new IllegalArgumentException("unsupported execution operation");
        };
    return Response.accepted(map(execution)).build();
  }

  @Override
  public Response getExecution(UUID executionId) {
    var context = contexts.current();
    var id = new ExecutionId(context.tenantId(), executionId);
    return queries
        .find(context.tenantId(), id)
        .<Response>map(projection -> Response.ok(map(projection)).build())
        .orElseGet(() -> Response.status(Response.Status.NOT_FOUND).build());
  }

  @Override
  public Response getExecutionHistory(UUID executionId, Long afterSequence, Integer limit) {
    var context = contexts.current();
    var id = new ExecutionId(context.tenantId(), executionId);
    List<ExecutionHistoryEntry> history =
        queries.history(context.tenantId(), id, afterSequence, limit).stream()
            .map(ExecutionResource::map)
            .toList();
    return Response.ok(history).build();
  }

  @Override
  public Response listExecutions(
      List<ExecutionState> states,
      String engineId,
      String correlationId,
      Date createdFrom,
      Date createdUntil,
      String cursor,
      Integer limit) {
    var context = contexts.current();
    var result =
        queries.search(
            new ExecutionSearch(
                context.tenantId(),
                (states == null ? List.<ExecutionState>of() : states)
                    .stream()
                        .map(
                            state ->
                                com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState
                                    .valueOf(state.name()))
                        .collect(java.util.stream.Collectors.toUnmodifiableSet()),
                engineId == null ? null : new EngineId(engineId),
                correlationId,
                createdFrom == null ? null : createdFrom.toInstant(),
                createdUntil == null ? null : createdUntil.toInstant(),
                cursor,
                limit));
    return Response.ok(
            new ExecutionPage(result.items().stream().map(ExecutionResource::map).toList())
                .nextCursor(result.nextCursor()))
        .build();
  }

  private static Execution map(CanonicalExecution execution) {
    return new Execution(
        execution.executionId().value(),
        execution.definition().revisionId(),
        execution.definition().definitionSha256(),
        execution.engineId().value(),
        ExecutionState.fromValue(execution.state().name()),
        execution.version(),
        execution.correlationId(),
        execution.input(),
        Date.from(execution.createdAt()),
        Date.from(execution.updatedAt()));
  }

  private static Execution map(ExecutionProjection projection) {
    return new Execution(
            projection.executionId().value(),
            projection.definition().revisionId(),
            projection.definition().definitionSha256(),
            projection.engineId().value(),
            ExecutionState.fromValue(projection.state().name()),
            projection.version(),
            projection.correlationId(),
            projection.input(),
            Date.from(projection.createdAt()),
            Date.from(projection.updatedAt()))
        .output(projection.output())
        .error(projection.error())
        .effects(List.copyOf(projection.effects()))
        .timers(List.copyOf(projection.timers()))
        .completedAt(projection.completedAt() == null ? null : Date.from(projection.completedAt()));
  }

  private static ExecutionHistoryEntry map(
      com.forwardmeasure.openworkflow.engine.api.ExecutionHistoryEntry entry) {
    return new ExecutionHistoryEntry(
            entry.eventId(),
            entry.sequence(),
            ExecutionState.fromValue(entry.state().name()),
            entry.type(),
            Date.from(entry.occurredAt()),
            entry.data())
        .taskPath(entry.taskPath());
  }
}
