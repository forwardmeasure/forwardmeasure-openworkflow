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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.definition.WorkflowCoordinates;
import com.forwardmeasure.openworkflow.engine.api.EngineEvent;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.WorkflowCloudEvent;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Exact Open Workflow 1.0.3 lifecycle CloudEvent materialization. */
public final class LifecycleCloudEventMapper {
  public static final String PREFIX = "io.serverlessworkflow.";

  public List<WorkflowCloudEvent> map(
      ExecutionId executionId, WorkflowCoordinates coordinates, EngineEvent event) {
    Objects.requireNonNull(executionId, "executionId");
    Objects.requireNonNull(coordinates, "coordinates");
    Objects.requireNonNull(event, "event");
    String workflow = qualifiedName(executionId, coordinates);
    var result = new ArrayList<WorkflowCloudEvent>();
    if (event instanceof EngineEvent.Started started) {
      ObjectNode data =
          workflowData(workflow)
              .set(
                  "definition",
                  JsonNodeFactory.instance
                      .objectNode()
                      .put("name", coordinates.name())
                      .put("namespace", coordinates.namespace())
                      .put("version", coordinates.version()));
      data.put("startedAt", started.occurredAt().toString());
      result.add(event(executionId, event, "workflow.started", workflow, data));
    } else if (event instanceof EngineEvent.Paused paused) {
      result.add(workflowAt(executionId, event, workflow, "workflow.suspended", "suspendedAt"));
      paused
          .activeTaskPaths()
          .forEach(
              path ->
                  result.add(
                      taskAt(executionId, event, workflow, path, "task.suspended", "suspendedAt")));
    } else if (event instanceof EngineEvent.Resumed resumed) {
      result.add(workflowAt(executionId, event, workflow, "workflow.resumed", "resumedAt"));
      resumed
          .activeTaskPaths()
          .forEach(
              path ->
                  result.add(
                      taskAt(executionId, event, workflow, path, "task.resumed", "resumedAt")));
    } else if (event instanceof EngineEvent.Cancelled cancelled) {
      result.add(workflowAt(executionId, event, workflow, "workflow.cancelled", "cancelledAt"));
      cancelled
          .activeTaskPaths()
          .forEach(
              path ->
                  result.add(
                      taskAt(executionId, event, workflow, path, "task.cancelled", "cancelledAt")));
    } else if (event instanceof EngineEvent.Completed completed) {
      ObjectNode data = workflowData(workflow).put("completedAt", event.occurredAt().toString());
      result.add(event(executionId, event, "workflow.completed", workflow, data));
    } else if (event instanceof EngineEvent.Failed failed) {
      ObjectNode error =
          JsonNodeFactory.instance
              .objectNode()
              .put("type", "https://open-workflow-specification.org/spec/1.0.0/errors/runtime")
              .put("title", "Workflow execution faulted")
              .put("status", 500)
              .put("detail", failed.message());
      ObjectNode data = workflowData(workflow).put("faultedAt", event.occurredAt().toString());
      data.set("error", error);
      result.add(event(executionId, event, "workflow.faulted", workflow, data));
    } else {
      if (event instanceof EngineEvent.ErrorRaised raised) {
        ObjectNode task =
            taskData(workflow, raised.taskPath()).put("faultedAt", event.occurredAt().toString());
        task.set("error", raised.error());
        result.add(event(executionId, event, "task.faulted", raised.taskPath(), task));
      }
      if (event instanceof EngineEvent.ListenStarted started) {
        result.add(
            correlationAt(
                executionId,
                event,
                workflow,
                started.taskPath(),
                "workflow.correlation-started",
                "startedAt",
                Map.of()));
      } else if (event instanceof EngineEvent.ForkBranchListenStarted started) {
        result.add(
            correlationAt(
                executionId,
                event,
                workflow,
                started.taskPath(),
                "workflow.correlation-started",
                "startedAt",
                Map.of()));
      } else if (event instanceof EngineEvent.ListenEventAccepted accepted
          && accepted.completed()) {
        result.add(
            correlationAt(
                executionId,
                event,
                workflow,
                accepted.taskPath(),
                "workflow.correlation-completed",
                "completedAt",
                accepted.correlations()));
      } else if (event instanceof EngineEvent.ForkBranchListenAccepted accepted) {
        accepted.updates().stream()
            .filter(update -> update.disposition() != EngineEvent.ForkListenDisposition.PARTIAL)
            .forEach(
                update ->
                    result.add(
                        correlationAt(
                            executionId,
                            event,
                            workflow,
                            update.taskPath(),
                            "workflow.correlation-completed",
                            "completedAt",
                            update.correlations())));
      }
      for (String entered : enteredTasks(event)) {
        result.add(taskAt(executionId, event, workflow, entered, "task.created", "createdAt"));
        result.add(taskAt(executionId, event, workflow, entered, "task.started", "startedAt"));
      }
      for (String completed : completedTasks(event)) {
        ObjectNode data =
            taskData(workflow, completed).put("completedAt", event.occurredAt().toString());
        result.add(event(executionId, event, "task.completed", completed, data));
      }
      if (event instanceof EngineEvent.RetryStarted retried) {
        result.add(
            taskAt(
                executionId, event, workflow, retried.tryTaskPath(), "task.retried", "retriedAt"));
      } else if (event instanceof EngineEvent.ForkBranchRetryStarted retried) {
        result.add(
            taskAt(
                executionId, event, workflow, retried.tryTaskPath(), "task.retried", "retriedAt"));
      }
    }
    return List.copyOf(result);
  }

  public static String qualifiedName(ExecutionId executionId, WorkflowCoordinates coordinates) {
    return coordinates.name() + "-" + executionId.value() + "." + coordinates.namespace();
  }

  private static List<String> enteredTasks(EngineEvent event) {
    return switch (event) {
      case EngineEvent.TaskEntered entered -> List.of(entered.taskPath());
      case EngineEvent.FunctionEntered entered -> List.of(entered.taskPath());
      case EngineEvent.ForEntered entered -> List.of(entered.taskPath());
      case EngineEvent.WaitScheduled entered -> List.of(entered.taskPath());
      case EngineEvent.TryEntered entered -> List.of(entered.taskPath());
      case EngineEvent.ForkEntered entered -> List.of(entered.taskPath());
      case EngineEvent.EmitRequested entered -> List.of(entered.taskPath());
      case EngineEvent.HttpCallRequested entered -> List.of(entered.taskPath());
      case EngineEvent.ProtocolCallRequested entered -> List.of(entered.taskPath());
      case EngineEvent.SubworkflowRequested entered -> List.of(entered.taskPath());
      case EngineEvent.ListenStarted entered -> List.of(entered.taskPath());
      case EngineEvent.ForkBranchTaskEntered entered -> List.of(entered.taskPath());
      case EngineEvent.ForkBranchFunctionEntered entered -> List.of(entered.taskPath());
      case EngineEvent.ForkBranchForEntered entered -> List.of(entered.taskPath());
      case EngineEvent.ForkNestedEntered entered -> List.of(entered.taskPath());
      case EngineEvent.ForkNestedTaskEntered entered -> List.of(entered.taskPath());
      case EngineEvent.ForkNestedFunctionEntered entered -> List.of(entered.taskPath());
      case EngineEvent.ForkNestedForEntered entered -> List.of(entered.taskPath());
      case EngineEvent.ForkBranchWaitScheduled entered -> List.of(entered.taskPath());
      case EngineEvent.ForkBranchTryEntered entered -> List.of(entered.taskPath());
      case EngineEvent.ForkBranchEmitRequested entered -> List.of(entered.taskPath());
      case EngineEvent.ForkBranchHttpCallRequested entered -> List.of(entered.taskPath());
      case EngineEvent.ForkBranchProtocolCallRequested entered -> List.of(entered.taskPath());
      case EngineEvent.ForkBranchListenStarted entered -> List.of(entered.taskPath());
      case EngineEvent.ForkBranchSubworkflowRequested entered -> List.of(entered.taskPath());
      default -> List.of();
    };
  }

  private static List<String> completedTasks(EngineEvent event) {
    return switch (event) {
      case EngineEvent.TaskCompleted completed -> List.of(completed.taskPath());
      case EngineEvent.EmitAcknowledged completed -> List.of(completed.taskPath());
      case EngineEvent.HttpCallCompleted completed -> List.of(completed.taskPath());
      case EngineEvent.ProtocolCallCompleted completed -> List.of(completed.taskPath());
      case EngineEvent.ProtocolCallIterationAdvanced completed ->
          completed.completed() ? List.of(completed.taskPath()) : List.of();
      case EngineEvent.SubworkflowRequested completed ->
          !completed.await() ? List.of(completed.taskPath()) : List.of();
      case EngineEvent.SubworkflowCompleted completed -> List.of(completed.taskPath());
      case EngineEvent.ListenEventAccepted completed ->
          completed.completed() ? List.of(completed.taskPath()) : List.of();
      case EngineEvent.ForkBranchTaskCompleted completed -> List.of(completed.taskPath());
      case EngineEvent.ForkNestedCompleted completed -> List.of(completed.taskPath());
      case EngineEvent.ForkNestedTaskCompleted completed -> List.of(completed.taskPath());
      case EngineEvent.ForkBranchWaitCompleted completed -> List.of(completed.taskPath());
      case EngineEvent.ForkBranchTryCompleted completed -> List.of(completed.taskPath());
      case EngineEvent.ForkBranchEmitAcknowledged completed -> List.of(completed.taskPath());
      case EngineEvent.ForkBranchHttpCallCompleted completed -> List.of(completed.taskPath());
      case EngineEvent.ForkBranchProtocolCallCompleted completed -> List.of(completed.taskPath());
      case EngineEvent.ForkBranchProtocolCallIterationAdvanced completed ->
          completed.completed() ? List.of(completed.taskPath()) : List.of();
      case EngineEvent.ForkBranchSubworkflowRequested completed ->
          !completed.await() ? List.of(completed.taskPath()) : List.of();
      case EngineEvent.ForkBranchSubworkflowCompleted completed -> List.of(completed.taskPath());
      case EngineEvent.ForkBranchListenAccepted completed ->
          completed.updates().stream()
              .filter(update -> update.disposition() != EngineEvent.ForkListenDisposition.PARTIAL)
              .map(EngineEvent.ForkListenUpdate::taskPath)
              .toList();
      default -> List.of();
    };
  }

  private static WorkflowCloudEvent workflowAt(
      ExecutionId id, EngineEvent event, String workflow, String type, String timestamp) {
    return event(
        id,
        event,
        type,
        workflow,
        workflowData(workflow).put(timestamp, event.occurredAt().toString()));
  }

  private static WorkflowCloudEvent taskAt(
      ExecutionId id,
      EngineEvent event,
      String workflow,
      String path,
      String type,
      String timestamp) {
    return event(
        id,
        event,
        type,
        path,
        taskData(workflow, path).put(timestamp, event.occurredAt().toString()));
  }

  private static WorkflowCloudEvent correlationAt(
      ExecutionId id,
      EngineEvent event,
      String workflow,
      String taskPath,
      String type,
      String timestamp,
      Map<String, JsonNode> correlations) {
    ObjectNode data = workflowData(workflow).put(timestamp, event.occurredAt().toString());
    if (correlations != null && !correlations.isEmpty()) {
      ObjectNode keys = JsonNodeFactory.instance.objectNode();
      correlations.forEach(keys::set);
      data.set("correlationKeys", keys);
    }
    return event(id, event, type, workflow, taskPath, data);
  }

  private static WorkflowCloudEvent event(
      ExecutionId executionId, EngineEvent event, String type, String subject, ObjectNode data) {
    return event(executionId, event, type, subject, subject, data);
  }

  private static WorkflowCloudEvent event(
      ExecutionId executionId,
      EngineEvent event,
      String type,
      String subject,
      String idDiscriminator,
      ObjectNode data) {
    return new WorkflowCloudEvent(
        "1.0",
        event.commandId() + ":" + type + ":" + idDiscriminator,
        URI.create("urn:openworkflow:execution:" + executionId.value()),
        PREFIX + type + ".v1",
        subject,
        event.occurredAt(),
        "application/json",
        data,
        Map.of(
            "tenant",
            JsonNodeFactory.instance.textNode(executionId.tenantId().value().toString())));
  }

  private static ObjectNode workflowData(String workflow) {
    return JsonNodeFactory.instance.objectNode().put("name", workflow);
  }

  private static ObjectNode taskData(String workflow, String path) {
    return JsonNodeFactory.instance.objectNode().put("workflow", workflow).put("task", path);
  }
}
