/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.forwardmeasure.openworkflow.humantask.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.forwardmeasure.openworkflow.data.DataReference;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Immutable task envelope and review policy supplied when a task is created. */
public record HumanTaskDefinition(
    HumanTaskId taskId,
    String taskType,
    String title,
    String description,
    int priority,
    TaskSource source,
    DataReference originalContent,
    Presentation presentation,
    ReviewPlan reviewPlan,
    Assignment initialAssignment,
    Instant dueAt,
    Instant expiresAt,
    Map<String, JsonNode> blotterFields) {

  public HumanTaskDefinition {
    Objects.requireNonNull(taskId, "taskId");
    taskType = requireText(taskType, "taskType");
    title = requireText(title, "title");
    description = description == null ? "" : description;
    if (priority < 0) {
      throw new IllegalArgumentException("priority must not be negative");
    }
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(originalContent, "originalContent");
    Objects.requireNonNull(presentation, "presentation");
    Objects.requireNonNull(reviewPlan, "reviewPlan");
    if (dueAt != null && expiresAt != null && dueAt.isAfter(expiresAt)) {
      throw new IllegalArgumentException("dueAt must not be after expiresAt");
    }
    blotterFields = blotterFields == null ? Map.of() : Map.copyOf(blotterFields);
  }

  public record TaskSource(
      SourceKind kind,
      String sourceId,
      String executionId,
      String workflowTaskPath,
      String correlationId) {
    public TaskSource {
      Objects.requireNonNull(kind, "kind");
      sourceId = requireText(sourceId, "sourceId");
      executionId = normalize(executionId);
      workflowTaskPath = normalize(workflowTaskPath);
      correlationId = normalize(correlationId);
      if (kind == SourceKind.WORKFLOW
          && (executionId == null || workflowTaskPath == null || correlationId == null)) {
        throw new IllegalArgumentException(
            "Workflow tasks require executionId, workflowTaskPath, and correlationId");
      }
    }
  }

  public enum SourceKind {
    WORKFLOW,
    API,
    AGENT,
    SYSTEM
  }

  public record Presentation(
      PresentationKind kind,
      JsonNode inputSchema,
      JsonNode uiSchema,
      String resourceUri,
      String resourceSha256) {
    public Presentation {
      Objects.requireNonNull(kind, "kind");
      inputSchema = inputSchema == null || inputSchema.isNull() ? null : inputSchema.deepCopy();
      uiSchema = uiSchema == null || uiSchema.isNull() ? null : uiSchema.deepCopy();
      resourceUri = normalize(resourceUri);
      resourceSha256 = normalize(resourceSha256);
      if ((resourceUri == null) != (resourceSha256 == null)) {
        throw new IllegalArgumentException(
            "Presentation resource URI and digest must appear together");
      }
      if (resourceSha256 != null && !resourceSha256.matches("[0-9a-f]{64}")) {
        throw new IllegalArgumentException("Presentation digest must be lowercase SHA-256");
      }
    }

    @Override
    public JsonNode inputSchema() {
      return inputSchema == null ? null : inputSchema.deepCopy();
    }

    @Override
    public JsonNode uiSchema() {
      return uiSchema == null ? null : uiSchema.deepCopy();
    }
  }

  public enum PresentationKind {
    RAW_JSON,
    JSON_SCHEMA,
    A2UI
  }

  public record ReviewPlan(List<ReviewStage> stages) {
    public ReviewPlan {
      stages = List.copyOf(Objects.requireNonNull(stages, "stages"));
      if (stages.isEmpty()) {
        throw new IllegalArgumentException("A review plan requires at least one stage");
      }
      long distinctIds = stages.stream().map(ReviewStage::stageId).distinct().count();
      if (distinctIds != stages.size()) {
        throw new IllegalArgumentException("Review stage identifiers must be unique");
      }
      for (ReviewStage stage : stages) {
        for (ReviewAction action : stage.actions()) {
          action.transition().validate(stages, stage);
        }
      }
    }

    public int indexOf(String stageId) {
      for (int index = 0; index < stages.size(); index++) {
        if (stages.get(index).stageId().equals(stageId)) {
          return index;
        }
      }
      throw new IllegalArgumentException("Unknown review stage: " + stageId);
    }
  }

  public record ReviewStage(
      String stageId,
      String name,
      Set<String> eligibleActors,
      Set<String> eligibleGroups,
      Set<String> eligibleRoles,
      List<ReviewAction> actions) {
    public ReviewStage {
      stageId = requireText(stageId, "stageId");
      name = requireText(name, "name");
      eligibleActors = immutableTextSet(eligibleActors, "eligibleActors");
      eligibleGroups = immutableTextSet(eligibleGroups, "eligibleGroups");
      eligibleRoles = immutableTextSet(eligibleRoles, "eligibleRoles");
      actions = List.copyOf(Objects.requireNonNull(actions, "actions"));
      if (actions.isEmpty()) {
        throw new IllegalArgumentException("A review stage requires at least one action");
      }
      if (actions.stream().map(ReviewAction::code).distinct().count() != actions.size()) {
        throw new IllegalArgumentException("Review action codes must be unique within a stage");
      }
    }

    public boolean eligible(Actor actor) {
      Objects.requireNonNull(actor, "actor");
      return (eligibleActors.isEmpty()
              && eligibleGroups.isEmpty()
              && eligibleRoles.isEmpty()
              && actor.kind() == ActorKind.HUMAN)
          || eligibleActors.contains(actor.actorId())
          || actor.groups().stream().anyMatch(eligibleGroups::contains)
          || actor.roles().stream().anyMatch(eligibleRoles::contains);
    }
  }

  public record ReviewAction(
      String code,
      String label,
      DispositionKind kind,
      ActionTransition transition,
      boolean commentRequired) {
    public ReviewAction {
      code = requireText(code, "code");
      label = requireText(label, "label");
      Objects.requireNonNull(kind, "kind");
      Objects.requireNonNull(transition, "transition");
    }
  }

  public enum DispositionKind {
    APPROVE,
    DECLINE,
    OTHER
  }

  public record ActionTransition(TransitionKind kind, String targetStageId) {
    public ActionTransition {
      Objects.requireNonNull(kind, "kind");
      targetStageId = normalize(targetStageId);
      if (kind.requiresTarget() != (targetStageId != null)) {
        throw new IllegalArgumentException(
            kind + " transition target requirement is not satisfied");
      }
    }

    private void validate(List<ReviewStage> stages, ReviewStage source) {
      if (targetStageId != null
          && stages.stream().noneMatch(s -> s.stageId().equals(targetStageId))) {
        throw new IllegalArgumentException("Unknown transition target stage: " + targetStageId);
      }
      if (kind == TransitionKind.ADVANCE && targetStageId.equals(source.stageId())) {
        throw new IllegalArgumentException("ADVANCE cannot target the current stage");
      }
    }
  }

  public enum TransitionKind {
    RESOLVE(false),
    ADVANCE(true),
    REWORK(true),
    ESCALATE(true),
    REMAIN_OPEN(false);

    private final boolean requiresTarget;

    TransitionKind(boolean requiresTarget) {
      this.requiresTarget = requiresTarget;
    }

    public boolean requiresTarget() {
      return requiresTarget;
    }
  }

  public record Actor(String actorId, ActorKind kind, Set<String> groups, Set<String> roles) {
    public Actor {
      actorId = requireText(actorId, "actorId");
      Objects.requireNonNull(kind, "kind");
      groups = immutableTextSet(groups, "groups");
      roles = immutableTextSet(roles, "roles");
    }
  }

  public enum ActorKind {
    HUMAN,
    SERVICE,
    SYSTEM
  }

  public record Assignment(AssignmentKind kind, String principal) {
    public Assignment {
      Objects.requireNonNull(kind, "kind");
      principal = requireText(principal, "principal");
    }

    public boolean permits(Actor actor) {
      return switch (kind) {
        case ACTOR -> principal.equals(actor.actorId());
        case GROUP -> actor.groups().contains(principal);
        case ROLE -> actor.roles().contains(principal);
      };
    }
  }

  public enum AssignmentKind {
    ACTOR,
    GROUP,
    ROLE
  }

  static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static String normalize(String value) {
    return value == null || value.isBlank() ? null : value;
  }

  private static Set<String> immutableTextSet(Set<String> values, String name) {
    if (values == null) {
      return Set.of();
    }
    values.forEach(value -> requireText(value, name + " member"));
    return Set.copyOf(values);
  }
}
