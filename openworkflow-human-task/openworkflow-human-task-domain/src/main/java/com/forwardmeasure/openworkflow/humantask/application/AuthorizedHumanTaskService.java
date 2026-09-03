/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.AuthorizationAction;
import com.forwardmeasure.openworkflow.authorization.AuthorizationRequest;
import com.forwardmeasure.openworkflow.authorization.AuthorizationResource;
import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskCommand;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskId;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Fail-closed application boundary for every Human Task query and mutation. */
public final class AuthorizedHumanTaskService {
  private final HumanTaskApplicationService commands;
  private final HumanTaskQueryService queries;
  private final AuthorizationService authorization;

  public AuthorizedHumanTaskService(
      HumanTaskApplicationService commands,
      HumanTaskQueryService queries,
      AuthorizationService authorization) {
    this.commands = Objects.requireNonNull(commands, "commands");
    this.queries = Objects.requireNonNull(queries, "queries");
    this.authorization = Objects.requireNonNull(authorization, "authorization");
  }

  public HumanTaskView get(ActiveOrganization actor, String correlationId, HumanTaskId taskId) {
    authorize(actor, correlationId, taskId, AuthorizationAction.HUMAN_TASK_READ, Map.of());
    return queries
        .find(taskId)
        .orElseThrow(() -> new HumanTaskNotFoundException("Human task does not exist: " + taskId));
  }

  public HumanTaskPage list(
      ActiveOrganization actor, String correlationId, HumanTaskListQuery query) {
    authorize(
        actor, correlationId, new HumanTaskId("*"), AuthorizationAction.HUMAN_TASK_LIST, Map.of());
    return queries.list(query);
  }

  public HumanTaskHistoryPage history(
      ActiveOrganization actor,
      String correlationId,
      HumanTaskId taskId,
      long afterSequence,
      int limit) {
    authorize(actor, correlationId, taskId, AuthorizationAction.HUMAN_TASK_READ_HISTORY, Map.of());
    return queries.history(taskId, afterSequence, limit);
  }

  public List<HumanTaskContentRevisionRecord> revisions(
      ActiveOrganization actor, String correlationId, HumanTaskId taskId) {
    authorize(actor, correlationId, taskId, AuthorizationAction.HUMAN_TASK_READ_HISTORY, Map.of());
    return queries.revisions(taskId);
  }

  public HumanTaskMutationResult execute(
      ActiveOrganization actor,
      String correlationId,
      AuthorizationAction action,
      HumanTaskCommand command,
      String requestSha256,
      Map<String, Object> context) {
    requireMutationAction(action);
    authorize(actor, correlationId, command.metadata().taskId(), action, context);
    HumanTaskCommandResult result = commands.handle(command, requestSha256);
    HumanTaskView view =
        queries
            .find(command.metadata().taskId())
            .orElseThrow(
                () ->
                    new HumanTaskNotFoundException(
                        "Human task disappeared after command: " + command.metadata().taskId()));
    return new HumanTaskMutationResult(result, view);
  }

  public HumanTaskRevisionMutationResult executeRevision(
      ActiveOrganization actor,
      String correlationId,
      HumanTaskCommand.SaveRevision command,
      String requestSha256) {
    HumanTaskMutationResult mutation =
        execute(
            actor,
            correlationId,
            AuthorizationAction.HUMAN_TASK_EDIT,
            command,
            requestSha256,
            Map.of());
    HumanTaskContentRevisionRecord revision =
        queries.revisions(command.metadata().taskId()).stream()
            .filter(
                candidate -> candidate.contentRevision() == command.basedOnContentRevision() + 1)
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Accepted Human Task revision is missing from persistence"));
    return new HumanTaskRevisionMutationResult(mutation, revision);
  }

  private void authorize(
      ActiveOrganization actor,
      String correlationId,
      HumanTaskId taskId,
      AuthorizationAction action,
      Map<String, Object> context) {
    authorization.requireAuthorized(
        new AuthorizationRequest(
            actor,
            AuthorizationResource.humanTask(taskId.value()),
            action,
            correlationId,
            context));
  }

  private static void requireMutationAction(AuthorizationAction action) {
    switch (action) {
      case HUMAN_TASK_BEGIN_REVIEW,
          HUMAN_TASK_RENEW_REVIEW,
          HUMAN_TASK_EDIT,
          HUMAN_TASK_COMMENT,
          HUMAN_TASK_RELEASE_REVIEW,
          HUMAN_TASK_DECIDE,
          HUMAN_TASK_ASSIGN,
          HUMAN_TASK_REASSIGN,
          HUMAN_TASK_CANCEL,
          HUMAN_TASK_EXPIRE -> {}
      default -> throw new IllegalArgumentException("Not a Human Task mutation action: " + action);
    }
  }
}
