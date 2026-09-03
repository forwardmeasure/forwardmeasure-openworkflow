/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package com.forwardmeasure.openworkflow.humantask.jaxrs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import com.forwardmeasure.openworkflow.authorization.ActiveOrganizationProvider;
import com.forwardmeasure.openworkflow.authorization.AuthorizationAction;
import com.forwardmeasure.openworkflow.humantask.api.HumanTaskAdministrationApi;
import com.forwardmeasure.openworkflow.humantask.api.HumanTaskReviewsApi;
import com.forwardmeasure.openworkflow.humantask.api.HumanTasksApi;
import com.forwardmeasure.openworkflow.humantask.api.model.AssignmentRequest;
import com.forwardmeasure.openworkflow.humantask.api.model.BeginReviewRequest;
import com.forwardmeasure.openworkflow.humantask.api.model.CommentRequest;
import com.forwardmeasure.openworkflow.humantask.api.model.DecisionRequest;
import com.forwardmeasure.openworkflow.humantask.api.model.HumanTaskStatus;
import com.forwardmeasure.openworkflow.humantask.api.model.ReasonRequest;
import com.forwardmeasure.openworkflow.humantask.api.model.ReassignmentRequest;
import com.forwardmeasure.openworkflow.humantask.api.model.RenewLeaseRequest;
import com.forwardmeasure.openworkflow.humantask.api.model.SaveRevisionRequest;
import com.forwardmeasure.openworkflow.humantask.application.AuthorizedHumanTaskService;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskListQuery;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskListQuery.Direction;
import com.forwardmeasure.openworkflow.humantask.application.HumanTaskMutationResult;
import com.forwardmeasure.openworkflow.humantask.application.ReviewSessionCredentialIssuer;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskCommand;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskCommand.CommandMetadata;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.Actor;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskDefinition.ActorKind;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskId;
import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskState;
import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Portable implementation of every generated Human Task management interface. */
public final class HumanTaskResource
    implements HumanTasksApi, HumanTaskReviewsApi, HumanTaskAdministrationApi {
  private final AuthorizedHumanTaskService service;
  private final ActiveOrganizationProvider organizations;
  private final CorrelationIdProvider correlationIds;
  private final ReviewSessionCredentialIssuer credentials;
  private final Clock clock;
  private final Duration defaultLease;
  private final ObjectMapper json;
  private final HumanTaskApiMapper mapper;

  public HumanTaskResource(
      AuthorizedHumanTaskService service,
      ActiveOrganizationProvider organizations,
      CorrelationIdProvider correlationIds,
      ReviewSessionCredentialIssuer credentials,
      Clock clock,
      Duration defaultLease,
      ObjectMapper json) {
    this.service = Objects.requireNonNull(service, "service");
    this.organizations = Objects.requireNonNull(organizations, "organizations");
    this.correlationIds = Objects.requireNonNull(correlationIds, "correlationIds");
    this.credentials = Objects.requireNonNull(credentials, "credentials");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.defaultLease = Objects.requireNonNull(defaultLease, "defaultLease");
    if (defaultLease.toSeconds() < 30 || defaultLease.toSeconds() > 3600) {
      throw new IllegalArgumentException("Default review lease must be 30 to 3600 seconds");
    }
    this.json = Objects.requireNonNull(json, "json");
    this.mapper = new HumanTaskApiMapper(json);
  }

  @Override
  public Response getHumanTask(String taskId) {
    var view = service.get(organization(), correlationId(), id(taskId));
    return taskResponse(view);
  }

  @Override
  public Response listHumanTasks(
      List<HumanTaskStatus> status,
      String taskType,
      String assignment,
      String reviewer,
      Boolean overdue,
      String cursor,
      Integer limit,
      String sort,
      String direction) {
    Set<String> statuses =
        status == null
            ? Set.of()
            : status.stream()
                .map(HumanTaskStatus::toString)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    var query =
        new HumanTaskListQuery(
            statuses,
            taskType,
            assignment,
            reviewer,
            Boolean.TRUE.equals(overdue),
            cursor,
            limit == null ? 50 : limit,
            sort,
            direction == null ? Direction.ASC : Direction.valueOf(direction.toUpperCase()),
            clock.instant());
    return Response.ok(mapper.toApi(service.list(organization(), correlationId(), query))).build();
  }

  @Override
  public Response getHumanTaskHistory(String taskId, Long afterSequence, Integer limit) {
    var page =
        service.history(
            organization(),
            correlationId(),
            id(taskId),
            afterSequence == null ? -1 : afterSequence,
            limit == null ? 100 : limit);
    return Response.ok(mapper.toApi(page)).build();
  }

  @Override
  public Response listHumanTaskRevisions(String taskId) {
    return Response.ok(
            mapper.toApiRevisions(service.revisions(organization(), correlationId(), id(taskId))))
        .build();
  }

  @Override
  public Response getHumanTaskPresentation(String taskId) {
    return Response.ok(
            mapper.presentation(service.get(organization(), correlationId(), id(taskId))))
        .build();
  }

  @Override
  public Response beginHumanTaskReview(
      String ifMatch, String idempotencyKey, String taskId, BeginReviewRequest request) {
    ActiveOrganization organization = organization();
    HumanTaskId id = id(taskId);
    var credential = credentials.issue(id, idempotencyKey, organization.actorId());
    Instant now = clock.instant();
    HumanTaskMutationResult result =
        service.execute(
            organization,
            correlationId(),
            AuthorizationAction.HUMAN_TASK_BEGIN_REVIEW,
            new HumanTaskCommand.BeginReview(
                metadata(id, idempotencyKey, organization, now, ifMatch),
                credential.reviewSessionId(),
                sha256(credential.token()),
                now.plusSeconds(
                    leaseSeconds(request == null ? null : request.getRequestedLeaseSeconds()))),
            requestDigest("begin-review", taskId, ifMatch, null, null, request),
            Map.of());
    HumanTaskState.Claimed claimed = requireClaimed(result);
    return Response.status(Response.Status.CREATED)
        .header("ETag", tag(result.view().state().revision()))
        .header(
            "Location",
            "/v1/human-tasks/"
                + taskId
                + "/review-sessions/"
                + claimed.reviewSession().reviewSessionId())
        .entity(
            mapper.reviewSession(
                claimed.reviewSession(), credential.token(), result.view().state().revision()))
        .build();
  }

  @Override
  public Response renewHumanTaskReviewLease(
      String ifMatch,
      String idempotencyKey,
      String xReviewToken,
      String taskId,
      String reviewSessionId,
      RenewLeaseRequest request) {
    ActiveOrganization organization = organization();
    Instant now = clock.instant();
    HumanTaskMutationResult result =
        service.execute(
            organization,
            correlationId(),
            AuthorizationAction.HUMAN_TASK_RENEW_REVIEW,
            new HumanTaskCommand.RenewReviewLease(
                metadata(id(taskId), idempotencyKey, organization, now, ifMatch),
                reviewSessionId,
                sha256(xReviewToken),
                now.plusSeconds(
                    leaseSeconds(request == null ? null : request.getRequestedLeaseSeconds()))),
            requestDigest("renew-review", taskId, ifMatch, reviewSessionId, xReviewToken, request),
            Map.of());
    HumanTaskState.Claimed claimed = requireClaimed(result);
    return Response.ok(
            mapper.reviewSession(claimed.reviewSession(), null, result.view().state().revision()))
        .header("ETag", tag(result.view().state().revision()))
        .build();
  }

  @Override
  public Response saveHumanTaskRevision(
      String ifMatch,
      String idempotencyKey,
      String xReviewToken,
      String taskId,
      String reviewSessionId,
      SaveRevisionRequest request) {
    ActiveOrganization organization = organization();
    Instant now = clock.instant();
    var result =
        service.executeRevision(
            organization,
            correlationId(),
            new HumanTaskCommand.SaveRevision(
                metadata(id(taskId), idempotencyKey, organization, now, ifMatch),
                reviewSessionId,
                sha256(xReviewToken),
                request.getBasedOnContentRevision(),
                mapper.toDomain(request.getContent()),
                mapper.toDomain(request.getJsonPatch()),
                request.getComment()),
            requestDigest(
                "save-revision", taskId, ifMatch, reviewSessionId, xReviewToken, request));
    return Response.status(Response.Status.CREATED)
        .header("ETag", tag(result.mutation().view().state().revision()))
        .entity(mapper.toApi(result.revision()))
        .build();
  }

  @Override
  public Response addHumanTaskComment(
      String ifMatch,
      String idempotencyKey,
      String xReviewToken,
      String taskId,
      String reviewSessionId,
      CommentRequest request) {
    ActiveOrganization organization = organization();
    execute(
        organization,
        AuthorizationAction.HUMAN_TASK_COMMENT,
        new HumanTaskCommand.AddComment(
            metadata(id(taskId), idempotencyKey, organization, clock.instant(), ifMatch),
            reviewSessionId,
            sha256(xReviewToken),
            request.getComment()),
        requestDigest("comment", taskId, ifMatch, reviewSessionId, xReviewToken, request),
        Map.of());
    return Response.noContent().build();
  }

  @Override
  public Response releaseHumanTaskReview(
      String ifMatch,
      String idempotencyKey,
      String xReviewToken,
      String taskId,
      String reviewSessionId,
      ReasonRequest request) {
    ActiveOrganization organization = organization();
    execute(
        organization,
        AuthorizationAction.HUMAN_TASK_RELEASE_REVIEW,
        new HumanTaskCommand.ReleaseReview(
            metadata(id(taskId), idempotencyKey, organization, clock.instant(), ifMatch),
            reviewSessionId,
            sha256(xReviewToken),
            request == null ? null : request.getReason()),
        requestDigest("release-review", taskId, ifMatch, reviewSessionId, xReviewToken, request),
        Map.of());
    return Response.noContent().build();
  }

  @Override
  public Response submitHumanTaskDecision(
      String ifMatch,
      String idempotencyKey,
      String xReviewToken,
      String taskId,
      String reviewSessionId,
      DecisionRequest request) {
    ActiveOrganization organization = organization();
    HumanTaskMutationResult result =
        execute(
            organization,
            AuthorizationAction.HUMAN_TASK_DECIDE,
            new HumanTaskCommand.SubmitDecision(
                metadata(id(taskId), idempotencyKey, organization, clock.instant(), ifMatch),
                reviewSessionId,
                sha256(xReviewToken),
                request.getContentRevision(),
                request.getActionCode(),
                request.getComment()),
            requestDigest("decision", taskId, ifMatch, reviewSessionId, xReviewToken, request),
            Map.of("action_code", request.getActionCode()));
    return taskResponse(result.view());
  }

  @Override
  public Response assignHumanTask(
      String ifMatch, String idempotencyKey, String taskId, AssignmentRequest request) {
    ActiveOrganization organization = organization();
    HumanTaskMutationResult result =
        execute(
            organization,
            AuthorizationAction.HUMAN_TASK_ASSIGN,
            new HumanTaskCommand.Assign(
                metadata(id(taskId), idempotencyKey, organization, clock.instant(), ifMatch),
                mapper.toDomain(request.getAssignment())),
            requestDigest("assign", taskId, ifMatch, null, null, request),
            Map.of());
    return taskResponse(result.view());
  }

  @Override
  public Response unassignHumanTask(String ifMatch, String idempotencyKey, String taskId) {
    ActiveOrganization organization = organization();
    execute(
        organization,
        AuthorizationAction.HUMAN_TASK_ASSIGN,
        new HumanTaskCommand.Unassign(
            metadata(id(taskId), idempotencyKey, organization, clock.instant(), ifMatch)),
        requestDigest("unassign", taskId, ifMatch, null, null, null),
        Map.of());
    return Response.noContent().build();
  }

  @Override
  public Response reassignHumanTask(
      String ifMatch, String idempotencyKey, String taskId, ReassignmentRequest request) {
    ActiveOrganization organization = organization();
    HumanTaskMutationResult result =
        execute(
            organization,
            AuthorizationAction.HUMAN_TASK_REASSIGN,
            new HumanTaskCommand.ReassignReview(
                metadata(id(taskId), idempotencyKey, organization, clock.instant(), ifMatch),
                mapper.toDomain(request.getAssignment()),
                request.getReason()),
            requestDigest("reassign", taskId, ifMatch, null, null, request),
            Map.of());
    return taskResponse(result.view());
  }

  @Override
  public Response cancelHumanTask(
      String ifMatch, String idempotencyKey, String taskId, ReasonRequest request) {
    ActiveOrganization organization = organization();
    HumanTaskMutationResult result =
        execute(
            organization,
            AuthorizationAction.HUMAN_TASK_CANCEL,
            new HumanTaskCommand.Cancel(
                metadata(id(taskId), idempotencyKey, organization, clock.instant(), ifMatch),
                request.getReason()),
            requestDigest("cancel", taskId, ifMatch, null, null, request),
            Map.of());
    return taskResponse(result.view());
  }

  @Override
  public Response expireHumanTask(
      String ifMatch, String idempotencyKey, String taskId, ReasonRequest request) {
    ActiveOrganization organization = organization();
    HumanTaskMutationResult result =
        execute(
            organization,
            AuthorizationAction.HUMAN_TASK_EXPIRE,
            new HumanTaskCommand.ExpireTask(
                metadata(id(taskId), idempotencyKey, organization, clock.instant(), ifMatch),
                request.getReason()),
            requestDigest("expire", taskId, ifMatch, null, null, request),
            Map.of());
    return taskResponse(result.view());
  }

  private HumanTaskMutationResult execute(
      ActiveOrganization organization,
      AuthorizationAction action,
      HumanTaskCommand command,
      String requestSha256,
      Map<String, Object> context) {
    return service.execute(organization, correlationId(), action, command, requestSha256, context);
  }

  private Response taskResponse(
      com.forwardmeasure.openworkflow.humantask.application.HumanTaskView view) {
    return Response.ok(mapper.toApi(view)).header("ETag", tag(view.state().revision())).build();
  }

  private static HumanTaskState.Claimed requireClaimed(HumanTaskMutationResult result) {
    if (result.view().state() instanceof HumanTaskState.Claimed claimed) return claimed;
    throw new IllegalStateException("Review operation did not produce a claimed Human Task");
  }

  private CommandMetadata metadata(
      HumanTaskId taskId,
      String commandId,
      ActiveOrganization organization,
      Instant now,
      String ifMatch) {
    return new CommandMetadata(taskId, commandId, actor(organization), now, parseTag(ifMatch));
  }

  private long leaseSeconds(Integer requested) {
    return requested == null ? defaultLease.toSeconds() : requested.longValue();
  }

  private String requestDigest(
      String operation,
      String taskId,
      String ifMatch,
      String reviewSessionId,
      String reviewToken,
      Object request) {
    ObjectNode envelope = json.createObjectNode();
    envelope.put("operation", operation);
    envelope.put("taskId", taskId);
    envelope.put("ifMatch", ifMatch);
    if (reviewSessionId != null) envelope.put("reviewSessionId", reviewSessionId);
    if (reviewToken != null) envelope.put("reviewTokenSha256", sha256(reviewToken));
    if (request != null) envelope.set("request", json.valueToTree(request));
    try {
      return sha256(json.writeValueAsBytes(envelope));
    } catch (JsonProcessingException failure) {
      throw new IllegalArgumentException("Human Task request cannot be canonicalized", failure);
    }
  }

  private ActiveOrganization organization() {
    return organizations.current();
  }

  private String correlationId() {
    return correlationIds.current();
  }

  private static Actor actor(ActiveOrganization organization) {
    return new Actor(
        organization.actorId(), ActorKind.HUMAN, Set.of(), organization.organizationRoles());
  }

  private static HumanTaskId id(String value) {
    return new HumanTaskId(value);
  }

  private static long parseTag(String tag) {
    if (tag == null || !tag.matches("^\"[0-9]+\"$")) {
      throw new IllegalArgumentException("If-Match must be a strong numeric ETag");
    }
    return Long.parseLong(tag.substring(1, tag.length() - 1));
  }

  private static String tag(long revision) {
    return "\"" + revision + "\"";
  }

  private static String sha256(String value) {
    return sha256(value.getBytes(StandardCharsets.UTF_8));
  }

  private static String sha256(byte[] value) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }
}
