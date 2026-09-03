/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to You under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 */
package com.forwardmeasure.openworkflow.humantask.application;

import com.forwardmeasure.openworkflow.humantask.domain.HumanTaskCommand;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Application-backed request port preserving durable command idempotency. */
public final class ApplicationHumanTaskRequestPort implements HumanTaskRequestPort {
  private final HumanTaskApplicationService service;

  public ApplicationHumanTaskRequestPort(HumanTaskApplicationService service) {
    this.service = Objects.requireNonNull(service, "service");
  }

  @Override
  public CompletionStage<HumanTaskAcceptance> request(HumanTaskRequest request) {
    Objects.requireNonNull(request, "request");
    HumanTaskCommand.Create command =
        new HumanTaskCommand.Create(
            new HumanTaskCommand.CommandMetadata(
                request.taskId(), request.requestId(), request.actor(), request.requestedAt(), 0),
            request.definition());
    HumanTaskCommandResult result = service.handle(command, request.requestSha256());
    HumanTaskAcceptance.AcceptanceStatus status =
        result.replayed()
            ? HumanTaskAcceptance.AcceptanceStatus.REPLAYED
            : HumanTaskAcceptance.AcceptanceStatus.ACCEPTED;
    return CompletableFuture.completedFuture(
        new HumanTaskAcceptance(request.requestId(), request.taskId(), status));
  }
}
