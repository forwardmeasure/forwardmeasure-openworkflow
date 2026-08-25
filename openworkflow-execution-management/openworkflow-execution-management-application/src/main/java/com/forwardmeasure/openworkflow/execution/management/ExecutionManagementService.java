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
package com.forwardmeasure.openworkflow.execution.management;

import com.forwardmeasure.jpa.tenancy.TenantSchema;
import com.forwardmeasure.jpa.tenancy.TenantScope;
import com.forwardmeasure.openworkflow.engine.api.CommandAcknowledgement;
import com.forwardmeasure.openworkflow.engine.api.EngineCommandException;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommand;
import com.forwardmeasure.openworkflow.engine.api.ExecutionCommandEnvelope;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProvider;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineProviders;
import com.forwardmeasure.openworkflow.engine.api.ExecutionEngineSelector;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.ExecutionLifecycleState;
import com.forwardmeasure.openworkflow.engine.api.TenantActorContext;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/** Engine-independent execution admission and command orchestration. */
public final class ExecutionManagementService {
  private final PublishedWorkflowResolverFactory publications;
  private final ExecutionAuthorizer authorizer;
  private final ExecutionRepositoryFactory executions;
  private final ExecutionEngineProviders providers;
  private final ExecutionEngineSelector selector;
  private final Clock clock;
  private final Supplier<UUID> executionIds;
  private final TenantScope tenants;
  private final ExecutionTransactionExecutor transactions;

  public ExecutionManagementService(
      PublishedWorkflowResolver publications,
      ExecutionAuthorizer authorizer,
      ExecutionRepository executions,
      ExecutionEngineProviders providers,
      ExecutionEngineSelector selector,
      Clock clock,
      Supplier<UUID> executionIds,
      TenantScope tenants,
      ExecutionTransactionExecutor transactions) {
    this(
        ignored -> Objects.requireNonNull(publications, "publications"),
        authorizer,
        ignored -> Objects.requireNonNull(executions, "executions"),
        providers,
        selector,
        clock,
        executionIds,
        tenants,
        transactions);
  }

  public ExecutionManagementService(
      PublishedWorkflowResolverFactory publications,
      ExecutionAuthorizer authorizer,
      ExecutionRepositoryFactory executions,
      ExecutionEngineProviders providers,
      ExecutionEngineSelector selector,
      Clock clock,
      Supplier<UUID> executionIds,
      TenantScope tenants,
      ExecutionTransactionExecutor transactions) {
    this.publications = Objects.requireNonNull(publications, "publications");
    this.authorizer = Objects.requireNonNull(authorizer, "authorizer");
    this.executions = Objects.requireNonNull(executions, "executions");
    this.providers = Objects.requireNonNull(providers, "providers");
    this.selector = Objects.requireNonNull(selector, "selector");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.executionIds = Objects.requireNonNull(executionIds, "executionIds");
    this.tenants = Objects.requireNonNull(tenants, "tenants");
    this.transactions = Objects.requireNonNull(transactions, "transactions");
  }

  private <T> T inTenant(
      com.forwardmeasure.openworkflow.engine.api.TenantId tenantId, Supplier<T> operation) {
    var schema =
        TenantSchema.forTenant(new com.forwardmeasure.jpa.tenancy.TenantId(tenantId.value()));
    return tenants.call(schema, () -> transactions.execute(operation));
  }

  public CompletionStage<CanonicalExecution> start(ExecutionAdmissionRequest request) {
    Objects.requireNonNull(request, "request");
    return inTenant(
        request.context().tenantId(),
        () -> {
          authorizer.authorize(
              request.context(),
              ExecutionAuthorizer.Action.START,
              request.revisionId().toString(),
              request.correlationId());
          ExecutionRepository repository = executions.forTenant(request.context().tenantId());
          var duplicate =
              repository.findByIdempotencyKey(
                  request.context().tenantId(), request.idempotencyKey());
          if (duplicate.isPresent()) {
            return java.util.concurrent.CompletableFuture.completedFuture(duplicate.orElseThrow());
          }
          PublishedWorkflow publication =
              publications
                  .resolverForTenant(request.context().tenantId())
                  .resolveAuthorizedPublished(
                      request.context(), request.revisionId(), request.correlationId());
          ExecutionEngineProvider provider =
              providers.select(request.context(), publication.revision(), selector);
          Instant now = clock.instant();
          var executionId = new ExecutionId(request.context().tenantId(), executionIds.get());
          var candidate =
              new CanonicalExecution(
                  executionId,
                  publication.revision(),
                  provider.engineId(),
                  ExecutionLifecycleState.NEW,
                  0,
                  request.idempotencyKey(),
                  request.correlationId(),
                  request.context().actorId(),
                  request.input(),
                  now,
                  now);
          ExecutionRepository.Admission admission = repository.admit(candidate);
          if (!admission.created()) {
            return java.util.concurrent.CompletableFuture.completedFuture(admission.execution());
          }
          repository.recordCommand(
              new CommandReceipt(
                  request.commandId(),
                  executionId,
                  "START",
                  0,
                  true,
                  null,
                  request.context().actorId(),
                  request.correlationId(),
                  now));
          var envelope =
              new ExecutionCommandEnvelope(
                  request.commandId(),
                  request.correlationId(),
                  request.context(),
                  provider.engineId(),
                  0,
                  now,
                  new ExecutionCommand.Start(
                      executionId,
                      publication.revision(),
                      publication.plan(),
                      publication.sourceDocument(),
                      request.input()));
          return dispatch(repository, provider, envelope, admission.execution(), 0);
        });
  }

  public CompletionStage<CanonicalExecution> pause(
      TenantActorContext context,
      ExecutionId executionId,
      UUID commandId,
      long expectedVersion,
      String correlationId) {
    return command(
        context,
        commandId,
        expectedVersion,
        correlationId,
        new ExecutionCommand.Pause(executionId));
  }

  public CompletionStage<CanonicalExecution> resume(
      TenantActorContext context,
      ExecutionId executionId,
      UUID commandId,
      long expectedVersion,
      String correlationId) {
    return command(
        context,
        commandId,
        expectedVersion,
        correlationId,
        new ExecutionCommand.Resume(executionId));
  }

  public CompletionStage<CanonicalExecution> cancel(
      TenantActorContext context,
      ExecutionId executionId,
      UUID commandId,
      long expectedVersion,
      String correlationId,
      String reason) {
    return command(
        context,
        commandId,
        expectedVersion,
        correlationId,
        new ExecutionCommand.Cancel(executionId, reason));
  }

  private CompletionStage<CanonicalExecution> command(
      TenantActorContext context,
      UUID commandId,
      long expectedVersion,
      String correlationId,
      ExecutionCommand command) {
    Objects.requireNonNull(context, "context");
    return inTenant(
        context.tenantId(),
        () -> {
          if (!context.tenantId().equals(command.executionId().tenantId())) {
            throw new ExecutionManagementException(
                ExecutionManagementException.Kind.NOT_FOUND, "execution does not exist");
          }
          authorizer.authorize(
              context, action(command), command.executionId().value().toString(), correlationId);
          ExecutionRepository repository = executions.forTenant(context.tenantId());
          CanonicalExecution execution =
              repository
                  .find(context.tenantId(), command.executionId())
                  .orElseThrow(
                      () ->
                          new ExecutionManagementException(
                              ExecutionManagementException.Kind.NOT_FOUND,
                              "execution does not exist"));
          var receipt =
              new CommandReceipt(
                  commandId,
                  execution.executionId(),
                  command.getClass().getSimpleName().toUpperCase(java.util.Locale.ROOT),
                  expectedVersion,
                  execution.version() == expectedVersion,
                  execution.version() == expectedVersion ? null : "stale execution version",
                  context.actorId(),
                  correlationId,
                  clock.instant());
          CommandReceipt recorded = repository.recordCommand(receipt);
          if (!recorded.accepted()) {
            throw new ExecutionManagementException(
                ExecutionManagementException.Kind.STALE_VERSION, "stale execution version");
          }
          var envelope =
              new ExecutionCommandEnvelope(
                  commandId,
                  correlationId,
                  context,
                  execution.engineId(),
                  expectedVersion,
                  clock.instant(),
                  command);
          return dispatch(
              repository,
              providers.require(execution.engineId()),
              envelope,
              execution,
              expectedVersion);
        });
  }

  /**
   * Blocks for the engine's acknowledgement and writes it synchronously on the calling thread,
   * rather than in an async {@code thenApply} callback. {@code provider.submit(...)} is a real
   * network call (see {@code HttpExecutionEngineProvider}); its completion callback would otherwise
   * run on the HTTP client's own executor thread, which has neither the transaction nor the {@link
   * com.forwardmeasure.jpa.tenancy.TenantScope} that {@code repository.acknowledge} needs - both
   * are bound to the thread that's inside this method's caller's {@code inTenant} scope, not to
   * whichever thread happens to complete the HTTP response. The only caller ({@code
   * ExecutionResource}) already blocks on {@code .join()} for the whole chain, so this changes
   * nothing observable - it just does the same blocking one level higher, where the thread is still
   * the right one.
   */
  private CompletionStage<CanonicalExecution> dispatch(
      ExecutionRepository repository,
      ExecutionEngineProvider provider,
      ExecutionCommandEnvelope envelope,
      CanonicalExecution execution,
      long expectedVersion) {
    CommandAcknowledgement acknowledgement;
    try {
      acknowledgement = provider.submit(envelope).toCompletableFuture().join();
    } catch (CompletionException failure) {
      Throwable cause = failure.getCause();
      if (cause instanceof RuntimeException runtime) {
        throw runtime;
      }
      throw new EngineCommandException(
          EngineCommandException.FailureKind.UNAVAILABLE, cause.getMessage());
    }
    return java.util.concurrent.CompletableFuture.completedFuture(
        acknowledge(repository, execution, expectedVersion, acknowledgement));
  }

  private static ExecutionAuthorizer.Action action(ExecutionCommand command) {
    return switch (command) {
      case ExecutionCommand.Start ignored -> ExecutionAuthorizer.Action.START;
      case ExecutionCommand.Pause ignored -> ExecutionAuthorizer.Action.PAUSE;
      case ExecutionCommand.Resume ignored -> ExecutionAuthorizer.Action.RESUME;
      case ExecutionCommand.Cancel ignored -> ExecutionAuthorizer.Action.CANCEL;
    };
  }

  private CanonicalExecution acknowledge(
      ExecutionRepository repository,
      CanonicalExecution execution,
      long expectedVersion,
      CommandAcknowledgement acknowledgement) {
    if (!execution.executionId().equals(acknowledgement.executionId())
        || !execution.engineId().equals(acknowledgement.engineId())
        || acknowledgement.version() < expectedVersion) {
      throw new IllegalStateException("engine returned an inconsistent acknowledgement");
    }
    return repository.acknowledge(
        execution.executionId().tenantId(), execution.executionId(), acknowledgement);
  }
}
