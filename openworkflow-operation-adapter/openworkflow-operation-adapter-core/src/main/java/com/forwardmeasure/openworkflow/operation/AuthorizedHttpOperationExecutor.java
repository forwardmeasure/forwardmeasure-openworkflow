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
package com.forwardmeasure.openworkflow.operation;

import com.forwardmeasure.openworkflow.authorization.AuthorizationService;
import com.forwardmeasure.openworkflow.engine.api.ExecutionId;
import com.forwardmeasure.openworkflow.engine.api.HttpOperationDescriptor;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** AuthZEN-enforcing boundary around a Pekko HTTP/OpenAPI transport. */
public final class AuthorizedHttpOperationExecutor implements HttpOperationExecutor {
  private final OperationAuthorization authorization;
  private final HttpOperationExecutor delegate;

  public AuthorizedHttpOperationExecutor(
      AuthorizationService authorization, HttpOperationExecutor delegate) {
    this.authorization = new OperationAuthorization(authorization);
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  @Override
  public CompletionStage<HttpOperationResult> execute(
      ExecutionId executionId, HttpOperationDescriptor operation) {
    try {
      authorization.require(
          executionId, operation.operationId(), operation.kind().name(), operation.requestedBy());
      return delegate.execute(executionId, operation);
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
  }
}
