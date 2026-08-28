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
package com.forwardmeasure.openworkflow.definition.management.jaxrs.mapper;

import com.forwardmeasure.openworkflow.common.model.Problem;
import com.forwardmeasure.openworkflow.common.model.Violation;
import com.forwardmeasure.openworkflow.definition.WorkflowDefinitionException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Translates every {@link WorkflowDefinitionException} (OpenWorkflowCompiler.compile() rejecting a
 * definition - a bad task shape, a dangling "then" target, an unproven schema-compatibility edge
 * from WorkflowContractAnalyzer, ...) into a {@link Problem} response, the same way
 * DefinitionManagementExceptionMapper already does for definition-lifecycle failures. Without this,
 * every compile failure on create/update/validate fell through to DebugThrowableExceptionMapper's
 * generic Throwable handling - a full Java stack trace as Problem.detail, correct as a temporary
 * catch-all for genuinely unmapped exceptions, but a page of Java internals is not a validation
 * message for something this routine and expected (a user's workflow simply doesn't compile yet).
 *
 * <p>422 Unprocessable Entity, matching what definition-management.openapi.yaml already documents
 * for create/update/validateWorkflowDefinition - the client-generated TypeScript types already
 * expect this shape, nothing to regenerate.
 */
@Provider
public final class WorkflowDefinitionExceptionMapper
    implements ExceptionMapper<WorkflowDefinitionException> {

  // Most of OpenWorkflowCompiler's violation strings lead with a JSON Pointer to where the
  // problem is ("/do/2/task3 [required] required property 'call' not found"); the
  // WorkflowContractAnalyzer ones don't ("Schema compatibility <finding>"). Split the pointer off
  // into Violation.field when present, fall back to the whole string as the message otherwise -
  // best-effort structure, never a hard requirement on the message shape.
  private static final Pattern POINTER_PREFIX =
      Pattern.compile("^(/\\S*)\\s+(.*)$", Pattern.DOTALL);

  @Override
  public Response toResponse(WorkflowDefinitionException exception) {
    List<Violation> violations = exception.violations().stream().map(this::toViolation).toList();
    Problem problem =
        new Problem()
            .type("about:blank")
            .title("Unprocessable Entity")
            .status(422)
            .detail(exception.getMessage())
            .violations(violations);
    return Response.status(422).type("application/problem+json").entity(problem).build();
  }

  private Violation toViolation(String text) {
    Matcher matcher = POINTER_PREFIX.matcher(text);
    return matcher.matches()
        ? new Violation().field(matcher.group(1)).message(matcher.group(2))
        : new Violation().message(text);
  }
}
