import { load } from "js-yaml";
import { ResponseError } from "@forwardmeasure/openworkflow-definition-management-client";

export const SAMPLE = `document:
  dsl: '1.0.3'
  namespace: forwardmeasure
  name: hello-studio
  version: '1.0.0'
do:
  - greet:
      set:
        message: Hello from OpenWorkflow Studio
`;

export function taskNames(source: string): string[] {
  try {
    const workflow = load(source) as { do?: Array<Record<string, unknown>> };
    return Array.isArray(workflow?.do)
      ? workflow.do.flatMap((task) => Object.keys(task))
      : [];
  } catch {
    return [];
  }
}

// The generated client's ResponseError only ever carries a generic message
// ("Response returned an error code") - the actual server-side explanation
// is in the response BODY, which nothing was reading. Two Problem shapes
// exist and both need reading, not just one: RFC 9457 Problems from
// openworkflow-definition-management-jaxrs's *ExceptionMapper classes carry
// the real text in "detail" (an exception message for known error types,
// and, temporarily, see DebugThrowableExceptionMapper - the full exception
// class/cause chain/stack trace for anything else); Quarkus's own built-in
// bean-validation failure response (quarkus-hibernate-validator rejecting a
// request before a resource method even runs, e.g. a missing required
// header) has no "detail" at all - the real text is in "violations"
// instead. Async because reading a Response body is a Promise; every call
// site already awaits inside a try/catch.
export async function diagnostic(error: unknown): Promise<string> {
  if (error instanceof ResponseError) {
    try {
      const problem = (await error.response.clone().json()) as {
        detail?: string;
        title?: string;
        violations?: Array<{ field?: string; message?: string }>;
      };
      if (problem.detail) {
        return problem.title
          ? `${problem.title}: ${problem.detail}`
          : problem.detail;
      }
      if (problem.violations?.length) {
        const summary = problem.violations
          .map(
            (violation) =>
              `${violation.field ?? "value"} ${violation.message ?? "is invalid"}`,
          )
          .join("; ");
        return problem.title ? `${problem.title}: ${summary}` : summary;
      }
      if (problem.title) return problem.title;
    } catch {
      // Response body wasn't JSON (or already consumed) - fall through to the generic message.
    }
  }
  if (error instanceof Error) return error.message;
  return String(error);
}

export function canPause(status: string): boolean {
  return status === "RUNNING" || status === "WAITING";
}

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
