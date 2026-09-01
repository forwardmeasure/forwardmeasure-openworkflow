import { ResponseError } from "@forwardmeasure/openworkflow-definition-management-client";
import { canPause, diagnostic } from "./workflow";
import { tenantFromToken } from "./session";
import { describe, expect, it } from "vitest";

describe("workflow helpers", () => {
  it("keeps invalid documents available for server diagnostics", async () => {
    expect(await diagnostic(new Error("line 1 is invalid"))).toBe(
      "line 1 is invalid",
    );
  });

  it("surfaces the server's RFC 9457 Problem detail instead of the generic client message", async () => {
    const response = new Response(
      JSON.stringify({
        title: "Unhandled Exception (debug mode)",
        detail:
          "java.lang.SecurityException: Organization client roles claim is required",
      }),
      { status: 500, headers: { "Content-Type": "application/problem+json" } },
    );
    const error = new ResponseError(
      response,
      "Response returned an error code",
    );
    expect(await diagnostic(error)).toBe(
      "Unhandled Exception (debug mode): java.lang.SecurityException: Organization client roles claim is required",
    );
  });

  it("surfaces a bean-validation constraint violation (Problem.detail is absent, not just empty)", async () => {
    const response = new Response(
      JSON.stringify({
        title: "Constraint Violation",
        status: 400,
        violations: [
          { field: "updateWorkflowDefinition.ifMatch", message: "must not be null" },
        ],
      }),
      { status: 400, headers: { "Content-Type": "application/json" } },
    );
    const error = new ResponseError(
      response,
      "Response returned an error code",
    );
    expect(await diagnostic(error)).toBe(
      "Constraint Violation: updateWorkflowDefinition.ifMatch must not be null",
    );
  });

  it("falls back to the generic message when the response body isn't a Problem", async () => {
    const response = new Response("not json", { status: 502 });
    const error = new ResponseError(
      response,
      "Response returned an error code",
    );
    expect(await diagnostic(error)).toBe("Response returned an error code");
  });

  it("displays tenant identity without treating an unreadable token as authority", () => {
    // tenant_did, not tenant_id - matches the forwardmeasure_identity client
    // scope's actual mapped claim name, confirmed against a real decoded
    // token (see session.ts's own comment on tenantFromToken).
    const payload = btoa(
      JSON.stringify({ tenant_did: "did:web:example:tenant:one" }),
    );
    expect(tenantFromToken(`x.${payload}.x`)).toBe(
      "did:web:example:tenant:one",
    );
    expect(tenantFromToken("opaque-token")).toBe("authenticated tenant");
  });

  it("allows pause at active and durable waiting boundaries", () => {
    expect(canPause("RUNNING")).toBe(true);
    expect(canPause("WAITING")).toBe(true);
    expect(canPause("PAUSED")).toBe(false);
    expect(canPause("CANCELLED")).toBe(false);
  });
});
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
