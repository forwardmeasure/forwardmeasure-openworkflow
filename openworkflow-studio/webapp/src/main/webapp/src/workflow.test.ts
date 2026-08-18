import { canPause, diagnostic, lineDiff, taskNames } from "./workflow";
import { tenantFromToken } from "./session";
import { describe, expect, it } from "vitest";

describe("lossless authoring helpers", () => {
  it("derives a diagram without rewriting source", () => {
    const source =
      "do:\n  - first:\n      set: {value: 1}\n  - second:\n      set: {value: 2}\n";
    expect(taskNames(source)).toEqual(["first", "second"]);
    expect(source).toContain("set: {value: 1}");
  });

  it("keeps invalid documents available for server diagnostics", () => {
    expect(taskNames("do: [")).toEqual([]);
    expect(diagnostic(new Error("line 1 is invalid"))).toBe(
      "line 1 is invalid",
    );
  });

  it("displays tenant identity without treating an unreadable token as authority", () => {
    const payload = btoa(
      JSON.stringify({ tenant_id: "did:web:example:tenant:one" }),
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

  it("diffs revisions without normalizing either source document", () => {
    expect(lineDiff("a: 1\n", "a: 2\n")).toEqual(["- a: 1\n+ a: 2", "  "]);
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
