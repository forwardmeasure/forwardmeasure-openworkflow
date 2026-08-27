import { describe, expect, it } from "vitest";
import { fromYaml, toYaml, UnsupportedTaskError } from "./dsl";
import { SAMPLE } from "../workflow";

describe("canvas <-> Serverless Workflow DSL conversion", () => {
  it("parses the studio sample into an editable set task", () => {
    const graph = fromYaml(SAMPLE);
    expect(graph.tasks).toEqual([
      {
        kind: "set",
        name: "greet",
        set: { message: "Hello from OpenWorkflow Studio" },
      },
    ]);
  });

  it("parses call tasks with their target and parameters", () => {
    const source = [
      "do:",
      "  - fetchPet:",
      "      call: http",
      "      with:",
      "        method: get",
      "        endpoint: https://example.com/pet",
      "",
    ].join("\n");
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "call",
        name: "fetchPet",
        call: "http",
        with: { method: "get", endpoint: "https://example.com/pet" },
      },
    ]);
  });

  it("round-trips a graph back into valid Serverless Workflow DSL, preserving document metadata", () => {
    const rewritten = toYaml(SAMPLE, {
      tasks: [
        { kind: "set", name: "greet", set: { message: "Updated" } },
        {
          kind: "call",
          name: "notify",
          call: "http",
          with: { endpoint: "https://example.com" },
        },
      ],
    });
    const reparsed = fromYaml(rewritten);
    expect(reparsed.tasks).toEqual([
      { kind: "set", name: "greet", set: { message: "Updated" } },
      {
        kind: "call",
        name: "notify",
        call: "http",
        with: { endpoint: "https://example.com" },
      },
    ]);
    // document metadata (dsl/namespace/name/version) must survive a round trip untouched -
    // toYaml() only replaces the "do:" key, it doesn't regenerate the whole document.
    expect(rewritten).toContain("namespace: forwardmeasure");
    expect(rewritten).toContain("name: hello-studio");
  });

  it("parses switch tasks with named cases, conditions, and targets", () => {
    const source = [
      "do:",
      "  - checkAge:",
      "      switch:",
      "        - adult:",
      "            when: ${ .age >= 18 }",
      "            then: notifyAdult",
      "        - default:",
      "            then: exit",
      "  - notifyAdult:",
      "      set:",
      "        message: welcome",
      "",
    ].join("\n");
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "switch",
        name: "checkAge",
        cases: [
          { name: "adult", when: "${ .age >= 18 }", then: "notifyAdult" },
          { name: "default", when: undefined, then: "exit" },
        ],
      },
      { kind: "set", name: "notifyAdult", set: { message: "welcome" } },
    ]);
  });

  it("round-trips a switch task's cases", () => {
    const rewritten = toYaml(SAMPLE, {
      tasks: [
        {
          kind: "switch",
          name: "route",
          cases: [
            { name: "vip", when: '${ .tier == "vip" }', then: "exit" },
            { name: "default", then: "exit" },
          ],
        },
      ],
    });
    expect(fromYaml(rewritten).tasks).toEqual([
      {
        kind: "switch",
        name: "route",
        cases: [
          { name: "vip", when: '${ .tier == "vip" }', then: "exit" },
          { name: "default", when: undefined, then: "exit" },
        ],
      },
    ]);
  });

  it('rejects a switch case with no "then" (positional fallthrough isn\'t modeled here)', () => {
    const source = "do:\n  - branch:\n      switch:\n        - default: {}\n";
    expect(() => fromYaml(source)).toThrow(UnsupportedTaskError);
  });

  it("parses a task's common cross-cutting properties alongside its kind-specific fields", () => {
    const source = [
      "do:",
      "  - greet:",
      "      if: ${ .enabled }",
      "      input:",
      "        schema: {}",
      "      timeout: PT30S",
      "      metadata:",
      "        owner: team-a",
      "      set:",
      "        message: hi",
      "",
    ].join("\n");
    expect(fromYaml(source).tasks).toEqual([
      {
        kind: "set",
        name: "greet",
        set: { message: "hi" },
        if: "${ .enabled }",
        input: { schema: {} },
        timeout: "PT30S",
        metadata: { owner: "team-a" },
      },
    ]);
  });

  it("round-trips a task's common cross-cutting properties, omitting unset ones", () => {
    const rewritten = toYaml(SAMPLE, {
      tasks: [
        {
          kind: "call",
          name: "fetchPet",
          call: "http",
          with: { endpoint: "https://example.com" },
          if: "${ .fetch }",
          timeout: "PT10S",
          export: { as: "$context" },
        },
      ],
    });
    expect(fromYaml(rewritten).tasks).toEqual([
      {
        kind: "call",
        name: "fetchPet",
        call: "http",
        with: { endpoint: "https://example.com" },
        if: "${ .fetch }",
        timeout: "PT10S",
        export: { as: "$context" },
      },
    ]);
    expect(rewritten).not.toContain("input:");
    expect(rewritten).not.toContain("output:");
    expect(rewritten).not.toContain("metadata:");
  });

  it("rejects task constructs the canvas doesn't support yet, rather than silently dropping them", () => {
    const source =
      "do:\n  - retry:\n      for:\n        each: item\n        in: ${ .items }\n";
    expect(() => fromYaml(source)).toThrow(UnsupportedTaskError);
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
