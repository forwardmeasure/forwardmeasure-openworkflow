import { describe, expect, it } from "vitest";
import { deriveEdges, layout } from "./WorkflowCanvas";
import type { Task } from "./dsl";

describe("deriveEdges", () => {
  it("chains a straight line of tasks Start -> ... -> End", () => {
    const tasks: Task[] = [
      { kind: "set", name: "a", set: {} },
      { kind: "set", name: "b", set: {} },
    ];
    const edges = deriveEdges(tasks);
    expect(edges.map((e) => [e.source, e.target])).toEqual([
      ["__start__", "a"],
      ["a", "b"],
      ["b", "__end__"],
    ]);
  });

  it("connects Start straight to End when there are no tasks", () => {
    expect(deriveEdges([]).map((e) => [e.source, e.target])).toEqual([
      ["__start__", "__end__"],
    ]);
  });

  it("fans a switch task's cases out to their own targets instead of falling through positionally", () => {
    const tasks: Task[] = [
      {
        kind: "switch",
        name: "route",
        cases: [
          { name: "vip", when: '${ .tier == "vip" }', then: "vipPath" },
          { name: "default", then: "exit" },
        ],
      },
      { kind: "set", name: "vipPath", set: {} },
    ];
    const edges = deriveEdges(tasks);
    // The switch's own positional successor ("vipPath", the next array
    // entry) is NOT an edge target here - only the cases' "then" values are,
    // matching the spec: a switch always redirects, it never falls through.
    expect(edges.map((e) => [e.source, e.sourceHandle, e.target])).toEqual([
      ["__start__", undefined, "route"],
      ["route", "vip", "vipPath"],
      ["route", "default", "__end__"],
      ["vipPath", undefined, "__end__"],
    ]);
  });
});

describe("layout", () => {
  it("places a linear flow in increasing columns", () => {
    const tasks: Task[] = [
      { kind: "set", name: "a", set: {} },
      { kind: "set", name: "b", set: {} },
    ];
    const { nodes } = layout(tasks);
    const columnOf = (id: string) => nodes.find((n) => n.id === id)!.position.x;
    expect(columnOf("__start__")).toBeLessThan(columnOf("a"));
    expect(columnOf("a")).toBeLessThan(columnOf("b"));
    expect(columnOf("b")).toBeLessThan(columnOf("__end__"));
  });

  it("does not hang or crash when a switch case points back at an earlier task", () => {
    const tasks: Task[] = [
      {
        kind: "switch",
        name: "retryLoop",
        cases: [
          { name: "again", when: "${ .attempt < 3 }", then: "retryLoop" },
          { name: "default", then: "exit" },
        ],
      },
    ];
    const { nodes, edges } = layout(tasks);
    expect(nodes.map((n) => n.id)).toEqual([
      "__start__",
      "retryLoop",
      "__end__",
    ]);
    expect(edges).toHaveLength(3);
  });

  it("falls back to do:-list order for a task no edge ever targets", () => {
    const tasks: Task[] = [
      {
        kind: "switch",
        name: "route",
        cases: [{ name: "default", then: "exit" }],
      },
      // Nothing points at "orphan" - the switch's cases skip it and it has
      // no predecessor, since a switch never falls through positionally.
      { kind: "set", name: "orphan", set: {} },
    ];
    const { nodes } = layout(tasks);
    const orphan = nodes.find((n) => n.id === "orphan")!;
    const start = nodes.find((n) => n.id === "__start__")!;
    expect(orphan.position.x).toBeGreaterThan(start.position.x);
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
