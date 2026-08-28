import { describe, expect, it } from "vitest";
import {
  deriveEdges,
  isPositionalEdge,
  layout,
  reconnectEdgeTarget,
  spliceTaskOnEdge,
} from "./WorkflowCanvas";
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

  it("routes a non-switch task's own \"then\" instead of positional fallthrough", () => {
    // Regression test: this previously always used tasks[index + 1]
    // unconditionally, silently ignoring "then" (added to CommonTaskProps
    // earlier this session) - the canvas drew a misleading edge for any
    // task using an explicit override.
    const tasks: Task[] = [
      { kind: "set", name: "a", set: {}, then: "c" },
      { kind: "set", name: "b", set: {} },
      { kind: "set", name: "c", set: {} },
    ];
    const edges = deriveEdges(tasks);
    expect(edges.map((e) => [e.source, e.target])).toEqual([
      ["__start__", "a"],
      ["a", "c"],
      ["b", "c"],
      ["c", "__end__"],
    ]);
  });

  it("treats then: continue the same as no then at all", () => {
    const tasks: Task[] = [
      { kind: "set", name: "a", set: {}, then: "continue" },
      { kind: "set", name: "b", set: {} },
    ];
    expect(deriveEdges(tasks).map((e) => [e.source, e.target])).toEqual([
      ["__start__", "a"],
      ["a", "b"],
      ["b", "__end__"],
    ]);
  });

  it("routes then: exit and then: end both to the End anchor", () => {
    const exitTasks: Task[] = [
      { kind: "set", name: "a", set: {}, then: "exit" },
      { kind: "set", name: "b", set: {} },
    ];
    const endTasks: Task[] = [
      { kind: "set", name: "a", set: {}, then: "end" },
      { kind: "set", name: "b", set: {} },
    ];
    expect(deriveEdges(exitTasks).find((e) => e.source === "a")?.target).toBe("__end__");
    expect(deriveEdges(endTasks).find((e) => e.source === "a")?.target).toBe("__end__");
  });
});

describe("isPositionalEdge", () => {
  const tasks: Task[] = [
    { kind: "set", name: "a", set: {} },
    { kind: "set", name: "b", set: {}, then: "continue" },
    { kind: "set", name: "c", set: {}, then: "e" },
    { kind: "switch", name: "d", cases: [{ name: "case1", then: "exit" }] },
  ];

  it("is true for the Start->first-task edge", () => {
    expect(isPositionalEdge(tasks, { source: "__start__", target: "a" })).toBe(true);
  });

  it("is true for a task with no then, or then: continue", () => {
    expect(isPositionalEdge(tasks, { source: "a", target: "b" })).toBe(true);
    expect(isPositionalEdge(tasks, { source: "b", target: "c" })).toBe(true);
  });

  it("is false for a task with an explicit then", () => {
    expect(isPositionalEdge(tasks, { source: "c", target: "e" })).toBe(false);
  });

  it("is false for any switch case edge, regardless of the task's own then", () => {
    expect(
      isPositionalEdge(tasks, { source: "d", target: "__end__", sourceHandle: "case1" }),
    ).toBe(false);
  });

  it("is false for an edge whose source isn't a known task", () => {
    expect(isPositionalEdge(tasks, { source: "ghost", target: "a" })).toBe(false);
  });
});

describe("spliceTaskOnEdge", () => {
  const newTask: Task = { kind: "set", name: "inserted", set: {} };

  it("inserts into a positional edge via array order alone, with no then fields", () => {
    const tasks: Task[] = [
      { kind: "set", name: "a", set: {} },
      { kind: "set", name: "b", set: {} },
    ];
    const next = spliceTaskOnEdge(tasks, { source: "a", target: "b" }, newTask);
    expect(next).toEqual([
      { kind: "set", name: "a", set: {} },
      { kind: "set", name: "inserted", set: {} },
      { kind: "set", name: "b", set: {} },
    ]);
  });

  it("prepends when inserting on the Start edge", () => {
    const tasks: Task[] = [{ kind: "set", name: "a", set: {} }];
    const next = spliceTaskOnEdge(tasks, { source: "__start__", target: "a" }, newTask);
    expect(next.map((t) => t.name)).toEqual(["inserted", "a"]);
  });

  it("appends after the last task when inserting on a positional edge to End", () => {
    const tasks: Task[] = [{ kind: "set", name: "a", set: {} }];
    const next = spliceTaskOnEdge(tasks, { source: "a", target: "__end__" }, newTask);
    expect(next.map((t) => t.name)).toEqual(["a", "inserted"]);
  });

  it("rewrites a switch case's then to point at the new task, which then points at the old target", () => {
    const tasks: Task[] = [
      {
        kind: "switch",
        name: "route",
        cases: [
          { name: "vip", when: "${ .tier }", then: "vipPath" },
          { name: "default", then: "exit" },
        ],
      },
      { kind: "set", name: "vipPath", set: {} },
    ];
    const next = spliceTaskOnEdge(
      tasks,
      { source: "route", target: "vipPath", sourceHandle: "vip" },
      newTask,
    );
    const route = next.find((t) => t.name === "route");
    expect(route).toEqual({
      kind: "switch",
      name: "route",
      cases: [
        { name: "vip", when: "${ .tier }", then: "inserted" },
        { name: "default", then: "exit" },
      ],
    });
    expect(next.find((t) => t.name === "inserted")).toEqual({
      kind: "set",
      name: "inserted",
      set: {},
      then: "vipPath",
    });
    // Untouched case keeps its own target unchanged.
    expect((route as { cases: { name: string; then: string }[] }).cases[1].then).toBe("exit");
  });

  it("resolves End as the new task's then: exit when splicing on a switch case targeting End", () => {
    const tasks: Task[] = [
      { kind: "switch", name: "route", cases: [{ name: "default", then: "exit" }] },
    ];
    const next = spliceTaskOnEdge(
      tasks,
      { source: "route", target: "__end__", sourceHandle: "default" },
      newTask,
    );
    expect(next.find((t) => t.name === "inserted")).toMatchObject({ then: "exit" });
  });

  it("rewrites a plain task's explicit then the same way", () => {
    const tasks: Task[] = [
      { kind: "set", name: "a", set: {}, then: "c" },
      { kind: "set", name: "b", set: {} },
      { kind: "set", name: "c", set: {} },
    ];
    const next = spliceTaskOnEdge(tasks, { source: "a", target: "c" }, newTask);
    expect(next.find((t) => t.name === "a")).toMatchObject({ then: "inserted" });
    expect(next.find((t) => t.name === "inserted")).toMatchObject({ then: "c" });
  });

  it("returns tasks unchanged for an edge whose source isn't a known task", () => {
    const tasks: Task[] = [{ kind: "set", name: "a", set: {} }];
    expect(spliceTaskOnEdge(tasks, { source: "ghost", target: "a" }, newTask)).toBe(tasks);
  });
});

describe("reconnectEdgeTarget", () => {
  it("sets a plain task's then to the new target", () => {
    const tasks: Task[] = [
      { kind: "set", name: "a", set: {} },
      { kind: "set", name: "b", set: {} },
      { kind: "set", name: "c", set: {} },
    ];
    const next = reconnectEdgeTarget(tasks, { source: "a", target: "b" }, "c");
    expect(next?.find((t) => t.name === "a")).toMatchObject({ then: "c" });
  });

  it("resolves reconnecting to End as then: exit", () => {
    const tasks: Task[] = [{ kind: "set", name: "a", set: {} }];
    const next = reconnectEdgeTarget(tasks, { source: "a", target: "__end__" }, "__end__");
    expect(next?.find((t) => t.name === "a")).toMatchObject({ then: "exit" });
  });

  it("updates only the matching switch case's then", () => {
    const tasks: Task[] = [
      {
        kind: "switch",
        name: "route",
        cases: [
          { name: "vip", when: "${ .tier }", then: "vipPath" },
          { name: "default", then: "exit" },
        ],
      },
    ];
    const next = reconnectEdgeTarget(
      tasks,
      { source: "route", target: "vipPath", sourceHandle: "vip" },
      "otherPath",
    );
    const route = next?.find((t) => t.name === "route");
    expect(route).toEqual({
      kind: "switch",
      name: "route",
      cases: [
        { name: "vip", when: "${ .tier }", then: "otherPath" },
        { name: "default", then: "exit" },
      ],
    });
  });

  it("moves the new target to the front when reconnecting Start's edge", () => {
    const tasks: Task[] = [
      { kind: "set", name: "a", set: {} },
      { kind: "set", name: "b", set: {} },
      { kind: "set", name: "c", set: {} },
    ];
    const next = reconnectEdgeTarget(tasks, { source: "__start__", target: "a" }, "c");
    expect(next?.map((t) => t.name)).toEqual(["c", "a", "b"]);
  });

  it("rejects reconnecting Start's edge directly to End", () => {
    const tasks: Task[] = [{ kind: "set", name: "a", set: {} }];
    expect(reconnectEdgeTarget(tasks, { source: "__start__", target: "a" }, "__end__")).toBeUndefined();
  });

  it("rejects an edge whose source isn't a known task", () => {
    const tasks: Task[] = [{ kind: "set", name: "a", set: {} }];
    expect(reconnectEdgeTarget(tasks, { source: "ghost", target: "a" }, "a")).toBeUndefined();
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
