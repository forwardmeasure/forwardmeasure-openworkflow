import { describe, expect, it } from "vitest";
import { diffWorkflows } from "./workflowDiff";

const DOC_HEADER = `document:
  dsl: "1.0.0"
  namespace: forwardmeasure
  name: sample
  version: "1.0.0"
`;

function workflow(doTasks: string): string {
  return `${DOC_HEADER}do:\n${doTasks}`;
}

describe("diffWorkflows", () => {
  it("reports no changes for identical sources", () => {
    const source = workflow(
      "  - greet:\n      set:\n        message: hi\n",
    );
    const result = diffWorkflows(source, source);
    expect(result).toEqual({ available: true, changes: [] });
  });

  it("detects an added top-level task", () => {
    const before = workflow("  - greet:\n      set:\n        message: hi\n");
    const after = workflow(
      "  - greet:\n      set:\n        message: hi\n" +
        "  - notify:\n      emit:\n        event:\n          with:\n            type: com.example.done\n",
    );
    const result = diffWorkflows(before, after);
    expect(result.available).toBe(true);
    if (!result.available) throw new Error("unreachable");
    expect(result.changes).toEqual([
      { changeKind: "added", path: [], name: "notify", kind: "emit" },
    ]);
  });

  it("detects a removed top-level task", () => {
    const before = workflow(
      "  - greet:\n      set:\n        message: hi\n" +
        "  - notify:\n      emit:\n        event:\n          with:\n            type: com.example.done\n",
    );
    const after = workflow("  - greet:\n      set:\n        message: hi\n");
    const result = diffWorkflows(before, after);
    expect(result.available).toBe(true);
    if (!result.available) throw new Error("unreachable");
    expect(result.changes).toEqual([
      { changeKind: "removed", path: [], name: "notify", kind: "emit" },
    ]);
  });

  it("detects a modified task's scalar fields, not just presence", () => {
    const before = workflow("  - greet:\n      set:\n        message: hi\n");
    const after = workflow("  - greet:\n      set:\n        message: hello\n");
    const result = diffWorkflows(before, after);
    expect(result.available).toBe(true);
    if (!result.available) throw new Error("unreachable");
    expect(result.changes).toEqual([
      { changeKind: "modified", path: [], name: "greet", kind: "set" },
    ]);
  });

  it("detects a task whose kind changed", () => {
    const before = workflow("  - step:\n      set:\n        message: hi\n");
    const after = workflow("  - step:\n      call: http\n");
    const result = diffWorkflows(before, after);
    expect(result.available).toBe(true);
    if (!result.available) throw new Error("unreachable");
    expect(result.changes).toEqual([
      { changeKind: "modified", path: [], name: "step", kind: "call", previousKind: "set" },
    ]);
  });

  it("reports a nested task's path inside a do container", () => {
    const before = workflow("  - group:\n      do:\n        - inner:\n            set:\n              a: 1\n");
    const after = workflow(
      "  - group:\n      do:\n        - inner:\n            set:\n              a: 1\n" +
        "        - second:\n            set:\n              b: 2\n",
    );
    const result = diffWorkflows(before, after);
    expect(result.available).toBe(true);
    if (!result.available) throw new Error("unreachable");
    expect(result.changes).toEqual([
      { changeKind: "added", path: ["group"], name: "second", kind: "set" },
    ]);
  });

  it("does not mark an unchanged task as modified just because a sibling shifted", () => {
    const before = workflow(
      "  - a:\n      set:\n        x: 1\n  - b:\n      set:\n        y: 2\n",
    );
    const after = workflow(
      "  - inserted:\n      set:\n        z: 0\n" +
        "  - a:\n      set:\n        x: 1\n  - b:\n      set:\n        y: 2\n",
    );
    const result = diffWorkflows(before, after);
    expect(result.available).toBe(true);
    if (!result.available) throw new Error("unreachable");
    expect(result.changes).toEqual([
      { changeKind: "added", path: [], name: "inserted", kind: "set" },
    ]);
  });

  it("walks a try task's catch block as its own path", () => {
    const before = workflow(
      "  - guarded:\n      try:\n        - inner:\n            set:\n              a: 1\n" +
        "      catch:\n        do:\n          - recover:\n              set:\n                r: 1\n",
    );
    const after = workflow(
      "  - guarded:\n      try:\n        - inner:\n            set:\n              a: 1\n" +
        "      catch:\n        do:\n          - recover:\n              set:\n                r: 2\n",
    );
    const result = diffWorkflows(before, after);
    expect(result.available).toBe(true);
    if (!result.available) throw new Error("unreachable");
    expect(result.changes).toEqual([
      { changeKind: "modified", path: ["guarded", "catch"], name: "recover", kind: "set" },
    ]);
  });

  it("returns available:false with a reason when a source fails to parse", () => {
    const before = workflow("  - greet:\n      set:\n        message: hi\n");
    const after = `${DOC_HEADER}do:\n  - broken:\n      notAKnownTaskKeyword: {}\n`;
    const result = diffWorkflows(before, after);
    expect(result.available).toBe(false);
    if (result.available) throw new Error("unreachable");
    expect(result.reason).toContain("current source");
  });
});
