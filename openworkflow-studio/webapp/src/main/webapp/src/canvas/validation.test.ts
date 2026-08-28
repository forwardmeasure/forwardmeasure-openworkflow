import { describe, expect, it } from "vitest";
import {
  parseContractViolations,
  taskPathForPointer,
  validateWorkflowSource,
} from "./validation";

const HEADER = `document:
  dsl: "1.0.0"
  namespace: forwardmeasure
  name: sample
  version: "1.0.0"
`;

function workflow(doBlock: string): string {
  return `${HEADER}do:\n${doBlock}`;
}

describe("validateWorkflowSource", () => {
  it("reports no issues for a valid workflow", () => {
    const source = workflow("  - greet:\n      set:\n        message: hi\n");
    expect(validateWorkflowSource(source)).toEqual([]);
  });

  it("flags an empty set (the reported real-world case) as exactly its own 2 branches, no 12-way cascade", () => {
    // Verified against real AJV output, not assumed: every error whose
    // instancePath is the task's own root ("/do/0/greet") and whose
    // keyword is required/unevaluatedProperties/oneOf is "which of the 12
    // kinds" noise once "set" is already known to be present - only
    // set's own {object-with->=1-property, or string} oneOf branches,
    // one level deeper at "/do/0/greet/set", are real.
    const source = workflow("  - greet:\n      set: {}\n");
    const issues = validateWorkflowSource(source);
    expect(issues).toHaveLength(2);
    for (const issue of issues) {
      expect(issue.taskPath).toEqual({ containerPath: [], taskName: "greet" });
      expect(issue.pointer).toBe("/do/0/greet/set");
    }
    // AJV's exact wording varies by version - check the substance
    // (fewer than 1 properties, and the string alternative), not pinned
    // exact strings for both.
    expect(issues.some((issue) => /propert/i.test(issue.message))).toBe(true);
    expect(issues.some((issue) => /string/i.test(issue.message))).toBe(true);
  });

  it("reports nothing for a valid task sitting alongside an invalid nested one", () => {
    const source = workflow(
      "  - greet:\n      set:\n        message: hi\n" +
        "  - group:\n      do:\n        - inner:\n            set: {}\n",
    );
    const issues = validateWorkflowSource(source);
    expect(issues.every((issue) => issue.taskPath?.taskName !== "greet")).toBe(true);
    expect(issues.some((issue) => issue.taskPath?.taskName === "inner")).toBe(true);
  });

  it("collapses a task matching no known kind into exactly one clear message, not a cascade", () => {
    const source = workflow("  - mystery:\n      notAKnownKeyword: true\n");
    const issues = validateWorkflowSource(source);
    expect(issues).toHaveLength(1);
    expect(issues[0].taskPath).toEqual({ containerPath: [], taskName: "mystery" });
    expect(issues[0].message).toContain("doesn't match any known task kind");
  });

  it("attributes a nested task's error to its own path inside the container", () => {
    const source = workflow(
      "  - group:\n      do:\n        - inner:\n            set: {}\n",
    );
    const issues = validateWorkflowSource(source);
    const inner = issues.find((issue) => issue.taskPath?.taskName === "inner");
    expect(inner?.taskPath).toEqual({ containerPath: ["group"], taskName: "inner" });
  });

  it("attributes a workflow-level problem to no task at all", () => {
    // Missing "do" entirely - a document-level, not task-level, problem.
    const source = HEADER;
    const issues = validateWorkflowSource(source);
    expect(issues.length).toBeGreaterThan(0);
    expect(issues.every((issue) => issue.taskPath === undefined)).toBe(true);
  });

  it("does not double-report a YAML syntax error already shown by the canvas's own parser", () => {
    const source = "do:\n  - broken: [unclosed\n";
    expect(validateWorkflowSource(source)).toEqual([]);
  });
});

describe("taskPathForPointer", () => {
  it("resolves a top-level task", () => {
    expect(taskPathForPointer("/do/0/greet")).toEqual({
      containerPath: [],
      taskName: "greet",
    });
  });

  it("resolves a field deep inside a task to that task, not undefined", () => {
    expect(taskPathForPointer("/do/2/task3/raise/error/type")).toEqual({
      containerPath: [],
      taskName: "task3",
    });
  });

  it("resolves a nested task inside a do container", () => {
    expect(taskPathForPointer("/do/0/group/do/1/inner")).toEqual({
      containerPath: ["group"],
      taskName: "inner",
    });
  });

  it("resolves through a try task's catch block", () => {
    expect(taskPathForPointer("/do/0/guarded/catch/do/0/recover")).toEqual({
      containerPath: ["guarded"],
      taskName: "recover",
    });
  });

  it("returns undefined for a pointer with no task list at all", () => {
    expect(taskPathForPointer("/document/namespace")).toBeUndefined();
  });
});

describe("parseContractViolations", () => {
  it("attributes a real WorkflowContractAnalyzer violation to the consuming task", () => {
    // Verbatim shape from WorkflowContractAnalyzerTest's
    // rejectsAnIncompatibleSequentialTaskEdge (Java) - producer emits a
    // string, consumer requires an integer.
    const violations = [
      "Schema compatibility /do/0/produce/output/schema -> /do/1/consume/input/schema [INCOMPATIBLE] producer type string is not accepted",
    ];
    const issues = parseContractViolations(violations);
    expect(issues).toHaveLength(1);
    expect(issues[0].taskPath).toEqual({ containerPath: [], taskName: "consume" });
    expect(issues[0].message).toContain("/do/0/produce/output/schema");
    expect(issues[0].message).toContain("producer type string is not accepted");
    expect(issues[0].message).toContain("INCOMPATIBLE");
  });

  it("attributes a nested-container violation correctly", () => {
    const issues = parseContractViolations([
      "Schema compatibility /do/0/group/do/1/consume/input/schema -> /do/2/produce/output/schema [UNPROVEN] cannot prove pattern constraint inclusion",
    ]);
    expect(issues[0].taskPath).toEqual({ containerPath: [], taskName: "produce" });
  });

  it("falls back to an untargeted issue for a string that doesn't match the known shape", () => {
    const issues = parseContractViolations(["some other backend error entirely"]);
    expect(issues).toHaveLength(1);
    expect(issues[0].taskPath).toBeUndefined();
    expect(issues[0].message).toBe("some other backend error entirely");
  });

  it("returns an empty array for no violations", () => {
    expect(parseContractViolations([])).toEqual([]);
  });
});
