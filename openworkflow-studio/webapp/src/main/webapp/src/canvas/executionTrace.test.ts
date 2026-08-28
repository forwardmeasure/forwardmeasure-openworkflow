import { describe, expect, it } from "vitest";
import { buildTraceMap, parseTaskPath, traceKey } from "./executionTrace";

describe("parseTaskPath", () => {
  it("parses a top-level task", () => {
    expect(parseTaskPath("/do/0/getPet")).toEqual({
      containerPath: [],
      taskName: "getPet",
    });
  });

  it("parses a task nested one level inside a do", () => {
    expect(parseTaskPath("/do/0/group/do/0/getPet")).toEqual({
      containerPath: ["group"],
      taskName: "getPet",
    });
  });

  it("parses a task nested two levels deep", () => {
    expect(parseTaskPath("/do/1/outer/do/0/inner/do/2/leaf")).toEqual({
      containerPath: ["outer", "inner"],
      taskName: "leaf",
    });
  });

  it("parses a task inside a try block", () => {
    expect(parseTaskPath("/do/0/tryStep/try/0/getPet")).toEqual({
      containerPath: ["tryStep"],
      taskName: "getPet",
    });
  });

  it("parses a task inside a try's catch block", () => {
    expect(parseTaskPath("/do/0/tryStep/catch/do/0/notify")).toEqual({
      containerPath: ["tryStep"],
      taskName: "notify",
    });
  });

  it("unescapes JSON Pointer ~0/~1 in task names", () => {
    expect(parseTaskPath("/do/0/a~1b~0c")).toEqual({
      containerPath: [],
      taskName: "a/b~c",
    });
  });

  it("returns undefined for a fork branch pointer (not yet resolvable)", () => {
    expect(parseTaskPath("/do/0/branch/fork/branches/0")).toBeUndefined();
  });

  it("returns undefined for an empty or malformed pointer", () => {
    expect(parseTaskPath("")).toBeUndefined();
    expect(parseTaskPath("/do/0")).toBeUndefined();
  });
});

describe("traceKey", () => {
  it("is stable and distinguishes different container paths", () => {
    expect(traceKey([], "getPet")).not.toBe(traceKey(["group"], "getPet"));
    expect(traceKey(["group"], "getPet")).toBe(traceKey(["group"], "getPet"));
  });
});

describe("buildTraceMap", () => {
  const at = (seconds: number) => new Date(2026, 0, 1, 0, 0, seconds);

  it("marks a task entered, then completed, taking the later status", () => {
    const trace = buildTraceMap([
      { type: "TASK_ENTERED", taskPath: "/do/0/getPet", occurredAt: at(1) },
      { type: "TASK_COMPLETED", taskPath: "/do/0/getPet", occurredAt: at(2) },
    ]);
    expect(trace.get(traceKey([], "getPet"))?.status).toBe("completed");
  });

  it("marks a task failed on ERROR_RAISED, and failure wins even if a later event exists", () => {
    const trace = buildTraceMap([
      { type: "TASK_ENTERED", taskPath: "/do/0/getPet", occurredAt: at(1) },
      {
        type: "ERROR_RAISED",
        taskPath: "/do/0/getPet",
        occurredAt: at(2),
        data: { message: "connection refused" },
      },
      { type: "TASK_ENTERED", taskPath: "/do/1/notify", occurredAt: at(3) },
    ]);
    expect(trace.get(traceKey([], "getPet"))).toEqual({
      status: "failed",
      occurredAt: at(2),
      message: "connection refused",
    });
    expect(trace.get(traceKey([], "notify"))?.status).toBe("entered");
  });

  it("ignores execution-level events with no taskPath", () => {
    const trace = buildTraceMap([
      { type: "STARTED", occurredAt: at(0) },
      { type: "TASK_ENTERED", taskPath: "/do/0/getPet", occurredAt: at(1) },
      { type: "COMPLETED", occurredAt: at(5) },
    ]);
    expect(trace.size).toBe(1);
    expect(trace.get(traceKey([], "getPet"))?.status).toBe("entered");
  });

  it("silently drops entries whose taskPath doesn't parse (e.g. a fork branch)", () => {
    const trace = buildTraceMap([
      {
        type: "TASK_ENTERED",
        taskPath: "/do/0/branch/fork/branches/0",
        occurredAt: at(1),
      },
    ]);
    expect(trace.size).toBe(0);
  });

  it("resolves nested-task paths to their container-scoped key", () => {
    const trace = buildTraceMap([
      {
        type: "TASK_COMPLETED",
        taskPath: "/do/0/group/do/0/inner1",
        occurredAt: at(1),
      },
    ]);
    expect(trace.get(traceKey(["group"], "inner1"))?.status).toBe("completed");
    expect(trace.get(traceKey([], "inner1"))).toBeUndefined();
  });
});
