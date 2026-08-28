import { describe, expect, it } from "vitest";
import { diffLines, diffStats } from "./textDiff";

describe("diffLines", () => {
  it("marks identical text as all context", () => {
    const lines = diffLines("a\nb\nc", "a\nb\nc");
    expect(lines.every((line) => line.kind === "context")).toBe(true);
    expect(lines.map((line) => line.text)).toEqual(["a", "b", "c"]);
  });

  it("finds a single-line insertion without disturbing the rest", () => {
    const lines = diffLines("a\nb\nc", "a\nx\nb\nc");
    expect(lines).toEqual([
      { kind: "context", text: "a", beforeLine: 1, afterLine: 1 },
      { kind: "add", text: "x", afterLine: 2 },
      { kind: "context", text: "b", beforeLine: 2, afterLine: 3 },
      { kind: "context", text: "c", beforeLine: 3, afterLine: 4 },
    ]);
  });

  it("finds a single-line deletion without disturbing the rest", () => {
    const lines = diffLines("a\nb\nc", "a\nc");
    expect(lines).toEqual([
      { kind: "context", text: "a", beforeLine: 1, afterLine: 1 },
      { kind: "remove", text: "b", beforeLine: 2 },
      { kind: "context", text: "c", beforeLine: 3, afterLine: 2 },
    ]);
  });

  it("does not cascade every later line as changed after an insertion", () => {
    // The old positional lineDiff() would have marked b/c/d as all changed
    // here purely because they shifted down by one index.
    const lines = diffLines("a\nb\nc\nd", "a\nx\nb\nc\nd");
    const nonContext = lines.filter((line) => line.kind !== "context");
    expect(nonContext).toEqual([{ kind: "add", text: "x", afterLine: 2 }]);
  });

  it("pairs a same-index changed line as remove+add, not a false context match", () => {
    const lines = diffLines("hello", "goodbye");
    expect(lines).toEqual([
      { kind: "remove", text: "hello", beforeLine: 1 },
      { kind: "add", text: "goodbye", afterLine: 1 },
    ]);
  });

  it("handles an empty before or after", () => {
    expect(diffLines("", "a\nb")).toEqual([
      { kind: "remove", text: "", beforeLine: 1 },
      { kind: "add", text: "a", afterLine: 1 },
      { kind: "add", text: "b", afterLine: 2 },
    ]);
  });
});

describe("diffStats", () => {
  it("counts added and removed lines", () => {
    const lines = diffLines("a\nb\nc", "a\nx\nc");
    expect(diffStats(lines)).toEqual({ added: 1, removed: 1 });
  });
});
