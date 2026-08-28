// A real LCS-based line diff, replacing workflow.ts's old lineDiff - that
// implementation compared lines purely by index, so a single inserted line
// shifted everything after it and marked the whole rest of the file as
// changed. This walks the standard dynamic-programming LCS table instead,
// so an insertion/deletion in the middle of a source only shows up as the
// lines that actually changed.
//
// O(n*m) time/space - fine here since workflow YAML sources are small
// (tens to low hundreds of lines), same scale assumption the old
// implementation already made.

export type DiffLineKind = "context" | "add" | "remove";

export type DiffLine = {
  kind: DiffLineKind;
  text: string;
  beforeLine?: number;
  afterLine?: number;
};

export function diffLines(before: string, after: string): DiffLine[] {
  const a = before.split("\n");
  const b = after.split("\n");
  const n = a.length;
  const m = b.length;

  const lcs: number[][] = Array.from({ length: n + 1 }, () =>
    new Array(m + 1).fill(0),
  );
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      lcs[i][j] =
        a[i] === b[j] ? lcs[i + 1][j + 1] + 1 : Math.max(lcs[i + 1][j], lcs[i][j + 1]);
    }
  }

  const result: DiffLine[] = [];
  let i = 0;
  let j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j]) {
      result.push({ kind: "context", text: a[i], beforeLine: i + 1, afterLine: j + 1 });
      i += 1;
      j += 1;
    } else if (lcs[i + 1][j] >= lcs[i][j + 1]) {
      result.push({ kind: "remove", text: a[i], beforeLine: i + 1 });
      i += 1;
    } else {
      result.push({ kind: "add", text: b[j], afterLine: j + 1 });
      j += 1;
    }
  }
  while (i < n) {
    result.push({ kind: "remove", text: a[i], beforeLine: i + 1 });
    i += 1;
  }
  while (j < m) {
    result.push({ kind: "add", text: b[j], afterLine: j + 1 });
    j += 1;
  }
  return result;
}

export function diffStats(lines: DiffLine[]): { added: number; removed: number } {
  let added = 0;
  let removed = 0;
  for (const line of lines) {
    if (line.kind === "add") added += 1;
    else if (line.kind === "remove") removed += 1;
  }
  return { added, removed };
}
