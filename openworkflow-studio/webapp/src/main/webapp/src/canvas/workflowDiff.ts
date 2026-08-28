// A structural diff over the parsed task tree, not the YAML text - "task
// `fetchPet` was added under `group`" instead of "17 lines changed
// somewhere." Flowise never shipped a diff UI at all (an unmerged
// community PR at the project's sunset), so there's no reference to match;
// this is closer to what a git-aware IDE gives you for a renamed function
// than a plain text diff would.
//
// Walks both trees with the same generic hasChildren() check dsl.ts's own
// tasksAtPath/setChildrenAtPath use, so a future container kind is picked
// up here for free. try's catch block is a second, separate task list
// (CatchClause.children) that hasChildren() doesn't see on the TryTask
// itself, so it's walked explicitly, one path segment ("catch") below its
// try task's own path.

import {
  fromYaml,
  hasChildren,
  type Task,
  type TaskType,
} from "./dsl";

export type FlatTask = {
  key: string;
  path: string[];
  name: string;
  kind: TaskType;
  fingerprint: string;
};

export type TaskChangeKind = "added" | "removed" | "modified";

export type TaskChange = {
  changeKind: TaskChangeKind;
  path: string[];
  name: string;
  kind: TaskType;
  previousKind?: TaskType;
};

export type WorkflowDiffResult =
  | { available: true; changes: TaskChange[] }
  | { available: false; reason: string };

// Everything about a task except its nested children - two tasks with the
// same name/kind/path but different scalar fields (a changed `if`, a
// different retry policy, a renamed catch variable) still need to read as
// "modified", not "unchanged".
function fingerprint(task: Task): string {
  const copy: Record<string, unknown> = { ...(task as unknown as Record<string, unknown>) };
  delete copy.children;
  if (task.kind === "try") {
    const catchRest: Record<string, unknown> = { ...task.catchClause };
    delete catchRest.children;
    copy.catchClause = catchRest;
  }
  return JSON.stringify(copy);
}

function flatten(tasks: Task[], path: string[], out: FlatTask[]): void {
  for (const task of tasks) {
    const key = [...path, task.name].join("/");
    out.push({
      key,
      path,
      name: task.name,
      kind: task.kind,
      fingerprint: fingerprint(task),
    });
    if (hasChildren(task)) {
      flatten(task.children, [...path, task.name], out);
    }
    if (task.kind === "try") {
      flatten(task.catchClause.children, [...path, task.name, "catch"], out);
    }
  }
}

function flattenSource(source: string): FlatTask[] {
  const out: FlatTask[] = [];
  flatten(fromYaml(source).tasks, [], out);
  return out;
}

export function diffWorkflows(before: string, after: string): WorkflowDiffResult {
  let beforeTasks: FlatTask[];
  let afterTasks: FlatTask[];
  try {
    beforeTasks = flattenSource(before);
  } catch (error) {
    return { available: false, reason: `previous revision: ${(error as Error).message}` };
  }
  try {
    afterTasks = flattenSource(after);
  } catch (error) {
    return { available: false, reason: `current source: ${(error as Error).message}` };
  }

  const beforeByKey = new Map(beforeTasks.map((task) => [task.key, task]));
  const afterByKey = new Map(afterTasks.map((task) => [task.key, task]));
  const changes: TaskChange[] = [];

  for (const task of afterTasks) {
    const previous = beforeByKey.get(task.key);
    if (!previous) {
      changes.push({ changeKind: "added", path: task.path, name: task.name, kind: task.kind });
    } else if (previous.kind !== task.kind || previous.fingerprint !== task.fingerprint) {
      changes.push({
        changeKind: "modified",
        path: task.path,
        name: task.name,
        kind: task.kind,
        previousKind: previous.kind !== task.kind ? previous.kind : undefined,
      });
    }
  }
  for (const task of beforeTasks) {
    if (!afterByKey.has(task.key)) {
      changes.push({ changeKind: "removed", path: task.path, name: task.name, kind: task.kind });
    }
  }

  return { available: true, changes };
}
