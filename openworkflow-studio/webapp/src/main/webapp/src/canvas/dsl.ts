import { dump, load } from "js-yaml";

// Task vocabulary: "set" (assign variables), "call" (invoke a
// function/HTTP/OpenAPI operation), "switch" (branch to a named task by a
// "when" condition), "raise" (terminate with a Problem-Details error),
// "wait" (pause for a duration), and "emit" (publish a CloudEvent) are
// modeled here - the Serverless Workflow DSL (https://serverlessworkflow.io,
// document.dsl "1.0.x") task kinds that need no nested-children support.
// "do"/"for"/"fork"/"try"/"listen"/"run" all carry nested task lists and are
// still out of scope until the canvas has a container/drill-down story.
// "switch" case targets ("then") are always either another task's name
// elsewhere in this flow, or the literal "exit" - a case omitting "then"
// relies on the spec's positional-fallthrough default, which this slice
// doesn't model (see switchCaseFromYamlEntry below) - use Source view for
// that shape.
//
// Every task kind also shares a common set of cross-cutting properties
// ("if"/"input"/"output"/"export"/"timeout"/"metadata") the spec allows on
// any task - CommonTaskProps below, spread into each kind-specific type.
export type TaskType = "set" | "call" | "switch" | "raise" | "wait" | "emit";

// All optional: a task with none of these set is the common case, and
// omitting an unset property from the serialized YAML (rather than writing
// it out as null/{}) is what taskToYamlEntry relies on below.
export type CommonTaskProps = {
  if?: string;
  input?: unknown;
  output?: unknown;
  export?: unknown;
  // A duration literal/expression (e.g. "PT30S"), or a name referencing the
  // workflow's "use.timeouts" catalog - both are plain strings on the wire,
  // so this stays untyped rather than modeling the (rarer) inline-object
  // timeout-definition shape.
  timeout?: unknown;
  metadata?: unknown;
};

export type SetTask = CommonTaskProps & {
  kind: "set";
  name: string;
  set: Record<string, unknown>;
};

export type CallTask = CommonTaskProps & {
  kind: "call";
  name: string;
  call: string;
  with: Record<string, unknown>;
};

export type SwitchCase = {
  name: string;
  // Omitted on the catch-all/default case.
  when?: string;
  // Another task's name in this flow, or the literal "exit".
  then: string;
};

export type SwitchTask = CommonTaskProps & {
  kind: "switch";
  name: string;
  cases: SwitchCase[];
};

// RFC 9457 Problem Details, matching the backend's ErrorPlan fields exactly.
export type RaiseError = {
  type: string;
  status: number;
  title: string;
  instance?: string;
  detail?: string;
};

export type RaiseTask = CommonTaskProps & {
  kind: "raise";
  name: string;
  error: RaiseError;
};

export type WaitTask = CommonTaskProps & {
  kind: "wait";
  name: string;
  // An ISO-8601 duration literal/expression (the common case, a plain
  // string) - or the rarer inline breakdown object ({days, hours, ...}),
  // kept as-is rather than normalized to one shape.
  wait: string | Record<string, unknown>;
};

export type EmitTask = CommonTaskProps & {
  kind: "emit";
  name: string;
  // The "emit.event.with" CloudEvents attribute template - same open-ended
  // shape as "call"'s "with", so it reuses that field's raw-JSON editing.
  with: Record<string, unknown>;
};

export type Task =
  | SetTask
  | CallTask
  | SwitchTask
  | RaiseTask
  | WaitTask
  | EmitTask;

export type TaskGraph = {
  tasks: Task[];
};

function switchCaseFromYamlEntry(
  switchName: string,
  entry: Record<string, unknown>,
): SwitchCase {
  const caseName = Object.keys(entry)[0];
  const body = (entry[caseName] ?? {}) as Record<string, unknown>;
  if (typeof body.then !== "string") {
    throw new UnsupportedTaskError(
      `${switchName} (case "${caseName}" has no "then" - positional ` +
        `fallthrough isn't supported here)`,
    );
  }
  return {
    name: caseName,
    when: typeof body.when === "string" ? body.when : undefined,
    then: body.then,
  };
}

function commonPropsFromYamlEntry(body: Record<string, unknown>): CommonTaskProps {
  const props: CommonTaskProps = {};
  if (typeof body.if === "string") props.if = body.if;
  if ("input" in body) props.input = body.input;
  if ("output" in body) props.output = body.output;
  if ("export" in body) props.export = body.export;
  if ("timeout" in body) props.timeout = body.timeout;
  if ("metadata" in body) props.metadata = body.metadata;
  return props;
}

function taskFromYamlEntry(entry: Record<string, unknown>): Task {
  const name = Object.keys(entry)[0];
  const body = entry[name] as Record<string, unknown>;
  const common = commonPropsFromYamlEntry(body);
  if ("set" in body) {
    return {
      kind: "set",
      name,
      set: (body.set as Record<string, unknown>) ?? {},
      ...common,
    };
  }
  if ("call" in body) {
    return {
      kind: "call",
      name,
      call: String(body.call),
      with: (body.with as Record<string, unknown>) ?? {},
      ...common,
    };
  }
  if ("switch" in body) {
    const rawCases = Array.isArray(body.switch) ? body.switch : [];
    return {
      kind: "switch",
      name,
      cases: rawCases.map((rawCase) =>
        switchCaseFromYamlEntry(name, rawCase as Record<string, unknown>),
      ),
      ...common,
    };
  }
  if ("raise" in body) {
    const raiseBody = (body.raise ?? {}) as Record<string, unknown>;
    const error = (raiseBody.error ?? {}) as Record<string, unknown>;
    if (typeof error.type !== "string" || typeof error.title !== "string") {
      throw new UnsupportedTaskError(
        `${name} (raise.error needs at least "type" and "title" - a named ` +
          `reference into "use.errors" isn't supported here)`,
      );
    }
    return {
      kind: "raise",
      name,
      error: {
        type: error.type,
        status: typeof error.status === "number" ? error.status : 0,
        title: error.title,
        instance: typeof error.instance === "string" ? error.instance : undefined,
        detail: typeof error.detail === "string" ? error.detail : undefined,
      },
      ...common,
    };
  }
  if ("wait" in body) {
    const rawWait = body.wait;
    return {
      kind: "wait",
      name,
      wait:
        typeof rawWait === "string"
          ? rawWait
          : ((rawWait as Record<string, unknown>) ?? {}),
      ...common,
    };
  }
  if ("emit" in body) {
    const emitBody = (body.emit ?? {}) as Record<string, unknown>;
    const event = (emitBody.event ?? {}) as Record<string, unknown>;
    return {
      kind: "emit",
      name,
      with: (event.with as Record<string, unknown>) ?? {},
      ...common,
    };
  }
  throw new UnsupportedTaskError(name);
}

export class UnsupportedTaskError extends Error {
  constructor(public readonly taskName: string) {
    super(
      `Task "${taskName}" uses a construct the canvas doesn't support yet ` +
        `(only "set", "call", "switch", "raise", "wait", and "emit" tasks ` +
        `are editable here) - edit it in Source view instead.`,
    );
  }
}

/**
 * Parses only the "do:" list into an editable, linear task sequence.
 * Everything else in the source (document metadata, comments, formatting)
 * is intentionally NOT modeled here - toYaml() below re-reads the ORIGINAL
 * source and only replaces the "do:" block, so unrelated content survives
 * a round trip even though this isn't a fully lossless (comment-preserving)
 * parser for the tasks themselves the way the plain YAML source editor is.
 */
export function fromYaml(source: string): TaskGraph {
  const parsed = load(source) as { do?: Array<Record<string, unknown>> };
  const entries = Array.isArray(parsed?.do) ? parsed.do : [];
  return { tasks: entries.map(taskFromYamlEntry) };
}

function commonPropsToYamlEntry(task: CommonTaskProps): Record<string, unknown> {
  const props: Record<string, unknown> = {};
  if (task.if !== undefined) props.if = task.if;
  if (task.input !== undefined) props.input = task.input;
  if (task.output !== undefined) props.output = task.output;
  if (task.export !== undefined) props.export = task.export;
  if (task.timeout !== undefined) props.timeout = task.timeout;
  if (task.metadata !== undefined) props.metadata = task.metadata;
  return props;
}

function taskToYamlEntry(task: Task): Record<string, unknown> {
  const common = commonPropsToYamlEntry(task);
  if (task.kind === "set") {
    return { [task.name]: { ...common, set: task.set } };
  }
  if (task.kind === "call") {
    return { [task.name]: { ...common, call: task.call, with: task.with } };
  }
  if (task.kind === "switch") {
    return {
      [task.name]: {
        ...common,
        switch: task.cases.map((switchCase) => ({
          [switchCase.name]: switchCase.when
            ? { when: switchCase.when, then: switchCase.then }
            : { then: switchCase.then },
        })),
      },
    };
  }
  if (task.kind === "raise") {
    const error: Record<string, unknown> = {
      type: task.error.type,
      status: task.error.status,
      title: task.error.title,
    };
    if (task.error.instance !== undefined) error.instance = task.error.instance;
    if (task.error.detail !== undefined) error.detail = task.error.detail;
    return { [task.name]: { ...common, raise: { error } } };
  }
  if (task.kind === "wait") {
    return { [task.name]: { ...common, wait: task.wait } };
  }
  return {
    [task.name]: { ...common, emit: { event: { with: task.with } } },
  };
}

/**
 * Replaces the "do:" list in `source` with `graph`'s tasks, leaving every
 * other top-level key (document metadata, anything else already in the
 * file) exactly as parsed - not regenerating the whole document from
 * scratch, so a full round trip through the canvas can't silently drop
 * fields this task vocabulary doesn't know about.
 */
export function toYaml(source: string, graph: TaskGraph): string {
  const parsed = (load(source) as Record<string, unknown>) ?? {};
  parsed.do = graph.tasks.map(taskToYamlEntry);
  return dump(parsed, { lineWidth: -1 });
}

export function emptyTask(kind: TaskType, name: string): Task {
  if (kind === "set") return { kind: "set", name, set: {} };
  if (kind === "call") return { kind: "call", name, call: "", with: {} };
  if (kind === "switch") {
    return { kind: "switch", name, cases: [{ name: "default", then: "exit" }] };
  }
  if (kind === "raise") {
    return { kind: "raise", name, error: { type: "", status: 400, title: "" } };
  }
  if (kind === "wait") return { kind: "wait", name, wait: "" };
  return { kind: "emit", name, with: {} };
}
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
