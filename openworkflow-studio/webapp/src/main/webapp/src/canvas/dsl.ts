import { dump, load } from "js-yaml";

// Task vocabulary: "set" (assign variables), "call" (invoke a
// function/HTTP/OpenAPI operation), "switch" (branch to a named task by a
// "when" condition), "raise" (terminate with a Problem-Details error),
// "wait" (pause for a duration), "emit" (publish a CloudEvent), and now
// "do" (a nested, ordered task group) are modeled here - the Serverless
// Workflow DSL (https://serverlessworkflow.io, document.dsl "1.0.x") task
// kinds this canvas covers so far. "do" is the first container kind - its
// "children" are edited by double-clicking into it on canvas (see
// WorkflowCanvas.tsx's drill-down), not inline in the Inspector, the same
// way the top-level "do:" list itself is edited. "for"/"fork"/"try" reuse
// this same drill-down once they land; "listen"/"run" don't need it at all.
// "switch" case targets ("then") are always either another task's name
// elsewhere in this flow, or the literal "exit" - a case omitting "then"
// relies on the spec's positional-fallthrough default, which this slice
// doesn't model (see switchCaseFromYamlEntry below) - use Source view for
// that shape.
//
// Every task kind also shares a common set of cross-cutting properties
// ("if"/"input"/"output"/"export"/"timeout"/"metadata") the spec allows on
// any task - CommonTaskProps below, spread into each kind-specific type.
export type TaskType =
  | "set"
  | "call"
  | "switch"
  | "raise"
  | "wait"
  | "emit"
  | "do"
  | "for"
  | "fork"
  | "try"
  | "listen"
  | "run";

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

// The first container kind: an ordered, nested task group with no other
// fields of its own. "children" is the same Task[] shape as the top-level
// "do:" list, so every helper that walks a task list (fromYaml/toYaml's
// list helpers below, layout/deriveEdges in WorkflowCanvas.tsx) works on it
// unchanged - the canvas just needs to know which list is "in view."
export type DoTask = CommonTaskProps & {
  kind: "do";
  name: string;
  children: Task[];
};

// The second container kind: iterates "collection" (an expression yielding
// an array), binding each element to "itemVariable" (and, optionally, its
// index to "indexVariable") for one run of "children" per element. Spec
// field names are "each"/"in"/"at" (see taskFromYamlEntry/taskToYamlEntry
// below) - named itemVariable/collection/indexVariable here instead so the
// Inspector's field labels aren't cryptic single letters.
export type ForTask = CommonTaskProps & {
  kind: "for";
  name: string;
  itemVariable: string;
  collection: string;
  indexVariable?: string;
  whileCondition?: string;
  children: Task[];
};

// The third container kind: runs every branch in "children" concurrently,
// each branch rejoining at one shared continuation once done ("compete:
// true" means the first branch to finish wins and the rest are cancelled,
// the default is every branch must finish). Spec-wise, "fork.branches" is
// structurally just another task list - the same shape as "do"'s children -
// so each branch here is one child task's own name/body, not a separate
// named-branch wrapper.
export type ForkTask = CommonTaskProps & {
  kind: "fork";
  name: string;
  compete: boolean;
  children: Task[];
};

// A Problem Details filter - unlike RaiseError, every field here is
// optional (an unset field matches any value on the actual error).
export type ErrorFilter = {
  type?: string;
  status?: number;
  instance?: string;
  title?: string;
  detail?: string;
};

// "delay"/"attemptDuration"/"totalDuration"/"jitterFrom"/"jitterTo" stay
// opaque (string or parsed JSON), same treatment as CommonTaskProps.timeout
// and WaitTask.wait - real values are almost always a plain ISO-8601
// duration string ("delay: {seconds: 3}" is the one fixture-confirmed
// object shape), and this survives whichever shape shows up without this
// canvas needing to model every duration variant the spec allows.
export type RetryPolicy = {
  delay?: unknown;
  backoff: "constant" | "linear" | "exponential";
  attemptCount?: number;
  attemptDuration?: unknown;
  totalDuration?: unknown;
  jitterFrom?: unknown;
  jitterTo?: unknown;
  when?: string;
  exceptWhen?: string;
};

export type CatchClause = {
  errors?: ErrorFilter;
  // Defaults to "error" server-side when omitted - kept undefined here
  // rather than writing that default in, so an untouched catch clause
  // round-trips without inventing a field the user never set.
  as?: string;
  when?: string;
  exceptWhen?: string;
  // Either a plain string naming a policy in "use.retries", or an inline
  // policy object.
  retry?: string | RetryPolicy;
  then?: string;
  // catch.do's recovery tasks - edited as raw JSON in the Inspector, NOT a
  // second canvas drill-down target the way "try"'s own block is. Doing
  // real dual drill-down would mean path segments needing to disambiguate
  // "task X's try block" from "task X's catch block", touching
  // tasksAtPath/setChildrenAtPath/the breadcrumb for every container kind,
  // not just try - deliberately out of scope here. Still parsed into real
  // Task[] (taskListFromYamlEntries), so nothing about it is opaque, only
  // how it's edited.
  children: Task[];
};

// The fourth container kind, and the only one with two task lists: its own
// "try:" block (children below, drillable exactly like do/for/fork) plus a
// "catch:" clause describing what to do if that block raises - which error
// to catch (errors), what variable to bind it to (as), optional retry
// policy, and its own recovery tasks (catchClause.children, see above).
export type TryTask = CommonTaskProps & {
  kind: "try";
  name: string;
  children: Task[];
  catchClause: CatchClause;
};

// The fifth container kind - surprising at first (the spec puts "foreach"
// as a SIBLING of "listen" at the task level, not nested inside it, unlike
// every other loop-shaped construct here), but structurally the same
// "optional loop, same children: Task[] shape" story as the rest: an empty
// "children" (foreach absent) is a plain "wait for one/all/any matching
// event" task; a non-empty "children" (foreach.do) runs those tasks once
// per consumed event, binding itemVariable/indexVariable from foreach.item/
// foreach.at.
export type ListenTask = CommonTaskProps & {
  kind: "listen";
  name: string;
  // The "to" event-consumption filter (one/all/any, with CloudEvents
  // attribute predicates and optional correlation/until) - kept opaque,
  // same reasoning as "emit"'s "with": a bag of filters isn't honestly
  // better as a generated form than as JSON.
  consumption?: unknown;
  readAs?: "data" | "envelope" | "raw";
  itemVariable?: string;
  indexVariable?: string;
  children: Task[];
};

// NOT a container - "run" has no nested task list in any variant, unlike
// every kind above. Which variant is active is a plain discriminant
// ("variant"); container/script/shell keep their own configuration object
// opaque (four meaningfully different open shapes - not worth four
// generated forms), but "workflow" gets real namespace/name/version/input
// fields since that's the one variant this canvas can usefully help fill
// in (no catalog-listing endpoint exists to autocomplete against, per
// Phase 1 planning - confirmed directly against openworkflow-api-
// specifications - so these stay plain text fields, not a picker).
export type RunTask = CommonTaskProps & {
  kind: "run";
  name: string;
  variant: "container" | "script" | "shell" | "workflow";
  configuration?: Record<string, unknown>;
  workflowNamespace?: string;
  workflowName?: string;
  workflowVersion?: string;
  workflowInput?: unknown;
  await?: boolean;
  returnMode?: "stdout" | "stderr" | "code" | "all" | "none";
};

export type Task =
  | SetTask
  | CallTask
  | SwitchTask
  | RaiseTask
  | WaitTask
  | EmitTask
  | DoTask
  | ForTask
  | ForkTask
  | TryTask
  | ListenTask
  | RunTask;

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

function errorFilterFromYamlBody(
  errorsBody: Record<string, unknown> | undefined,
): ErrorFilter | undefined {
  const withBody = errorsBody?.with as Record<string, unknown> | undefined;
  if (!withBody) return undefined;
  return {
    type: typeof withBody.type === "string" ? withBody.type : undefined,
    status: typeof withBody.status === "number" ? withBody.status : undefined,
    instance: typeof withBody.instance === "string" ? withBody.instance : undefined,
    title: typeof withBody.title === "string" ? withBody.title : undefined,
    detail: typeof withBody.detail === "string" ? withBody.detail : undefined,
  };
}

function retryFromYamlValue(
  rawRetry: unknown,
): string | RetryPolicy | undefined {
  if (typeof rawRetry === "string") return rawRetry;
  if (!rawRetry || typeof rawRetry !== "object") return undefined;
  const retryBody = rawRetry as Record<string, unknown>;
  const backoffBody = (retryBody.backoff ?? {}) as Record<string, unknown>;
  const limitBody = (retryBody.limit ?? {}) as Record<string, unknown>;
  const attemptBody = (limitBody.attempt ?? {}) as Record<string, unknown>;
  const jitterBody = (retryBody.jitter ?? {}) as Record<string, unknown>;
  return {
    delay: retryBody.delay,
    backoff:
      "linear" in backoffBody
        ? "linear"
        : "exponential" in backoffBody
          ? "exponential"
          : "constant",
    attemptCount: typeof attemptBody.count === "number" ? attemptBody.count : undefined,
    attemptDuration: attemptBody.duration,
    totalDuration: limitBody.duration,
    jitterFrom: jitterBody.from,
    jitterTo: jitterBody.to,
    when: typeof retryBody.when === "string" ? retryBody.when : undefined,
    exceptWhen: typeof retryBody.exceptWhen === "string" ? retryBody.exceptWhen : undefined,
  };
}

function catchClauseFromYamlBody(catchBody: Record<string, unknown>): CatchClause {
  const rawCatchDo = Array.isArray(catchBody.do) ? catchBody.do : [];
  return {
    errors: errorFilterFromYamlBody(catchBody.errors as Record<string, unknown> | undefined),
    as: typeof catchBody.as === "string" ? catchBody.as : undefined,
    when: typeof catchBody.when === "string" ? catchBody.when : undefined,
    exceptWhen: typeof catchBody.exceptWhen === "string" ? catchBody.exceptWhen : undefined,
    retry: retryFromYamlValue(catchBody.retry),
    then: typeof catchBody.then === "string" ? catchBody.then : undefined,
    children: taskListFromYamlEntries(
      rawCatchDo as Array<Record<string, unknown>>,
    ),
  };
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
  // Checked before "do": a "for" task also carries a "do:" key (its loop
  // body), so testing "do" first would misparse every "for" as a plain "do".
  if ("for" in body) {
    const forBody = (body.for ?? {}) as Record<string, unknown>;
    const rawChildren = Array.isArray(body.do) ? body.do : [];
    return {
      kind: "for",
      name,
      itemVariable: typeof forBody.each === "string" ? forBody.each : "item",
      collection: typeof forBody.in === "string" ? forBody.in : "",
      indexVariable: typeof forBody.at === "string" ? forBody.at : undefined,
      whileCondition: typeof body.while === "string" ? body.while : undefined,
      children: taskListFromYamlEntries(
        rawChildren as Array<Record<string, unknown>>,
      ),
      ...common,
    };
  }
  if ("fork" in body) {
    const forkBody = (body.fork ?? {}) as Record<string, unknown>;
    const rawBranches = Array.isArray(forkBody.branches) ? forkBody.branches : [];
    return {
      kind: "fork",
      name,
      compete: forkBody.compete === true,
      children: taskListFromYamlEntries(
        rawBranches as Array<Record<string, unknown>>,
      ),
      ...common,
    };
  }
  if ("try" in body) {
    const rawTrySteps = Array.isArray(body.try) ? body.try : [];
    const catchBody = (body.catch ?? {}) as Record<string, unknown>;
    return {
      kind: "try",
      name,
      children: taskListFromYamlEntries(
        rawTrySteps as Array<Record<string, unknown>>,
      ),
      catchClause: catchClauseFromYamlBody(catchBody),
      ...common,
    };
  }
  if ("listen" in body) {
    const listenBody = (body.listen ?? {}) as Record<string, unknown>;
    // "foreach" is a SIBLING of "listen" at the task-body level, not
    // nested inside it - confirmed directly against the real compiler and
    // its own conformance fixtures (listen-*.yaml), not assumed from spec
    // summary.
    const foreachBody = (body.foreach ?? {}) as Record<string, unknown>;
    const rawForeachDo = Array.isArray(foreachBody.do) ? foreachBody.do : [];
    return {
      kind: "listen",
      name,
      consumption: listenBody.to,
      readAs:
        listenBody.read === "data" ||
        listenBody.read === "envelope" ||
        listenBody.read === "raw"
          ? listenBody.read
          : undefined,
      itemVariable: typeof foreachBody.item === "string" ? foreachBody.item : undefined,
      indexVariable: typeof foreachBody.at === "string" ? foreachBody.at : undefined,
      children: taskListFromYamlEntries(
        rawForeachDo as Array<Record<string, unknown>>,
      ),
      ...common,
    };
  }
  if ("run" in body) {
    const runBody = (body.run ?? {}) as Record<string, unknown>;
    const variant: RunTask["variant"] =
      "container" in runBody
        ? "container"
        : "script" in runBody
          ? "script"
          : "shell" in runBody
            ? "shell"
            : "workflow";
    const rawReturnMode = runBody.return;
    const returnMode: RunTask["returnMode"] =
      rawReturnMode === "stdout" ||
      rawReturnMode === "stderr" ||
      rawReturnMode === "code" ||
      rawReturnMode === "all" ||
      rawReturnMode === "none"
        ? rawReturnMode
        : undefined;
    const base = {
      kind: "run" as const,
      name,
      variant,
      await: typeof runBody.await === "boolean" ? runBody.await : undefined,
      returnMode,
      ...common,
    };
    if (variant === "workflow") {
      const workflowBody = (runBody.workflow ?? {}) as Record<string, unknown>;
      return {
        ...base,
        workflowNamespace:
          typeof workflowBody.namespace === "string" ? workflowBody.namespace : undefined,
        workflowName: typeof workflowBody.name === "string" ? workflowBody.name : undefined,
        workflowVersion:
          typeof workflowBody.version === "string" ? workflowBody.version : undefined,
        workflowInput: workflowBody.input,
      };
    }
    return {
      ...base,
      configuration: (runBody[variant] as Record<string, unknown>) ?? {},
    };
  }
  if ("do" in body) {
    const rawChildren = Array.isArray(body.do) ? body.do : [];
    return {
      kind: "do",
      name,
      children: taskListFromYamlEntries(
        rawChildren as Array<Record<string, unknown>>,
      ),
      ...common,
    };
  }
  throw new UnsupportedTaskError(name);
}

// Shared by fromYaml (the top-level "do:" list) and taskFromYamlEntry's own
// "do" branch (a nested list at the same shape) - a task list is a task
// list regardless of nesting depth.
function taskListFromYamlEntries(entries: Array<Record<string, unknown>>): Task[] {
  return entries.map(taskFromYamlEntry);
}

export class UnsupportedTaskError extends Error {
  constructor(public readonly taskName: string) {
    super(
      // Every Serverless Workflow DSL task kind (set/call/switch/raise/
      // wait/emit/do/for/fork/try/listen/run) is modeled here as of this
      // task type list - reaching this means the task's own body doesn't
      // match any of their shapes (e.g. a switch case with no "then",
      // relying on positional fallthrough this canvas doesn't model - see
      // switchCaseFromYamlEntry above), not that a kind is unimplemented.
      `Task "${taskName}" uses a shape the canvas doesn't support yet - ` +
        `edit it in Source view instead.`,
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
  return { tasks: taskListFromYamlEntries(entries) };
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
  if (task.kind === "emit") {
    return {
      [task.name]: { ...common, emit: { event: { with: task.with } } },
    };
  }
  if (task.kind === "for") {
    const forObj: Record<string, unknown> = {
      each: task.itemVariable,
      in: task.collection,
    };
    if (task.indexVariable !== undefined) forObj.at = task.indexVariable;
    const entry: Record<string, unknown> = { ...common, for: forObj };
    if (task.whileCondition !== undefined) entry.while = task.whileCondition;
    entry.do = taskListToYamlEntries(task.children);
    return { [task.name]: entry };
  }
  if (task.kind === "fork") {
    const forkObj: Record<string, unknown> = {
      branches: taskListToYamlEntries(task.children),
    };
    if (task.compete) forkObj.compete = true;
    return { [task.name]: { ...common, fork: forkObj } };
  }
  if (task.kind === "try") {
    return {
      [task.name]: {
        ...common,
        try: taskListToYamlEntries(task.children),
        catch: catchClauseToYamlBody(task.catchClause),
      },
    };
  }
  if (task.kind === "listen") {
    const listenObj: Record<string, unknown> = {};
    if (task.consumption !== undefined) listenObj.to = task.consumption;
    if (task.readAs !== undefined) listenObj.read = task.readAs;
    const entry: Record<string, unknown> = { ...common, listen: listenObj };
    // "foreach" only appears at all if the task actually uses it - an
    // empty children list with no item/index binding is a plain listen,
    // not a (vacuous) foreach loop.
    if (
      task.itemVariable !== undefined ||
      task.indexVariable !== undefined ||
      task.children.length > 0
    ) {
      const foreachObj: Record<string, unknown> = {};
      if (task.itemVariable !== undefined) foreachObj.item = task.itemVariable;
      if (task.indexVariable !== undefined) foreachObj.at = task.indexVariable;
      foreachObj.do = taskListToYamlEntries(task.children);
      entry.foreach = foreachObj;
    }
    return { [task.name]: entry };
  }
  if (task.kind === "run") {
    const runObj: Record<string, unknown> = {};
    if (task.variant === "workflow") {
      const workflowObj: Record<string, unknown> = {};
      if (task.workflowNamespace !== undefined) workflowObj.namespace = task.workflowNamespace;
      if (task.workflowName !== undefined) workflowObj.name = task.workflowName;
      if (task.workflowVersion !== undefined) workflowObj.version = task.workflowVersion;
      if (task.workflowInput !== undefined) workflowObj.input = task.workflowInput;
      runObj.workflow = workflowObj;
    } else {
      runObj[task.variant] = task.configuration ?? {};
    }
    if (task.await !== undefined) runObj.await = task.await;
    if (task.returnMode !== undefined) runObj.return = task.returnMode;
    return { [task.name]: { ...common, run: runObj } };
  }
  return {
    [task.name]: { ...common, do: taskListToYamlEntries(task.children) },
  };
}

function errorFilterToYamlBody(errors: ErrorFilter): Record<string, unknown> | undefined {
  const withObj: Record<string, unknown> = {};
  if (errors.type !== undefined) withObj.type = errors.type;
  if (errors.status !== undefined) withObj.status = errors.status;
  if (errors.instance !== undefined) withObj.instance = errors.instance;
  if (errors.title !== undefined) withObj.title = errors.title;
  if (errors.detail !== undefined) withObj.detail = errors.detail;
  return Object.keys(withObj).length > 0 ? { with: withObj } : undefined;
}

function retryToYamlValue(retry: string | RetryPolicy): unknown {
  if (typeof retry === "string") return retry;
  const retryObj: Record<string, unknown> = {};
  if (retry.delay !== undefined) retryObj.delay = retry.delay;
  // "constant" is the server-side default when backoff is omitted entirely.
  if (retry.backoff !== "constant") retryObj.backoff = { [retry.backoff]: {} };
  const limit: Record<string, unknown> = {};
  if (retry.attemptCount !== undefined || retry.attemptDuration !== undefined) {
    const attempt: Record<string, unknown> = {};
    if (retry.attemptCount !== undefined) attempt.count = retry.attemptCount;
    if (retry.attemptDuration !== undefined) attempt.duration = retry.attemptDuration;
    limit.attempt = attempt;
  }
  if (retry.totalDuration !== undefined) limit.duration = retry.totalDuration;
  if (Object.keys(limit).length > 0) retryObj.limit = limit;
  if (retry.jitterFrom !== undefined || retry.jitterTo !== undefined) {
    const jitter: Record<string, unknown> = {};
    if (retry.jitterFrom !== undefined) jitter.from = retry.jitterFrom;
    if (retry.jitterTo !== undefined) jitter.to = retry.jitterTo;
    retryObj.jitter = jitter;
  }
  if (retry.when !== undefined) retryObj.when = retry.when;
  if (retry.exceptWhen !== undefined) retryObj.exceptWhen = retry.exceptWhen;
  return retryObj;
}

function catchClauseToYamlBody(catchClause: CatchClause): Record<string, unknown> {
  const catchObj: Record<string, unknown> = {};
  const errorsYaml = catchClause.errors && errorFilterToYamlBody(catchClause.errors);
  if (errorsYaml) catchObj.errors = errorsYaml;
  if (catchClause.as !== undefined) catchObj.as = catchClause.as;
  if (catchClause.when !== undefined) catchObj.when = catchClause.when;
  if (catchClause.exceptWhen !== undefined) catchObj.exceptWhen = catchClause.exceptWhen;
  if (catchClause.retry !== undefined) {
    catchObj.retry = retryToYamlValue(catchClause.retry);
  }
  if (catchClause.then !== undefined) catchObj.then = catchClause.then;
  catchObj.do = taskListToYamlEntries(catchClause.children);
  return catchObj;
}

// Shared by toYaml (the top-level "do:" list) and taskToYamlEntry's own
// "do" branch - the reverse of taskListFromYamlEntries above.
function taskListToYamlEntries(tasks: Task[]): Record<string, unknown>[] {
  return tasks.map(taskToYamlEntry);
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
  parsed.do = taskListToYamlEntries(graph.tasks);
  return dump(parsed, { lineWidth: -1 });
}

// Document-level (not per-task) properties: a workflow-level "timeout"/
// "schedule", and the "use:" catalog of reusable, named components other
// tasks reference by name - "authentications" (bearer/basic/oauth2/oidc/
// digest schemes), "errors" (Problem Details templates "raise" can
// reference), "extensions" (before/after/when hooks around a task kind),
// "retries" (what "try"'s catch.retry can reference by name - see
// RetryPolicy in TaskInspector.tsx), "functions" (custom function
// definitions "call" can invoke by name), "timeouts" (what a task's own
// "timeout" can reference by name), and "catalogs" (external workflow
// catalog registrations "run.workflow" resolves against). Confirmed this
// exact field list directly against OpenWorkflowCompiler.java's
// reusableComponents() - the CNCF spec-summary term "resources" some
// earlier planning used doesn't actually exist here; "secrets" also isn't
// a "use:" map (unlike the other seven), it's a flat array of secret
// names, referenced from these definitions rather than defining anything
// itself.
export type WorkflowSettings = {
  timeout?: unknown;
  schedule?: unknown;
  authentications?: unknown;
  errors?: unknown;
  extensions?: unknown;
  retries?: unknown;
  functions?: unknown;
  timeouts?: unknown;
  catalogs?: unknown;
  secrets?: unknown;
};

export function parseWorkflowSettings(source: string): WorkflowSettings {
  const parsed = (load(source) as Record<string, unknown>) ?? {};
  const use = (parsed.use ?? {}) as Record<string, unknown>;
  return {
    timeout: parsed.timeout,
    schedule: parsed.schedule,
    authentications: use.authentications,
    errors: use.errors,
    extensions: use.extensions,
    retries: use.retries,
    functions: use.functions,
    timeouts: use.timeouts,
    catalogs: use.catalogs,
    secrets: use.secrets,
  };
}

/**
 * The WorkflowSettings analogue of toYaml above: replaces only the
 * document-level "timeout"/"schedule" keys and the "use:" catalog's own
 * sub-keys, leaving "do:" and everything else in `source` untouched.
 */
export function applyWorkflowSettings(
  source: string,
  settings: WorkflowSettings,
): string {
  const parsed = (load(source) as Record<string, unknown>) ?? {};
  if (settings.timeout !== undefined) parsed.timeout = settings.timeout;
  else delete parsed.timeout;
  if (settings.schedule !== undefined) parsed.schedule = settings.schedule;
  else delete parsed.schedule;

  const use = { ...((parsed.use as Record<string, unknown>) ?? {}) };
  const useKeys: Array<keyof WorkflowSettings> = [
    "authentications",
    "errors",
    "extensions",
    "retries",
    "functions",
    "timeouts",
    "catalogs",
    "secrets",
  ];
  for (const key of useKeys) {
    if (settings[key] !== undefined) use[key] = settings[key];
    else delete use[key];
  }
  if (Object.keys(use).length > 0) parsed.use = use;
  else delete parsed.use;

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
  if (kind === "emit") return { kind: "emit", name, with: {} };
  if (kind === "for") {
    return { kind: "for", name, itemVariable: "item", collection: "", children: [] };
  }
  if (kind === "fork") {
    return { kind: "fork", name, compete: false, children: [] };
  }
  if (kind === "try") {
    return { kind: "try", name, children: [], catchClause: { children: [] } };
  }
  if (kind === "listen") {
    return { kind: "listen", name, children: [] };
  }
  if (kind === "run") {
    return { kind: "run", name, variant: "container", configuration: {} };
  }
  return { kind: "do", name, children: [] };
}

// Every container kind ("do", "for", and eventually "fork"/"try") has a
// "children: Task[]" field - checking for that field generically, rather
// than listing kinds by name, is what lets tasksAtPath/setChildrenAtPath
// below (and WorkflowCanvas.tsx's drill-in) support a new container kind
// with no changes here when one lands.
export function hasChildren(task: Task): task is Task & { children: Task[] } {
  return "children" in task;
}

/**
 * Walks down a path of container-task names to find "the task list
 * currently in view" for WorkflowCanvas.tsx's drill-down navigation - an
 * empty path means the top-level list itself. A path segment that no
 * longer resolves (the container was renamed/deleted/emptied out from
 * under an open drill-down) returns [] rather than throwing - the same
 * "best effort" philosophy layout() already applies to a switch case
 * pointing at a since-removed task, not a new failure mode.
 */
export function tasksAtPath(tasks: Task[], path: string[]): Task[] {
  if (path.length === 0) return tasks;
  const [head, ...rest] = path;
  const container = tasks.find((task) => task.name === head);
  if (!container || !hasChildren(container)) return [];
  return tasksAtPath(container.children, rest);
}

/**
 * The inverse of tasksAtPath: replaces the task list at `path` with `next`,
 * returning a new root list with every container along the way
 * shallow-copied (so `path`'s container tasks get new object identity, but
 * everything outside the path is untouched). Used to write a drilled-down
 * edit back into the full tree before serializing.
 */
export function setChildrenAtPath(
  tasks: Task[],
  path: string[],
  next: Task[],
): Task[] {
  if (path.length === 0) return next;
  const [head, ...rest] = path;
  return tasks.map((task) => {
    if (task.name !== head || !hasChildren(task)) return task;
    return { ...task, children: setChildrenAtPath(task.children, rest, next) };
  });
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
