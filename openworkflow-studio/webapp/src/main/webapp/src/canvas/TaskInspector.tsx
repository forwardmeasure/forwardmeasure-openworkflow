import { useEffect, useState } from "react";
import DeleteIcon from "@mui/icons-material/Delete";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import Accordion from "@mui/material/Accordion";
import AccordionDetails from "@mui/material/AccordionDetails";
import AccordionSummary from "@mui/material/AccordionSummary";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Checkbox from "@mui/material/Checkbox";
import FormControl from "@mui/material/FormControl";
import FormControlLabel from "@mui/material/FormControlLabel";
import IconButton from "@mui/material/IconButton";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import Select from "@mui/material/Select";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import type {
  CatchClause,
  CommonTaskProps,
  ErrorFilter,
  RaiseError,
  RetryPolicy,
  SwitchCase,
  Task,
} from "./dsl";

// A raw JSON textarea for "set"/"with"/"emit"'s event properties, not a
// generated form per task type - pragmatic for this slice (every shape is
// representable, nothing is hidden or unsupported), a per-field form is a
// natural follow-up once real usage shows which shapes are common enough to
// warrant one.
function paramsOf(task: Task): Record<string, unknown> {
  if (task.kind === "set") return task.set;
  if (task.kind === "call") return task.with;
  if (task.kind === "emit") return task.with;
  return {};
}

const KIND_LABEL: Record<Task["kind"], string> = {
  set: "Set task",
  call: "Call task",
  switch: "Switch task",
  raise: "Raise task",
  wait: "Wait task",
  emit: "Emit task",
  do: "Do task",
  for: "For task",
  fork: "Fork task",
  try: "Try task",
  listen: "Listen task",
  run: "Run task",
};

// The cross-cutting properties every task kind shares (see CommonTaskProps
// in dsl.ts), edited as text below the kind-specific fields regardless of
// kind. "timeout" stays a plain string here even though the underlying
// value can be JSON (an inline timeout definition) - most real values are
// either a duration literal or a "use.timeouts" name, and a value that
// happens to be JSON still round-trips correctly (see toCommonProps' JSON
// fallback below).
type AdvancedState = {
  if: string;
  timeout: string;
  input: string;
  output: string;
  export: string;
  metadata: string;
};

const ADVANCED_JSON_FIELDS = ["input", "output", "export", "metadata"] as const;

function toText(value: unknown): string {
  if (value === undefined) return "";
  if (typeof value === "string") return value;
  return JSON.stringify(value, null, 2);
}

function advancedStateOf(task: Task): AdvancedState {
  return {
    if: task.if ?? "",
    timeout: toText(task.timeout),
    input: toText(task.input),
    output: toText(task.output),
    export: toText(task.export),
    metadata: toText(task.metadata),
  };
}

/**
 * Parses the Advanced section's text fields back into CommonTaskProps.
 * "input"/"output"/"export"/"metadata" must be valid JSON if non-empty
 * (reported per-field in `errors`); "timeout" is JSON-if-it-parses,
 * otherwise kept as the plain string the user typed.
 */
function resolveCommonProps(state: AdvancedState): {
  props: CommonTaskProps;
  errors: Partial<Record<keyof AdvancedState, string>>;
} {
  const props: CommonTaskProps = {};
  const errors: Partial<Record<keyof AdvancedState, string>> = {};
  if (state.if.trim()) props.if = state.if;
  if (state.timeout.trim()) {
    try {
      props.timeout = JSON.parse(state.timeout);
    } catch {
      props.timeout = state.timeout;
    }
  }
  for (const field of ADVANCED_JSON_FIELDS) {
    const text = state[field];
    if (!text.trim()) continue;
    try {
      props[field] = JSON.parse(text);
    } catch (error) {
      errors[field] = error instanceof Error ? error.message : String(error);
    }
  }
  return { props, errors };
}

function parseMaybeJson(text: string): unknown {
  if (!text.trim()) return undefined;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

// "raise"'s error is either a plain string naming a "use.errors" entry, or
// an inline Problem-Details object - same string-or-inline shape and same
// bundled-state treatment as "try"'s RetryState below. `refName` non-empty
// means "use the reference," ignoring the inline fields.
type RaiseErrorState = {
  refName: string;
  type: string;
  status: string;
  title: string;
  instance: string;
  detail: string;
};

function raiseErrorStateOf(error: string | RaiseError | undefined): RaiseErrorState {
  if (typeof error === "string") {
    return { refName: error, type: "", status: "", title: "", instance: "", detail: "" };
  }
  return {
    refName: "",
    type: error?.type ?? "",
    status: error?.status !== undefined ? String(error.status) : "",
    title: error?.title ?? "",
    instance: error?.instance ?? "",
    detail: error?.detail ?? "",
  };
}

function resolveRaiseError(state: RaiseErrorState): string | RaiseError {
  if (state.refName.trim()) return state.refName.trim();
  return {
    type: state.type,
    status: Number(state.status) || 0,
    title: state.title.trim() || undefined,
    instance: state.instance.trim() || undefined,
    detail: state.detail.trim() || undefined,
  };
}

// "try"'s retry policy is either a plain string naming a "use.retries"
// entry, or an inline policy - one bundled state (like AdvancedState above)
// rather than ~10 separate useState calls. `refName` non-empty means "use
// the reference," ignoring every other field; empty means "build an inline
// policy from the rest" (or none at all, if every field is also empty).
type RetryState = {
  refName: string;
  delay: string;
  backoff: RetryPolicy["backoff"];
  attemptCount: string;
  attemptDuration: string;
  totalDuration: string;
  jitterFrom: string;
  jitterTo: string;
  when: string;
  exceptWhen: string;
};

function retryStateOf(retry: string | RetryPolicy | undefined): RetryState {
  if (typeof retry === "string") {
    return {
      refName: retry,
      delay: "",
      backoff: "constant",
      attemptCount: "",
      attemptDuration: "",
      totalDuration: "",
      jitterFrom: "",
      jitterTo: "",
      when: "",
      exceptWhen: "",
    };
  }
  return {
    refName: "",
    delay: toText(retry?.delay),
    backoff: retry?.backoff ?? "constant",
    attemptCount: retry?.attemptCount !== undefined ? String(retry.attemptCount) : "",
    attemptDuration: toText(retry?.attemptDuration),
    totalDuration: toText(retry?.totalDuration),
    jitterFrom: toText(retry?.jitterFrom),
    jitterTo: toText(retry?.jitterTo),
    when: retry?.when ?? "",
    exceptWhen: retry?.exceptWhen ?? "",
  };
}

function resolveRetry(state: RetryState): string | RetryPolicy | undefined {
  if (state.refName.trim()) return state.refName.trim();
  const hasInlineField =
    state.delay.trim() ||
    state.attemptCount.trim() ||
    state.attemptDuration.trim() ||
    state.totalDuration.trim() ||
    state.jitterFrom.trim() ||
    state.jitterTo.trim() ||
    state.when.trim() ||
    state.exceptWhen.trim() ||
    state.backoff !== "constant";
  if (!hasInlineField) return undefined;
  return {
    delay: parseMaybeJson(state.delay),
    backoff: state.backoff,
    attemptCount: state.attemptCount.trim() ? Number(state.attemptCount) : undefined,
    attemptDuration: parseMaybeJson(state.attemptDuration),
    totalDuration: parseMaybeJson(state.totalDuration),
    jitterFrom: parseMaybeJson(state.jitterFrom),
    jitterTo: parseMaybeJson(state.jitterTo),
    when: state.when.trim() || undefined,
    exceptWhen: state.exceptWhen.trim() || undefined,
  };
}

// The rest of "try"'s catch clause, bundled the same way. "errorsText" and
// "doText" are raw JSON (an error filter and a recovery task list
// respectively) - the same pragmatic default as "set"/"with" elsewhere in
// this file, and (for "doText") a deliberate v1 simplification: catch.do
// is a second nested task list "try" carries, and building real dual
// canvas drill-down for it (path segments disambiguating a task's try
// block from its catch block) is out of scope here - see CatchClause's own
// comment in dsl.ts.
type CatchState = {
  errorsText: string;
  as: string;
  when: string;
  exceptWhen: string;
  then: string;
  retry: RetryState;
  doText: string;
};

function catchStateOf(task: Task): CatchState {
  const catchClause: CatchClause =
    task.kind === "try" ? task.catchClause : { children: [] };
  return {
    errorsText: catchClause.errors
      ? JSON.stringify(catchClause.errors, null, 2)
      : "",
    as: catchClause.as ?? "",
    when: catchClause.when ?? "",
    exceptWhen: catchClause.exceptWhen ?? "",
    then: catchClause.then ?? "",
    retry: retryStateOf(catchClause.retry),
    doText: JSON.stringify(catchClause.children, null, 2),
  };
}

function resolveCatchClause(state: CatchState): {
  catchClause: CatchClause;
  errors: Partial<Record<"errorsText" | "doText", string>>;
} {
  const errors: Partial<Record<"errorsText" | "doText", string>> = {};
  let errorFilter: ErrorFilter | undefined;
  if (state.errorsText.trim()) {
    try {
      errorFilter = JSON.parse(state.errorsText) as ErrorFilter;
    } catch (error) {
      errors.errorsText = error instanceof Error ? error.message : String(error);
    }
  }
  let children: Task[] = [];
  if (state.doText.trim()) {
    try {
      children = JSON.parse(state.doText) as Task[];
    } catch (error) {
      errors.doText = error instanceof Error ? error.message : String(error);
    }
  }
  return {
    catchClause: {
      errors: errorFilter,
      as: state.as.trim() || undefined,
      when: state.when.trim() || undefined,
      exceptWhen: state.exceptWhen.trim() || undefined,
      retry: resolveRetry(state.retry),
      then: state.then.trim() || undefined,
      children,
    },
    errors,
  };
}

// "listen"'s consumption filter stays raw JSON (same reasoning as "emit"'s
// "with"); readAs/itemVariable/indexVariable get real fields since they're
// small, bounded values. "children" (foreach.do) is edited via canvas
// drill-down, same as every other container kind - not represented here.
type ListenState = {
  consumptionText: string;
  readAs: "data" | "envelope" | "raw";
  itemVariable: string;
  indexVariable: string;
};

function listenStateOf(task: Task): ListenState {
  if (task.kind !== "listen") {
    return { consumptionText: "", readAs: "data", itemVariable: "", indexVariable: "" };
  }
  return {
    consumptionText: task.consumption !== undefined ? JSON.stringify(task.consumption, null, 2) : "",
    readAs: task.readAs ?? "data",
    itemVariable: task.itemVariable ?? "",
    indexVariable: task.indexVariable ?? "",
  };
}

// "run"'s container/script/shell variants keep their own configuration
// object opaque (raw JSON) - four meaningfully different open shapes, not
// worth four generated forms in v1. "workflow" gets real fields since it's
// small and this canvas can usefully help fill it in.
type RunState = {
  variant: "container" | "script" | "shell" | "workflow";
  configurationText: string;
  workflowNamespace: string;
  workflowName: string;
  workflowVersion: string;
  workflowInputText: string;
  awaitResult: boolean;
  returnMode: "stdout" | "stderr" | "code" | "all" | "none";
};

function runStateOf(task: Task): RunState {
  if (task.kind !== "run") {
    return {
      variant: "container",
      configurationText: "{}",
      workflowNamespace: "",
      workflowName: "",
      workflowVersion: "",
      workflowInputText: "",
      awaitResult: true,
      returnMode: "stdout",
    };
  }
  return {
    variant: task.variant,
    configurationText: JSON.stringify(task.configuration ?? {}, null, 2),
    workflowNamespace: task.workflowNamespace ?? "",
    workflowName: task.workflowName ?? "",
    workflowVersion: task.workflowVersion ?? "",
    workflowInputText:
      task.workflowInput !== undefined ? JSON.stringify(task.workflowInput, null, 2) : "",
    awaitResult: task.await ?? true,
    returnMode: task.returnMode ?? "stdout",
  };
}

function resolveListenState(state: ListenState): {
  consumption: unknown;
  error?: string;
} {
  if (!state.consumptionText.trim()) return { consumption: undefined };
  try {
    return { consumption: JSON.parse(state.consumptionText) };
  } catch (error) {
    return {
      consumption: undefined,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

function resolveRunState(state: RunState): {
  configuration?: Record<string, unknown>;
  workflowInput?: unknown;
  errors: Partial<Record<"configurationText" | "workflowInputText", string>>;
} {
  const errors: Partial<Record<"configurationText" | "workflowInputText", string>> = {};
  let configuration: Record<string, unknown> | undefined;
  if (state.variant !== "workflow" && state.configurationText.trim()) {
    try {
      configuration = JSON.parse(state.configurationText) as Record<string, unknown>;
    } catch (error) {
      errors.configurationText = error instanceof Error ? error.message : String(error);
    }
  }
  let workflowInput: unknown;
  if (state.variant === "workflow" && state.workflowInputText.trim()) {
    try {
      workflowInput = JSON.parse(state.workflowInputText);
    } catch (error) {
      errors.workflowInputText = error instanceof Error ? error.message : String(error);
    }
  }
  return { configuration, workflowInput, errors };
}

export function TaskInspector({
  task,
  onChange,
  onDelete,
}: {
  task: Task;
  onChange: (task: Task) => void;
  onDelete: () => void;
}) {
  const [name, setName] = useState(task.name);
  const [callTarget, setCallTarget] = useState(
    task.kind === "call" ? task.call : "",
  );
  const [paramsText, setParamsText] = useState(() =>
    JSON.stringify(paramsOf(task), null, 2),
  );
  const [paramsError, setParamsError] = useState<string>();
  const [cases, setCases] = useState<SwitchCase[]>(
    task.kind === "switch" ? task.cases : [],
  );
  const [raiseError, setRaiseError] = useState<RaiseErrorState>(() =>
    raiseErrorStateOf(task.kind === "raise" ? task.error : undefined),
  );
  const [waitText, setWaitText] = useState(() =>
    task.kind === "wait" ? toText(task.wait) : "",
  );
  const [forItemVariable, setForItemVariable] = useState(
    task.kind === "for" ? task.itemVariable : "",
  );
  const [forCollection, setForCollection] = useState(
    task.kind === "for" ? task.collection : "",
  );
  const [forIndexVariable, setForIndexVariable] = useState(
    task.kind === "for" ? (task.indexVariable ?? "") : "",
  );
  const [forWhile, setForWhile] = useState(
    task.kind === "for" ? (task.whileCondition ?? "") : "",
  );
  const [forkCompete, setForkCompete] = useState(
    task.kind === "fork" ? task.compete : false,
  );
  const [catchState, setCatchState] = useState<CatchState>(() =>
    catchStateOf(task),
  );
  const [catchErrors, setCatchErrors] = useState<
    Partial<Record<"errorsText" | "doText", string>>
  >({});
  const [listenState, setListenState] = useState<ListenState>(() =>
    listenStateOf(task),
  );
  const [listenError, setListenError] = useState<string>();
  const [runState, setRunState] = useState<RunState>(() => runStateOf(task));
  const [runErrors, setRunErrors] = useState<
    Partial<Record<"configurationText" | "workflowInputText", string>>
  >({});
  const [advanced, setAdvanced] = useState<AdvancedState>(() =>
    advancedStateOf(task),
  );
  const [advancedErrors, setAdvancedErrors] = useState<
    Partial<Record<keyof AdvancedState, string>>
  >({});

  useEffect(() => {
    setName(task.name);
    setCallTarget(task.kind === "call" ? task.call : "");
    setParamsText(JSON.stringify(paramsOf(task), null, 2));
    setParamsError(undefined);
    setCases(task.kind === "switch" ? task.cases : []);
    setRaiseError(raiseErrorStateOf(task.kind === "raise" ? task.error : undefined));
    setWaitText(task.kind === "wait" ? toText(task.wait) : "");
    setForItemVariable(task.kind === "for" ? task.itemVariable : "");
    setForCollection(task.kind === "for" ? task.collection : "");
    setForIndexVariable(task.kind === "for" ? (task.indexVariable ?? "") : "");
    setForWhile(task.kind === "for" ? (task.whileCondition ?? "") : "");
    setForkCompete(task.kind === "fork" ? task.compete : false);
    setCatchState(catchStateOf(task));
    setCatchErrors({});
    setListenState(listenStateOf(task));
    setListenError(undefined);
    setRunState(runStateOf(task));
    setRunErrors({});
    setAdvanced(advancedStateOf(task));
    setAdvancedErrors({});
  }, [task]);

  function setRaiseErrorField<K extends keyof RaiseErrorState>(
    field: K,
    value: RaiseErrorState[K],
  ) {
    setRaiseError((current) => ({ ...current, [field]: value }));
  }

  function setListenField<K extends keyof ListenState>(field: K, value: ListenState[K]) {
    setListenState((current) => ({ ...current, [field]: value }));
  }

  function setRunField<K extends keyof RunState>(field: K, value: RunState[K]) {
    setRunState((current) => ({ ...current, [field]: value }));
  }

  function setCatchField<K extends keyof CatchState>(field: K, value: CatchState[K]) {
    setCatchState((current) => ({ ...current, [field]: value }));
  }

  function setRetryField<K extends keyof RetryState>(field: K, value: RetryState[K]) {
    setCatchState((current) => ({
      ...current,
      retry: { ...current.retry, [field]: value },
    }));
  }

  function setAdvancedField(field: keyof AdvancedState, value: string) {
    setAdvanced((current) => ({ ...current, [field]: value }));
  }

  function commit(
    next: Partial<{
      name: string;
      callTarget: string;
      paramsText: string;
      forkCompete: boolean;
      retryBackoff: RetryPolicy["backoff"];
      listenReadAs: ListenState["readAs"];
      runVariant: RunState["variant"];
      runAwait: boolean;
      runReturnMode: RunState["returnMode"];
    }>,
  ) {
    if (task.kind === "switch") return;
    const resolvedName = next.name ?? name;
    const { props: commonProps, errors: commonErrors } =
      resolveCommonProps(advanced);
    setAdvancedErrors(commonErrors);
    if (Object.keys(commonErrors).length > 0) return;

    if (task.kind === "raise") {
      onChange({
        kind: "raise",
        name: resolvedName,
        error: resolveRaiseError(raiseError),
        ...commonProps,
      });
      return;
    }
    if (task.kind === "wait") {
      let waitValue: string | Record<string, unknown> = waitText;
      try {
        waitValue = JSON.parse(waitText) as Record<string, unknown>;
      } catch {
        // Not JSON - keep it as the plain duration-literal/expression string.
      }
      onChange({ kind: "wait", name: resolvedName, wait: waitValue, ...commonProps });
      return;
    }
    if (task.kind === "do") {
      // "children" is edited by drilling into this task on canvas, not
      // here - the Inspector only ever touches name/common props for it.
      onChange({
        kind: "do",
        name: resolvedName,
        children: task.children,
        ...commonProps,
      });
      return;
    }
    if (task.kind === "for") {
      onChange({
        kind: "for",
        name: resolvedName,
        itemVariable: forItemVariable || "item",
        collection: forCollection,
        indexVariable: forIndexVariable.trim() || undefined,
        whileCondition: forWhile.trim() || undefined,
        children: task.children,
        ...commonProps,
      });
      return;
    }
    if (task.kind === "fork") {
      // "children" (the branches) is edited by drilling into this task on
      // canvas, same as "do"/"for" - only compete/name/common props here.
      // Unlike the text fields above, the checkbox below commits from its
      // own onChange (no blur to wait on for a boolean toggle), so it must
      // pass its new value through `next` rather than read the (still
      // stale, pre-re-render) `forkCompete` closure directly.
      onChange({
        kind: "fork",
        name: resolvedName,
        compete: next.forkCompete ?? forkCompete,
        children: task.children,
        ...commonProps,
      });
      return;
    }
    if (task.kind === "try") {
      // "children" (the try block) is edited by drilling into this task on
      // canvas, same as every other container kind - only the catch clause
      // (errors/as/when/exceptWhen/retry/then, plus catch.do as raw JSON)
      // is edited here. "retryBackoff" is threaded through `next` for the
      // same reason forkCompete/runAwait are: the backoff Select commits
      // from onClose, which (confirmed live) fires in the same synchronous
      // batch as its own onChange, before this render's retryState closure
      // would see the update - unlike a text field's onBlur, which
      // genuinely happens on a later tick after React has already
      // re-rendered.
      const effectiveCatchState =
        next.retryBackoff !== undefined
          ? { ...catchState, retry: { ...catchState.retry, backoff: next.retryBackoff } }
          : catchState;
      const { catchClause, errors: catchClauseErrors } =
        resolveCatchClause(effectiveCatchState);
      setCatchErrors(catchClauseErrors);
      if (Object.keys(catchClauseErrors).length > 0) return;
      onChange({
        kind: "try",
        name: resolvedName,
        children: task.children,
        catchClause,
        ...commonProps,
      });
      return;
    }
    if (task.kind === "listen") {
      // "children" (foreach.do, if this listen loops) is edited by
      // drilling into this task on canvas, same as every other container
      // kind - not touched here. "listenReadAs" is threaded through `next`
      // for the same reason as retryBackoff above - the readAs Select
      // commits from onClose, same-batch as its own onChange.
      const { consumption, error } = resolveListenState(listenState);
      setListenError(error);
      if (error) return;
      const resolvedReadAs = next.listenReadAs ?? listenState.readAs;
      onChange({
        kind: "listen",
        name: resolvedName,
        consumption,
        readAs: resolvedReadAs === "data" ? undefined : resolvedReadAs,
        itemVariable: listenState.itemVariable.trim() || undefined,
        indexVariable: listenState.indexVariable.trim() || undefined,
        children: task.children,
        ...commonProps,
      });
      return;
    }
    if (task.kind === "run") {
      // "runVariant"/"runReturnMode" are threaded through `next` for the
      // same reason as retryBackoff/listenReadAs above - both Selects
      // commit from onClose, same-batch as their own onChange.
      const resolvedVariant = next.runVariant ?? runState.variant;
      const resolvedReturnMode = next.runReturnMode ?? runState.returnMode;
      const effectiveRunState = { ...runState, variant: resolvedVariant };
      const { configuration, workflowInput, errors: runFieldErrors } =
        resolveRunState(effectiveRunState);
      setRunErrors(runFieldErrors);
      if (Object.keys(runFieldErrors).length > 0) return;
      const resolvedAwait = next.runAwait ?? runState.awaitResult;
      onChange({
        kind: "run",
        name: resolvedName,
        variant: resolvedVariant,
        configuration: resolvedVariant === "workflow" ? undefined : configuration,
        workflowNamespace:
          resolvedVariant === "workflow" ? runState.workflowNamespace.trim() || undefined : undefined,
        workflowName:
          resolvedVariant === "workflow" ? runState.workflowName.trim() || undefined : undefined,
        workflowVersion:
          resolvedVariant === "workflow" ? runState.workflowVersion.trim() || undefined : undefined,
        workflowInput: resolvedVariant === "workflow" ? workflowInput : undefined,
        await: resolvedAwait === true ? undefined : resolvedAwait,
        returnMode: resolvedReturnMode === "stdout" ? undefined : resolvedReturnMode,
        ...commonProps,
      });
      return;
    }

    const resolvedCallTarget = next.callTarget ?? callTarget;
    const resolvedParamsText = next.paramsText ?? paramsText;
    let params: Record<string, unknown>;
    try {
      params = JSON.parse(resolvedParamsText);
      setParamsError(undefined);
    } catch (error) {
      setParamsError(error instanceof Error ? error.message : String(error));
      return;
    }
    onChange(
      task.kind === "set"
        ? { kind: "set", name: resolvedName, set: params, ...commonProps }
        : task.kind === "call"
          ? {
              kind: "call",
              name: resolvedName,
              call: resolvedCallTarget,
              with: params,
              ...commonProps,
            }
          : { kind: "emit", name: resolvedName, with: params, ...commonProps },
    );
  }

  function commitCases(next: SwitchCase[], nextName = name) {
    setCases(next);
    const { props: commonProps, errors: commonErrors } =
      resolveCommonProps(advanced);
    setAdvancedErrors(commonErrors);
    if (Object.keys(commonErrors).length > 0) return;
    onChange({ kind: "switch", name: nextName, cases: next, ...commonProps });
  }

  // Shared onBlur target for the name field and every Advanced field: which
  // commit path applies still depends on task.kind, same split as before.
  function commitAdvanced() {
    if (task.kind === "switch") commitCases(cases);
    else commit({});
  }

  function addCase() {
    let index = cases.length + 1;
    while (cases.some((c) => c.name === `case${index}`)) index += 1;
    commitCases([...cases, { name: `case${index}`, when: "", then: "exit" }]);
  }

  return (
    <Box
      sx={{
        p: 2,
        width: 320,
        display: "flex",
        flexDirection: "column",
        gap: 2,
      }}
    >
      <Typography variant="overline" color="text.secondary">
        {KIND_LABEL[task.kind]}
      </Typography>
      <TextField
        label="Task name"
        size="small"
        value={name}
        onChange={(event) => setName(event.target.value)}
        onBlur={() =>
          task.kind === "switch" ? commitCases(cases, name) : commitAdvanced()
        }
      />
      {task.kind === "call" && (
        <TextField
          label="Call target"
          size="small"
          placeholder="http, openapi, or a function name"
          value={callTarget}
          onChange={(event) => setCallTarget(event.target.value)}
          onBlur={() => commit({})}
        />
      )}
      {(task.kind === "set" || task.kind === "call" || task.kind === "emit") && (
        <TextField
          label={
            task.kind === "set"
              ? "set (JSON)"
              : task.kind === "call"
                ? "with (JSON)"
                : "event.with (JSON)"
          }
          multiline
          minRows={8}
          size="small"
          value={paramsText}
          error={Boolean(paramsError)}
          helperText={paramsError}
          onChange={(event) => setParamsText(event.target.value)}
          onBlur={() => commit({})}
        />
      )}
      {task.kind === "raise" && (
        <Box sx={{ display: "flex", flexDirection: "column", gap: 1.5 }}>
          <TextField
            label="Error: named entry from use.errors"
            size="small"
            placeholder="leave blank for an inline error below"
            value={raiseError.refName}
            onChange={(event) => setRaiseErrorField("refName", event.target.value)}
            onBlur={() => commit({})}
          />
          {!raiseError.refName.trim() && (
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                gap: 1.5,
                pl: 1.5,
                borderLeft: 2,
                borderColor: "divider",
              }}
            >
              <TextField
                label="Error type"
                size="small"
                placeholder="https://example.com/errors/not-found"
                value={raiseError.type}
                onChange={(event) => setRaiseErrorField("type", event.target.value)}
                onBlur={() => commit({})}
              />
              <TextField
                label="Status"
                size="small"
                type="number"
                value={raiseError.status}
                onChange={(event) => setRaiseErrorField("status", event.target.value)}
                onBlur={() => commit({})}
              />
              <TextField
                label="Title (optional)"
                size="small"
                value={raiseError.title}
                onChange={(event) => setRaiseErrorField("title", event.target.value)}
                onBlur={() => commit({})}
              />
              <TextField
                label="Instance (optional)"
                size="small"
                value={raiseError.instance}
                onChange={(event) => setRaiseErrorField("instance", event.target.value)}
                onBlur={() => commit({})}
              />
              <TextField
                label="Detail (optional)"
                size="small"
                multiline
                minRows={2}
                value={raiseError.detail}
                onChange={(event) => setRaiseErrorField("detail", event.target.value)}
                onBlur={() => commit({})}
              />
            </Box>
          )}
        </Box>
      )}
      {task.kind === "wait" && (
        <TextField
          label="Duration"
          size="small"
          placeholder="PT30S, an expression, or JSON ({days, hours, ...})"
          value={waitText}
          onChange={(event) => setWaitText(event.target.value)}
          onBlur={() => commit({})}
        />
      )}
      {task.kind === "for" && (
        <Box sx={{ display: "flex", flexDirection: "column", gap: 1.5 }}>
          <Typography variant="caption" color="text.secondary">
            Double-click this task on canvas to edit its loop body.
          </Typography>
          <TextField
            label="Item variable"
            size="small"
            placeholder="item"
            value={forItemVariable}
            onChange={(event) => setForItemVariable(event.target.value)}
            onBlur={() => commit({})}
          />
          <TextField
            label="Collection (expression)"
            size="small"
            placeholder="${ .items }"
            value={forCollection}
            onChange={(event) => setForCollection(event.target.value)}
            onBlur={() => commit({})}
          />
          <TextField
            label="Index variable (optional)"
            size="small"
            placeholder="index"
            value={forIndexVariable}
            onChange={(event) => setForIndexVariable(event.target.value)}
            onBlur={() => commit({})}
          />
          <TextField
            label="While condition (optional)"
            size="small"
            placeholder="${ .continue }"
            value={forWhile}
            onChange={(event) => setForWhile(event.target.value)}
            onBlur={() => commit({})}
          />
        </Box>
      )}
      {task.kind === "fork" && (
        <Box sx={{ display: "flex", flexDirection: "column", gap: 1.5 }}>
          <Typography variant="caption" color="text.secondary">
            Double-click this task on canvas to edit its parallel branches -
            each task inside runs as its own branch.
          </Typography>
          <FormControlLabel
            control={
              <Checkbox
                size="small"
                checked={forkCompete}
                onChange={(event) => {
                  const checked = event.target.checked;
                  setForkCompete(checked);
                  commit({ forkCompete: checked });
                }}
              />
            }
            label="Compete (first branch to finish wins, others are cancelled)"
          />
        </Box>
      )}
      {task.kind === "try" && (
        <Box sx={{ display: "flex", flexDirection: "column", gap: 1.5 }}>
          <Typography variant="caption" color="text.secondary">
            Double-click this task on canvas to edit its try block.
          </Typography>
          <TextField
            label="Catch: errors filter (JSON, optional)"
            multiline
            minRows={2}
            size="small"
            placeholder={'{ "status": 503 }'}
            value={catchState.errorsText}
            error={Boolean(catchErrors.errorsText)}
            helperText={catchErrors.errorsText}
            onChange={(event) => setCatchField("errorsText", event.target.value)}
            onBlur={() => commit({})}
          />
          <TextField
            label='Catch: bind error to variable (default "error")'
            size="small"
            value={catchState.as}
            onChange={(event) => setCatchField("as", event.target.value)}
            onBlur={() => commit({})}
          />
          <TextField
            label="Catch: when (optional)"
            size="small"
            value={catchState.when}
            onChange={(event) => setCatchField("when", event.target.value)}
            onBlur={() => commit({})}
          />
          <TextField
            label="Catch: except when (optional)"
            size="small"
            value={catchState.exceptWhen}
            onChange={(event) => setCatchField("exceptWhen", event.target.value)}
            onBlur={() => commit({})}
          />
          <TextField
            label="Catch: then (optional)"
            size="small"
            placeholder="another task's name, or exit"
            value={catchState.then}
            onChange={(event) => setCatchField("then", event.target.value)}
            onBlur={() => commit({})}
          />
          <Typography variant="caption" color="text.secondary">
            Retry policy (optional)
          </Typography>
          <TextField
            label="Retry: named policy from use.retries"
            size="small"
            placeholder="leave blank for an inline policy below"
            value={catchState.retry.refName}
            onChange={(event) => setRetryField("refName", event.target.value)}
            onBlur={() => commit({})}
          />
          {!catchState.retry.refName.trim() && (
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                gap: 1.5,
                pl: 1.5,
                borderLeft: 2,
                borderColor: "divider",
              }}
            >
              <TextField
                label="Retry: delay"
                size="small"
                placeholder='PT1S, or {"seconds": 3}'
                value={catchState.retry.delay}
                onChange={(event) => setRetryField("delay", event.target.value)}
                onBlur={() => commit({})}
              />
              <FormControl size="small">
                <InputLabel id="retry-backoff-label">Backoff</InputLabel>
                <Select
                  labelId="retry-backoff-label"
                  label="Backoff"
                  value={catchState.retry.backoff}
                  onChange={(event) => {
                    const backoff = event.target.value as RetryState["backoff"];
                    setRetryField("backoff", backoff);
                    commit({ retryBackoff: backoff });
                  }}
                >
                  <MenuItem value="constant">Constant</MenuItem>
                  <MenuItem value="linear">Linear</MenuItem>
                  <MenuItem value="exponential">Exponential</MenuItem>
                </Select>
              </FormControl>
              <TextField
                label="Retry: attempt count"
                size="small"
                type="number"
                value={catchState.retry.attemptCount}
                onChange={(event) => setRetryField("attemptCount", event.target.value)}
                onBlur={() => commit({})}
              />
              <TextField
                label="Retry: attempt duration"
                size="small"
                value={catchState.retry.attemptDuration}
                onChange={(event) =>
                  setRetryField("attemptDuration", event.target.value)
                }
                onBlur={() => commit({})}
              />
              <TextField
                label="Retry: total duration"
                size="small"
                value={catchState.retry.totalDuration}
                onChange={(event) =>
                  setRetryField("totalDuration", event.target.value)
                }
                onBlur={() => commit({})}
              />
              <TextField
                label="Retry: jitter from"
                size="small"
                value={catchState.retry.jitterFrom}
                onChange={(event) => setRetryField("jitterFrom", event.target.value)}
                onBlur={() => commit({})}
              />
              <TextField
                label="Retry: jitter to"
                size="small"
                value={catchState.retry.jitterTo}
                onChange={(event) => setRetryField("jitterTo", event.target.value)}
                onBlur={() => commit({})}
              />
              <TextField
                label="Retry: when"
                size="small"
                value={catchState.retry.when}
                onChange={(event) => setRetryField("when", event.target.value)}
                onBlur={() => commit({})}
              />
              <TextField
                label="Retry: except when"
                size="small"
                value={catchState.retry.exceptWhen}
                onChange={(event) => setRetryField("exceptWhen", event.target.value)}
                onBlur={() => commit({})}
              />
            </Box>
          )}
          <TextField
            label="Catch: recovery tasks (JSON, catch.do)"
            multiline
            minRows={4}
            size="small"
            value={catchState.doText}
            error={Boolean(catchErrors.doText)}
            helperText={catchErrors.doText}
            onChange={(event) => setCatchField("doText", event.target.value)}
            onBlur={() => commit({})}
          />
        </Box>
      )}
      {task.kind === "listen" && (
        <Box sx={{ display: "flex", flexDirection: "column", gap: 1.5 }}>
          <Typography variant="caption" color="text.secondary">
            Set an item variable below to loop over each matching event -
            double-click this task on canvas to edit that loop's tasks.
          </Typography>
          <TextField
            label="Consumption filter (JSON: one/all/any)"
            multiline
            minRows={4}
            size="small"
            placeholder={'{ "one": { "with": { "type": "com.example.event" } } }'}
            value={listenState.consumptionText}
            error={Boolean(listenError)}
            helperText={listenError}
            onChange={(event) => setListenField("consumptionText", event.target.value)}
            onBlur={() => commit({})}
          />
          <FormControl size="small">
            <InputLabel id="listen-read-label">Read as</InputLabel>
            <Select
              labelId="listen-read-label"
              label="Read as"
              value={listenState.readAs}
              onChange={(event) => {
                const readAs = event.target.value as ListenState["readAs"];
                setListenField("readAs", readAs);
                commit({ listenReadAs: readAs });
              }}
            >
              <MenuItem value="data">Data</MenuItem>
              <MenuItem value="envelope">Envelope</MenuItem>
              <MenuItem value="raw">Raw</MenuItem>
            </Select>
          </FormControl>
          <TextField
            label="Loop: item variable (optional)"
            size="small"
            placeholder="event"
            value={listenState.itemVariable}
            onChange={(event) => setListenField("itemVariable", event.target.value)}
            onBlur={() => commit({})}
          />
          <TextField
            label="Loop: index variable (optional)"
            size="small"
            placeholder="index"
            value={listenState.indexVariable}
            onChange={(event) => setListenField("indexVariable", event.target.value)}
            onBlur={() => commit({})}
          />
        </Box>
      )}
      {task.kind === "run" && (
        <Box sx={{ display: "flex", flexDirection: "column", gap: 1.5 }}>
          <FormControl size="small">
            <InputLabel id="run-variant-label">Variant</InputLabel>
            <Select
              labelId="run-variant-label"
              label="Variant"
              value={runState.variant}
              onChange={(event) => {
                const variant = event.target.value as RunState["variant"];
                setRunField("variant", variant);
                commit({ runVariant: variant });
              }}
            >
              <MenuItem value="container">Container</MenuItem>
              <MenuItem value="script">Script</MenuItem>
              <MenuItem value="shell">Shell</MenuItem>
              <MenuItem value="workflow">Workflow (subflow)</MenuItem>
            </Select>
          </FormControl>
          {runState.variant === "workflow" ? (
            <>
              <TextField
                label="Workflow: namespace"
                size="small"
                value={runState.workflowNamespace}
                onChange={(event) => setRunField("workflowNamespace", event.target.value)}
                onBlur={() => commit({})}
              />
              <TextField
                label="Workflow: name"
                size="small"
                value={runState.workflowName}
                onChange={(event) => setRunField("workflowName", event.target.value)}
                onBlur={() => commit({})}
              />
              <TextField
                label="Workflow: version"
                size="small"
                value={runState.workflowVersion}
                onChange={(event) => setRunField("workflowVersion", event.target.value)}
                onBlur={() => commit({})}
              />
              <TextField
                label="Workflow: input (JSON, optional)"
                multiline
                minRows={3}
                size="small"
                value={runState.workflowInputText}
                error={Boolean(runErrors.workflowInputText)}
                helperText={runErrors.workflowInputText}
                onChange={(event) => setRunField("workflowInputText", event.target.value)}
                onBlur={() => commit({})}
              />
            </>
          ) : (
            <TextField
              label={`${runState.variant} configuration (JSON)`}
              multiline
              minRows={6}
              size="small"
              value={runState.configurationText}
              error={Boolean(runErrors.configurationText)}
              helperText={runErrors.configurationText}
              onChange={(event) => setRunField("configurationText", event.target.value)}
              onBlur={() => commit({})}
            />
          )}
          <FormControlLabel
            control={
              <Checkbox
                size="small"
                checked={runState.awaitResult}
                onChange={(event) => {
                  const checked = event.target.checked;
                  setRunField("awaitResult", checked);
                  commit({ runAwait: checked });
                }}
              />
            }
            label="Await result before continuing"
          />
          <FormControl size="small">
            <InputLabel id="run-return-label">Return</InputLabel>
            <Select
              labelId="run-return-label"
              label="Return"
              value={runState.returnMode}
              onChange={(event) => {
                const returnMode = event.target.value as RunState["returnMode"];
                setRunField("returnMode", returnMode);
                commit({ runReturnMode: returnMode });
              }}
            >
              <MenuItem value="stdout">Stdout</MenuItem>
              <MenuItem value="stderr">Stderr</MenuItem>
              <MenuItem value="code">Exit code</MenuItem>
              <MenuItem value="all">All</MenuItem>
              <MenuItem value="none">None</MenuItem>
            </Select>
          </FormControl>
        </Box>
      )}
      {task.kind === "switch" && (
        <Box sx={{ display: "flex", flexDirection: "column", gap: 1.5 }}>
          <Typography variant="caption" color="text.secondary">
            Cases are evaluated in order; the first with a true (or empty)
            "when" wins. "then" is another task's name in this flow, or "exit"
            to end the workflow.
          </Typography>
          {cases.map((switchCase, index) => (
            <Box
              key={index}
              sx={{
                display: "flex",
                flexDirection: "column",
                gap: 1,
                p: 1,
                border: 1,
                borderColor: "divider",
                borderRadius: 1,
              }}
            >
              <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                <TextField
                  label="Case name"
                  size="small"
                  fullWidth
                  value={switchCase.name}
                  onChange={(event) =>
                    setCases((current) =>
                      current.map((c, i) =>
                        i === index ? { ...c, name: event.target.value } : c,
                      ),
                    )
                  }
                  onBlur={() => commitCases(cases)}
                />
                <IconButton
                  aria-label="Delete case"
                  size="small"
                  onClick={() =>
                    commitCases(cases.filter((_, i) => i !== index))
                  }
                >
                  <DeleteIcon fontSize="small" />
                </IconButton>
              </Box>
              <TextField
                label="when (empty = default)"
                size="small"
                placeholder="${.age >= 18}"
                value={switchCase.when ?? ""}
                onChange={(event) =>
                  setCases((current) =>
                    current.map((c, i) =>
                      i === index
                        ? { ...c, when: event.target.value || undefined }
                        : c,
                    ),
                  )
                }
                onBlur={() => commitCases(cases)}
              />
              <TextField
                label="then"
                size="small"
                placeholder="another task's name, or exit"
                value={switchCase.then}
                onChange={(event) =>
                  setCases((current) =>
                    current.map((c, i) =>
                      i === index ? { ...c, then: event.target.value } : c,
                    ),
                  )
                }
                onBlur={() => commitCases(cases)}
              />
            </Box>
          ))}
          <Button size="small" onClick={addCase}>
            Add case
          </Button>
        </Box>
      )}
      <Accordion
        disableGutters
        elevation={0}
        sx={{ border: 1, borderColor: "divider", "&:before": { display: "none" } }}
      >
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="body2">Advanced</Typography>
        </AccordionSummary>
        <AccordionDetails
          sx={{ display: "flex", flexDirection: "column", gap: 1.5 }}
        >
          <TextField
            label="if (expression, optional)"
            size="small"
            placeholder="${ .age >= 18 }"
            value={advanced.if}
            onChange={(event) => setAdvancedField("if", event.target.value)}
            onBlur={commitAdvanced}
          />
          <TextField
            label="timeout (optional)"
            size="small"
            placeholder="PT30S, or a name from use.timeouts"
            value={advanced.timeout}
            onChange={(event) =>
              setAdvancedField("timeout", event.target.value)
            }
            onBlur={commitAdvanced}
          />
          {ADVANCED_JSON_FIELDS.map((field) => (
            <TextField
              key={field}
              label={`${field} (JSON, optional)`}
              multiline
              minRows={2}
              size="small"
              value={advanced[field]}
              error={Boolean(advancedErrors[field])}
              helperText={advancedErrors[field]}
              onChange={(event) => setAdvancedField(field, event.target.value)}
              onBlur={commitAdvanced}
            />
          ))}
        </AccordionDetails>
      </Accordion>
      <Box>
        <Button color="error" size="small" onClick={onDelete}>
          Delete task
        </Button>
      </Box>
    </Box>
  );
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
