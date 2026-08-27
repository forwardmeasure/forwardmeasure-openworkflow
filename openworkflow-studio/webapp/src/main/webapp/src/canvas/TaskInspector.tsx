import { useEffect, useState } from "react";
import DeleteIcon from "@mui/icons-material/Delete";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import Accordion from "@mui/material/Accordion";
import AccordionDetails from "@mui/material/AccordionDetails";
import AccordionSummary from "@mui/material/AccordionSummary";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import IconButton from "@mui/material/IconButton";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import type { CommonTaskProps, SwitchCase, Task } from "./dsl";

// A raw JSON textarea for "set"/"with", not a generated form per task type -
// pragmatic for this first slice (every "set"/"call" parameter shape is
// representable, nothing is hidden or unsupported), a per-field form is a
// natural follow-up once real usage shows which shapes are common enough to
// warrant one.
function paramsOf(task: Task): Record<string, unknown> {
  if (task.kind === "set") return task.set;
  if (task.kind === "call") return task.with;
  return {};
}

const KIND_LABEL: Record<Task["kind"], string> = {
  set: "Set task",
  call: "Call task",
  switch: "Switch task",
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
    setAdvanced(advancedStateOf(task));
    setAdvancedErrors({});
  }, [task]);

  function setAdvancedField(field: keyof AdvancedState, value: string) {
    setAdvanced((current) => ({ ...current, [field]: value }));
  }

  function commit(
    next: Partial<{ name: string; callTarget: string; paramsText: string }>,
  ) {
    if (task.kind === "switch") return;
    const resolvedName = next.name ?? name;
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
    const { props: commonProps, errors: commonErrors } =
      resolveCommonProps(advanced);
    setAdvancedErrors(commonErrors);
    if (Object.keys(commonErrors).length > 0) return;
    onChange(
      task.kind === "set"
        ? { kind: "set", name: resolvedName, set: params, ...commonProps }
        : {
            kind: "call",
            name: resolvedName,
            call: resolvedCallTarget,
            with: params,
            ...commonProps,
          },
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
      {(task.kind === "set" || task.kind === "call") && (
        <TextField
          label={task.kind === "set" ? "set (JSON)" : "with (JSON)"}
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
