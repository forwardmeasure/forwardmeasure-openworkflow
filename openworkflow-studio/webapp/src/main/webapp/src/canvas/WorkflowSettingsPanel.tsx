import { useEffect, useState } from "react";
import Box from "@mui/material/Box";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import {
  applyWorkflowSettings,
  parseWorkflowSettings,
  type WorkflowSettings,
} from "./dsl";

// Document-level properties, not per-task ones - editing the same "use:"
// catalog and workflow-level timeout/schedule fields WorkflowCanvas.tsx's
// per-task Inspector deliberately doesn't touch. Every field here is a raw
// JSON textarea (workflow-level "timeout" is JSON-if-it-parses-else-string,
// same as CommonTaskProps.timeout on a task) - these are named-component
// catalogs (or, for "secrets", a flat list) edited far less often than task
// bodies, so a generated form per catalog entry type isn't worth building
// for this slice, matching the same "pragmatic raw JSON" call already made
// throughout canvas/dsl.ts and TaskInspector.tsx.
type SettingsState = {
  timeout: string;
  schedule: string;
  authentications: string;
  errors: string;
  extensions: string;
  retries: string;
  functions: string;
  timeouts: string;
  catalogs: string;
  secrets: string;
};

const FIELD_ORDER: (keyof SettingsState)[] = [
  "timeout",
  "schedule",
  "authentications",
  "errors",
  "extensions",
  "retries",
  "functions",
  "timeouts",
  "catalogs",
  "secrets",
];

const FIELD_LABEL: Record<keyof SettingsState, string> = {
  timeout: "Workflow timeout",
  schedule: "Schedule",
  authentications: "use.authentications",
  errors: "use.errors",
  extensions: "use.extensions",
  retries: "use.retries",
  functions: "use.functions",
  timeouts: "use.timeouts",
  catalogs: "use.catalogs",
  secrets: "use.secrets",
};

// A short reminder of which task-level field references which catalog, so
// this panel doesn't read as ten disconnected JSON blobs.
const FIELD_HINT: Partial<Record<keyof SettingsState, string>> = {
  authentications: "referenced from a call task's endpoint authentication",
  errors: 'referenced by name from a "raise" task\'s error',
  retries: 'referenced by name from a "try" task\'s catch.retry',
  functions: 'invoked by name from a "call" task\'s target',
  timeouts: "referenced by name from any task's timeout",
  catalogs: 'resolved against by a "run" task\'s workflow variant',
};

function toText(value: unknown): string {
  if (value === undefined) return "";
  if (typeof value === "string") return value;
  return JSON.stringify(value, null, 2);
}

function stateOf(settings: WorkflowSettings): SettingsState {
  return {
    timeout: toText(settings.timeout),
    schedule: toText(settings.schedule),
    authentications: toText(settings.authentications),
    errors: toText(settings.errors),
    extensions: toText(settings.extensions),
    retries: toText(settings.retries),
    functions: toText(settings.functions),
    timeouts: toText(settings.timeouts),
    catalogs: toText(settings.catalogs),
    secrets: toText(settings.secrets),
  };
}

function resolveSettings(state: SettingsState): {
  settings: WorkflowSettings;
  errors: Partial<Record<keyof SettingsState, string>>;
} {
  const settings: WorkflowSettings = {};
  const errors: Partial<Record<keyof SettingsState, string>> = {};
  if (state.timeout.trim()) {
    try {
      settings.timeout = JSON.parse(state.timeout);
    } catch {
      settings.timeout = state.timeout;
    }
  }
  const jsonOnlyFields = FIELD_ORDER.filter((field) => field !== "timeout");
  for (const field of jsonOnlyFields) {
    const text = state[field];
    if (!text.trim()) continue;
    try {
      settings[field] = JSON.parse(text);
    } catch (error) {
      errors[field] = error instanceof Error ? error.message : String(error);
    }
  }
  return { settings, errors };
}

export function WorkflowSettingsPanel({
  source,
  onSourceChange,
}: {
  source: string;
  onSourceChange: (source: string) => void;
}) {
  const [state, setState] = useState<SettingsState>(() =>
    stateOf(parseWorkflowSettings(source)),
  );
  const [errors, setErrors] = useState<
    Partial<Record<keyof SettingsState, string>>
  >({});

  useEffect(() => {
    setState(stateOf(parseWorkflowSettings(source)));
    setErrors({});
  }, [source]);

  function setField(field: keyof SettingsState, value: string) {
    setState((current) => ({ ...current, [field]: value }));
  }

  function commit() {
    const { settings, errors: fieldErrors } = resolveSettings(state);
    setErrors(fieldErrors);
    if (Object.keys(fieldErrors).length > 0) return;
    onSourceChange(applyWorkflowSettings(source, settings));
  }

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        gap: 2,
        p: 2,
        height: "100%",
        overflowY: "auto",
      }}
    >
      <Typography variant="overline" color="text.secondary">
        Workflow settings
      </Typography>
      <Typography variant="caption" color="text.secondary">
        Document-level properties, not per-task ones. Each "use.*" field
        below is a named-component catalog (or a flat list, for
        "use.secrets") that tasks reference by name elsewhere in this
        workflow.
      </Typography>
      {FIELD_ORDER.map((field) => (
        <TextField
          key={field}
          label={
            field === "timeout"
              ? FIELD_LABEL[field]
              : `${FIELD_LABEL[field]} (JSON)`
          }
          placeholder={
            field === "timeout" ? "PT30S, or a name from use.timeouts" : undefined
          }
          helperText={errors[field] ?? FIELD_HINT[field]}
          error={Boolean(errors[field])}
          multiline={field !== "timeout"}
          minRows={field === "timeout" ? undefined : 3}
          size="small"
          value={state[field]}
          onChange={(event) => setField(field, event.target.value)}
          onBlur={commit}
        />
      ))}
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
