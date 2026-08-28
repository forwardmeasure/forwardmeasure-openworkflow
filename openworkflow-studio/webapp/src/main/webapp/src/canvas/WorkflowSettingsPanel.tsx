import { useEffect, useState } from "react";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import Accordion from "@mui/material/Accordion";
import AccordionDetails from "@mui/material/AccordionDetails";
import AccordionSummary from "@mui/material/AccordionSummary";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import FormControl from "@mui/material/FormControl";
import FormHelperText from "@mui/material/FormHelperText";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import Select from "@mui/material/Select";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import {
  applyWorkflowSettings,
  parseWorkflowSettings,
  type WorkflowSettings,
} from "./dsl";

// Document-level properties, not per-task ones - editing the same "use:"
// catalog, workflow-level timeout/schedule/input/output, and "document:"
// metadata fields WorkflowCanvas.tsx's per-task Inspector deliberately
// doesn't touch. Most fields here are raw JSON textareas (workflow-level
// "timeout" is JSON-if-it-parses-else-string, same as CommonTaskProps.timeout
// on a task) - these are named-component catalogs (or, for "secrets", a flat
// list) edited far less often than task bodies, so a generated form per
// catalog entry type isn't worth building for this slice, matching the same
// "pragmatic raw JSON" call already made throughout canvas/dsl.ts and
// TaskInspector.tsx. "documentTitle"/"documentSummary" are the two plain
// strings in the mix, so they get real TextFields instead.
type SettingsState = {
  documentTitle: string;
  documentSummary: string;
  documentTags: string;
  documentMetadata: string;
  input: string;
  output: string;
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

// "makes no sense whatsoever" (real user feedback on the previous version
// of this panel) traced to one concrete problem, confirmed by actually
// rendering it: sixteen fields, most of them raw-JSON textareas, ALL
// always visible and equally weighted - "Document title" sat with the
// same visual prominence as "use.secrets (JSON)", a field the overwhelming
// majority of workflows never touch. Two fixes, not a rewrite of the
// underlying edit model (still raw JSON for open-ended shapes - see the
// file-level comment below for why): (1) only the fields most workflows
// actually set stay always-open (title/summary, and the whole-workflow
// data/timeout/schedule/expression-mode group); (2) the two genuinely
// rare groups - freeform tags/metadata, and the eight "use.*" catalogs -
// move into collapsed accordions with a live count badge, so they're
// discoverable without dominating the panel on every open.
const OPEN_SECTIONS: { heading: string; fields: (keyof SettingsState)[] }[] = [
  {
    heading: "Basics",
    fields: ["documentTitle", "documentSummary"],
  },
  {
    heading: "Data & expressions",
    fields: ["input", "output", "timeout", "schedule"],
  },
];
const COLLAPSED_SECTIONS: {
  heading: string;
  description: string;
  fields: (keyof SettingsState)[];
}[] = [
  {
    heading: "Labels & extra metadata",
    description: "Freeform - most workflows leave these unset.",
    fields: ["documentTags", "documentMetadata"],
  },
  {
    heading: "Reusable components",
    description:
      'Named things tasks elsewhere in this workflow refer back to by name (e.g. a "raise" task\'s error, or a "call" task\'s authentication).',
    fields: [
      "authentications",
      "errors",
      "extensions",
      "retries",
      "functions",
      "timeouts",
      "catalogs",
      "secrets",
    ],
  },
];
const FIELD_ORDER: (keyof SettingsState)[] = [
  ...OPEN_SECTIONS.flatMap((section) => section.fields),
  ...COLLAPSED_SECTIONS.flatMap((section) => section.fields),
];

// "documentTitle"/"documentSummary" are plain strings on the wire (never
// JSON) - distinct from "timeout" below (JSON-if-it-parses-else-string) and
// from every other field here (JSON-only).
const PLAIN_TEXT_FIELDS: (keyof SettingsState)[] = ["documentTitle", "documentSummary"];

// Plain-language first - this is what reads as the field's name. The raw
// DSL wire path (shown separately via FIELD_WIRE_PATH, only for the fields
// where that path is itself the jargon a user got stuck on) is secondary.
const FIELD_LABEL: Record<keyof SettingsState, string> = {
  documentTitle: "Title",
  documentSummary: "Summary",
  documentTags: "Tags",
  documentMetadata: "Metadata",
  input: "Workflow input",
  output: "Workflow output",
  timeout: "Timeout",
  schedule: "Schedule",
  authentications: "Authentication schemes",
  errors: "Error definitions",
  extensions: "Extensions",
  retries: "Retry policies",
  functions: "Reusable functions",
  timeouts: "Named timeouts",
  catalogs: "Resource catalogs",
  secrets: "Secrets",
};

// Shown as a small monospace caption next to the label, only where the raw
// DSL path is worth surfacing (the "use.*" catalogs - so an "authentication
// schemes" field is still recognizable as "use.authentications" to someone
// reading the YAML). Skipped for document.tags/metadata and workflow
// input/output/timeout/schedule, which read fine as plain English alone.
const FIELD_WIRE_PATH: Partial<Record<keyof SettingsState, string>> = {
  authentications: "use.authentications",
  errors: "use.errors",
  extensions: "use.extensions",
  retries: "use.retries",
  functions: "use.functions",
  timeouts: "use.timeouts",
  catalogs: "use.catalogs",
  secrets: "use.secrets",
};

// A short reminder of what each field means or which task-level field
// references it, so this panel doesn't read as a wall of disconnected text
// boxes.
const FIELD_HINT: Partial<Record<keyof SettingsState, string>> = {
  documentTitle: 'Shown inside the workflow file itself. Different from "Display name" above the editor, which is only used by this catalog UI.',
  documentTags: "an object of freeform labels, e.g. {\"team\": \"platform\"}",
  documentMetadata: "an object of freeform business metadata carried through to the compiled plan",
  input: 'schema + "from" transform applied to the whole workflow\'s input, same shape as a task\'s own input',
  output: 'schema + "as" transform applied to the whole workflow\'s output, same shape as a task\'s own output',
  authentications: "referenced from a call task's endpoint authentication",
  errors: 'referenced by name from a "raise" task\'s error',
  retries: 'referenced by name from a "try" task\'s catch.retry',
  functions: 'invoked by name from a "call" task\'s target',
  timeouts: "referenced by name from any task's timeout",
  catalogs: 'resolved against by a "run" task\'s workflow variant',
};

const EMPTY_STATE: SettingsState = {
  documentTitle: "",
  documentSummary: "",
  documentTags: "",
  documentMetadata: "",
  input: "",
  output: "",
  timeout: "",
  schedule: "",
  authentications: "",
  errors: "",
  extensions: "",
  retries: "",
  functions: "",
  timeouts: "",
  catalogs: "",
  secrets: "",
};

// Mirrors WorkflowCanvas.tsx's own try/catch around fromYaml(source) - a
// syntax error mid-edit in the Source tab shouldn't crash this panel (no
// error boundary sits above it), it should show the same "not valid enough
// yet" message and leave the fields blank until the source parses again.
function safeStateOf(source: string): {
  state: SettingsState;
  evaluateMode: "strict" | "loose";
  parseError?: string;
} {
  try {
    const settings = parseWorkflowSettings(source);
    return { state: stateOf(settings), evaluateMode: settings.evaluateMode ?? "strict" };
  } catch (error) {
    return {
      state: EMPTY_STATE,
      evaluateMode: "strict",
      parseError: `This source isn't valid enough to load settings from yet: ${
        error instanceof Error ? error.message : String(error)
      }`,
    };
  }
}

function toText(value: unknown): string {
  if (value === undefined) return "";
  if (typeof value === "string") return value;
  return JSON.stringify(value, null, 2);
}

function stateOf(settings: WorkflowSettings): SettingsState {
  return {
    documentTitle: settings.documentTitle ?? "",
    documentSummary: settings.documentSummary ?? "",
    documentTags: toText(settings.documentTags),
    documentMetadata: toText(settings.documentMetadata),
    input: toText(settings.input),
    output: toText(settings.output),
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

  settings.documentTitle = state.documentTitle.trim() || undefined;
  settings.documentSummary = state.documentSummary.trim() || undefined;

  if (state.timeout.trim()) {
    try {
      settings.timeout = JSON.parse(state.timeout);
    } catch {
      settings.timeout = state.timeout;
    }
  }

  const jsonOnlyFields = FIELD_ORDER.filter(
    (field) => field !== "timeout" && !PLAIN_TEXT_FIELDS.includes(field),
  );
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
  const [state, setState] = useState<SettingsState>(() => safeStateOf(source).state);
  const [evaluateMode, setEvaluateMode] = useState<"strict" | "loose">(
    () => safeStateOf(source).evaluateMode,
  );
  const [parseError, setParseError] = useState<string>();
  const [errors, setErrors] = useState<
    Partial<Record<keyof SettingsState, string>>
  >({});

  useEffect(() => {
    const { state: nextState, evaluateMode: nextEvaluateMode, parseError: nextParseError } =
      safeStateOf(source);
    setState(nextState);
    setEvaluateMode(nextEvaluateMode);
    setParseError(nextParseError);
    setErrors({});
  }, [source]);

  function setField(field: keyof SettingsState, value: string) {
    setState((current) => ({ ...current, [field]: value }));
  }

  function commit(nextEvaluateMode?: "strict" | "loose") {
    // Editing while the source doesn't parse would just throw again inside
    // applyWorkflowSettings (it calls the same js-yaml load() fromYaml
    // does) - field edits stay local until the source is valid again, same
    // as the canvas leaving nodes uneditable while its own parse fails.
    if (parseError) return;
    const { settings, errors: fieldErrors } = resolveSettings(state);
    setErrors(fieldErrors);
    if (Object.keys(fieldErrors).length > 0) return;
    // Same stale-closure trap this session's TaskInspector.tsx hit with MUI
    // Select: onChange fires in the same tick as its own setState, so
    // reading "evaluateMode" here would still see the OLD value on the
    // very change that's supposed to update it - an explicit override
    // parameter sidesteps that instead of relying on the closure.
    settings.evaluateMode = nextEvaluateMode ?? evaluateMode;
    onSourceChange(applyWorkflowSettings(source, settings));
  }

  function renderField(field: keyof SettingsState) {
    const isPlainText = PLAIN_TEXT_FIELDS.includes(field);
    const isJson = field !== "timeout" && !isPlainText;
    const wirePath = FIELD_WIRE_PATH[field];
    return (
      <TextField
        key={field}
        label={
          <>
            {FIELD_LABEL[field]}
            {isJson ? " (JSON)" : ""}
            {wirePath && (
              <Box
                component="span"
                sx={{ ml: 0.75, fontFamily: "monospace", opacity: 0.7 }}
              >
                {wirePath}
              </Box>
            )}
          </>
        }
        placeholder={
          field === "timeout" ? "PT30S, or a name from use.timeouts" : undefined
        }
        helperText={errors[field] ?? FIELD_HINT[field]}
        error={Boolean(errors[field])}
        multiline={isJson}
        minRows={isJson ? 3 : undefined}
        size="small"
        value={state[field]}
        onChange={(event) => setField(field, event.target.value)}
        onBlur={() => commit()}
      />
    );
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
        Document-level properties, not per-task ones - the workflow's own
        identity and metadata, its whole-workflow input/output and
        timeout/schedule, and the reusable, named things tasks elsewhere in
        this workflow refer back to.
      </Typography>
      {parseError && (
        <Typography variant="body2" color="error">
          {parseError}
        </Typography>
      )}
      {OPEN_SECTIONS.map((section) => (
        <Box key={section.heading} sx={{ display: "flex", flexDirection: "column", gap: 1.5 }}>
          <Typography variant="subtitle2">{section.heading}</Typography>
          {section.fields.map(renderField)}
          {section.heading === "Data & expressions" && (
            <FormControl size="small">
              <InputLabel id="evaluate-mode-label">Evaluate mode</InputLabel>
              <Select
                labelId="evaluate-mode-label"
                label="Evaluate mode"
                value={evaluateMode}
                onChange={(event) => {
                  const next = event.target.value as "strict" | "loose";
                  setEvaluateMode(next);
                  commit(next);
                }}
              >
                <MenuItem value="strict">Strict</MenuItem>
                <MenuItem value="loose">Loose</MenuItem>
              </Select>
              <FormHelperText>
                Runtime-expression recognition for every "${'{'} ... {'}'}" in
                this workflow. Strict (the default) rejects malformed
                expressions; loose is more permissive.
              </FormHelperText>
            </FormControl>
          )}
        </Box>
      ))}
      {COLLAPSED_SECTIONS.map((section) => {
        const setCount = section.fields.filter((field) => state[field].trim() !== "").length;
        return (
          <Accordion
            key={section.heading}
            disableGutters
            elevation={0}
            sx={{ border: 1, borderColor: "divider", "&:before": { display: "none" } }}
          >
            <AccordionSummary expandIcon={<ExpandMoreIcon />}>
              <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                <Typography variant="body2">{section.heading}</Typography>
                {setCount > 0 && (
                  <Chip
                    size="small"
                    label={`${setCount} of ${section.fields.length} set`}
                    color="primary"
                    variant="outlined"
                  />
                )}
              </Box>
            </AccordionSummary>
            <AccordionDetails sx={{ display: "flex", flexDirection: "column", gap: 1.5 }}>
              <Typography variant="caption" color="text.secondary">
                {section.description}
              </Typography>
              {section.fields.map(renderField)}
            </AccordionDetails>
          </Accordion>
        );
      })}
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
