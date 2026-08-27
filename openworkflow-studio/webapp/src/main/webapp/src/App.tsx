import { useEffect, useMemo, useState } from "react";
import { ThemeProvider } from "@mui/material/styles";
import type {
  Workflow,
  WorkflowDefinition,
} from "@forwardmeasure/openworkflow-definition-management-client";
import type {
  Execution,
  ExecutionHistoryEntry,
} from "@forwardmeasure/openworkflow-execution-client";
import { authorizationDecisions, clients, correlationId } from "./api";
import { WorkflowCanvas } from "./canvas/WorkflowCanvas";
import type { StudioIdentity } from "./runtime";
import { tenantFromToken } from "./session";
import { createStudioMuiTheme } from "./theme";
import { canPause, diagnostic, lineDiff, SAMPLE, taskNames } from "./workflow";

// Built once at module scope, not per-render - it only depends on THEMES in
// theme.ts, never on component state.
const muiTheme = createStudioMuiTheme();

type View = "author" | "executions";
type EditorView = "source" | "canvas";
type GovernanceAction = "submit" | "approve" | "reject" | "publish";
type ExecutionControlAction = "pause" | "resume" | "cancel";

// Refresh well before the access token's own 1-hour lifetime (see the real
// expires_in on an issued token) - 30s poll, refresh once under 60s
// remaining, matching Keycloak's own recommended updateToken() margin.
const TOKEN_REFRESH_POLL_MS = 30_000;
const TOKEN_REFRESH_MIN_VALIDITY_SECONDS = 60;

function entityTag(revision: number): string {
  return `"${revision}"`;
}

export function App({ identity }: { identity: StudioIdentity }) {
  const [token, setToken] = useState(identity.token);

  // clients(token)/api.ts takes a plain string, not a dynamic supplier the
  // way PlatformDashboard's generated client does - so staying authenticated
  // past the token's own lifetime means polling keycloak-js for a refresh
  // and pushing the new token through React state, which recreates the api
  // client below (useMemo keyed on token).
  useEffect(() => {
    const interval = setInterval(() => {
      identity.keycloak
        .updateToken(TOKEN_REFRESH_MIN_VALIDITY_SECONDS)
        .then((refreshed) => {
          if (refreshed && identity.keycloak.token) {
            setToken(identity.keycloak.token);
          }
        })
        .catch(() => {
          // Refresh failed (session/refresh-token expired) - a fresh
          // top-level login is the only recovery, same as
          // PlatformDashboard's own onLoad: "login-required" would force on
          // a reload.
          void identity.keycloak.login();
        });
    }, TOKEN_REFRESH_POLL_MS);
    return () => clearInterval(interval);
  }, [identity.keycloak]);

  return (
    <ThemeProvider theme={muiTheme}>
      <Studio
        token={token}
        logout={() =>
          void identity.keycloak.logout({ redirectUri: window.location.origin })
        }
      />
    </ThemeProvider>
  );
}

function Studio({ token, logout }: { token: string; logout: () => void }) {
  const api = useMemo(() => clients(token), [token]);
  const [view, setView] = useState<View>("author");
  const [editorView, setEditorView] = useState<EditorView>("source");
  const [source, setSource] = useState(SAMPLE);
  const [definitionKey, setDefinitionKey] = useState(
    "forwardmeasure-hello-studio",
  );
  const [definitionVersion, setDefinitionVersion] = useState("1.0.0");
  const [displayName, setDisplayName] = useState("Hello Studio");
  const [diagnostics, setDiagnostics] = useState("Not yet validated.");
  const [busy, setBusy] = useState(false);
  const [workflows, setWorkflows] = useState<Workflow[]>([]);
  const [definitions, setDefinitions] = useState<WorkflowDefinition[]>([]);
  const [executions, setExecutions] = useState<Execution[]>([]);
  const [selected, setSelected] = useState<Execution>();
  const [history, setHistory] = useState<ExecutionHistoryEntry[]>([]);
  const [input, setInput] = useState("{}");
  const [permissions, setPermissions] = useState<Record<string, boolean>>({});
  const tasks = taskNames(source);
  const workflowById = new Map(
    workflows.map((workflow) => [workflow.id, workflow]),
  );
  const currentWorkflow = workflows.find(
    (workflow) => workflow.name === definitionKey,
  );
  const currentDefinition = definitions.find(
    (definition) =>
      definition.workflowId === currentWorkflow?.id &&
      definition.version === definitionVersion,
  );
  const previousSource = currentDefinition?.source;

  useEffect(() => {
    void refreshDefinitions();
  }, []);
  useEffect(() => {
    const mergePermissions = (decisions: Record<string, boolean>) =>
      setPermissions((previous) => ({ ...previous, ...decisions }));
    // Two calls, not one: the batch-authorization endpoint evaluates every
    // action against a single resource, so definition:* and execution:*
    // actions - which are authorized against different resources
    // (openworkflow-definition/definitions vs openworkflow-execution/
    // executions) - can't share one request without evaluating the
    // execution actions against the wrong resource.
    void authorizationDecisions(
      token,
      [
        "definition:create",
        "definition:validate",
        "definition:update",
        "definition:submit",
        "definition:approve",
        "definition:reject",
        "definition:publish",
      ],
      "openworkflow-definition",
      "definitions",
    )
      .then(mergePermissions)
      .catch(async (error) => setDiagnostics(await diagnostic(error)));
    void authorizationDecisions(
      token,
      ["execution:start", "execution:pause", "execution:resume", "execution:cancel"],
      "openworkflow-execution",
      "executions",
    )
      .then(mergePermissions)
      .catch(async (error) => setDiagnostics(await diagnostic(error)));
  }, [token]);

  async function refreshDefinitions() {
    try {
      const [workflowPage, definitionPage] = await Promise.all([
        api.workflows.listWorkflows({ limit: 100 }),
        api.definitions.searchWorkflowDefinitions({ limit: 100 }),
      ]);
      setWorkflows(workflowPage.data);
      setDefinitions(definitionPage.data);
    } catch (error) {
      setDiagnostics(await diagnostic(error));
    }
  }

  async function validate() {
    setBusy(true);
    try {
      if (!currentDefinition) {
        throw new Error("Save this workflow definition before validating it.");
      }
      if (currentDefinition.source !== source) {
        throw new Error("Save the current source before validating it.");
      }
      const result = await api.governance.validateWorkflowDefinition({
        ifMatch: entityTag(currentDefinition.revision),
        workflowId: currentDefinition.workflowId,
        definitionId: currentDefinition.id,
      });
      setDiagnostics(
        result.valid
          ? `Valid Open Workflow definition. Source digest ${result.sourceSha256.slice(0, 12)}.`
          : `Validation failed: ${result.violations.join("; ")}`,
      );
    } catch (error) {
      setDiagnostics(await diagnostic(error));
    } finally {
      setBusy(false);
    }
  }

  async function saveDraft() {
    setBusy(true);
    try {
      let workflow = workflows.find(
        (candidate) => candidate.name === definitionKey,
      );
      if (!workflow) {
        workflow = await api.workflows.createWorkflow({
          createWorkflowRequest: { name: definitionKey, title: displayName },
        });
      } else if (workflow.title !== displayName) {
        workflow = await api.workflows.updateWorkflow({
          ifMatch: entityTag(workflow.revision),
          workflowId: workflow.id,
          updateWorkflowRequest: {
            name: workflow.name,
            title: displayName,
            description: workflow.description,
          },
        });
      }

      const existing = definitions.find(
        (definition) =>
          definition.workflowId === workflow.id &&
          definition.version === definitionVersion,
      );
      if (existing && existing.status !== "draft") {
        throw new Error(
          `Version ${definitionVersion} is ${existing.status}; choose a new version to author another revision.`,
        );
      }
      const saved = existing
        ? await api.definitions.updateWorkflowDefinition({
            ifMatch: entityTag(existing.revision),
            workflowId: workflow.id,
            definitionId: existing.id,
            updateWorkflowDefinitionRequest: { source },
          })
        : await api.definitions.createWorkflowDefinition({
            workflowId: workflow.id,
            createWorkflowDefinitionRequest: {
              version: definitionVersion,
              source,
            },
          });
      setDiagnostics(
        `Saved ${workflow.name} version ${saved.version} as ${saved.status}.`,
      );
      await refreshDefinitions();
    } catch (error) {
      setDiagnostics(await diagnostic(error));
    } finally {
      setBusy(false);
    }
  }

  async function transition(
    definition: WorkflowDefinition,
    action: GovernanceAction,
  ) {
    setBusy(true);
    try {
      const request = {
        ifMatch: entityTag(definition.revision),
        workflowId: definition.workflowId,
        definitionId: definition.id,
      };
      const changed =
        action === "submit"
          ? await api.governance.submitWorkflowDefinition(request)
          : action === "approve"
            ? await api.governance.approveWorkflowDefinition(request)
            : action === "reject"
              ? await api.governance.rejectWorkflowDefinition({
                  ...request,
                  reviewDecisionRequest: {
                    reason: "Rejected in OpenWorkflow Studio",
                  },
                })
              : await api.governance.publishWorkflowDefinition(request);
      const workflow = workflowById.get(changed.workflowId);
      setDiagnostics(
        `${workflow?.title ?? changed.namespace} is now ${changed.status}.`,
      );
      await refreshDefinitions();
    } catch (error) {
      setDiagnostics(await diagnostic(error));
    } finally {
      setBusy(false);
    }
  }

  async function start(definition: WorkflowDefinition) {
    setBusy(true);
    try {
      const execution = await api.executions.startExecution({
        idempotencyKey: crypto.randomUUID(),
        xCorrelationID: correlationId(),
        executionStart: { revisionId: definition.id, input: JSON.parse(input) },
      });
      setDiagnostics(
        `Execution ${execution.id} accepted in ${execution.state}.`,
      );
      setView("executions");
      await refreshExecutions();
    } catch (error) {
      setDiagnostics(await diagnostic(error));
    } finally {
      setBusy(false);
    }
  }

  async function refreshExecutions() {
    try {
      setExecutions((await api.executions.listExecutions({ limit: 50 })).items);
    } catch (error) {
      setDiagnostics(await diagnostic(error));
    }
  }

  async function inspect(executionId: string) {
    try {
      const [execution, events] = await Promise.all([
        api.executions.getExecution({ executionId }),
        api.executions.getExecutionHistory({ executionId, limit: 500 }),
      ]);
      setSelected(execution);
      setHistory(events.items);
    } catch (error) {
      setDiagnostics(await diagnostic(error));
    }
  }

  async function control(operation: ExecutionControlAction) {
    if (!selected) return;
    setBusy(true);
    try {
      const request = {
        executionId: selected.id,
        xCorrelationID: correlationId(),
        ifMatch: entityTag(selected.version),
        executionControl: { reason: `Studio ${operation}` },
      };
      const changed =
        operation === "pause"
          ? await api.executions.pauseExecution(request)
          : operation === "resume"
            ? await api.executions.resumeExecution(request)
            : await api.executions.cancelExecution(request);
      setSelected(changed);
      setDiagnostics(`${operation} accepted: ${changed.state}.`);
      await inspect(selected.id);
      await refreshExecutions();
    } catch (error) {
      setDiagnostics(await diagnostic(error));
    } finally {
      setBusy(false);
    }
  }

  function exportSource() {
    const link = document.createElement("a");
    link.href = URL.createObjectURL(
      new Blob([source], { type: "application/yaml" }),
    );
    link.download = "workflow.yaml";
    link.click();
    URL.revokeObjectURL(link.href);
  }

  return (
    <div className="app-shell">
      <header>
        <div>
          <p className="eyebrow">ForwardMeasure · {tenantFromToken(token)}</p>
          <h1>OpenWorkflow Studio</h1>
        </div>
        <nav aria-label="Primary">
          <button
            className={view === "author" ? "active" : ""}
            onClick={() => setView("author")}
          >
            Author
          </button>
          <button
            className={view === "executions" ? "active" : ""}
            onClick={() => {
              setView("executions");
              void refreshExecutions();
            }}
          >
            Executions
          </button>
          <button className="quiet" onClick={logout}>
            Sign out
          </button>
        </nav>
      </header>
      <div className="status" role="status" aria-live="polite">
        {diagnostics}
      </div>
      {view === "author" ? (
        <main className="author-grid">
          <section
            className="panel editor-panel"
            aria-labelledby="editor-title"
          >
            <div className="panel-heading">
              <div>
                <p className="eyebrow">Lossless source</p>
                <h2 id="editor-title">Workflow definition</h2>
              </div>
              <div className="actions">
                <button
                  className={editorView === "source" ? "active" : "secondary"}
                  onClick={() => setEditorView("source")}
                >
                  Source
                </button>
                <button
                  className={editorView === "canvas" ? "active" : "secondary"}
                  onClick={() => setEditorView("canvas")}
                >
                  Canvas
                </button>
                <label className="button-label">
                  Import
                  <input
                    className="visually-hidden"
                    type="file"
                    accept=".yaml,.yml,.json"
                    onChange={async (event) => {
                      const file = event.target.files?.[0];
                      if (file) setSource(await file.text());
                    }}
                  />
                </label>
                <button className="secondary" onClick={exportSource}>
                  Export
                </button>
                {permissions["definition:validate"] && (
                  <button disabled={busy} onClick={() => void validate()}>
                    Validate
                  </button>
                )}
                {(permissions["definition:create"] ||
                  permissions["definition:update"]) && (
                  <button disabled={busy} onClick={() => void saveDraft()}>
                    Save draft
                  </button>
                )}
              </div>
            </div>
            <div className="metadata">
              <label>
                Workflow name
                <input
                  value={definitionKey}
                  onChange={(event) => setDefinitionKey(event.target.value)}
                />
              </label>
              <label>
                Display name
                <input
                  value={displayName}
                  onChange={(event) => setDisplayName(event.target.value)}
                />
              </label>
              <label>
                Definition version
                <input
                  value={definitionVersion}
                  onChange={(event) => setDefinitionVersion(event.target.value)}
                />
              </label>
            </div>
            {editorView === "source" ? (
              <textarea
                className="editor"
                aria-label="Workflow YAML or JSON"
                spellCheck={false}
                value={source}
                onChange={(event) => setSource(event.target.value)}
              />
            ) : (
              <div className="canvas-shell">
                <WorkflowCanvas source={source} onSourceChange={setSource} />
              </div>
            )}
            {previousSource && previousSource !== source && (
              <details>
                <summary>Revision diff</summary>
                <pre aria-label="Revision diff">
                  {lineDiff(previousSource, source).join("\n")}
                </pre>
              </details>
            )}
          </section>
          <aside className="panel diagram" aria-labelledby="diagram-title">
            <p className="eyebrow">Derived view</p>
            <h2 id="diagram-title">Execution flow</h2>
            <p className="muted">The diagram never rewrites the source.</p>
            <ol>
              {tasks.length ? (
                tasks.map((task, index) => (
                  <li key={`${task}-${index}`}>
                    <span>{index + 1}</span>
                    {task}
                  </li>
                ))
              ) : (
                <li className="empty">No sequential tasks detected.</li>
              )}
            </ol>
            <h3>Governed revisions</h3>
            {definitions.length === 0 ? (
              <p className="muted">No definitions in this tenant.</p>
            ) : (
              definitions.map((definition) => {
                const workflow = workflowById.get(definition.workflowId);
                return (
                  <article className="definition" key={definition.id}>
                    <strong>{workflow?.title ?? definition.namespace}</strong>
                    <small>
                      {workflow?.name ?? definition.workflowId} · version{" "}
                      {definition.version} · {definition.status}
                    </small>
                    <button
                      className="quiet"
                      onClick={() => {
                        setSource(definition.source);
                        setDefinitionKey(
                          workflow?.name ?? definition.namespace,
                        );
                        setDefinitionVersion(definition.version);
                        setDisplayName(workflow?.title ?? definition.namespace);
                      }}
                    >
                      Open source
                    </button>
                    <div className="actions">
                      {definition.status === "draft" &&
                        permissions["definition:submit"] && (
                          <button
                            disabled={busy}
                            onClick={() =>
                              void transition(definition, "submit")
                            }
                          >
                            Submit
                          </button>
                        )}
                      {definition.status === "in_review" && (
                        <>
                          {permissions["definition:approve"] && (
                            <button
                              disabled={busy}
                              onClick={() =>
                                void transition(definition, "approve")
                              }
                            >
                              Approve
                            </button>
                          )}
                          {permissions["definition:reject"] && (
                            <button
                              disabled={busy}
                              onClick={() =>
                                void transition(definition, "reject")
                              }
                            >
                              Reject
                            </button>
                          )}
                        </>
                      )}
                      {definition.status === "approved" &&
                        permissions["definition:publish"] && (
                          <button
                            disabled={busy}
                            onClick={() =>
                              void transition(definition, "publish")
                            }
                          >
                            Publish
                          </button>
                        )}
                    </div>
                    {definition.status === "published" &&
                      permissions["execution:start"] && (
                        <>
                          <label>
                            Execution input
                            <textarea
                              rows={3}
                              value={input}
                              onChange={(event) => setInput(event.target.value)}
                            />
                          </label>
                          <button
                            disabled={busy}
                            onClick={() => void start(definition)}
                          >
                            Start workflow
                          </button>
                        </>
                      )}
                  </article>
                );
              })
            )}
          </aside>
        </main>
      ) : (
        <main className="execution-grid">
          <section className="panel execution-list">
            <div className="panel-heading">
              <h2>Executions</h2>
              <button
                className="secondary"
                onClick={() => void refreshExecutions()}
              >
                Refresh
              </button>
            </div>
            {executions.length === 0 ? (
              <p className="muted">No projected executions.</p>
            ) : (
              executions.map((execution) => (
                <button
                  className="execution-row"
                  key={execution.id}
                  onClick={() => void inspect(execution.id)}
                >
                  <span>
                    <strong>{execution.engineId}</strong>
                    <small>{execution.id}</small>
                  </span>
                  <mark data-status={execution.state}>{execution.state}</mark>
                </button>
              ))
            )}
          </section>
          <section className="panel detail" aria-live="polite">
            {!selected ? (
              <p className="muted">
                Select an execution to inspect state, history, output, or
                failure.
              </p>
            ) : (
              <>
                <div className="panel-heading">
                  <div>
                    <p className="eyebrow">{selected.id}</p>
                    <h2>{selected.engineId}</h2>
                  </div>
                  <mark data-status={selected.state}>{selected.state}</mark>
                </div>
                <div className="actions">
                  {permissions["execution:pause"] && (
                    <button
                      disabled={busy || !canPause(selected.state)}
                      onClick={() => void control("pause")}
                    >
                      Pause
                    </button>
                  )}
                  {permissions["execution:resume"] && (
                    <button
                      disabled={busy || selected.state !== "PAUSED"}
                      onClick={() => void control("resume")}
                    >
                      Resume
                    </button>
                  )}
                  {permissions["execution:cancel"] && (
                    <button
                      className="danger"
                      disabled={
                        busy ||
                        ["COMPLETED", "CANCELLED", "FAILED"].includes(
                          selected.state,
                        )
                      }
                      onClick={() => void control("cancel")}
                    >
                      Cancel
                    </button>
                  )}
                </div>
                <h3>Timeline</h3>
                <ol className="history">
                  {history.map((item) => (
                    <li key={item.eventId}>
                      <span>{item.sequence}</span>
                      <strong>{item.type}</strong>
                      <time>{item.occurredAt.toLocaleString()}</time>
                    </li>
                  ))}
                </ol>
                <h3>Output</h3>
                <pre>{JSON.stringify(selected.output, null, 2)}</pre>
                {selected.error && (
                  <>
                    <h3>Failure</h3>
                    <pre className="failure">
                      {JSON.stringify(selected.error, null, 2)}
                    </pre>
                  </>
                )}
              </>
            )}
          </section>
        </main>
      )}
    </div>
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
