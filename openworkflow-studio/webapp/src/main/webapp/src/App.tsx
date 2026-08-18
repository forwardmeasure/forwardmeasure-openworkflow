import { useEffect, useMemo, useState } from "react";
import type {
  DefinitionRevision,
  LifecycleAction,
} from "@forwardmeasure/openworkflow-definition-management-client";
import type {
  ControlExecutionOperationEnum,
  Execution,
  ExecutionHistoryEntry,
} from "@forwardmeasure/openworkflow-execution-client";
import { authorizationDecisions, clients, correlationId, listDefinitionRevisions } from "./api";
import { getToken, setToken, tenantFromToken } from "./session";
import { canPause, diagnostic, lineDiff, SAMPLE, taskNames } from "./workflow";

type View = "author" | "executions";
type StudioDefinition = DefinitionRevision & { revisionId: string };

export function App() {
  const [token, updateToken] = useState(getToken);
  const [draftToken, setDraftToken] = useState("");
  if (!token) {
    return (
      <main className="login-shell">
        <section className="login-card" aria-labelledby="login-title">
          <p className="eyebrow">ForwardMeasure</p>
          <h1 id="login-title">OpenWorkflow Studio</h1>
          <p>Use an access token issued for your tenant.</p>
          <label htmlFor="token">Bearer access token</label>
          <textarea id="token" rows={5} value={draftToken} onChange={(event) => setDraftToken(event.target.value)} />
          <button onClick={() => { const value = draftToken.trim(); if (value) { setToken(value); updateToken(value); } }}>Enter Studio</button>
        </section>
      </main>
    );
  }
  return <Studio token={token} logout={() => { setToken(""); updateToken(""); }} />;
}

function Studio({ token, logout }: { token: string; logout: () => void }) {
  const api = useMemo(() => clients(token), [token]);
  const [view, setView] = useState<View>("author");
  const [source, setSource] = useState(SAMPLE);
  const [definitionKey, setDefinitionKey] = useState("forwardmeasure.hello-studio");
  const [displayName, setDisplayName] = useState("Hello Studio");
  const [diagnostics, setDiagnostics] = useState("Not yet validated.");
  const [busy, setBusy] = useState(false);
  const [definitions, setDefinitions] = useState<StudioDefinition[]>([]);
  const [executions, setExecutions] = useState<Execution[]>([]);
  const [selected, setSelected] = useState<Execution>();
  const [history, setHistory] = useState<ExecutionHistoryEntry[]>([]);
  const [input, setInput] = useState("{}");
  const [permissions, setPermissions] = useState<Record<string, boolean>>({});
  const tasks = taskNames(source);
  const previousSource = definitions.find((item) => item.definitionKey === definitionKey)?.sourceDocument;

  useEffect(() => { void refreshDefinitions(); }, []);
  useEffect(() => {
    void authorizationDecisions(token, ["definition:create", "definition:validate", "definition:update", "definition:submit", "definition:approve", "definition:reject", "definition:publish", "execution:start"], "openworkflow-definition", "definitions")
      .then(setPermissions)
      .catch((error) => { setPermissions({}); setDiagnostics(diagnostic(error)); });
  }, [token]);

  async function refreshDefinitions() {
    try {
      setDefinitions(await listDefinitionRevisions(token) as StudioDefinition[]);
    } catch (error) { setDiagnostics(diagnostic(error)); }
  }

  async function validate() {
    setBusy(true);
    try {
      const result = await api.definitions.validateDefinition({
        xCorrelationID: correlationId(),
        definitionValidationRequest: { definitionKey, sourceDocument: source },
      });
      setDiagnostics(`Valid Open Workflow ${result.specificationVersion}. Source digest ${result.sourceDigest.slice(0, 12)}.`);
    } catch (error) { setDiagnostics(diagnostic(error)); } finally { setBusy(false); }
  }

  async function saveDraft() {
    setBusy(true);
    try {
      const existing = definitions.filter((item) => item.definitionKey === definitionKey);
      const saved = existing.length
        ? await api.definitions.reviseDefinition({ definitionKey, xCorrelationID: correlationId(), revisionWrite: { displayName, sourceDocument: source } })
        : await api.definitions.createDefinition({ xCorrelationID: correlationId(), definitionWrite: { definitionKey, displayName, sourceDocument: source } });
      setDiagnostics(`Saved ${saved.definitionKey} revision ${saved.revisionNumber} as ${saved.lifecycleState}.`);
      await refreshDefinitions();
    } catch (error) { setDiagnostics(diagnostic(error)); } finally { setBusy(false); }
  }

  async function transition(definition: StudioDefinition, action: LifecycleAction) {
    setBusy(true);
    try {
      const changed = await api.definitions.transitionDefinition({ definitionKey: definition.definitionKey, revisionNumber: definition.revisionNumber, action, xCorrelationID: correlationId() });
      setDiagnostics(`${changed.displayName} is now ${changed.lifecycleState}.`);
      await refreshDefinitions();
    } catch (error) { setDiagnostics(diagnostic(error)); } finally { setBusy(false); }
  }

  async function start(definition: StudioDefinition) {
    setBusy(true);
    try {
      const execution = await api.executions.startExecution({ idempotencyKey: crypto.randomUUID(), xCorrelationId: correlationId(), executionStart: { revisionId: definition.revisionId, input: JSON.parse(input) } });
      setDiagnostics(`Execution ${execution.id} accepted in ${execution.state}.`);
      setView("executions");
      await refreshExecutions();
    } catch (error) { setDiagnostics(diagnostic(error)); } finally { setBusy(false); }
  }

  async function refreshExecutions() {
    try { setExecutions((await api.executions.listExecutions({ limit: 50 })).items); }
    catch (error) { setDiagnostics(diagnostic(error)); }
  }

  async function inspect(executionId: string) {
    try {
      const [execution, events] = await Promise.all([
        api.executions.getExecution({ executionId }),
        api.executions.getExecutionHistory({ executionId, limit: 500 }),
      ]);
      setSelected(execution); setHistory(events);
    } catch (error) { setDiagnostics(diagnostic(error)); }
  }

  async function control(operation: ControlExecutionOperationEnum) {
    if (!selected) return;
    setBusy(true);
    try {
      const changed = await api.executions.controlExecution({ executionId: selected.id, operation, xCorrelationId: correlationId(), ifMatch: selected.version, executionControl: { reason: `Studio ${operation}` } });
      setSelected(changed); setDiagnostics(`${operation} accepted: ${changed.state}.`); await inspect(selected.id); await refreshExecutions();
    } catch (error) { setDiagnostics(diagnostic(error)); } finally { setBusy(false); }
  }

  function exportSource() {
    const link = document.createElement("a");
    link.href = URL.createObjectURL(new Blob([source], { type: "application/yaml" }));
    link.download = "workflow.yaml"; link.click(); URL.revokeObjectURL(link.href);
  }

  return (
    <div className="app-shell">
      <header><div><p className="eyebrow">ForwardMeasure · {tenantFromToken(token)}</p><h1>OpenWorkflow Studio</h1></div>
        <nav aria-label="Primary"><button className={view === "author" ? "active" : ""} onClick={() => setView("author")}>Author</button><button className={view === "executions" ? "active" : ""} onClick={() => { setView("executions"); void refreshExecutions(); }}>Executions</button><button className="quiet" onClick={logout}>Sign out</button></nav>
      </header>
      <div className="status" role="status" aria-live="polite">{diagnostics}</div>
      {view === "author" ? <main className="author-grid">
        <section className="panel editor-panel" aria-labelledby="editor-title">
          <div className="panel-heading"><div><p className="eyebrow">Lossless source</p><h2 id="editor-title">Workflow definition</h2></div><div className="actions">
            <label className="button-label">Import<input className="visually-hidden" type="file" accept=".yaml,.yml,.json" onChange={async (event) => { const file = event.target.files?.[0]; if (file) setSource(await file.text()); }} /></label>
            <button className="secondary" onClick={exportSource}>Export</button>{permissions["definition:validate"] && <button disabled={busy} onClick={() => void validate()}>Validate</button>}{(permissions["definition:create"] || permissions["definition:update"]) && <button disabled={busy} onClick={() => void saveDraft()}>Save draft</button>}
          </div></div>
          <div className="metadata"><label>Definition key<input value={definitionKey} onChange={(event) => setDefinitionKey(event.target.value)} /></label><label>Display name<input value={displayName} onChange={(event) => setDisplayName(event.target.value)} /></label></div>
          <textarea className="editor" aria-label="Workflow YAML or JSON" spellCheck={false} value={source} onChange={(event) => setSource(event.target.value)} />
          {previousSource && previousSource !== source && <details><summary>Revision diff</summary><pre aria-label="Revision diff">{lineDiff(previousSource, source).join("\n")}</pre></details>}
        </section>
        <aside className="panel diagram" aria-labelledby="diagram-title"><p className="eyebrow">Derived view</p><h2 id="diagram-title">Execution flow</h2><p className="muted">The diagram never rewrites the source.</p><ol>{tasks.length ? tasks.map((task, index) => <li key={`${task}-${index}`}><span>{index + 1}</span>{task}</li>) : <li className="empty">No sequential tasks detected.</li>}</ol>
          <h3>Governed revisions</h3>{definitions.length === 0 ? <p className="muted">No definitions in this tenant.</p> : definitions.map((definition) => <article className="definition" key={definition.revisionId}><strong>{definition.displayName}</strong><small>{definition.definitionKey} · revision {definition.revisionNumber} · {definition.lifecycleState}</small><button className="quiet" onClick={() => { setSource(definition.sourceDocument); setDefinitionKey(definition.definitionKey); setDisplayName(definition.displayName); }}>Open source</button><div className="actions">{definition.lifecycleState === "DRAFT" && permissions["definition:submit"] && <button disabled={busy} onClick={() => void transition(definition, "submit")}>Submit</button>}{definition.lifecycleState === "IN_REVIEW" && <>{permissions["definition:approve"] && <button disabled={busy} onClick={() => void transition(definition, "approve")}>Approve</button>}{permissions["definition:reject"] && <button disabled={busy} onClick={() => void transition(definition, "reject")}>Reject</button>}</>}{definition.lifecycleState === "APPROVED" && permissions["definition:publish"] && <button disabled={busy} onClick={() => void transition(definition, "publish")}>Publish</button>}</div>{definition.lifecycleState === "PUBLISHED" && permissions["execution:start"] && <><label>Execution input<textarea rows={3} value={input} onChange={(event) => setInput(event.target.value)} /></label><button disabled={busy} onClick={() => void start(definition)}>Start workflow</button></>}</article>)}
        </aside>
      </main> : <main className="execution-grid"><section className="panel execution-list"><div className="panel-heading"><h2>Executions</h2><button className="secondary" onClick={() => void refreshExecutions()}>Refresh</button></div>{executions.length === 0 ? <p className="muted">No projected executions.</p> : executions.map((execution) => <button className="execution-row" key={execution.id} onClick={() => void inspect(execution.id)}><span><strong>{execution.engineId}</strong><small>{execution.id}</small></span><mark data-status={execution.state}>{execution.state}</mark></button>)}</section>
        <section className="panel detail" aria-live="polite">{!selected ? <p className="muted">Select an execution to inspect state, history, output, or failure.</p> : <><div className="panel-heading"><div><p className="eyebrow">{selected.id}</p><h2>{selected.engineId}</h2></div><mark data-status={selected.state}>{selected.state}</mark></div><div className="actions">{permissions["execution:pause"] && <button disabled={busy || !canPause(selected.state)} onClick={() => void control("pause")}>Pause</button>}{permissions["execution:resume"] && <button disabled={busy || selected.state !== "PAUSED"} onClick={() => void control("resume")}>Resume</button>}{permissions["execution:cancel"] && <button className="danger" disabled={busy || ["COMPLETED", "CANCELLED", "FAILED"].includes(selected.state)} onClick={() => void control("cancel")}>Cancel</button>}</div><h3>Timeline</h3><ol className="history">{history.map((item) => <li key={item.eventId}><span>{item.sequence}</span><strong>{item.type}</strong><time>{item.occurredAt.toLocaleString()}</time></li>)}</ol><h3>Output</h3><pre>{JSON.stringify(selected.output, null, 2)}</pre>{selected.error && <><h3>Failure</h3><pre className="failure">{JSON.stringify(selected.error, null, 2)}</pre></>}</>}</section>
      </main>}
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
