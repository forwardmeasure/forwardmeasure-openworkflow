# Handoff: Grow OpenWorkflow Studio toward Flowise-level capability

## What this is

OpenWorkflow Studio's workflow canvas is a lightweight, Flowise-inspired editor — built on the
same base library Flowise uses (`@xyflow/react` v12), following some of its conventions (Start/End
anchor nodes, per-task node cards) — but it currently covers only a thin slice of what a real
Flowise-class tool does. This doc is a handoff so a new chat can start planning/implementing without
re-deriving orientation from scratch.

**This is a live production repo.** The cluster it deploys to (`openworkflow-prod` on GKE, tenant
`lux`, domain `kriyagentic.com`) just came off a multi-day debugging saga to get the existing
Studio→backend path working end-to-end for the first time — see this session's memory
(`project_openworkflow_studio_debugging_state.md`) for the full list of what was fixed and what's
still deliberately left open (a temporary debug exception mapper, some flagged-not-fixed duplication
risk). Read that before assuming anything about current backend reliability.

## Where the code lives

Repo: `/home/pn/Documents/code/forwardmeasure/forwardmeasure-openworkflow`

```
openworkflow-studio/
  webapp/
    src/main/webapp/src/
      App.tsx              # Main app shell: view state, save/validate/execute flows, permission gating
      api.ts                # Generated-client wiring (definition-management + execution-management)
      workflow.ts            # DSL sample source, task-name extraction, error-diagnostic formatting
      theme.ts                # CSS-custom-property theme tokens + MUI theme construction
      styles.css                # All visual styling, keyed off theme.ts's CSS variables
      canvas/
        dsl.ts                    # Parses/serializes CNCF Serverless Workflow YAML <-> canvas model
        WorkflowCanvas.tsx          # React Flow graph: layout, edges, node drag/persist, add/delete task
        TaskNode.tsx                  # Visual node card per task kind
        TaskInspector.tsx               # Side-panel editor for a selected task's fields
        AnchorNode.tsx                    # Non-editable Start/End nodes
    src/main/java/.../StudioApiProxy.java   # Framework-neutral proxy: browser -> Studio backend -> real APIs
  framework-bindings/{quarkus,spring,micronaut}/   # Per-framework REST hosting; only quarkus is deployed live

openworkflow-api-specifications/openworkflow-services-api-spec/  # OpenAPI specs (source of truth for all API shapes)
```

Frontend stack: React + `@xyflow/react` 12.11.5 (React Flow) + MUI v9 + Vite + Vitest.

## Current state — what's actually implemented

- **Task types**: only `set` (assign variables), `call` (invoke function/HTTP/OpenAPI operation),
  and `switch` (branch to a named task). Confirmed directly in `canvas/dsl.ts`'s own comment: `"for"/
  "fork"/"try" and other flow-control constructs are still out of scope for this slice.`
- **Layout**: a from-scratch layered auto-layout (`layerColumns()` in `WorkflowCanvas.tsx`) — no
  save/restore of a user-arranged layout beyond node-drag persistence within one session.
- **Editing**: source (raw YAML/JSON) view and canvas view, kept losslessly in sync — no visual
  diffing beyond a simple line-diff `<details>` panel.
- **Governance**: validate / save draft / submit / approve / reject / publish / deprecate lifecycle,
  gated by AuthZEN permission checks fetched from the backend.
- **Execution**: list/start/pause/resume/cancel, execution history view — this path was only just
  wired up correctly (Studio's proxy never even reached the execution-management backend until this
  session's fixes).
- **No**: node/tool palette beyond the 3 task types, marketplace/templates, credentials management UI,
  chat/agent-flow mode, embeddable widget, webhook triggers, memory/vector-store node types, execution
  tracing/observability UI beyond the plain history list, versioning UI beyond raw revision numbers.

## The actual question to scope in the new chat

Flowise's real capability set splits roughly into two different axes — worth deciding which one (or
what mix) is the goal before writing any code, since they pull the plan in different directions:

1. **Breadth** — many integration/tool node types, a plugin-style extensible node palette, a
   marketplace of starter templates.
2. **UX depth** — polish around the existing small node set: execution tracing/observability,
   versioning/diffing, a richer inspector, better canvas ergonomics (multi-select, copy/paste,
   grouping).

Also worth deciding early: is this an extension of the *existing* `set`/`call`/`switch` DSL model
(CNCF Serverless Workflow), or does "Flowise-level" mean introducing a different/broader task
vocabulary (`for`/`fork`/`try`, or something entirely new) — the DSL itself
(`openworkflow-api-specifications`) is the actual constraint on what the canvas can ever represent,
not just the frontend code.

## Suggested first steps for the new chat

1. Actually look at what Flowise (the real product) provides — its node palette categories, its
   chatflow vs agentflow distinction, its marketplace — to ground "capabilities we don't leverage" in
   specifics rather than a general impression.
2. Cross-reference against the CNCF Serverless Workflow DSL spec (`https://serverlessworkflow.io`,
   already the source of truth this canvas targets) to see which Flowise-style capabilities map onto
   DSL constructs already supported by the backend but not yet the canvas (`for`/`fork`/`try`), versus
   which would require new backend capability entirely.
3. Decide breadth vs. depth (above) with the user before scoping a plan.
