import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import {
  applyNodeChanges,
  Background,
  Controls,
  MarkerType,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  type Edge,
  type Node,
  type NodeChange,
  type OnConnect,
  type OnReconnect,
  type ReactFlowInstance,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import AddCommentOutlinedIcon from "@mui/icons-material/AddCommentOutlined";
import AutoFixHighOutlinedIcon from "@mui/icons-material/AutoFixHighOutlined";
import GroupWorkOutlinedIcon from "@mui/icons-material/GroupWorkOutlined";
import WarningAmberOutlinedIcon from "@mui/icons-material/WarningAmberOutlined";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Breadcrumbs from "@mui/material/Breadcrumbs";
import Button from "@mui/material/Button";
import Chip from "@mui/material/Chip";
import IconButton from "@mui/material/IconButton";
import Link from "@mui/material/Link";
import Tooltip from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";
import { alpha, useTheme } from "@mui/material/styles";
import { AnchorNode, type AnchorNodeData } from "./AnchorNode";
import { InsertableEdge, type InsertableEdgeData } from "./InsertableEdge";
import { NodePalette, PALETTE_DRAG_MIME } from "./NodePalette";
import { StickyNoteNode, type StickyNoteData } from "./StickyNoteNode";
import { traceKey, type TraceEntry } from "./executionTrace";
import { CATEGORY_COLOR, KIND_CATEGORY } from "./taskKindMeta";
import { TaskInspector } from "./TaskInspector";
import { TaskNode, type TaskNodeData } from "./TaskNode";
import { validateWorkflowSource, type ValidationIssue } from "./validation";
import {
  emptyTask,
  fromYaml,
  setChildrenAtPath,
  tasksAtPath,
  toYaml,
  UnsupportedTaskError,
  type Task,
} from "./dsl";

const NODE_TYPES = { task: TaskNode, anchor: AnchorNode, sticky: StickyNoteNode };
const EDGE_TYPES = { insertable: InsertableEdge };
type CanvasNodeData = TaskNodeData | AnchorNodeData | StickyNoteData;
const COLUMN_WIDTH = 260;
const ROW_HEIGHT = 130;
const GRID_SIZE = 16;
const SNAP_GRID: [number, number] = [GRID_SIZE, GRID_SIZE];
// Sentinel ids for the Start/End anchors - unlikely enough not to collide
// with a real task name, and never fed back through dsl.ts, so a collision
// would at worst mis-position one node visually, not corrupt saved YAML.
const START_ID = "__start__";
const END_ID = "__end__";

/**
 * Every task's default successor is whichever task follows it by array
 * position (Serverless Workflow's implicit "then" when none is given) -
 * except a "switch" task, which always redirects via its cases instead of
 * falling through positionally, matching the spec. A case's "then: exit"
 * (or the final task's positional fallthrough) targets the End anchor.
 * "raise" only gets an outgoing edge when its own "then" is explicitly set
 * (see the "raise" branch below) - reaching one with no "then" faults or
 * terminates the workflow rather than falling through positionally, unlike
 * every other kind here.
 */
// "continue"/"exit"/"end" are the three CNCF terminal directives a "then"
// can hold besides a task name (confirmed against OpenWorkflowCompiler's
// validateFlowTarget()) - "continue" (or omitted) falls through to
// whatever's positionally next, "exit"/"end" both terminate to the End
// anchor here (this canvas doesn't distinguish them - see deriveEdges'
// own doc comment on "raise" for the same "one End anchor" simplification).
function resolveThenTarget(then: string | undefined, positionalTarget: string): string {
  if (then === undefined || then === "continue") return positionalTarget;
  if (then === "exit" || then === "end") return END_ID;
  return then;
}

export function deriveEdges(tasks: Task[]): Edge[] {
  const edges: Edge[] = [];
  const firstTask = tasks[0];
  edges.push(
    firstTask
      ? {
          id: `${START_ID}->${firstTask.name}`,
          source: START_ID,
          target: firstTask.name,
        }
      : { id: `${START_ID}->${END_ID}`, source: START_ID, target: END_ID },
  );
  tasks.forEach((task, index) => {
    if (task.kind === "switch") {
      task.cases.forEach((switchCase) => {
        const target = resolveThenTarget(switchCase.then, END_ID);
        edges.push({
          id: `${task.name}:${switchCase.name}->${target}`,
          source: task.name,
          sourceHandle: switchCase.name,
          target,
          label: switchCase.when
            ? `${switchCase.name}: ${switchCase.when}`
            : switchCase.name,
        });
      });
      return;
    }
    if (task.kind === "raise") {
      // Unlike every other kind below, reaching a raise task with no
      // explicit "then" faults/terminates the workflow rather than falling
      // through positionally - so no edge is drawn by default. It still
      // gets a source handle in TaskNode.tsx, though: dragging a connection
      // out of it (via WorkflowCanvas's onConnect) or setting "then" in the
      // Inspector is how you explicitly route what happens after it, which
      // "no handle at all" previously made impossible without leaving the
      // canvas for Source view.
      if (task.then !== undefined) {
        const next = tasks[index + 1];
        const target = resolveThenTarget(task.then, next ? next.name : END_ID);
        edges.push({ id: `${task.name}->${target}`, source: task.name, target });
      }
      return;
    }
    const next = tasks[index + 1];
    const positionalTarget = next ? next.name : END_ID;
    // The gap that motivated this fix: every non-switch/raise task can
    // carry its own "then" (CommonTaskProps, see dsl.ts) overriding this
    // positional fallthrough - deriveEdges previously always used
    // positionalTarget unconditionally, so the canvas silently drew the
    // WRONG edge for any task using an explicit override.
    const target = resolveThenTarget(task.then, positionalTarget);
    edges.push({ id: `${task.name}->${target}`, source: task.name, target });
  });
  return edges;
}

export type EdgeEndpoint = { source: string; target: string; sourceHandle?: string | null };

// True for an edge that exists purely because nothing overrides it - the
// Start->first-task edge (no field to override it with at all), or a
// regular task whose own "then" is unset/"continue". False for every switch
// case (sourceHandle set - always an explicit routing decision) and every
// task using an explicit "then". Mirrors resolveThenTarget's own notion of
// "positional" above.
export function isPositionalEdge(tasks: Task[], edge: EdgeEndpoint): boolean {
  if (edge.sourceHandle) return false;
  if (edge.source === START_ID) return true;
  const sourceTask = tasks.find((t) => t.name === edge.source);
  return sourceTask ? sourceTask.then === undefined || sourceTask.then === "continue" : false;
}

/**
 * Splices a new task directly into an existing edge - "inject a step
 * between two already-connected tasks, anywhere," via InsertableEdge's "+"
 * button, not just append-after-the-last-task the way the node
 * palette/drag-drop already does.
 *
 * A positional edge (the common case) needs nothing beyond array position:
 * inserting the new task right between source and target in "do:" order
 * makes source -> newTask -> target true by pure positional fallthrough, no
 * "then" anywhere. An explicit-override edge (a switch case, or a task's
 * own "then") instead gets the new task's own "then" pointed at the old
 * target, and the source's specific routing field (the switch case, or the
 * task's "then") rewritten to the new task's name - array position there is
 * cosmetic (still placed right after source, for a sane initial layout),
 * the "then" chain is what's actually load-bearing. Returns `tasks`
 * unchanged if `edge.source` isn't a real task and isn't Start.
 */
export function spliceTaskOnEdge(tasks: Task[], edge: EdgeEndpoint, newTask: Task): Task[] {
  const { source, target, sourceHandle } = edge;
  if (source === START_ID) {
    // Always positional (Start has no routing field to override) - prepend,
    // letting array order alone make it the new first task.
    return [newTask, ...tasks];
  }

  const sourceIndex = tasks.findIndex((t) => t.name === source);
  if (sourceIndex < 0) return tasks;

  if (isPositionalEdge(tasks, edge)) {
    return [...tasks.slice(0, sourceIndex + 1), newTask, ...tasks.slice(sourceIndex + 1)];
  }

  const withThen: Task = { ...newTask, then: target === END_ID ? "exit" : target };
  const next = [...tasks.slice(0, sourceIndex + 1), withThen, ...tasks.slice(sourceIndex + 1)];
  const sourceTask = next[sourceIndex];
  next[sourceIndex] =
    sourceTask.kind === "switch" && sourceHandle
      ? {
          ...sourceTask,
          cases: sourceTask.cases.map((switchCase) =>
            switchCase.name === sourceHandle ? { ...switchCase, then: newTask.name } : switchCase,
          ),
        }
      : { ...sourceTask, then: newTask.name };
  return next;
}

/**
 * Dragging an edge's target handle to a different node - "re-route" without
 * going through Source view. `newTargetName` is whatever `Connection.target`
 * React Flow resolved the drag to; returns `undefined` for "reject this
 * reconnection" (an unrecognized source, or dragging Start's edge onto End
 * itself, which isn't a meaningful "run nothing" operation here).
 *
 * Reconnecting FROM Start is a different operation entirely (Start has no
 * "then" to rewrite) - handled as "move the new target to the front of
 * do:", the only way to change what runs first at all.
 */
export function reconnectEdgeTarget(
  tasks: Task[],
  oldEdge: EdgeEndpoint,
  newTargetName: string,
): Task[] | undefined {
  if (oldEdge.source === START_ID) {
    if (newTargetName === END_ID) return undefined;
    const targetTask = tasks.find((t) => t.name === newTargetName);
    if (!targetTask) return undefined;
    return [targetTask, ...tasks.filter((t) => t.name !== newTargetName)];
  }

  const sourceIndex = tasks.findIndex((t) => t.name === oldEdge.source);
  if (sourceIndex < 0) return undefined;
  const sourceTask = tasks[sourceIndex];
  const resolvedTarget = newTargetName === END_ID ? "exit" : newTargetName;
  const nextSourceTask: Task =
    sourceTask.kind === "switch" && oldEdge.sourceHandle
      ? {
          ...sourceTask,
          cases: sourceTask.cases.map((switchCase) =>
            switchCase.name === oldEdge.sourceHandle
              ? { ...switchCase, then: resolvedTarget }
              : switchCase,
          ),
        }
      : { ...sourceTask, then: resolvedTarget };
  return tasks.map((task, index) => (index === sourceIndex ? nextSourceTask : task));
}

/**
 * Which edge a drop position is closest to, by distance to that edge's
 * source/target node midpoint - used so a palette drag-drop lands the new
 * task where it visually looks like it goes (spliced between whatever it
 * was dropped near), instead of always executing last regardless of where
 * it landed on screen. Returns undefined only if there are no edges at all
 * (never happens in practice - Start always has at least one, to the first
 * task or straight to End).
 */
export function nearestEdge(
  position: { x: number; y: number },
  nodePositions: Map<string, { x: number; y: number }>,
  edges: Edge[],
): Edge | undefined {
  let best: Edge | undefined;
  let bestDistance = Infinity;
  for (const edge of edges) {
    const sourcePos = nodePositions.get(edge.source);
    const targetPos = nodePositions.get(edge.target);
    if (!sourcePos || !targetPos) continue;
    const midX = (sourcePos.x + targetPos.x) / 2;
    const midY = (sourcePos.y + targetPos.y) / 2;
    const distance = Math.hypot(position.x - midX, position.y - midY);
    if (distance < bestDistance) {
      bestDistance = distance;
      best = edge;
    }
  }
  return best;
}

/**
 * Lightweight layered auto-layout: each node's column is the longest path
 * (in edge hops) from Start to it, computed by Bellman-Ford-style relaxation
 * bounded to nodeCount rounds. That bound also keeps this from looping
 * forever if a "switch" case points back at an earlier task (a legitimate
 * retry-loop pattern the DSL allows even without "for" support) - a node
 * still stuck in a cycle after the bound just keeps whatever column it last
 * relaxed to, which is an honest "best effort" for a shape this slice
 * doesn't fully model rather than a hang or a crash.
 */
export function layerColumns(
  nodeIds: string[],
  edges: Edge[],
): Map<string, number> {
  const column = new Map(nodeIds.map((id) => [id, 0]));
  for (let round = 0; round < nodeIds.length; round += 1) {
    let changed = false;
    for (const edge of edges) {
      const sourceColumn = column.get(edge.source) ?? 0;
      const targetColumn = column.get(edge.target) ?? 0;
      if (sourceColumn + 1 > targetColumn) {
        column.set(edge.target, sourceColumn + 1);
        changed = true;
      }
    }
    if (!changed) break;
  }
  return column;
}

export function layout(tasks: Task[]): {
  nodes: Node<TaskNodeData | AnchorNodeData>[];
  edges: Edge[];
} {
  const edges = deriveEdges(tasks);
  const nodeIds = [START_ID, ...tasks.map((task) => task.name), END_ID];
  const column = layerColumns(nodeIds, edges);
  // A task a "switch" case's targets skip over (no edge ever points at it)
  // never gets relaxed off its column-0 initial value, which would draw it
  // stacked on Start - fall back to its position in the "do:" list instead
  // so a dead/unreachable branch still renders somewhere sane.
  const hasIncomingEdge = new Set(edges.map((edge) => edge.target));
  tasks.forEach((task, index) => {
    if (!hasIncomingEdge.has(task.name)) column.set(task.name, index + 1);
  });

  const rowInColumn = new Map<number, number>();
  function nextPosition(id: string): { x: number; y: number } {
    const col = column.get(id) ?? 0;
    const row = rowInColumn.get(col) ?? 0;
    rowInColumn.set(col, row + 1);
    return { x: col * COLUMN_WIDTH, y: row * ROW_HEIGHT };
  }

  const startNode: Node<AnchorNodeData> = {
    id: START_ID,
    type: "anchor",
    position: nextPosition(START_ID),
    data: { label: "Start" },
  };
  const taskNodes: Node<TaskNodeData>[] = tasks.map((task) => ({
    id: task.name,
    type: "task",
    position: nextPosition(task.name),
    data: { task },
  }));
  const endNode: Node<AnchorNodeData> = {
    id: END_ID,
    type: "anchor",
    position: nextPosition(END_ID),
    data: { label: "End" },
  };

  return { nodes: [startNode, ...taskNodes, endNode], edges };
}

function uniqueTaskName(existing: string[]): string {
  let index = existing.length + 1;
  while (existing.includes(`task${index}`)) index += 1;
  return `task${index}`;
}

export function WorkflowCanvas({
  source,
  onSourceChange,
  trace,
  serverIssues,
}: {
  source: string;
  onSourceChange: (source: string) => void;
  // Optional execution-trace overlay (see executionTrace.ts) - keyed by
  // traceKey(containerPath, taskName), so it naturally follows drill-down:
  // a task's trace status shows whether you're viewing it at the top level
  // or having drilled into its parent "do"/"for"/"try". undefined (the
  // normal authoring case) renders exactly as before - trace is additive,
  // nothing about the editable canvas changes when it's absent.
  trace?: Map<string, TraceEntry>;
  // Schema-compatibility findings from the last real backend Validate call
  // (App.tsx's validate(), parsed via validation.ts's
  // parseContractViolations) - unlike validationIssues below, these can't
  // be recomputed on every keystroke: they need the actually-compiled
  // WorkflowPlan, which only exists on the backend. Stays stale until the
  // next Validate click, same as the source status banner already did -
  // this just ALSO attributes them to a task badge instead of only a
  // sentence in that banner.
  serverIssues?: ValidationIssue[];
}) {
  const theme = useTheme();
  const parsed = useMemo(() => {
    try {
      return {
        tasks: fromYaml(source).tasks,
        error: undefined as string | undefined,
      };
    } catch (error) {
      return {
        tasks: [] as Task[],
        error:
          error instanceof UnsupportedTaskError
            ? error.message
            : `This source isn't valid enough to load in the canvas yet: ${
                error instanceof Error ? error.message : String(error)
              }`,
      };
    }
  }, [source]);

  // Always computed against the FULL source, not tasksInView below - a
  // validation issue nested three "do"s deep still needs to show up on
  // its own node once you drill in that far, regardless of where the
  // breadcrumb currently sits. Keyed the same way trace already is
  // (traceKey(containerPath, taskName)) so both attach to nodes via the
  // identical lookup below.
  const clientValidationIssues = useMemo(() => validateWorkflowSource(source), [source]);
  // Client-side (instant, every keystroke) and server-side (only as fresh
  // as the last Validate click) issues are just concatenated - both are
  // already the same ValidationIssue shape, and there's no meaningful
  // overlap to de-duplicate: AJV's schema check and
  // WorkflowContractAnalyzer's cross-task compatibility check are
  // disjoint concerns, never flagging the identical thing twice.
  const validationIssues = useMemo(
    () => [...clientValidationIssues, ...(serverIssues ?? [])],
    [clientValidationIssues, serverIssues],
  );
  const validationIssuesByKey = useMemo(() => {
    const map = new Map<string, ValidationIssue[]>();
    for (const issue of validationIssues) {
      if (!issue.taskPath) continue;
      const key = traceKey(issue.taskPath.containerPath, issue.taskPath.taskName);
      const existing = map.get(key);
      if (existing) existing.push(issue);
      else map.set(key, [issue]);
    }
    return map;
  }, [validationIssues]);

  const [selectedTaskName, setSelectedTaskName] = useState<string>();
  // Breadcrumb of "do" task names drilled into - [] means the top-level
  // "do:" list itself. Every operation below (add/update/delete/layout)
  // operates on tasksInView, the task list this path currently resolves to,
  // not always parsed.tasks - that's what makes drilling into a "do" reuse
  // this same canvas rather than needing a second component.
  const [path, setPath] = useState<string[]>([]);
  const tasksInView = useMemo(
    () => tasksAtPath(parsed.tasks, path),
    [parsed.tasks, path],
  );

  // Nodes are local, draggable state, not a value re-derived from
  // tasksInView on every render - that's what lets a manual drag stick.
  // layout()'s auto-computed positions only seed a node the first time it
  // appears; the effect below re-syncs on every task-list change but keeps
  // each already-known node's current (possibly hand-dragged) position,
  // only placing genuinely new nodes algorithmically. Position is lost if
  // this component unmounts (e.g. switching to Source view and back) -
  // known first-slice limitation, not a persisted layout.
  const [nodes, setNodes] = useState<Node<CanvasNodeData>[]>(
    () => layout(tasksInView).nodes,
  );
  const [edges, setEdges] = useState<Edge[]>(() => layout(tasksInView).edges);
  // Set by a palette drag-drop just before commitTasks triggers this
  // component's own re-render (see onDrop below) - the resync effect below
  // consumes it once, to seed that one new node at the exact drop position
  // instead of layout()'s algorithmic column/row placement. A ref, not
  // state, since it must be readable synchronously inside the very next
  // effect run without itself causing a render.
  const pendingPositionRef = useRef<
    { name: string; position: { x: number; y: number } } | undefined
  >(undefined);
  const reactFlowInstanceRef = useRef<
    ReactFlowInstance<Node<CanvasNodeData>, Edge> | undefined
  >(undefined);
  // Set right before commitTasks by an edit that changes the graph's
  // SHAPE, not just one task's own content - inserting a task via
  // InsertableEdge's "+" button is the motivating case: the new node gets
  // a correct fresh column, but every already-existing downstream node
  // would otherwise keep its old (now one column too far left) preserved
  // position, landing exactly on top of the node that used to sit there.
  // "Preserve positions" is the right default for the common case (editing
  // one task's fields shouldn't reshuffle everything) - this is the
  // explicit opt-out for the structural edits where reflowing the rest of
  // the graph is exactly what the user asked for.
  const forceFullLayoutRef = useRef(false);

  // Every edge renders as InsertableEdge (its "+" button is the only way to
  // inject a task between two already-connected tasks anywhere in the flow,
  // not just append at the end) - attached here rather than baked into
  // deriveEdges/layout, which stay pure data functions with no callback
  // wiring or component-lifetime concerns.
  function withEdgeData(rawEdges: Edge[]): Edge[] {
    const data: InsertableEdgeData = { onInsert: insertTaskOnEdge };
    return rawEdges.map((edge) => ({ ...edge, type: "insertable", data }));
  }

  useEffect(() => {
    const computed = layout(tasksInView);
    const forceFullLayout = forceFullLayoutRef.current;
    forceFullLayoutRef.current = false;
    setNodes((current) => {
      const existingById = new Map(current.map((node) => [node.id, node]));
      // Sticky notes are pure canvas-layer annotations layout() knows
      // nothing about (see StickyNoteNode.tsx) - carried forward as-is
      // across every resync rather than being wiped out by
      // computed.nodes, which only ever contains task/anchor nodes.
      const stickyNodes = current.filter((node) => node.type === "sticky");
      const taskAndAnchorNodes = computed.nodes.map((node) => {
        const withTrace: Node<CanvasNodeData> =
          node.type === "task"
            ? {
                ...node,
                data: {
                  ...node.data,
                  trace: trace?.get(traceKey(path, node.id)),
                  validationIssues: validationIssuesByKey.get(traceKey(path, node.id)),
                },
              }
            : node;
        if (forceFullLayout) return withTrace;
        const pending = pendingPositionRef.current;
        if (pending && node.id === pending.name) {
          pendingPositionRef.current = undefined;
          return { ...withTrace, position: pending.position };
        }
        const existing = existingById.get(node.id);
        return existing
          ? { ...withTrace, position: existing.position }
          : withTrace;
      });
      return [...taskAndAnchorNodes, ...stickyNodes];
    });
    setEdges(withEdgeData(computed.edges));
    // Deliberately keyed only on tasksInView (which already changes
    // reference whenever `path` does, so drilling in/out re-attaches trace
    // at the new depth too), `trace`, and `validationIssuesByKey` - not
    // nodes/setNodes, since a drag's own setNodes call must not re-trigger
    // this resync.
  }, [tasksInView, trace, validationIssuesByKey, path]);

  const onNodesChange = useCallback(
    (changes: NodeChange<Node<CanvasNodeData>>[]) =>
      setNodes((current) => applyNodeChanges(changes, current)),
    [],
  );

  const updateStickyText = useCallback((id: string, text: string) => {
    setNodes((current) =>
      current.map((node) =>
        node.id === id ? { ...node, data: { ...node.data, text } } : node,
      ),
    );
  }, []);

  const deleteSticky = useCallback((id: string) => {
    setNodes((current) => current.filter((node) => node.id !== id));
  }, []);

  function addStickyNote() {
    const position = reactFlowInstanceRef.current
      ? reactFlowInstanceRef.current.screenToFlowPosition({ x: 320, y: 160 })
      : { x: 0, y: 0 };
    const id = `sticky-${Date.now()}`;
    const data: StickyNoteData = {
      text: "",
      onTextChange: updateStickyText,
      onDelete: deleteSticky,
    };
    setNodes((current) => [
      ...current,
      { id, type: "sticky", position, data, draggable: true },
    ]);
  }

  // Unlike the resync effect above (which deliberately keeps every existing
  // node's current position, manual drags included), this discards them and
  // reseeds every task/anchor node from layout()'s columns - the explicit
  // "start over" action for when manual dragging (or a lot of edited
  // routing) has left the graph a mess. Sticky notes are layout()'s blind
  // spot the same way they are there, so still carried forward untouched.
  function autoLayout() {
    const computed = layout(tasksInView);
    setNodes((current) => [
      ...computed.nodes,
      ...current.filter((node) => node.type === "sticky"),
    ]);
    setEdges(withEdgeData(computed.edges));
  }

  const selectedTask = tasksInView.find((t) => t.name === selectedTaskName);
  // React Flow already tracks per-node multi-selection (shift/ctrl-click,
  // or a drag-selection box) in node.selected - onNodesChange already
  // applies those "select" NodeChanges via applyNodeChanges, so this is
  // purely a read, no separate tracking needed. Independent of
  // selectedTaskName (which still drives the single-task Inspector) -
  // multi-selecting doesn't change which task's Inspector is open, it only
  // enables "Group into Do" below.
  const selectedTaskIds = nodes
    .filter((node) => node.type === "task" && node.selected)
    .map((node) => node.id);

  const commitTasks = useCallback(
    (tasks: Task[]) =>
      onSourceChange(
        toYaml(source, { tasks: setChildrenAtPath(parsed.tasks, path, tasks) }),
      ),
    [source, onSourceChange, parsed.tasks, path],
  );

  /**
   * Wraps every selected task into one new "do" task, in their original
   * order - not necessarily contiguous in tasksInView (nothing here
   * requires or checks that). A non-contiguous selection still groups
   * cleanly: every non-selected task keeps its own relative order, and the
   * new group task is inserted where the FIRST selected task used to sit
   * among what's left - but this does mean grouping tasks A and C while
   * skipping B moves B to now run after the new group instead of between
   * A and C, since a "do" task can't interleave with tasks outside it.
   * That's a real behavior change worth knowing about, not silently masked.
   */
  function groupSelectedIntoDo() {
    if (selectedTaskIds.length < 2) return;
    const selected = new Set(selectedTaskIds);
    const grouped = tasksInView.filter((t) => selected.has(t.name));
    const remaining = tasksInView.filter((t) => !selected.has(t.name));
    const firstSelectedIndex = tasksInView.findIndex((t) => selected.has(t.name));
    const insertAt = tasksInView
      .slice(0, firstSelectedIndex)
      .filter((t) => !selected.has(t.name)).length;
    const groupName = uniqueTaskName(tasksInView.map((t) => t.name));
    const groupTask: Task = { kind: "do", name: groupName, children: grouped };
    const next = [
      ...remaining.slice(0, insertAt),
      groupTask,
      ...remaining.slice(insertAt),
    ];
    forceFullLayoutRef.current = true;
    commitTasks(next);
    setSelectedTaskName(groupName);
  }

  function addTask(kind: Task["kind"], position?: { x: number; y: number }) {
    const name = uniqueTaskName(tasksInView.map((t) => t.name));
    if (position) {
      // A palette drag/drop chose this exact spot deliberately - preserve
      // every other node's position and only seed this one, same as
      // before. Without a position (a palette click), the new task just
      // appends before End, which otherwise has ITS OLD (now one column
      // too far left) preserved position waiting right where the new task
      // needs to go - the same collision insertTaskOnEdge below fixes for
      // the "+"-on-edge case, here for plain append.
      pendingPositionRef.current = { name, position };
    } else {
      forceFullLayoutRef.current = true;
    }
    const next = [...tasksInView, emptyTask(kind, name)];
    commitTasks(next);
    setSelectedTaskName(name);
  }

  function insertTaskOnEdge(edgeId: string, kind: Task["kind"]) {
    const edge = edges.find((e) => e.id === edgeId);
    if (!edge) return;
    const name = uniqueTaskName(tasksInView.map((t) => t.name));
    forceFullLayoutRef.current = true;
    commitTasks(spliceTaskOnEdge(tasksInView, edge, emptyTask(kind, name)));
    setSelectedTaskName(name);
  }

  // A palette CLICK (as opposed to a drag - see onDrop below) carries no
  // drop position at all, so this was the other half of "you still
  // automatically add a new task to the end of the flow": onDrop got
  // fixed, but a plain click still fell straight through to addTask's
  // unconditional append. First choice: splice onto whatever task is
  // currently SELECTED - the one piece of real context a click does
  // carry. Second choice, when nothing's selected (the common case for a
  // very first click): reuse nearestEdge against the current viewport's
  // own center, so even a contextless click lands wherever the canvas
  // happens to be showing right now, not blindly at the array's end -
  // that blind append was a real, confirmed bug of its own: with ANY
  // upstream task using an explicit "then" (skipping the positional
  // fallthrough), the appended task landed after something unreachable
  // and was orphaned the instant it was created. Only genuinely falls
  // back to a bare append when there's no ReactFlow instance yet to
  // convert a screen position from (shouldn't happen post-mount).
  function addTaskFromPalette(kind: Task["kind"]) {
    const outgoingEdge = selectedTaskName
      ? edges.find((e) => e.source === selectedTaskName)
      : undefined;
    if (outgoingEdge) {
      insertTaskOnEdge(outgoingEdge.id, kind);
      return;
    }
    const instance = reactFlowInstanceRef.current;
    if (instance) {
      const center = instance.screenToFlowPosition({
        x: window.innerWidth / 2,
        y: window.innerHeight / 2,
      });
      const edge = nearestEdge(
        center,
        new Map(nodes.map((node) => [node.id, node.position])),
        edges,
      );
      if (edge) {
        insertTaskOnEdge(edge.id, kind);
        return;
      }
    }
    addTask(kind);
  }

  // Scoped to the target end only: if the source end moved instead
  // (newConnection.source !== oldEdge.source), this no-ops and the drag
  // visually snaps back on the next resync, since properly supporting that
  // would mean tearing down the old source's override AND creating a new
  // one elsewhere - a materially bigger, easier-to-get-wrong operation than
  // "point this edge somewhere else," left out of this pass rather than
  // half-built.
  const handleReconnect: OnReconnect = (oldEdge, newConnection) => {
    if (newConnection.source !== oldEdge.source) return;
    const next = reconnectEdgeTarget(tasksInView, oldEdge, newConnection.target);
    if (!next) return;
    forceFullLayoutRef.current = true;
    commitTasks(next);
  };

  // Drawing a BRAND NEW connection - dragging out of a handle that doesn't
  // already have an edge, the only way this canvas previously had none of:
  // "raise" (no handle at all, before this fix), or any other task's
  // already-positional handle if you want to give it an explicit override
  // without going near an existing edge's endpoint. Reuses
  // reconnectEdgeTarget wholesale - it only ever reads .source/.sourceHandle
  // off its "old edge" argument, so a synthetic one built straight from the
  // Connection works identically to a real pre-existing edge.
  const handleConnect: OnConnect = (connection) => {
    if (!connection.source || !connection.target) return;
    const next = reconnectEdgeTarget(
      tasksInView,
      { source: connection.source, sourceHandle: connection.sourceHandle, target: connection.target },
      connection.target,
    );
    if (!next) return;
    forceFullLayoutRef.current = true;
    commitTasks(next);
  };

  function updateTask(updated: Task) {
    const originalName = selectedTaskName;
    const next = tasksInView.map((t) =>
      t.name === originalName ? updated : t,
    );
    commitTasks(next);
    setSelectedTaskName(updated.name);
  }

  function deleteSelectedTask() {
    // Doesn't repair other tasks' switch cases that "then" the one being
    // removed - their edge just stops rendering (the target no longer
    // exists as a node) and the saved YAML keeps a dangling reference until
    // someone fixes it, in Source view or here. Server-side validation
    // (governance.validateWorkflowDefinition) is the backstop for that, the
    // same way it already is for other hand-editable mistakes.
    commitTasks(tasksInView.filter((t) => t.name !== selectedTaskName));
    setSelectedTaskName(undefined);
  }

  function drillInto(taskName: string) {
    setPath((current) => [...current, taskName]);
    setSelectedTaskName(undefined);
  }

  function drillToDepth(depth: number) {
    setPath((current) => current.slice(0, depth));
    setSelectedTaskName(undefined);
  }

  if (parsed.error) {
    return (
      <Box sx={{ p: 2 }}>
        <Alert severity="info">{parsed.error}</Alert>
      </Box>
    );
  }

  return (
    <Box sx={{ display: "flex", height: "100%", minHeight: 480 }}>
      <NodePalette onAddTask={addTaskFromPalette} />
      <Box sx={{ flex: 1, position: "relative" }}>
        <Box sx={{ position: "absolute", top: 8, right: 8, zIndex: 1, display: "flex", gap: 1 }}>
          {validationIssues.length > 0 && (
            <Tooltip
              title={
                <Box component="span" sx={{ whiteSpace: "pre-line" }}>
                  {validationIssues
                    .map((issue) =>
                      issue.taskPath
                        ? `${[...issue.taskPath.containerPath, issue.taskPath.taskName].join(" / ")}: ${issue.message}`
                        : issue.message,
                    )
                    .join("\n")}
                </Box>
              }
            >
              <Chip
                size="small"
                color="warning"
                variant="outlined"
                icon={<WarningAmberOutlinedIcon fontSize="small" />}
                label={`${validationIssues.length} issue${validationIssues.length === 1 ? "" : "s"}`}
                sx={{ bgcolor: "background.paper" }}
              />
            </Tooltip>
          )}
          <Tooltip title="Auto layout">
            <IconButton
              size="small"
              onClick={autoLayout}
              sx={{
                bgcolor: "background.paper",
                border: 1,
                borderColor: "divider",
                "&:hover": { bgcolor: "action.hover" },
              }}
            >
              <AutoFixHighOutlinedIcon fontSize="small" />
            </IconButton>
          </Tooltip>
          <Tooltip title="Add a sticky note">
            <IconButton
              size="small"
              onClick={addStickyNote}
              sx={{
                bgcolor: "background.paper",
                border: 1,
                borderColor: "divider",
                "&:hover": { bgcolor: "action.hover" },
              }}
            >
              <AddCommentOutlinedIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        </Box>
        {selectedTaskIds.length >= 2 && (
          <Button
            size="small"
            variant="contained"
            startIcon={<GroupWorkOutlinedIcon fontSize="small" />}
            onClick={groupSelectedIntoDo}
            sx={{ position: "absolute", top: 8, left: "50%", transform: "translateX(-50%)", zIndex: 1 }}
          >
            Group {selectedTaskIds.length} tasks into Do
          </Button>
        )}
        {path.length > 0 && (
          <Breadcrumbs
            sx={{
              position: "absolute",
              top: 8,
              left: 8,
              zIndex: 1,
              bgcolor: "background.paper",
              px: 1,
              py: 0.5,
              borderRadius: 1,
            }}
          >
            <Link component="button" underline="hover" onClick={() => drillToDepth(0)}>
              Top level
            </Link>
            {path.map((segment, index) =>
              index === path.length - 1 ? (
                <Typography key={segment} variant="body2" color="text.primary">
                  {segment}
                </Typography>
              ) : (
                <Link
                  key={segment}
                  component="button"
                  underline="hover"
                  onClick={() => drillToDepth(index + 1)}
                >
                  {segment}
                </Link>
              ),
            )}
          </Breadcrumbs>
        )}
        <ReactFlowProvider>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={NODE_TYPES}
            edgeTypes={EDGE_TYPES}
            onInit={(instance) => {
              reactFlowInstanceRef.current = instance;
            }}
            onNodesChange={onNodesChange}
            onReconnect={handleReconnect}
            onConnect={handleConnect}
            onNodeClick={(_event, node) =>
              setSelectedTaskName(node.type === "task" ? node.id : undefined)
            }
            onNodeDoubleClick={(_event, node) => {
              const task = tasksInView.find((t) => t.name === node.id);
              // Any container kind ("do", "for", ...) drills in - checking
              // for "children" generically mirrors dsl.ts's own
              // hasChildren, so a new container kind needs no change here.
              if (task && "children" in task) drillInto(task.name);
            }}
            onPaneClick={() => setSelectedTaskName(undefined)}
            onDragOver={(event) => {
              if (!event.dataTransfer.types.includes(PALETTE_DRAG_MIME)) return;
              event.preventDefault();
              event.dataTransfer.dropEffect = "move";
            }}
            onDrop={(event) => {
              const kind = event.dataTransfer.getData(
                PALETTE_DRAG_MIME,
              ) as Task["kind"];
              if (!kind || !reactFlowInstanceRef.current) return;
              event.preventDefault();
              const position = reactFlowInstanceRef.current.screenToFlowPosition({
                x: event.clientX,
                y: event.clientY,
              });
              // Splice into whatever edge the drop is closest to, so a task
              // dropped between two others actually runs between them -
              // addTask always ran the new task last regardless of where it
              // visually landed, which is exactly the "why does it need to
              // be wired up separately" complaint this fixes.
              const target = nearestEdge(
                position,
                new Map(nodes.map((node) => [node.id, node.position])),
                edges,
              );
              if (target) insertTaskOnEdge(target.id, kind);
              else addTask(kind, position);
            }}
            fitView
            proOptions={{ hideAttribution: true }}
            snapToGrid
            snapGrid={SNAP_GRID}
            defaultEdgeOptions={{
              type: "smoothstep",
              markerEnd: { type: MarkerType.ArrowClosed, color: theme.palette.text.secondary },
              style: { stroke: theme.palette.text.secondary, strokeWidth: 1.5 },
            }}
            style={
              {
                // React Flow themes Controls/MiniMap entirely through these
                // custom properties (see @xyflow/react/dist/style.css) - it
                // ships light-mode defaults (white button/minimap
                // backgrounds) with no dark-mode awareness, so left unset
                // its control icons render in this app's light "--text"
                // token color on a near-white button background and
                // disappear, and the minimap is a stark white box. Themed
                // here with the same MUI tokens the rest of this component
                // already reads via useTheme(), rather than a separate CSS
                // file that would drift from theme.ts.
                "--xy-controls-button-background-color": theme.palette.background.paper,
                "--xy-controls-button-background-color-hover": theme.palette.action.hover,
                "--xy-controls-button-color": theme.palette.text.primary,
                "--xy-controls-button-color-hover": theme.palette.primary.main,
                "--xy-controls-button-border-color": theme.palette.divider,
                "--xy-minimap-background-color": theme.palette.background.paper,
                "--xy-minimap-mask-background-color": alpha(theme.palette.text.primary, 0.12),
                "--xy-minimap-mask-stroke-color": theme.palette.primary.main,
                "--xy-minimap-mask-stroke-width": "2",
              } as CSSProperties
            }
          >
            <Background />
            <Controls showZoom showFitView showInteractive style={{ boxShadow: theme.shadows[2] }} />
            <MiniMap
              pannable
              zoomable
              style={{ border: `1px solid ${theme.palette.divider}`, borderRadius: 4 }}
              nodeColor={(node) => {
                const task = tasksInView.find((t) => t.name === node.id);
                if (!task) return theme.palette.divider;
                return theme.palette[CATEGORY_COLOR[KIND_CATEGORY[task.kind]]].main;
              }}
            />
          </ReactFlow>
        </ReactFlowProvider>
      </Box>
      {selectedTask && (
        <Box sx={{ borderLeft: 1, borderColor: "divider" }}>
          <TaskInspector
            key={selectedTask.name}
            task={selectedTask}
            onChange={updateTask}
            onDelete={deleteSelectedTask}
          />
        </Box>
      )}
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
