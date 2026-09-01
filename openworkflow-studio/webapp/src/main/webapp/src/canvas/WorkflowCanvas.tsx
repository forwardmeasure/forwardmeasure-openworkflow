import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import {
  applyEdgeChanges,
  applyNodeChanges,
  Background,
  Controls,
  MarkerType,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  type Edge,
  type EdgeChange,
  type Node,
  type NodeChange,
  type OnConnect,
  type OnReconnect,
  type ReactFlowInstance,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import AddCommentOutlinedIcon from "@mui/icons-material/AddCommentOutlined";
import AutoFixHighOutlinedIcon from "@mui/icons-material/AutoFixHighOutlined";
import ViewColumnOutlinedIcon from "@mui/icons-material/ViewColumnOutlined";
import ViewStreamOutlinedIcon from "@mui/icons-material/ViewStreamOutlined";
import GroupWorkOutlinedIcon from "@mui/icons-material/GroupWorkOutlined";
import WarningAmberOutlinedIcon from "@mui/icons-material/WarningAmberOutlined";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Breadcrumbs from "@mui/material/Breadcrumbs";
import Button from "@mui/material/Button";
import Chip from "@mui/material/Chip";
import Drawer from "@mui/material/Drawer";
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
import {
  validateTaskReferences,
  validateWorkflowSource,
  type ValidationIssue,
} from "./validation";
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

export function layout(
  tasks: Task[],
  orientation: "horizontal" | "vertical" = "horizontal",
): {
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

  const taskByName = new Map(tasks.map((task) => [task.name, task]));
  // Rough estimate of a card's actual rendered height - "auto-layout is
  // completely broken" turned out to be real and specific: ROW_HEIGHT was
  // a flat 130px for every card regardless of content, so a switch with
  // several cases (each rendering its own line) grew taller than that and
  // visually covered whatever sibling landed below it in the same column.
  // Confirmed live: a 5-case switch's card bottom overlapped a plain "set"
  // sibling sharing its column. This can't measure the REAL DOM (layout()
  // runs before anything renders), so it's an approximation matched
  // against what TaskNode.tsx actually draws per line, not a made-up
  // constant - close enough to stop cards colliding, not pixel-perfect.
  const BASE_CARD_SIZE = 95;
  const SWITCH_CASE_LINE = 20;
  const GAP = 20;
  function estimateNodeSize(id: string): number {
    const task = taskByName.get(id);
    if (!task) return BASE_CARD_SIZE; // Start/End anchors
    return task.kind === "switch"
      ? BASE_CARD_SIZE + Math.max(0, task.cases.length - 1) * SWITCH_CASE_LINE
      : BASE_CARD_SIZE;
  }

  // Vertical mode's depth axis (Y) has the identical overlap risk one axis
  // over: consecutive DEPTH bands stacking top-to-bottom, where an earlier
  // band's tallest card needs to be cleared before the next band starts.
  // Precomputed once, per column, as the cumulative height of every
  // earlier column's tallest member - unlike the sibling axis below, this
  // can't be built incrementally node-by-node (a later column's start
  // position depends on EVERY node already placed in every earlier
  // column, not just the ones before it in tasks[] order).
  const columnDepthOffset = new Map<number, number>();
  if (orientation === "vertical") {
    const maxSizeByColumn = new Map<number, number>();
    for (const id of nodeIds) {
      const col = column.get(id) ?? 0;
      maxSizeByColumn.set(col, Math.max(maxSizeByColumn.get(col) ?? 0, estimateNodeSize(id)));
    }
    const maxColumn = Math.max(0, ...maxSizeByColumn.keys());
    let cursor = 0;
    for (let col = 0; col <= maxColumn; col += 1) {
      columnDepthOffset.set(col, cursor);
      cursor += (maxSizeByColumn.get(col) ?? BASE_CARD_SIZE) + GAP;
    }
  }

  const rowInColumn = new Map<number, number>();
  const siblingOffset = new Map<number, number>();
  function nextPosition(id: string): { x: number; y: number } {
    const col = column.get(id) ?? 0;
    if (orientation === "vertical") {
      // Sibling axis (X): card WIDTH barely varies with content (cases add
      // lines, not columns), so plain fixed spacing is fine here - only
      // the depth axis (Y, precomputed above) needed to become
      // content-aware.
      const siblingIndex = rowInColumn.get(col) ?? 0;
      rowInColumn.set(col, siblingIndex + 1);
      return { x: siblingIndex * COLUMN_WIDTH, y: columnDepthOffset.get(col) ?? 0 };
    }
    // Horizontal: depth axis (X) fixed, same reasoning (width is roughly
    // constant). Sibling axis (Y): content-aware, cumulative - this is the
    // one that was actually observed overlapping.
    const offset = siblingOffset.get(col) ?? 0;
    siblingOffset.set(col, offset + estimateNodeSize(id) + GAP);
    return { x: col * COLUMN_WIDTH, y: offset };
  }

  const startNode: Node<AnchorNodeData> = {
    id: START_ID,
    type: "anchor",
    position: nextPosition(START_ID),
    data: { label: "Start", orientation },
  };
  const taskNodes: Node<TaskNodeData>[] = tasks.map((task) => ({
    id: task.name,
    type: "task",
    position: nextPosition(task.name),
    data: { task, orientation },
  }));
  const endNode: Node<AnchorNodeData> = {
    id: END_ID,
    type: "anchor",
    position: nextPosition(END_ID),
    data: { label: "End", orientation },
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
  const clientValidationIssues = useMemo(
    () => [...validateWorkflowSource(source), ...validateTaskReferences(parsed.tasks)],
    [source, parsed.tasks],
  );
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
  // "Can we get the ability to force the layout to be portrait v/s
  // landscape?" - a whole-canvas setting, not per-node/per-edge (see
  // TaskNodeData.orientation and layout()'s own orientation parameter).
  // Declared before nodes/edges below so their own useState initializers
  // can read it.
  const [orientation, setOrientation] = useState<"horizontal" | "vertical">(
    "horizontal",
  );
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
    () => layout(tasksInView, orientation).nodes,
  );
  const [edges, setEdges] = useState<Edge[]>(
    () => layout(tasksInView, orientation).edges,
  );
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
    return rawEdges.map((edge) => {
      const data: InsertableEdgeData = {
        onInsert: insertTaskOnEdge,
        onDelete: disconnectEdge,
        // See disconnectEdge above - Start's outgoing edge can't be cut,
        // so its delete button never renders in the first place rather
        // than rendering a button that silently does nothing on click.
        canDelete: edge.source !== START_ID,
        isHovered: false,
      };
      return { ...edge, type: "insertable", data };
    });
  }

  // "The + sign on an edge makes it impossible to horizontally align task
  // boxes" - it used to render at every edge's midpoint unconditionally,
  // permanent visual clutter sitting exactly on the line you're trying to
  // judge alignment against. Now only the actually-hovered edge shows its
  // button; every other edge's data.isHovered stays false. Cheap even for
  // a large graph - toggling one boolean on already-existing edge objects,
  // not recomputing topology.
  const handleEdgeMouseEnter = useCallback((_event: unknown, hovered: Edge) => {
    setEdges((current) =>
      current.map((edge) => ({
        ...edge,
        data: { ...(edge.data as InsertableEdgeData), isHovered: edge.id === hovered.id },
      })),
    );
  }, []);
  const handleEdgeMouseLeave = useCallback(() => {
    setEdges((current) =>
      current.map((edge) => ({
        ...edge,
        data: { ...(edge.data as InsertableEdgeData), isHovered: false },
      })),
    );
  }, []);

  useEffect(() => {
    const computed = layout(tasksInView, orientation);
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
    // at the new depth too), `trace`, `validationIssuesByKey`, and
    // `orientation` - not nodes/setNodes, since a drag's own setNodes call
    // must not re-trigger this resync. Orientation alone changing (with
    // forceFullLayout NOT set) still only refreshes each node's data -
    // toggleOrientation below is what actually sets forceFullLayoutRef so
    // positions get recomputed for the new axis, not just handle sides.
  }, [tasksInView, trace, validationIssuesByKey, path, orientation]);

  const onNodesChange = useCallback(
    (changes: NodeChange<Node<CanvasNodeData>>[]) =>
      setNodes((current) => applyNodeChanges(changes, current)),
    [],
  );

  // Without this, clicking an edge fired React Flow's internal "select"
  // interaction with nowhere to apply it - edges is a controlled prop, so
  // the click's SelectionChange just had no handler to write edge.selected
  // back to state through, and nothing ever visibly happened. Confirmed
  // live: before this, a clicked edge's DOM class stayed exactly
  // "react-flow__edge react-flow__edge-insertable nopan selectable" with
  // no "selected" ever appearing - not a missing style, a missing handler.
  // Mirrors onNodesChange above exactly.
  const onEdgesChange = useCallback(
    (changes: EdgeChange<Edge>[]) =>
      setEdges((current) => applyEdgeChanges(changes, current)),
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
    const computed = layout(tasksInView, orientation);
    setNodes((current) => [
      ...computed.nodes,
      ...current.filter((node) => node.type === "sticky"),
    ]);
    setEdges(withEdgeData(computed.edges));
  }

  // Existing positions are meaningless once the axis itself changes (a
  // horizontal-mode column of X coordinates has no sensible reading as
  // vertical-mode Y coordinates) - unlike every other edit in this
  // component, this is the one case where discarding manual placement
  // outright is correct, not just convenient.
  function toggleOrientation() {
    setOrientation((current) => (current === "horizontal" ? "vertical" : "horizontal"));
    forceFullLayoutRef.current = true;
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

  // React Flow's OWN default Backspace/Delete handling (disabled below via
  // deleteKeyCode={null}) only ever removed the node/edge from this
  // component's local nodes/edges state via applyNodeChanges/
  // applyEdgeChanges - it never touched tasksInView, so the task stayed in
  // the saved YAML the whole time. The card visually vanished; the source
  // didn't change at all until some unrelated edit forced a resync, at
  // which point the "deleted" task silently reappeared. Every path here
  // instead goes through commitTasks, exactly like every other edit in this
  // component - deleting on canvas is now indistinguishable from deleting
  // in Source view.
  useEffect(() => {
    function onKeyDown(event: KeyboardEvent) {
      if (event.key !== "Backspace" && event.key !== "Delete") return;
      const target = event.target as HTMLElement | null;
      // Don't hijack Backspace while the user is editing text anywhere
      // (Inspector fields, sticky note text, palette search, ...) - only a
      // click on the canvas itself (a node or edge, never a form control)
      // should ever mean "delete this."
      if (target?.closest("input, textarea, [contenteditable='true']")) return;
      if (selectedTaskIds.length > 0) {
        const toDelete = new Set(selectedTaskIds);
        commitTasks(tasksInView.filter((t) => !toDelete.has(t.name)));
        setSelectedTaskName(undefined);
        return;
      }
      if (selectedTaskName) {
        commitTasks(tasksInView.filter((t) => t.name !== selectedTaskName));
        setSelectedTaskName(undefined);
        return;
      }
      const selectedSticky = nodes.find((n) => n.type === "sticky" && n.selected);
      if (selectedSticky) {
        deleteSticky(selectedSticky.id);
        return;
      }
      const selectedEdge = edges.find((e) => e.selected);
      if (selectedEdge) disconnectEdge(selectedEdge.id);
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [selectedTaskIds, selectedTaskName, nodes, edges, tasksInView, commitTasks, deleteSticky]);

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

  // "When I add a task, I want it dropped on the canvas so I can drag it to
  // wherever I want and connect it to whatever node I want" - a task added
  // via the palette (click or drag) is now always a self-contained,
  // disconnected node ("then: exit") the author wires in deliberately,
  // never auto-spliced into the live flow. Mirrors duplicateSelectedTask's
  // same reasoning, and shares its same fix: appending to the END of
  // tasksInView would otherwise silently change whichever task USED TO be
  // last - if it relied on positional fallthrough (no explicit "then"), it
  // would now positionally fall into the newly-appended task instead of
  // End, a real, silent rewiring bug. Pinning that task to an explicit
  // "exit" preserves its exact prior behavior.
  function appendDisconnectedTask(tasks: Task[], task: Task): Task[] {
    const last = tasks[tasks.length - 1];
    const patched =
      last && last.kind !== "switch" && last.then === undefined
        ? [...tasks.slice(0, -1), { ...last, then: "exit" }]
        : tasks;
    return [...patched, task];
  }

  function addTask(kind: Task["kind"], position?: { x: number; y: number }) {
    const name = uniqueTaskName(tasksInView.map((t) => t.name));
    const instance = reactFlowInstanceRef.current;
    const seededPosition =
      position ??
      instance?.screenToFlowPosition({
        x: window.innerWidth / 2,
        y: window.innerHeight / 2,
      });
    // A palette drag/drop (or, now, a plain click) chose or implies a
    // specific spot - preserve every other node's position and only seed
    // this one, same as insertTaskOnEdge does for the "+"-on-edge case.
    // Only genuinely falls back to a full relayout when there's no
    // ReactFlow instance yet to resolve a screen position from (shouldn't
    // happen post-mount).
    if (seededPosition) pendingPositionRef.current = { name, position: seededPosition };
    else forceFullLayoutRef.current = true;
    const next = appendDisconnectedTask(tasksInView, { ...emptyTask(kind, name), then: "exit" });
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

  // "Delete" for a connection, not a task: cuts the edge by rewriting its
  // SOURCE's "then" (or switch case's "then") to "exit" - reuses
  // reconnectEdgeTarget wholesale, exactly the same way handleConnect above
  // does, by treating END_ID as the reconnection target. That function
  // already refuses to touch the Start->first-task edge (returns undefined),
  // which is also why InsertableEdge never renders a delete button on it -
  // "cut the entry point" isn't a meaningful operation here.
  function disconnectEdge(edgeId: string) {
    const edge = edges.find((e) => e.id === edgeId);
    if (!edge) return;
    const next = reconnectEdgeTarget(tasksInView, edge, END_ID);
    if (!next) return;
    commitTasks(next);
  }

  // A palette CLICK lands the new task at the current viewport center, via
  // addTask's own position fallback - disconnected, same as a drag-drop
  // (see onDrop below). Splicing into whatever was nearest used to be the
  // behavior here; the author explicitly asked for the opposite ("dropped
  // on the canvas so I can drag it wherever and connect it to whatever
  // node I want"), so this no longer looks at selection or edge proximity
  // at all.
  function addTaskFromPalette(kind: Task["kind"]) {
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
    // Deliberately NOT forcing a full relayout here (or in handleConnect
    // below) - re-pointing an existing edge doesn't create a new node, so
    // there's nothing that NEEDS a fresh position the way inserting a task
    // does. Wiping every manually-placed node's position just because one
    // edge changed was real, reported friction ("why does reconnecting
    // trigger automatic layout?") - auto-layout stays exactly one explicit
    // click away for anyone who wants a clean re-flow after rewiring.
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
    // Same reasoning as handleReconnect above - no new node, no forced
    // relayout.
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

  // Unlike inserting on an edge (which deliberately splices into the live
  // flow), a duplicate is NOT spliced into the task array next to the
  // original - in this DSL, array position IS positional-fallthrough
  // wiring, so inserting right after the original would silently
  // reroute the original's own "next task" through the clone instead of
  // wherever it used to go. Appended at the end instead, with every
  // outbound "then" (including per-case, for "switch") reset to "exit" -
  // a self-contained, disconnected copy the author wires up deliberately,
  // matching Flowise's own "clear inbound bindings on the clone"
  // behavior. Placed a fixed offset from the original's current canvas
  // position (mirroring how a palette drag-drop seeds pendingPositionRef)
  // rather than wherever auto-layout's column algorithm would put it.
  function duplicateSelectedTask() {
    if (!selectedTask) return;
    const name = uniqueTaskName(tasksInView.map((t) => t.name));
    const cloneBase: Task = { ...selectedTask, name, then: "exit" };
    const clone: Task =
      cloneBase.kind === "switch"
        ? { ...cloneBase, cases: cloneBase.cases.map((c) => ({ ...c, then: "exit" })) }
        : cloneBase;
    const originalNode = nodes.find((n) => n.id === selectedTask.name);
    if (originalNode) {
      pendingPositionRef.current = {
        name,
        position: { x: originalNode.position.x + 40, y: originalNode.position.y + 40 },
      };
    } else {
      forceFullLayoutRef.current = true;
    }
    commitTasks(appendDisconnectedTask(tasksInView, clone));
    setSelectedTaskName(name);
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
          <Tooltip
            title={
              orientation === "horizontal"
                ? "Switch to portrait (top-to-bottom) layout"
                : "Switch to landscape (left-to-right) layout"
            }
          >
            <IconButton
              size="small"
              onClick={toggleOrientation}
              sx={{
                bgcolor: "background.paper",
                border: 1,
                borderColor: "divider",
                "&:hover": { bgcolor: "action.hover" },
              }}
            >
              {orientation === "horizontal" ? (
                <ViewStreamOutlinedIcon fontSize="small" />
              ) : (
                <ViewColumnOutlinedIcon fontSize="small" />
              )}
            </IconButton>
          </Tooltip>
          <Tooltip title="Auto Layout">
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
            onEdgesChange={onEdgesChange}
            onEdgeMouseEnter={handleEdgeMouseEnter}
            onEdgeMouseLeave={handleEdgeMouseLeave}
            onReconnect={handleReconnect}
            onConnect={handleConnect}
            // Rejected live, mid-drag (React Flow highlights the connection
            // line red while dragging over a handle this returns false
            // for), not just after drop: a self-loop is never meaningful,
            // and Start has no target handle rendered in the first place
            // (AnchorNode only gives it a source handle) - this is the
            // defense-in-depth backstop for both, evaluated against every
            // handle the pointer passes over during the drag.
            isValidConnection={(connection) =>
              connection.source !== connection.target && connection.target !== START_ID
            }
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
              // Dropped exactly where the pointer released, disconnected -
              // see addTask/appendDisconnectedTask above. Used to splice
              // into whatever edge the drop landed nearest; the author
              // explicitly asked for a plain, manually-wired drop instead.
              addTask(kind, position);
            }}
            fitView
            proOptions={{ hideAttribution: true }}
            snapToGrid
            snapGrid={SNAP_GRID}
            // React Flow's built-in Backspace/Delete handling only ever
            // updates local nodes/edges state, bypassing tasksInView
            // entirely - a real desync bug (see the keydown effect above,
            // which replaces it with task-aware deletion routed through
            // commitTasks).
            deleteKeyCode={null}
            // When two+ edges land on the same handle, their reconnect
            // drag-targets sit exactly on top of each other - whichever
            // rendered last wins every click, making the others
            // ungrabbable. Selecting an edge (now that clicking one
            // actually applies selection - see onEdgesChange above)
            // raises it above its siblings, so click-then-drag reaches
            // the one you actually want.
            elevateEdgesOnSelect
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
      <Drawer
        anchor="right"
        variant="temporary"
        open={Boolean(selectedTask)}
        onClose={() => setSelectedTaskName(undefined)}
        // No backdrop, no scrim, no focus trap: "selecting a task opens the
        // inspector on the main canvas and makes it impossible to work
        // with" - the previous inline sidebar permanently reserved 320px
        // of layout width (compounding with the palette's 220px and the
        // Derived view's 300px+), squeezing the actual canvas down to
        // almost nothing. A floating overlay never reserves layout space
        // at all - it draws on top, closes itself via the exact same
        // "click empty canvas clears selectedTaskName" path that already
        // existed (onPaneClick above), and the canvas underneath stays
        // fully visible and interactive everywhere the panel doesn't
        // physically cover.
        hideBackdrop
        ModalProps={{ keepMounted: true }}
        slotProps={{
          paper: {
            sx: {
              width: 320,
              boxShadow: 6,
              pointerEvents: "auto",
            },
          },
        }}
        sx={{ pointerEvents: "none" }}
      >
        {selectedTask && (
          <TaskInspector
            key={selectedTask.name}
            task={selectedTask}
            onChange={updateTask}
            onDelete={deleteSelectedTask}
            onDuplicate={duplicateSelectedTask}
          />
        )}
      </Drawer>
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
