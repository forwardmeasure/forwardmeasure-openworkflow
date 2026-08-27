import { useCallback, useEffect, useMemo, useRef, useState } from "react";
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
  type ReactFlowInstance,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import AddCommentOutlinedIcon from "@mui/icons-material/AddCommentOutlined";
import GroupWorkOutlinedIcon from "@mui/icons-material/GroupWorkOutlined";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Breadcrumbs from "@mui/material/Breadcrumbs";
import Button from "@mui/material/Button";
import IconButton from "@mui/material/IconButton";
import Link from "@mui/material/Link";
import Tooltip from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";
import { useTheme } from "@mui/material/styles";
import { AnchorNode, type AnchorNodeData } from "./AnchorNode";
import { NodePalette, PALETTE_DRAG_MIME } from "./NodePalette";
import { StickyNoteNode, type StickyNoteData } from "./StickyNoteNode";
import { CATEGORY_COLOR, KIND_CATEGORY } from "./taskKindMeta";
import { TaskInspector } from "./TaskInspector";
import { TaskNode, type TaskNodeData } from "./TaskNode";
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
type CanvasNodeData = TaskNodeData | AnchorNodeData | StickyNoteData;
const COLUMN_WIDTH = 260;
const ROW_HEIGHT = 130;
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
 * "raise" gets no outgoing edge at all (see TaskNode.tsx) - it always
 * terminates or transitions to error handling, never falls through.
 */
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
        const target = switchCase.then === "exit" ? END_ID : switchCase.then;
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
      return;
    }
    const next = tasks[index + 1];
    const target = next ? next.name : END_ID;
    edges.push({ id: `${task.name}->${target}`, source: task.name, target });
  });
  return edges;
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
}: {
  source: string;
  onSourceChange: (source: string) => void;
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

  useEffect(() => {
    const computed = layout(tasksInView);
    setNodes((current) => {
      const existingById = new Map(current.map((node) => [node.id, node]));
      // Sticky notes are pure canvas-layer annotations layout() knows
      // nothing about (see StickyNoteNode.tsx) - carried forward as-is
      // across every resync rather than being wiped out by
      // computed.nodes, which only ever contains task/anchor nodes.
      const stickyNodes = current.filter((node) => node.type === "sticky");
      const taskAndAnchorNodes = computed.nodes.map((node) => {
        const pending = pendingPositionRef.current;
        if (pending && node.id === pending.name) {
          pendingPositionRef.current = undefined;
          return { ...node, position: pending.position };
        }
        const existing = existingById.get(node.id);
        return existing ? { ...node, position: existing.position } : node;
      });
      return [...taskAndAnchorNodes, ...stickyNodes];
    });
    setEdges(computed.edges);
    // Deliberately keyed only on tasksInView, not on nodes/setNodes - a
    // drag's own setNodes call must not re-trigger this resync.
  }, [tasksInView]);

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
    commitTasks(next);
    setSelectedTaskName(groupName);
  }

  function addTask(kind: Task["kind"], position?: { x: number; y: number }) {
    const name = uniqueTaskName(tasksInView.map((t) => t.name));
    if (position) pendingPositionRef.current = { name, position };
    const next = [...tasksInView, emptyTask(kind, name)];
    commitTasks(next);
    setSelectedTaskName(name);
  }

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
      <NodePalette onAddTask={(kind) => addTask(kind)} />
      <Box sx={{ flex: 1, position: "relative" }}>
        <Tooltip title="Add a sticky note">
          <IconButton
            size="small"
            onClick={addStickyNote}
            sx={{
              position: "absolute",
              top: 8,
              right: 8,
              zIndex: 1,
              bgcolor: "background.paper",
              border: 1,
              borderColor: "divider",
              "&:hover": { bgcolor: "action.hover" },
            }}
          >
            <AddCommentOutlinedIcon fontSize="small" />
          </IconButton>
        </Tooltip>
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
            onInit={(instance) => {
              reactFlowInstanceRef.current = instance;
            }}
            onNodesChange={onNodesChange}
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
              addTask(kind, position);
            }}
            fitView
            proOptions={{ hideAttribution: true }}
            defaultEdgeOptions={{
              type: "smoothstep",
              markerEnd: { type: MarkerType.ArrowClosed, color: theme.palette.text.secondary },
              style: { stroke: theme.palette.text.secondary, strokeWidth: 1.5 },
            }}
          >
            <Background />
            <Controls />
            <MiniMap
              pannable
              zoomable
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
