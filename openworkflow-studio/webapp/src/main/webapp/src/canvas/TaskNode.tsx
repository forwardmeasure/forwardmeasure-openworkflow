import { Handle, NodeResizer, Position, type NodeProps } from "@xyflow/react";
import CheckCircleIcon from "@mui/icons-material/CheckCircle";
import ErrorIcon from "@mui/icons-material/Error";
import PendingIcon from "@mui/icons-material/Pending";
import WarningAmberIcon from "@mui/icons-material/WarningAmber";
import Avatar from "@mui/material/Avatar";
import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Tooltip from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";
import type { TraceEntry } from "./executionTrace";
import { CATEGORY_COLOR, KIND_CATEGORY, KIND_ICON } from "./taskKindMeta";
import type { ValidationIssue } from "./validation";
import type { Task } from "./dsl";

export type TaskNodeData = {
  task: Task;
  trace?: TraceEntry;
  // Author-time, advisory only (see validation.ts) - distinct badge
  // (top-left, amber) from the trace badge (top-right) below, both by
  // position and by the fact the two are never populated together: trace
  // only appears in the Executions view, validation only while authoring.
  validationIssues?: ValidationIssue[];
  // "Boxes only allow connectors at the ends... if tasks accepted
  // connections at even just the top and bottom in the middle, we'd have
  // much better layout" - horizontal (the original, columns left-to-right,
  // handles on Left/Right) or vertical (rows top-to-bottom, handles on
  // Top/Bottom so a vertically-stacked flow connects in a straight line
  // instead of an S-curve). A whole-canvas setting (see WorkflowCanvas.tsx's
  // orientation state / the layout()/autoLayout() direction toggle), not
  // per-node - defaults to horizontal so existing saved positions/exports
  // that never set this keep rendering exactly as before.
  orientation?: "horizontal" | "vertical";
};

// Container kinds hold a variable number of nested children (rendered
// elsewhere via drill-down, not on this card) - their natural content
// height is just a fixed caption line regardless of how much they
// actually contain, which is exactly what made auto-layout's flat-height
// assumption wrong before (see WorkflowCanvas.tsx's estimateNodeSize).
// Letting the author manually resize these gives them a visual cue for
// "this holds more than the caption suggests" independent of that
// estimate. Leaf kinds (set/call/switch/...) keep their auto-sizing -
// resizing a single-line card has no real content to reveal.
const RESIZABLE_KINDS = new Set<Task["kind"]>(["do", "for", "fork", "try", "switch"]);

const TRACE_ICON = {
  entered: PendingIcon,
  completed: CheckCircleIcon,
  failed: ErrorIcon,
};

const TRACE_COLOR = {
  entered: "warning",
  completed: "success",
  failed: "error",
} as const;

export function TaskNode({
  data,
  selected,
}: NodeProps & { data: TaskNodeData }) {
  const { task, trace, validationIssues, orientation = "horizontal" } = data;
  const targetPosition = orientation === "vertical" ? Position.Top : Position.Left;
  const sourcePosition = orientation === "vertical" ? Position.Bottom : Position.Right;
  const KindIcon = KIND_ICON[task.kind];
  const category = KIND_CATEGORY[task.kind];
  const categoryColor = CATEGORY_COLOR[category];
  const TraceIcon = trace ? TRACE_ICON[trace.status] : undefined;
  const isResizable = RESIZABLE_KINDS.has(task.kind);
  return (
    <Box sx={{ position: "relative", height: isResizable ? "100%" : undefined }}>
      {isResizable && <NodeResizer minWidth={220} minHeight={72} isVisible={selected} />}
      <Card
        variant="outlined"
        sx={{
          minWidth: 220,
          height: isResizable ? "100%" : undefined,
          borderRadius: 3,
          borderColor: trace
            ? `${TRACE_COLOR[trace.status]}.main`
            : selected
              ? `${categoryColor}.main`
              : "divider",
          borderWidth: trace || selected ? 2 : 1,
          borderTop: 3,
          borderTopColor: `${categoryColor}.main`,
          boxShadow: selected ? 4 : 1,
          transition: "box-shadow 120ms ease, border-color 120ms ease",
        }}
      >
      <Handle type="target" position={targetPosition} />
      <CardContent sx={{ p: 1.5, "&:last-child": { pb: 1.5 } }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
          <Avatar
            variant="rounded"
            sx={{
              width: 28,
              height: 28,
              bgcolor: `${categoryColor}.main`,
              color: `${categoryColor}.contrastText`,
            }}
          >
            <KindIcon fontSize="small" />
          </Avatar>
          <Box sx={{ minWidth: 0, flex: 1 }}>
            <Typography variant="subtitle2" noWrap title={task.name}>
              {task.name}
            </Typography>
            <Typography
              variant="caption"
              color="text.secondary"
              noWrap
              sx={{
                display: "block",
                textTransform: "uppercase",
                letterSpacing: 0.5,
                fontSize: "0.65rem",
                lineHeight: 1.4,
              }}
            >
              {task.kind}
            </Typography>
          </Box>
        </Box>
        {task.kind === "call" && task.call && (
          <Typography variant="caption" color="text.secondary" noWrap>
            {task.call}
          </Typography>
        )}
        {task.kind === "switch" &&
          task.cases.map((switchCase, index) => (
            <Typography
              key={switchCase.name}
              variant="caption"
              color="text.secondary"
              noWrap
              sx={{ display: "block", pr: 2 }}
            >
              {index + 1}.{" "}
              {switchCase.when
                ? `${switchCase.name} (${switchCase.when})`
                : switchCase.name}
            </Typography>
          ))}
        {task.kind === "raise" &&
          (typeof task.error === "string" ? (
            <Typography variant="caption" color="text.secondary" noWrap>
              use.errors: {task.error}
            </Typography>
          ) : (
            (task.error.title || task.error.type) && (
              <Typography variant="caption" color="text.secondary" noWrap>
                {task.error.title || task.error.type}
              </Typography>
            )
          ))}
        {task.kind === "wait" && typeof task.wait === "string" && task.wait && (
          <Typography variant="caption" color="text.secondary" noWrap>
            {task.wait}
          </Typography>
        )}
        {task.kind === "emit" && (
          <Typography variant="caption" color="text.secondary" noWrap>
            {Object.keys(task.with).length} event {Object.keys(task.with).length === 1 ? "property" : "properties"}
          </Typography>
        )}
        {task.kind === "do" && (
          <Typography variant="caption" color="text.secondary" noWrap>
            {task.children.length} task{task.children.length === 1 ? "" : "s"}{" "}
            inside — double-click to open
          </Typography>
        )}
        {task.kind === "for" && (
          <Typography variant="caption" color="text.secondary" noWrap>
            {task.itemVariable} in {task.collection || "…"} ·{" "}
            {task.children.length} task{task.children.length === 1 ? "" : "s"}{" "}
            — double-click to open
          </Typography>
        )}
        {task.kind === "fork" && (
          <Typography variant="caption" color="text.secondary" noWrap>
            {task.children.length} parallel branch
            {task.children.length === 1 ? "" : "es"}
            {task.compete ? " (first wins)" : ""} — double-click to open
          </Typography>
        )}
        {task.kind === "try" && (
          <Typography variant="caption" color="text.secondary" noWrap>
            {task.children.length} task{task.children.length === 1 ? "" : "s"}{" "}
            · catches{" "}
            {task.catchClause.errors?.type ?? task.catchClause.errors?.status ?? "any error"}{" "}
            — double-click to open
          </Typography>
        )}
        {task.kind === "listen" &&
          (task.children.length > 0 ? (
            <Typography variant="caption" color="text.secondary" noWrap>
              foreach {task.itemVariable || "item"} · {task.children.length}{" "}
              task{task.children.length === 1 ? "" : "s"} — double-click to
              open
            </Typography>
          ) : (
            <Typography variant="caption" color="text.secondary" noWrap>
              waits for a matching event
            </Typography>
          ))}
        {task.kind === "run" && (
          <Typography variant="caption" color="text.secondary" noWrap>
            {task.variant}
            {task.variant === "workflow" && task.workflowName
              ? `: ${task.workflowName}`
              : ""}
          </Typography>
        )}
      </CardContent>
      {task.kind === "switch" ? (
        // One named source handle per case, spread along the source edge
        // (down the right edge in horizontal mode, across the bottom edge
        // in vertical mode - same "%" trick, just the other axis), so each
        // branch's edge visibly leaves from its own case rather than all
        // cases bunching into a single point.
        task.cases.map((switchCase, index) => (
          <Handle
            key={switchCase.name}
            type="source"
            position={sourcePosition}
            id={switchCase.name}
            style={{
              [orientation === "vertical" ? "left" : "top"]:
                `${((index + 1) / (task.cases.length + 1)) * 100}%`,
            }}
          />
        ))
      ) : (
        <Handle type="source" position={sourcePosition} />
      )}
      </Card>
      {trace && TraceIcon && (
        <Tooltip title={trace.message || trace.status}>
          <TraceIcon
            fontSize="small"
            color={TRACE_COLOR[trace.status]}
            sx={{
              position: "absolute",
              top: -8,
              right: -8,
              bgcolor: "background.paper",
              borderRadius: "50%",
            }}
          />
        </Tooltip>
      )}
      {validationIssues && validationIssues.length > 0 && (
        <Tooltip
          title={
            <Box component="span" sx={{ whiteSpace: "pre-line" }}>
              {validationIssues.map((issue) => issue.message).join("\n")}
            </Box>
          }
        >
          <WarningAmberIcon
            fontSize="small"
            color="warning"
            sx={{
              position: "absolute",
              top: -8,
              left: -8,
              bgcolor: "background.paper",
              borderRadius: "50%",
            }}
          />
        </Tooltip>
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
