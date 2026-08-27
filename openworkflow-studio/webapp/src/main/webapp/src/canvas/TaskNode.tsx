import { Handle, Position, type NodeProps } from "@xyflow/react";
import Avatar from "@mui/material/Avatar";
import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Typography from "@mui/material/Typography";
import { CATEGORY_COLOR, KIND_CATEGORY, KIND_ICON } from "./taskKindMeta";
import type { Task } from "./dsl";

export type TaskNodeData = { task: Task };

export function TaskNode({
  data,
  selected,
}: NodeProps & { data: TaskNodeData }) {
  const { task } = data;
  const KindIcon = KIND_ICON[task.kind];
  const category = KIND_CATEGORY[task.kind];
  const categoryColor = CATEGORY_COLOR[category];
  return (
    <Card
      variant="outlined"
      sx={{
        minWidth: 220,
        borderRadius: 3,
        borderColor: selected ? `${categoryColor}.main` : "divider",
        borderWidth: selected ? 2 : 1,
        borderTop: 3,
        borderTopColor: `${categoryColor}.main`,
        boxShadow: selected ? 4 : 1,
        transition: "box-shadow 120ms ease, border-color 120ms ease",
      }}
    >
      <Handle type="target" position={Position.Left} />
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
        {task.kind === "raise" && (task.error.type || task.error.title) && (
          <Typography variant="caption" color="text.secondary" noWrap>
            {task.error.title || task.error.type}
          </Typography>
        )}
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
      {task.kind === "raise" ? (
        // raise always terminates or transitions to error handling, never
        // falls through positionally like every other kind here - no source
        // handle at all, rather than a misleading "next" edge.
        undefined
      ) : task.kind === "switch" ? (
        // One named source handle per case, spread down the right edge, so
        // each branch's edge visibly leaves from its own case rather than
        // all cases bunching into a single point.
        task.cases.map((switchCase, index) => (
          <Handle
            key={switchCase.name}
            type="source"
            position={Position.Right}
            id={switchCase.name}
            style={{ top: `${((index + 1) / (task.cases.length + 1)) * 100}%` }}
          />
        ))
      ) : (
        <Handle type="source" position={Position.Right} />
      )}
    </Card>
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
