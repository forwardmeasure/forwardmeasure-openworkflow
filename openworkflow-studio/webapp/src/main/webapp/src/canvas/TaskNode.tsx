import type { ComponentType } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";
import AccountTreeIcon from "@mui/icons-material/AccountTree";
import AltRouteIcon from "@mui/icons-material/AltRoute";
import CallMadeIcon from "@mui/icons-material/CallMade";
import CallSplitIcon from "@mui/icons-material/CallSplit";
import CampaignIcon from "@mui/icons-material/Campaign";
import EditNoteIcon from "@mui/icons-material/EditNote";
import HealingIcon from "@mui/icons-material/Healing";
import HourglassEmptyIcon from "@mui/icons-material/HourglassEmpty";
import LoopIcon from "@mui/icons-material/Loop";
import PlayCircleOutlineIcon from "@mui/icons-material/PlayCircleOutlined";
import ReportProblemIcon from "@mui/icons-material/ReportProblem";
import SensorsIcon from "@mui/icons-material/Sensors";
import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Chip from "@mui/material/Chip";
import type { SvgIconProps } from "@mui/material/SvgIcon";
import Typography from "@mui/material/Typography";
import type { Task } from "./dsl";

export type TaskNodeData = { task: Task };

// Per-kind icons rather than colors: MUI's Chip color palette only offers
// ~7 named values, too few to stay visually distinct once every Serverless
// Workflow task kind (12 total) is supported, so this map is the thing that
// needs a new entry per new kind, not KIND_COLOR.
const KIND_ICON: Record<Task["kind"], ComponentType<SvgIconProps>> = {
  set: EditNoteIcon,
  call: CallMadeIcon,
  switch: AltRouteIcon,
  raise: ReportProblemIcon,
  wait: HourglassEmptyIcon,
  emit: CampaignIcon,
  do: AccountTreeIcon,
  for: LoopIcon,
  fork: CallSplitIcon,
  try: HealingIcon,
  listen: SensorsIcon,
  run: PlayCircleOutlineIcon,
};

export function TaskNode({
  data,
  selected,
}: NodeProps & { data: TaskNodeData }) {
  const { task } = data;
  const KindIcon = KIND_ICON[task.kind];
  return (
    <Card
      variant="outlined"
      sx={{
        minWidth: 200,
        borderColor: selected ? "primary.main" : "divider",
        borderWidth: selected ? 2 : 1,
      }}
    >
      <Handle type="target" position={Position.Left} />
      <CardContent sx={{ p: 1.5, "&:last-child": { pb: 1.5 } }}>
        <Box
          sx={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: 1,
          }}
        >
          <Typography variant="subtitle2" noWrap title={task.name}>
            {task.name}
          </Typography>
          <Chip
            size="small"
            icon={<KindIcon fontSize="small" />}
            label={task.kind}
            variant="outlined"
          />
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
