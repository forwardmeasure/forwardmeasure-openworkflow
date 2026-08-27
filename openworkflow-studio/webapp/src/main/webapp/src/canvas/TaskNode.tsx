import type { ComponentType } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";
import AltRouteIcon from "@mui/icons-material/AltRoute";
import CallMadeIcon from "@mui/icons-material/CallMade";
import EditNoteIcon from "@mui/icons-material/EditNote";
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
      </CardContent>
      {task.kind === "switch" ? (
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
