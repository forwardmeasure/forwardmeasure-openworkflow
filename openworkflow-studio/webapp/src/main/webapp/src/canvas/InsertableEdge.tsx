import { useState } from "react";
import { BaseEdge, EdgeLabelRenderer, getSmoothStepPath, type EdgeProps } from "@xyflow/react";
import AddIcon from "@mui/icons-material/Add";
import Avatar from "@mui/material/Avatar";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import ListSubheader from "@mui/material/ListSubheader";
import Menu from "@mui/material/Menu";
import MenuItem from "@mui/material/MenuItem";
import {
  ALL_KINDS,
  CATEGORY_COLOR,
  CATEGORY_LABEL,
  KIND_CATEGORY,
  KIND_ICON,
  KIND_PALETTE_LABEL,
  type TaskCategory,
} from "./taskKindMeta";
import type { Task } from "./dsl";

const CATEGORY_ORDER: TaskCategory[] = ["action", "control", "integration"];

export type InsertableEdgeData = {
  onInsert: (edgeId: string, kind: Task["kind"]) => void;
};

/**
 * A "smoothstep"-shaped edge (same path type WorkflowCanvas.tsx's
 * defaultEdgeOptions already used) with one addition: a small "+" button at
 * its midpoint that opens a categorized kind picker (the same data
 * NodePalette.tsx draws from) and splices a new task directly into this
 * edge - "inject a step between two already-connected tasks, anywhere,"
 * not just append-at-the-end via the palette/drag-drop. Offset below the
 * edge's own label (a switch case's name/condition) when one is present, so
 * the two don't overlap.
 */
export function InsertableEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  style,
  markerEnd,
  label,
  data,
}: EdgeProps) {
  const [anchorEl, setAnchorEl] = useState<HTMLElement | null>(null);
  const [edgePath, labelX, labelY] = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });
  const onInsert = (data as InsertableEdgeData | undefined)?.onInsert;
  const buttonY = label ? labelY + 16 : labelY;

  return (
    <>
      <BaseEdge id={id} path={edgePath} style={style} markerEnd={markerEnd} label={label} />
      {onInsert && (
        <EdgeLabelRenderer>
          <Avatar
            className="nodrag nopan"
            onClick={(event) => setAnchorEl(event.currentTarget)}
            sx={{
              position: "absolute",
              left: labelX,
              top: buttonY,
              transform: "translate(-50%, -50%)",
              width: 18,
              height: 18,
              bgcolor: "background.paper",
              color: "text.secondary",
              border: 1,
              borderColor: "divider",
              cursor: "pointer",
              pointerEvents: "all",
              "&:hover": { color: "primary.main", borderColor: "primary.main" },
            }}
          >
            <AddIcon sx={{ fontSize: 13 }} />
          </Avatar>
        </EdgeLabelRenderer>
      )}
      <Menu anchorEl={anchorEl} open={Boolean(anchorEl)} onClose={() => setAnchorEl(null)}>
        {CATEGORY_ORDER.flatMap((category) => [
          <ListSubheader key={`heading-${category}`} sx={{ lineHeight: 2 }}>
            {CATEGORY_LABEL[category]}
          </ListSubheader>,
          ...ALL_KINDS.filter((kind) => KIND_CATEGORY[kind] === category).map((kind) => {
            const KindIcon = KIND_ICON[kind];
            const color = CATEGORY_COLOR[category];
            return (
              <MenuItem
                key={kind}
                onClick={() => {
                  setAnchorEl(null);
                  onInsert?.(id, kind);
                }}
              >
                <ListItemIcon>
                  <Avatar
                    variant="rounded"
                    sx={{ width: 22, height: 22, bgcolor: `${color}.main`, color: `${color}.contrastText` }}
                  >
                    <KindIcon sx={{ fontSize: 13 }} />
                  </Avatar>
                </ListItemIcon>
                <ListItemText>{KIND_PALETTE_LABEL[kind]}</ListItemText>
              </MenuItem>
            );
          }),
        ])}
      </Menu>
    </>
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
