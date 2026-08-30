import { useState } from "react";
import { BaseEdge, EdgeLabelRenderer, getSmoothStepPath, type EdgeProps } from "@xyflow/react";
import AddIcon from "@mui/icons-material/Add";
import Avatar from "@mui/material/Avatar";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import ListSubheader from "@mui/material/ListSubheader";
import Menu from "@mui/material/Menu";
import MenuItem from "@mui/material/MenuItem";
import { useTheme } from "@mui/material/styles";
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
  // Only true for whichever edge the mouse is currently over (see
  // WorkflowCanvas.tsx's onEdgeMouseEnter/Leave) - the "+" button used to
  // render unconditionally at every edge's midpoint, permanent clutter
  // sitting exactly on the line a user is trying to judge alignment
  // against ("the + sign makes it impossible to horizontally align task
  // boxes"). Kept visible while its own menu is open even if the mouse
  // has moved off the edge in the meantime (see anchorEl below).
  isHovered: boolean;
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
  selected,
}: EdgeProps) {
  const theme = useTheme();
  const [anchorEl, setAnchorEl] = useState<HTMLElement | null>(null);
  const [edgePath, labelX, labelY] = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });
  const insertableData = data as InsertableEdgeData | undefined;
  const onInsert = insertableData?.onInsert;
  // "I can't align nodes because the + sign in the vertex is throwing
  // things off" - the button sat exactly ON the edge's own path, dead
  // center at its midpoint. For the common case (a plain edge, no label)
  // that's precisely the pixel line someone needs a clear, unobstructed
  // view of to judge whether two OTHER nodes line up against it - hiding
  // it until hover (the earlier fix) didn't help, since hovering near
  // that exact spot is what aligning nodes against it naturally involves.
  // Now offset perpendicular to the edge's own dominant direction (off
  // the line, not on it) instead of dead center - away from the label
  // when there is one (switch case text), or straight off the line
  // itself when there isn't.
  const isMoreHorizontal = Math.abs(targetX - sourceX) >= Math.abs(targetY - sourceY);
  const PERPENDICULAR_OFFSET = 14;
  const buttonX = isMoreHorizontal ? labelX : labelX + PERPENDICULAR_OFFSET;
  const buttonY =
    (isMoreHorizontal ? labelY - PERPENDICULAR_OFFSET : labelY) + (label ? 16 : 0);
  const showInsertButton = Boolean(onInsert) && (insertableData?.isHovered || Boolean(anchorEl));
  // React Flow tracks edge.selected internally (a plain click sets it) -
  // this edge type never READ that prop before, so a click had genuinely
  // no visible effect at all, which read as "there's no concept of
  // selecting the connector." A selected edge now gets the same
  // primary-color, thicker-stroke treatment a selected node already gets
  // elsewhere in this canvas, and - the more important part - the small
  // drag-to-reconnect handles at each endpoint (rendered by React Flow
  // itself, not this component) are real and already work; they just had
  // zero visual affordance pointing anyone at them.
  const selectedStyle = selected
    ? { ...style, stroke: theme.palette.primary.main, strokeWidth: 2.5 }
    : style;

  return (
    <>
      <BaseEdge id={id} path={edgePath} style={selectedStyle} markerEnd={markerEnd} label={label} />
      {showInsertButton && (
        <EdgeLabelRenderer>
          <Avatar
            className="nodrag nopan"
            onClick={(event) => setAnchorEl(event.currentTarget)}
            sx={{
              position: "absolute",
              left: buttonX,
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
