import Avatar from "@mui/material/Avatar";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
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

export const PALETTE_DRAG_MIME = "application/x-openworkflow-task-kind";

const CATEGORY_ORDER: TaskCategory[] = ["action", "control", "integration"];

/**
 * A persistent, categorized, draggable node palette - the primary way to
 * add a task, replacing the earlier small "Add task" dropdown menu. Drag a
 * row onto the canvas to splice a task into whichever edge it's dropped
 * closest to (WorkflowCanvas.tsx's onDrop); click a row to splice it onto
 * the currently selected task's own outgoing edge instead (or append at
 * the very end if nothing's selected) - see addTaskFromPalette there.
 */
export function NodePalette({
  onAddTask,
}: {
  onAddTask: (kind: Task["kind"]) => void;
}) {
  return (
    <Box
      sx={{
        width: 220,
        flexShrink: 0,
        borderRight: 1,
        borderColor: "divider",
        overflowY: "auto",
        display: "flex",
        flexDirection: "column",
        gap: 2,
        p: 1.5,
      }}
    >
      <Typography variant="overline" color="text.secondary" sx={{ px: 0.5 }}>
        Add a task
      </Typography>
      {CATEGORY_ORDER.map((category) => (
        <Box key={category} sx={{ display: "flex", flexDirection: "column", gap: 0.5 }}>
          <Typography
            variant="caption"
            color="text.secondary"
            sx={{ px: 0.5, textTransform: "uppercase", letterSpacing: 0.5 }}
          >
            {CATEGORY_LABEL[category]}
          </Typography>
          {ALL_KINDS.filter((kind) => KIND_CATEGORY[kind] === category).map((kind) => {
            const KindIcon = KIND_ICON[kind];
            const color = CATEGORY_COLOR[category];
            return (
              <Box
                key={kind}
                draggable
                onDragStart={(event) => {
                  event.dataTransfer.setData(PALETTE_DRAG_MIME, kind);
                  event.dataTransfer.effectAllowed = "move";
                }}
                onClick={() => onAddTask(kind)}
                sx={{
                  display: "flex",
                  alignItems: "center",
                  gap: 1,
                  px: 1,
                  py: 0.75,
                  borderRadius: 2,
                  cursor: "grab",
                  userSelect: "none",
                  "&:hover": { bgcolor: "action.hover" },
                  "&:active": { cursor: "grabbing" },
                }}
              >
                <Avatar
                  variant="rounded"
                  sx={{
                    width: 24,
                    height: 24,
                    bgcolor: `${color}.main`,
                    color: `${color}.contrastText`,
                  }}
                >
                  <KindIcon sx={{ fontSize: 14 }} />
                </Avatar>
                <Typography variant="body2" noWrap>
                  {KIND_PALETTE_LABEL[kind]}
                </Typography>
              </Box>
            );
          })}
        </Box>
      ))}
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
