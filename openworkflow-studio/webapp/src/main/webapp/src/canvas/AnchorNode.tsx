import { Handle, Position, type NodeProps } from "@xyflow/react";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";

export type AnchorNodeData = { label: "Start" | "End" };

// Purely a visual anchor - "start" and "end" aren't Serverless Workflow DSL
// constructs (a "do:" list has no literal start/end task the way BPMN has
// start/end events), so these never round-trip through dsl.ts and are never
// selectable/editable. They exist so the flow's entry/exit point reads at a
// glance, matching the convention Flowise's own canvas uses. Only one End
// node is drawn today because the canvas only models a straight-line "do:"
// list; once branching ("switch") support lands, each unterminated branch
// tip should get its own End node instead of all branches converging on one.
export function AnchorNode({ data }: NodeProps & { data: AnchorNodeData }) {
  const isStart = data.label === "Start";
  return (
    <Box
      sx={{
        px: 2,
        py: 0.75,
        borderRadius: 999,
        border: 1.5,
        borderColor: isStart ? "success.main" : "text.secondary",
        color: isStart ? "success.main" : "text.secondary",
        bgcolor: "background.paper",
        fontWeight: 600,
      }}
    >
      {isStart ? undefined : <Handle type="target" position={Position.Left} />}
      <Typography
        variant="caption"
        sx={{ fontWeight: "inherit", color: "inherit" }}
      >
        {data.label}
      </Typography>
      {isStart ? <Handle type="source" position={Position.Right} /> : undefined}
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
