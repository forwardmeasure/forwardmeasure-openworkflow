import type { NodeProps } from "@xyflow/react";
import DeleteIcon from "@mui/icons-material/Delete";
import Box from "@mui/material/Box";
import IconButton from "@mui/material/IconButton";

export type StickyNoteData = {
  text: string;
  onTextChange: (id: string, text: string) => void;
  onDelete: (id: string) => void;
};

// A pure canvas-layer annotation - never touches the Task union, dsl.ts, or
// the serialized "do:" list at all, so it can't corrupt or even affect a
// saved workflow document; it's exactly as ephemeral as a hand-dragged node
// position already is (see WorkflowCanvas.tsx's own comment on that - lost
// on unmount, not a new limitation this introduces). Deliberately a plain
// <textarea>, not a MUI TextField - a sticky note reads as a sticky note
// (a scrap of paper) specifically because it doesn't have Studio's usual
// form-field chrome (label, outline, helper text).
export function StickyNoteNode({
  id,
  data,
  selected,
}: NodeProps & { data: StickyNoteData }) {
  return (
    <Box
      className="nodrag"
      sx={{
        width: 220,
        minHeight: 140,
        // Deliberately fixed (not theme-token-driven) - a sticky note reads
        // as a sticky note by looking like a scrap of yellow paper
        // regardless of Studio's own dark theme, the same way a real
        // sticky note doesn't change color to match the desk it's on.
        bgcolor: "#fde68a",
        color: "#422006",
        borderRadius: 1,
        boxShadow: selected ? 6 : 2,
        outline: selected ? "2px solid #f59e0b" : "none",
        outlineOffset: 2,
        p: 1,
        position: "relative",
        "&:hover .sticky-delete": { opacity: 1 },
      }}
    >
      <IconButton
        className="sticky-delete nodrag"
        size="small"
        onClick={() => data.onDelete(id)}
        aria-label="Delete note"
        sx={{
          position: "absolute",
          top: 2,
          right: 2,
          opacity: 0,
          transition: "opacity 120ms ease",
          color: "inherit",
        }}
      >
        <DeleteIcon fontSize="inherit" />
      </IconButton>
      <textarea
        className="nodrag"
        value={data.text}
        onChange={(event) => data.onTextChange(id, event.target.value)}
        placeholder="Add a note…"
        style={{
          width: "100%",
          height: 120,
          resize: "none",
          border: "none",
          outline: "none",
          background: "transparent",
          color: "inherit",
          font: "inherit",
          fontSize: "0.8rem",
        }}
      />
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
