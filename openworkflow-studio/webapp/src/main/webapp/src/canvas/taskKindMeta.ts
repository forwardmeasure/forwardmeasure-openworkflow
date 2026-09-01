import type { ComponentType } from "react";
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
import type { SvgIconProps } from "@mui/material/SvgIcon";
import type { Task } from "./dsl";

// One shared source of truth for how a task kind presents, consumed by
// TaskNode.tsx (the canvas card) and NodePalette.tsx (the sidebar) - both
// need the same icon/category/label per kind, and a third near-identical
// map (this file's predecessor: TaskNode.tsx had its own KIND_ICON,
// WorkflowCanvas.tsx had its own hardcoded Add-task MenuItem labels) is
// exactly the "three uses, worth factoring out" threshold this codebase's
// own conventions already apply elsewhere (see dsl.ts's taskListFromYaml/
// ToYamlEntries).
export const ALL_KINDS: Task["kind"][] = [
  "set",
  "call",
  "emit",
  "wait",
  "raise",
  "switch",
  "do",
  "for",
  "fork",
  "try",
  "listen",
  "run",
];

export const KIND_ICON: Record<Task["kind"], ComponentType<SvgIconProps>> = {
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

// Loosely mirrors Flowise's own palette grouping (simple actions vs.
// control-flow/looping constructs vs. external-system integration) - not a
// CNCF spec concept, purely a UI grouping so the palette doesn't read as
// one flat list of twelve items.
export type TaskCategory = "action" | "control" | "integration";

export const KIND_CATEGORY: Record<Task["kind"], TaskCategory> = {
  set: "action",
  call: "action",
  emit: "action",
  wait: "action",
  raise: "action",
  switch: "control",
  do: "control",
  for: "control",
  fork: "control",
  try: "control",
  listen: "integration",
  run: "integration",
};

export const CATEGORY_LABEL: Record<TaskCategory, string> = {
  action: "Actions",
  control: "Control Flow",
  integration: "Integration",
};

// MUI theme palette colors already defined in theme.ts (accent/warning/
// success) - reused here rather than inventing new hex values, so the
// palette's category coloring stays consistent with the rest of the app's
// existing accent/warning/success usage.
export const CATEGORY_COLOR: Record<TaskCategory, "primary" | "warning" | "success"> = {
  action: "primary",
  control: "warning",
  integration: "success",
};

// Short, palette-friendly labels ("Do (group)") - distinct from
// TaskInspector.tsx's KIND_LABEL ("Do task"), which is a full-sentence
// Inspector heading, not a compact palette row.
export const KIND_PALETTE_LABEL: Record<Task["kind"], string> = {
  set: "Set",
  call: "Call",
  emit: "Emit",
  wait: "Wait",
  raise: "Raise",
  switch: "Switch",
  do: "Do (Group)",
  for: "For (Loop)",
  fork: "Fork (Parallel)",
  try: "Try (Error Handling)",
  listen: "Listen (Event)",
  run: "Run",
};
