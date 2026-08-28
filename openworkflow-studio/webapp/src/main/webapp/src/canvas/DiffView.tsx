import AddCircleOutlinedIcon from "@mui/icons-material/AddCircleOutlined";
import ChangeCircleOutlinedIcon from "@mui/icons-material/ChangeCircleOutlined";
import RemoveCircleOutlinedIcon from "@mui/icons-material/RemoveCircleOutlined";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import { useMemo } from "react";
import { KIND_ICON, KIND_PALETTE_LABEL } from "./taskKindMeta";
import { diffLines, diffStats } from "./textDiff";
import { diffWorkflows, type TaskChange } from "./workflowDiff";

const CHANGE_COLOR: Record<TaskChange["changeKind"], "success" | "error" | "warning"> = {
  added: "success",
  removed: "error",
  modified: "warning",
};

const CHANGE_ICON = {
  added: AddCircleOutlinedIcon,
  removed: RemoveCircleOutlinedIcon,
  modified: ChangeCircleOutlinedIcon,
};

function TaskChangeRow({ change }: { change: TaskChange }) {
  const ChangeIcon = CHANGE_ICON[change.changeKind];
  const KindIcon = KIND_ICON[change.kind];
  const location = change.path.length > 0 ? `${change.path.join(" / ")} / ` : "";
  return (
    <Stack direction="row" spacing={1} sx={{ alignItems: "center", py: 0.25 }}>
      <ChangeIcon fontSize="small" color={CHANGE_COLOR[change.changeKind]} />
      <KindIcon fontSize="small" color="disabled" />
      <Typography variant="body2">
        <Box component="span" color="text.secondary">
          {location}
        </Box>
        <strong>{change.name}</strong>
        <Box component="span" color="text.secondary">
          {" "}
          ({KIND_PALETTE_LABEL[change.kind]}
          {change.previousKind
            ? ` - was ${KIND_PALETTE_LABEL[change.previousKind]}`
            : ""}
          )
        </Box>
      </Typography>
    </Stack>
  );
}

// Unified (single-column) rather than side-by-side - workflow YAML sources
// are narrow and this reads top-to-bottom the same way the source editor
// tab does, no horizontal eye movement between two panes.
function LineDiff({ before, after }: { before: string; after: string }) {
  const lines = useMemo(() => diffLines(before, after), [before, after]);
  const stats = useMemo(() => diffStats(lines), [lines]);
  if (stats.added === 0 && stats.removed === 0) {
    return (
      <Typography variant="body2" color="text.secondary">
        No textual changes.
      </Typography>
    );
  }
  return (
    <Box
      component="pre"
      aria-label="Revision diff"
      sx={{
        m: 0,
        p: 1,
        fontFamily: "monospace",
        fontSize: "0.8rem",
        lineHeight: 1.5,
        overflowX: "auto",
        bgcolor: "background.default",
        border: 1,
        borderColor: "divider",
        borderRadius: 1,
      }}
    >
      {lines.map((line, index) => (
        <Box
          key={index}
          component="div"
          sx={{
            display: "flex",
            bgcolor:
              line.kind === "add"
                ? "rgba(46, 160, 67, 0.18)"
                : line.kind === "remove"
                  ? "rgba(248, 81, 73, 0.18)"
                  : undefined,
          }}
        >
          <Box
            component="span"
            sx={{ width: 34, flexShrink: 0, color: "text.disabled", textAlign: "right", pr: 1, userSelect: "none" }}
          >
            {line.beforeLine ?? ""}
          </Box>
          <Box
            component="span"
            sx={{ width: 34, flexShrink: 0, color: "text.disabled", textAlign: "right", pr: 1, userSelect: "none" }}
          >
            {line.afterLine ?? ""}
          </Box>
          <Box component="span" sx={{ width: 14, flexShrink: 0, userSelect: "none" }}>
            {line.kind === "add" ? "+" : line.kind === "remove" ? "-" : " "}
          </Box>
          <Box component="span" sx={{ whiteSpace: "pre-wrap", wordBreak: "break-all" }}>
            {line.text || " "}
          </Box>
        </Box>
      ))}
    </Box>
  );
}

export function DiffView({
  before,
  after,
  label,
}: {
  before: string;
  after: string;
  label: string;
}) {
  const structural = useMemo(() => diffWorkflows(before, after), [before, after]);
  const lineStats = useMemo(() => diffStats(diffLines(before, after)), [before, after]);

  return (
    <Box>
      <Stack direction="row" spacing={1} sx={{ alignItems: "center", mb: 1 }}>
        <Typography variant="subtitle2">{label}</Typography>
        {lineStats.added > 0 && (
          <Chip size="small" color="success" variant="outlined" label={`+${lineStats.added}`} />
        )}
        {lineStats.removed > 0 && (
          <Chip size="small" color="error" variant="outlined" label={`-${lineStats.removed}`} />
        )}
      </Stack>
      {structural.available ? (
        structural.changes.length > 0 ? (
          <Box sx={{ mb: 1.5 }}>
            {structural.changes.map((change) => (
              <TaskChangeRow key={`${change.changeKind}-${change.path.join("/")}-${change.name}`} change={change} />
            ))}
          </Box>
        ) : (
          <Typography variant="body2" color="text.secondary" sx={{ mb: 1.5 }}>
            No task-level changes.
          </Typography>
        )
      ) : (
        <Alert severity="warning" sx={{ mb: 1.5 }}>
          Task-level diff unavailable ({structural.reason}) - showing text diff only.
        </Alert>
      )}
      <LineDiff before={before} after={after} />
    </Box>
  );
}
