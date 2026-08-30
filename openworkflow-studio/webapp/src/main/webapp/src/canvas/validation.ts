// Real JSON Schema validation against the *official* CNCF Serverless
// Workflow schema (io.serverlessworkflow:serverlessworkflow-types'
// bundled schema/workflow.yaml, the exact resource
// OpenWorkflowCompiler.compile() itself validates against - converted to
// JSON once via js-yaml and committed alongside this file, not hand-rolled
// per-kind heuristics that would drift from what the backend actually
// enforces). "there is no validation of steps in the canvas... an empty
// set blows up in the backend" - this is the fix: the same rule
// (SetTask.set needs minProperties: 1) now runs client-side, instantly,
// non-blocking.
//
// Deliberately advisory, never a save-blocker: draft saves no longer have
// to compile at all (see openworkflow-280 and WorkflowGovernanceServiceImpl)
// - this exists to make problems visible immediately, in-place, not to
// reintroduce the "can't save WIP" friction that change just removed.
import type { ErrorObject } from "ajv";
import { load } from "js-yaml";
import type { Task } from "./dsl";
// Precompiled by scripts/generate-workflow-validator.mjs, not ajv.compile()'d
// here at runtime - AJV's default compilation runs the schema through
// `new Function(...)`, which the deployed app's CSP (script-src 'self', no
// 'unsafe-eval') blocks outright. That surfaced as an uncaught exception at
// module-load time in production, which is worse than validation merely not
// working - it broke evaluation of the whole bundle. See that script for
// the regeneration command (only needed if workflow-schema.json changes).
import validateDocument from "./generated-workflow-validator.js";

export type ValidationIssue = {
  // undefined = a workflow-level problem (document/, input/, use/, ...),
  // not attributable to one task node.
  taskPath?: { containerPath: string[]; taskName: string };
  message: string;
  pointer: string;
};

/**
 * Parses a JSON Pointer into this canvas's {containerPath, taskName}
 * address, same do/try/catch-walking rules as executionTrace.ts's
 * parseTaskPath - but lenient about what follows the task name instead of
 * bailing entirely: a validation error's pointer routinely reaches INTO a
 * task ("/do/2/task3/raise/error/type", a bad "type" pattern), not just
 * at it, and that whole subtree still belongs to task3's badge. Kept
 * separate from parseTaskPath rather than generalizing it in place -
 * that one's contract (exact task-level pointers only, execution events
 * never point deeper) is real and worth keeping strict.
 */
export function taskPathForPointer(
  pointer: string,
): { containerPath: string[]; taskName: string } | undefined {
  const segments = pointer.split("/").filter((segment) => segment.length > 0);
  const containerPath: string[] = [];
  let index = 0;
  let last: { containerPath: string[]; taskName: string } | undefined;

  while (index < segments.length) {
    const listKey = segments[index];
    if (listKey === "do" || listKey === "try") {
      index += 2; // consume listKey and the numeric list index
      if (index >= segments.length) return last;
      const name = unescapeJsonPointerSegment(segments[index]);
      index += 1;
      last = { containerPath: [...containerPath], taskName: name };
      if (index >= segments.length) return last;
      // Only descend if what follows genuinely continues into a nested
      // task list - anything else (a field within this task, like
      // "raise"/"error"/"type") still belongs to `last`, so stop here
      // rather than mis-attributing it to a container that doesn't exist.
      if (segments[index] !== "do" && segments[index] !== "try" && segments[index] !== "catch") {
        return last;
      }
      containerPath.push(name);
      continue;
    }
    if (listKey === "catch") {
      index += 1; // always immediately followed by "do"
      continue;
    }
    return last;
  }
  return last;
}

function unescapeJsonPointerSegment(segment: string): string {
  return segment.replace(/~1/g, "/").replace(/~0/g, "~");
}

function getAtPointer(root: unknown, pointer: string): unknown {
  if (pointer === "") return root;
  let current = root;
  for (const segment of pointer.split("/").filter((s) => s.length > 0)) {
    if (current === null || typeof current !== "object") return undefined;
    const key = unescapeJsonPointerSegment(segment);
    current = (current as Record<string, unknown>)[key];
  }
  return current;
}

// The 12 CNCF task keywords a task body can carry - the same set
// taskKindMeta.ts's ALL_KINDS models, duplicated here (not imported) since
// this is about the wire keyword, not the canvas's Task["kind"] union.
const TASK_KEYWORDS = new Set([
  "set",
  "call",
  "switch",
  "raise",
  "wait",
  "emit",
  "do",
  "for",
  "fork",
  "try",
  "listen",
  "run",
]);

// AJV keyword names that only ever show up, at a task's own root path, as
// an artifact of trying (and failing) each of the 12 alternatives in
// turn - confirmed empirically against real AJV output, not assumed: a
// genuinely task-specific problem (a bad "if" expression, a bad "set"
// value, ...) always shows up through a $ref'd sub-schema instead, one
// level deeper than the task root.
const GENERIC_ALTERNATION_KEYWORDS = new Set(["required", "unevaluatedProperties", "oneOf"]);

function missingTaskKeyword(error: ErrorObject): string | undefined {
  if (error.keyword !== "required") return undefined;
  const missing = (error.params as { missingProperty?: unknown } | undefined)?.missingProperty;
  return typeof missing === "string" && TASK_KEYWORDS.has(missing) ? missing : undefined;
}

/**
 * Real JSON Schema validation, distilled into something a person can act
 * on. AJV's raw `errors` for one bad task otherwise reads like the
 * reported crash's stack trace: the 12-way task-kind oneOf fans out into
 * one full "required property X not found" per kind that ISN'T present,
 * plus a generic "must match exactly one schema in oneOf" wrapper - none
 * of it says what's actually wrong. Two passes fix this: find every path
 * where that 12-way alternation lives (a "required": missing-task-keyword
 * error is the tell), then for each one, either synthesize one "doesn't
 * match any known kind" message (only when the parsed document has NONE
 * of the 12 keywords there at all) or drop the alternation noise entirely
 * (when it does have one - the real problem, if any, shows up one level
 * deeper, e.g. "/do/0/greet/set" itself, and is left untouched).
 */
export function validateWorkflowSource(source: string): ValidationIssue[] {
  let parsed: unknown;
  try {
    parsed = load(source);
  } catch {
    // A YAML syntax error is already surfaced by the canvas's own parse
    // error banner (see WorkflowCanvas.tsx) - don't double-report it here.
    return [];
  }
  if (parsed === null || typeof parsed !== "object") return [];
  const valid = validateDocument(parsed);
  if (valid || !validateDocument.errors) return [];
  const errors = validateDocument.errors;

  const taskAlternationPaths = new Set(
    errors.filter((error) => missingTaskKeyword(error) !== undefined).map((error) => error.instancePath),
  );

  function presentTaskKeyword(pointer: string): string | undefined {
    const value = getAtPointer(parsed, pointer);
    if (value === null || typeof value !== "object") return undefined;
    return Object.keys(value as Record<string, unknown>).find((key) => TASK_KEYWORDS.has(key));
  }

  const seen = new Set<string>();
  const issues: ValidationIssue[] = [];

  for (const path of taskAlternationPaths) {
    if (presentTaskKeyword(path) !== undefined) continue; // matches a known kind - say nothing here, only deeper
    issues.push({
      taskPath: taskPathForPointer(path),
      message: "doesn't match any known task kind (set/call/switch/raise/wait/emit/do/for/fork/try/listen/run)",
      pointer: path,
    });
  }

  for (const error of errors) {
    if (taskAlternationPaths.has(error.instancePath) && GENERIC_ALTERNATION_KEYWORDS.has(error.keyword)) {
      continue;
    }
    // A generic oneOf wrapper elsewhere (e.g. SetTaskConfiguration's own
    // narrower object-or-string oneOf) whose specific branch errors are
    // already listed right beside it adds nothing beyond "these
    // disagree" - drop it too, same reasoning as the task-kind wrapper.
    if (
      error.keyword === "oneOf" &&
      errors.some((sibling) => sibling !== error && sibling.instancePath === error.instancePath)
    ) {
      continue;
    }
    const message = error.message ?? "is invalid";
    const dedupeKey = `${error.instancePath}::${message}`;
    if (seen.has(dedupeKey)) continue;
    seen.add(dedupeKey);
    issues.push({
      taskPath: taskPathForPointer(error.instancePath),
      message,
      pointer: error.instancePath,
    });
  }
  return issues;
}

// Matches WorkflowContractAnalyzer's SchemaCompatibilityFinding.diagnostic()
// (Java: producerSchemaPath + " -> " + consumerSchemaPath + " [" + status +
// "] " + reason), prefixed with "Schema compatibility " by
// OpenWorkflowCompiler before it's thrown - see
// WorkflowGovernanceServiceImpl's validateWorkflowDefinition, the only
// place this string shape reaches the browser today (as one opaque entry
// in WorkflowDefinitionValidation.violations). Both schema paths are kept
// even though only the consumer's is used to attribute a canvas badge -
// the message itself still names both ends, so opening either task's
// tooltip explains the mismatch, not just the one that's flagged.
const CONTRACT_VIOLATION = /^Schema compatibility (\S+) -> (\S+) \[(\w+)\] (.*)$/;

/**
 * "Wire two tasks together and have their schemas checked for
 * compatibility" (the real ask behind "I cannot select the output of a
 * step and attach it to the input of another... we enforce those and
 * ensure compatibility between outputs and inputs") already runs on the
 * backend, via WorkflowContractAnalyzer - OpenWorkflowCompiler.compile()
 * calls it and validateWorkflowDefinition already surfaces its failures.
 * What was missing was ever showing them anywhere but one raw joined
 * string in App.tsx's generic status banner. This turns each one back
 * into the same ValidationIssue shape validateWorkflowSource produces, so
 * they attribute to a task badge instead of a sentence nobody reads.
 *
 * Unlike validateWorkflowSource, this can't run client-side - it needs
 * the actually-compiled WorkflowPlan (resolved $refs, control-flow-aware
 * edge discovery through named "then" jumps and nested containers), which
 * only exists on the backend. Called with whatever
 * WorkflowDefinitionValidation.violations comes back from a real
 * validateWorkflowDefinition call (see App.tsx's validate()).
 */
export function parseContractViolations(violations: string[]): ValidationIssue[] {
  return violations.map((violation) => {
    const match = CONTRACT_VIOLATION.exec(violation);
    if (!match) return { message: violation, pointer: "" };
    const [, producerPath, consumerPath, status, reason] = match;
    return {
      taskPath: taskPathForPointer(consumerPath),
      message: `incompatible with ${producerPath}: ${reason} (${status})`,
      pointer: consumerPath,
    };
  });
}

const TERMINAL_DIRECTIVES = new Set(["continue", "exit", "end"]);

function invalidThenTarget(then: string | undefined, namesInScope: Set<string>): boolean {
  return then !== undefined && !TERMINAL_DIRECTIVES.has(then) && !namesInScope.has(then);
}

/**
 * "I can have identical default and named cases... the target of the then
 * clause can be a task that doesn't even exist" - real gaps in the parsed
 * Task tree itself (workflow (3).yaml is the confirmed real-world case:
 * switch cases with then: lhljhlj / then: pppp, neither ever a real task
 * anywhere), not something the JSON-Schema pass above can catch - the
 * schema only knows "then is a string," it has no notion of what other
 * task names exist. Walks the ALREADY-PARSED task tree (not raw source,
 * unlike validateWorkflowSource) since every "which names are in scope
 * here" question is trivial once cases/children are already real arrays,
 * not YAML entries to re-walk.
 *
 * A "then" only ever resolves within its OWN task list - a case in a
 * switch nested inside a "do" can't jump to a task outside that "do", so
 * namesInScope is always just "the sibling task names in this exact
 * list," recomputed fresh at each recursion level. try's catch.do is its
 * own separate list (a different scope from the try block's own
 * children), matching CatchClause.children being a distinct field, not a
 * sibling of TryTask.children.
 *
 * Deliberately NOT validating "when" expression syntax here - that would
 * need a real jq/runtime-expression parser this canvas doesn't have; this
 * only catches structural reference errors (nonexistent targets,
 * duplicate names), which is most of what made workflow (3).yaml
 * concretely broken.
 */
export function validateTaskReferences(
  tasks: Task[],
  containerPath: string[] = [],
): ValidationIssue[] {
  const issues: ValidationIssue[] = [];
  const namesInScope = new Set(tasks.map((task) => task.name));
  const seenNames = new Set<string>();

  for (const task of tasks) {
    const taskPath = { containerPath, taskName: task.name };
    const pointerBase = [...containerPath, task.name].join("/");

    if (seenNames.has(task.name)) {
      issues.push({
        taskPath,
        message: `Another task in this same list is also named "${task.name}" - task names must be unique within a "do" list.`,
        pointer: `/${pointerBase}`,
      });
    }
    seenNames.add(task.name);

    if (invalidThenTarget(task.then, namesInScope)) {
      issues.push({
        taskPath,
        message: `"then: ${task.then}" doesn't match any task in this same list, and isn't continue/exit/end.`,
        pointer: `/${pointerBase}/then`,
      });
    }

    if (task.kind === "switch") {
      const seenCaseNames = new Set<string>();
      for (const switchCase of task.cases) {
        if (seenCaseNames.has(switchCase.name)) {
          issues.push({
            taskPath,
            message: `Two cases here are both named "${switchCase.name}" - only the first one can ever match.`,
            pointer: `/${pointerBase}/${switchCase.name}`,
          });
        }
        seenCaseNames.add(switchCase.name);
        if (invalidThenTarget(switchCase.then, namesInScope)) {
          issues.push({
            taskPath,
            message: `Case "${switchCase.name}"'s "then: ${switchCase.then}" doesn't match any task in this same list, and isn't continue/exit/end.`,
            pointer: `/${pointerBase}/${switchCase.name}/then`,
          });
        }
      }
    }

    if (task.kind === "try" && invalidThenTarget(task.catchClause.then, namesInScope)) {
      issues.push({
        taskPath,
        message: `The catch block's "then: ${task.catchClause.then}" doesn't match any task in this same list, and isn't continue/exit/end.`,
        pointer: `/${pointerBase}/catch/then`,
      });
    }

    if ("children" in task) {
      issues.push(...validateTaskReferences(task.children, [...containerPath, task.name]));
    }
    if (task.kind === "try") {
      issues.push(
        ...validateTaskReferences(task.catchClause.children, [...containerPath, task.name]),
      );
    }
  }

  return issues;
}
