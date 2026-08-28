// Regenerates src/canvas/generated-workflow-validator.js from
// src/canvas/workflow-schema.json. Run this whenever workflow-schema.json
// changes:
//
//   node scripts/generate-workflow-validator.mjs
//
// Why this exists at all, instead of just calling ajv.compile(schema) at
// runtime (what validation.ts originally did): AJV compiles a schema into a
// validator function via `new Function(...)` by default - fast, but it's
// runtime code generation, which the deployed app's own CSP (script-src
// 'self', no 'unsafe-eval') correctly blocks. That threw an uncaught
// exception at module-load time in production (confirmed live against
// https://lux.kriyagentic.com/owf/studio/), which is a lot worse than a
// validation feature not working - an unhandled throw in a statically
// imported module breaks the whole bundle's evaluation.
//
// AJV's own answer to this is standalone code generation: compile the
// schema once, HERE, at build time (where eval is irrelevant - this runs
// under plain Node, not the browser's CSP), and commit the resulting plain
// JS validator function. Zero runtime code generation, so it's CSP-safe by
// construction, not by a try/catch working around the symptom.
import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import Ajv2020 from "ajv/dist/2020.js";
import standaloneCode from "ajv/dist/standalone/index.js";
import addFormats from "ajv-formats";
import schema from "../src/canvas/workflow-schema.json" with { type: "json" };

const outFile = fileURLToPath(
  new URL("../src/canvas/generated-workflow-validator.js", import.meta.url),
);

const ajv = new Ajv2020({ allErrors: true, strict: false, code: { source: true, esm: true } });
addFormats(ajv);
const validate = ajv.compile(schema);
const rawModuleCode = standaloneCode(ajv, validate);

// standaloneCode's esm:true only converts its OWN top-level export syntax -
// it still emits inline require("...") calls for AJV's runtime helpers
// (ucs2length, ajv-formats' regex table), a known AJV limitation the docs
// themselves point out (their suggested fix is "pipe this through a
// bundler"). Vite's production build (Rollup, with CJS interop built in)
// swallows those fine, which is why this looked CSP-clean when checked
// against the built bundle - but Vite's DEV server doesn't run project
// source through the same interop, so a plain `require` reaches the
// browser and throws "require is not defined". Rewriting these into real
// static imports here makes the generated file honestly ESM, not just
// ESM-shaped, so it behaves the same under both.
const requireTargets = [
  ...new Set([...rawModuleCode.matchAll(/require\("([^"]+)"\)/g)].map((m) => m[1])),
];
const importNames = new Map(requireTargets.map((spec, i) => [spec, `__ajvRuntime${i}`]));
const imports = requireTargets
  .map((spec) => `import * as ${importNames.get(spec)} from ${JSON.stringify(spec)};`)
  .join("\n");
const moduleCode = rawModuleCode.replace(
  /require\("([^"]+)"\)/g,
  (_match, spec) => importNames.get(spec),
);

writeFileSync(
  outFile,
  `// GENERATED FILE - do not edit by hand.
// Regenerate with: node scripts/generate-workflow-validator.mjs
// (see that script for why this is precompiled instead of ajv.compile()'d
// at runtime - CSP's script-src blocks the new Function() that requires)
${imports}
${moduleCode}`,
);

console.log(`Wrote ${outFile} (${moduleCode.length} bytes)`);
