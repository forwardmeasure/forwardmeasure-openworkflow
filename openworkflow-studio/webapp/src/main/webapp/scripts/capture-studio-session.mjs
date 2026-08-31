// Run this yourself, in your own terminal, from this directory
// (openworkflow-studio/webapp/src/main/webapp):
//
//   node scripts/capture-studio-session.mjs
//
// Opens a REAL, visible browser window. Log in normally, by hand - your
// password never touches this script or Claude. Once you're actually
// inside Studio (past the Keycloak redirect), come back to this terminal
// and press Enter; it saves the session (cookies + localStorage) to
// studio-session.json in this directory and closes the browser.
//
// That file is what to hand over for UI-level debugging - Claude loads it
// via Playwright's storageState option to start already logged in,
// without ever needing your credentials. Treat it like a session cookie
// (because it is one): don't commit it, and it's only as long-lived as
// Keycloak's own SSO session - if it stops working, just recapture it.
import { chromium } from "playwright";
import { createInterface } from "node:readline/promises";

const browser = await chromium.launch({ headless: false });
const context = await browser.newContext();
const page = await context.newPage();
await page.goto("https://lux.kriyagentic.com/owf/studio/");

console.log("Log in in the browser window that just opened.");
console.log("Once you're actually inside Studio, come back here and press Enter.");
const rl = createInterface({ input: process.stdin, output: process.stdout });
await rl.question("");
rl.close();

await context.storageState({ path: "studio-session.json" });
console.log("Saved studio-session.json");
await browser.close();
