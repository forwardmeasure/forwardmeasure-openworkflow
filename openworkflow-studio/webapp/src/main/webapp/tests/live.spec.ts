import { expect, test } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";

const token = process.env.OPENWORKFLOW_STUDIO_TOKEN;
test.skip(
  !token,
  "requires OPENWORKFLOW_STUDIO_BASE_URL and OPENWORKFLOW_STUDIO_TOKEN",
);

test("authors and controls a real durable workflow", async ({ page }) => {
  const name = `studio-live-${Date.now()}`;
  await page.addInitScript(
    (value) => sessionStorage.setItem("openworkflow.accessToken", value),
    token!,
  );
  await page.emulateMedia({ reducedMotion: "reduce" });
  await page.goto("./");
  await expect(
    (
      await new AxeBuilder({ page })
        .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"])
        .analyze()
    ).violations,
  ).toEqual([]);

  await page.getByLabel("Workflow YAML or JSON").fill(`document:
  dsl: '1.0.3'
  namespace: forwardmeasure-studio-acceptance
  name: ${name}
  version: '1.0.0'
do:
  - admitted:
      set:
        studio: true
  - hold:
      wait: PT5M
  - must-not-run-after-cancel:
      set:
        cancelledInvariantBroken: true
`);
  await page.getByRole("button", { name: "Validate & admit" }).click();
  await expect(page.getByRole("status")).toContainText(
    `Admitted forwardmeasure-studio-acceptance/${name}/1.0.0`,
  );

  const definition = page
    .locator("article.definition")
    .filter({ hasText: name });
  await definition.getByRole("button", { name: "Start workflow" }).click();
  await expect(page.getByRole("status")).toContainText("accepted in WAITING", {
    timeout: 30_000,
  });

  const row = page.locator("button.execution-row").filter({ hasText: name });
  await expect(async () => {
    await page.getByRole("button", { name: "Refresh" }).click();
    await expect(row).toBeVisible();
    await row.click();
    await expect(page.locator("section.detail mark")).toHaveText("WAITING");
  }).toPass({ timeout: 30_000 });
  await expect(
    page.getByRole("button", { name: "Pause", exact: true }),
  ).toBeEnabled();
  await page.getByRole("button", { name: "Pause", exact: true }).click();
  await expect(page.getByRole("status")).toContainText(
    "pause accepted: PAUSED",
  );
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await expect(page.getByRole("status")).toContainText(
    "cancel accepted: CANCELLED",
  );

  await expect(async () => {
    await page.getByRole("button", { name: "Refresh" }).click();
    await expect(row.locator("mark")).toHaveText("CANCELLED");
  }).toPass({ timeout: 30_000 });
  await expect(
    (
      await new AxeBuilder({ page })
        .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"])
        .analyze()
    ).violations,
  ).toEqual([]);
});
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
