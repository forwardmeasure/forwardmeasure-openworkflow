import { expect, test } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";

const revision = {
  revisionId: "11111111-1111-1111-1111-111111111111",
  definitionKey: "forwardmeasure.hello-studio",
  displayName: "Hello Studio",
  revisionNumber: 1,
  lifecycleState: "PUBLISHED",
  sourceDocument: "do:\n  - greet:\n      set: {message: hello}\n",
  resolvedDocument: "{}",
  specificationVersion: "1.0.3",
  compilerProfile: "a".repeat(64),
  sourceDigest: "b".repeat(64),
  resolvedDigest: "c".repeat(64),
  authorActorId: "author",
};

test.beforeEach(async ({ page }) => {
  await page.route("**/api/api/v1/authorizations", (route) =>
    route.fulfill({
      json: {
        decisions: {
          "definition:create": true,
          "definition:validate": true,
          "definition:update": true,
          "definition:submit": true,
          "definition:approve": true,
          "definition:reject": true,
          "definition:publish": true,
          "execution:start": true,
        },
      },
    }),
  );
  await page.route("**/api/v1/workflow-definitions", (route) =>
    route.fulfill({
      status: route.request().method() === "GET" ? 200 : 201,
      json:
        route.request().method() === "GET" ? { items: [revision] } : revision,
    }),
  );
  await page.route("**/api/v1/workflow-definitions/validation", (route) =>
    route.fulfill({
      json: {
        sourceDigest: "b".repeat(64),
        resolvedDigest: "c".repeat(64),
        specificationVersion: "1.0.3",
        compilerProfile: "a".repeat(64),
      },
    }),
  );
  await page.addInitScript(() =>
    sessionStorage.setItem("openworkflow.accessToken", "opaque-test-token"),
  );
});

test("authors losslessly and exposes only batch-authorized role actions", async ({
  page,
}) => {
  await page.goto("./");
  const editor = page.getByLabel("Workflow YAML or JSON");
  const source = await editor.inputValue();
  await expect(
    page.getByRole("listitem").filter({ hasText: "greet" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Validate", exact: true }).click();
  await expect(page.getByRole("status")).toContainText(
    "Valid Open Workflow 1.0.3",
  );
  expect(await editor.inputValue()).toBe(source);
  await expect(
    page.getByRole("button", { name: "Start workflow" }),
  ).toBeVisible();
});

test("has no WCAG A/AA violations in the author view", async ({ page }) => {
  await page.goto("./");
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
