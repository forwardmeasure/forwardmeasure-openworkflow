import { load } from "js-yaml";

export const SAMPLE = `document:
  dsl: '1.0.3'
  namespace: forwardmeasure
  name: hello-studio
  version: '1.0.0'
do:
  - greet:
      set:
        message: Hello from OpenWorkflow Studio
`;

export function taskNames(source: string): string[] {
  try {
    const workflow = load(source) as { do?: Array<Record<string, unknown>> };
    return Array.isArray(workflow?.do)
      ? workflow.do.flatMap((task) => Object.keys(task))
      : [];
  } catch {
    return [];
  }
}

export function diagnostic(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}

export function canPause(status: string): boolean {
  return status === "RUNNING" || status === "WAITING";
}

export function lineDiff(previous: string, current: string): string[] {
  const before = previous.split("\n");
  const after = current.split("\n");
  const length = Math.max(before.length, after.length);
  return Array.from({ length }, (_, index) =>
    before[index] === after[index]
      ? `  ${after[index] ?? ""}`
      : `- ${before[index] ?? ""}\n+ ${after[index] ?? ""}`,
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
