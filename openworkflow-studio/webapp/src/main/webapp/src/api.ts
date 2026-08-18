import {
  Configuration as DefinitionConfiguration,
  DefinitionsApi,
} from "@forwardmeasure/openworkflow-definition-management-client";
import {
  Configuration as ExecutionConfiguration,
  ExecutionsApi,
} from "@forwardmeasure/openworkflow-execution-client";

export function clients(
  token: string,
  basePath = window.__OPENWORKFLOW_STUDIO_CONFIG__?.apiBasePath ?? "/api",
) {
  const auth = { basePath, accessToken: token };
  return {
    definitions: new DefinitionsApi(new DefinitionConfiguration(auth)),
    executions: new ExecutionsApi(new ExecutionConfiguration(auth)),
  };
}

export function correlationId(): string {
  return crypto.randomUUID();
}

export async function authorizationDecisions(
  token: string,
  actions: string[],
  resourceType: string,
  resourceId: string,
): Promise<Record<string, boolean>> {
  const basePath = window.__OPENWORKFLOW_STUDIO_CONFIG__?.apiBasePath ?? "/api";
  const response = await fetch(`${basePath}/api/v1/authorizations`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      "X-Correlation-ID": correlationId(),
    },
    body: JSON.stringify({ resourceType, resourceId, properties: {}, actions }),
  });
  if (!response.ok) throw new Error(`Authorization evaluation failed with HTTP ${response.status}`);
  return (await response.json() as { decisions: Record<string, boolean> }).decisions;
}

export async function listDefinitionRevisions(token: string): Promise<unknown[]> {
  const basePath = window.__OPENWORKFLOW_STUDIO_CONFIG__?.apiBasePath ?? "/api";
  const response = await fetch(`${basePath}/v1/workflow-definitions`, {
    headers: { Authorization: `Bearer ${token}`, "X-Correlation-ID": correlationId() },
  });
  if (!response.ok) throw new Error(`Definition listing failed with HTTP ${response.status}`);
  return (await response.json() as { items: unknown[] }).items;
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
