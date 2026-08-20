import {
  AuthorizationApi,
  Configuration as DefinitionConfiguration,
  WorkflowDefinitionGovernanceApi,
  WorkflowDefinitionsApi,
  WorkflowsApi,
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
    authorization: new AuthorizationApi(new DefinitionConfiguration(auth)),
    definitions: new WorkflowDefinitionsApi(new DefinitionConfiguration(auth)),
    governance: new WorkflowDefinitionGovernanceApi(
      new DefinitionConfiguration(auth),
    ),
    workflows: new WorkflowsApi(new DefinitionConfiguration(auth)),
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
  const response = await clients(
    token,
  ).authorization.batchEvaluateAuthorizations({
    xCorrelationID: correlationId(),
    batchAuthorizationRequest: {
      resourceType,
      resourceId,
      properties: {},
      actions,
    },
  });
  return response.decisions;
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
