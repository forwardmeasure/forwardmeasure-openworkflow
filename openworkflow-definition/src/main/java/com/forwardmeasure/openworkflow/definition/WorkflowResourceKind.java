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
package com.forwardmeasure.openworkflow.definition;

/**
 * Semantic use of an Open Workflow external resource.
 *
 * <p>The kind is carried by references and loader requests, not by the resolved bytes themselves.
 * One immutable resource may therefore be reused by more than one workflow construct without
 * duplicating its content.
 */
public enum WorkflowResourceKind {
  DATA_SCHEMA,
  ASYNC_API_DOCUMENT,
  GRPC_PROTO,
  OPEN_API_DOCUMENT,
  A2A_AGENT_CARD,
  SCRIPT_SOURCE,
  FUNCTION_DEFINITION
}
