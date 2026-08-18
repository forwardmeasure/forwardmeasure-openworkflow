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
package com.forwardmeasure.openworkflow.definition.management;

import com.forwardmeasure.openworkflow.authorization.ActiveOrganization;
import java.util.List;

/** Transaction-proxyable boundary for the governed definition use cases. */
public interface DefinitionManagementOperations {
  ManagedWorkflowRevision create(
      ActiveOrganization actor,
      String correlationId,
      String definitionKey,
      String displayName,
      String sourceDocument);

  DefinitionValidation validate(
      ActiveOrganization actor, String correlationId, String definitionKey, String sourceDocument);

  ManagedWorkflowRevision revise(
      ActiveOrganization actor,
      String correlationId,
      String definitionKey,
      String displayName,
      String sourceDocument);

  ManagedWorkflowRevision submit(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber);

  ManagedWorkflowRevision withdraw(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber);

  ManagedWorkflowRevision approve(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber);

  ManagedWorkflowRevision reject(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber);

  ManagedWorkflowRevision publish(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber);

  ManagedWorkflowRevision deprecate(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber);

  ManagedWorkflowRevision retrieve(
      ActiveOrganization actor, String correlationId, String definitionKey, int revisionNumber);

  List<ManagedWorkflowRevision> list(ActiveOrganization actor, String correlationId);
}
