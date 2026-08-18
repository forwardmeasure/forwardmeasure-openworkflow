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
package com.forwardmeasure.openworkflow.binding.micronaut;

import com.forwardmeasure.jpa.identity.entity.Actor;
import com.forwardmeasure.openworkflow.definition.management.api.model.DefinitionList;
import com.forwardmeasure.openworkflow.definition.management.api.model.DefinitionRevision;
import com.forwardmeasure.openworkflow.definition.management.api.model.DefinitionValidation;
import com.forwardmeasure.openworkflow.definition.management.api.model.DefinitionValidationRequest;
import com.forwardmeasure.openworkflow.definition.management.api.model.DefinitionWrite;
import com.forwardmeasure.openworkflow.definition.management.api.model.LifecycleAction;
import com.forwardmeasure.openworkflow.definition.management.api.model.LifecycleState;
import com.forwardmeasure.openworkflow.definition.management.api.model.RevisionWrite;
import com.forwardmeasure.openworkflow.definition.persistence.WorkflowDefinitionEntity;
import com.forwardmeasure.openworkflow.definition.persistence.WorkflowRevisionEntity;
import com.forwardmeasure.openworkflow.execution.api.model.Execution;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionControl;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionHistoryEntry;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionPage;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionStart;
import com.forwardmeasure.openworkflow.execution.api.model.ExecutionState;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.serde.annotation.SerdeImport;

/** Compile-time serialization metadata for the framework-neutral generated API models. */
@SerdeImport(DefinitionList.class)
@SerdeImport(DefinitionRevision.class)
@SerdeImport(DefinitionValidation.class)
@SerdeImport(DefinitionValidationRequest.class)
@SerdeImport(DefinitionWrite.class)
@SerdeImport(LifecycleAction.class)
@SerdeImport(LifecycleState.class)
@SerdeImport(RevisionWrite.class)
@SerdeImport(Execution.class)
@SerdeImport(ExecutionControl.class)
@SerdeImport(ExecutionHistoryEntry.class)
@SerdeImport(ExecutionPage.class)
@SerdeImport(ExecutionStart.class)
@SerdeImport(ExecutionState.class)
@Introspected(
    classes = {
      Actor.class,
      WorkflowDefinitionEntity.class,
      WorkflowRevisionEntity.class,
      DefinitionList.class,
      DefinitionRevision.class,
      DefinitionValidation.class,
      DefinitionValidationRequest.class,
      DefinitionWrite.class,
      LifecycleAction.class,
      LifecycleState.class,
      RevisionWrite.class,
      Execution.class,
      ExecutionControl.class,
      ExecutionHistoryEntry.class,
      ExecutionPage.class,
      ExecutionStart.class,
      ExecutionState.class
    })
final class OpenWorkflowMicronautSerde {
  private OpenWorkflowMicronautSerde() {}
}
