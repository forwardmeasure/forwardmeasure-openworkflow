package com.forwardmeasure.openworkflow.workflow.runtime.kafka;

import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionAdmissionEvent;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionBundle;
import com.forwardmeasure.openworkflow.workflow.runtime.api.WorkflowDefinitionCatalogueEvent;
import java.util.Objects;

sealed interface OksDefinitionOutput {
  record Decision(WorkflowDefinitionAdmissionEvent event) implements OksDefinitionOutput {
    public Decision {
      Objects.requireNonNull(event, "event");
    }
  }

  record Bundle(WorkflowDefinitionBundle bundle) implements OksDefinitionOutput {
    public Bundle {
      Objects.requireNonNull(bundle, "bundle");
    }
  }

  record Catalogue(WorkflowDefinitionCatalogueEvent event) implements OksDefinitionOutput {
    public Catalogue {
      Objects.requireNonNull(event, "event");
    }
  }
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
