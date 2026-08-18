package com.forwardmeasure.openworkflow.workflow.runtime.api;

import java.util.Objects;

/**
 * Atomic definition-admission projection consumed by the persistent catalogue.
 *
 * <p>Successful decisions carry the exact immutable bundle. Rejections carry only their bounded
 * audit decision.
 */
public record WorkflowDefinitionCatalogueEvent(
    WorkflowDefinitionAdmissionEvent admission, WorkflowDefinitionBundle bundle) {

  public WorkflowDefinitionCatalogueEvent {
    Objects.requireNonNull(admission, "admission");
    boolean rejected = admission.status() == WorkflowDefinitionAdmissionStatus.REJECTED;
    if (rejected != (bundle == null)) {
      throw new IllegalArgumentException(
          "Rejected catalogue events must omit a bundle and " + "successful events must carry one");
    }
    if (bundle != null
        && (!admission.key().equals(bundle.key())
            || (admission.status() == WorkflowDefinitionAdmissionStatus.ADMITTED
                && !admission.commandId().equals(bundle.admissionCommandId()))
            || !admission.sourceSha256().equals(bundle.plan().sourceSha256())
            || !admission.definitionSha256().equals(bundle.plan().definitionSha256())
            || !admission.compilerSha256().equals(bundle.compilerSha256()))) {
      throw new IllegalArgumentException("Catalogue admission and bundle do not match");
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
