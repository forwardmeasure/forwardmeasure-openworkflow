package com.forwardmeasure.openworkflow.workflow.runtime.api;

import com.forwardmeasure.openworkflow.definition.OpenWorkflowCompiler;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceLoader;
import com.forwardmeasure.openworkflow.definition.WorkflowResourceResolver;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;

/**
 * Publication-edge preparation for an immutable workflow definition.
 *
 * <p>This operation may perform external I/O through the supplied loader and must run before a
 * command is written to Kafka. The Kafka Streams processor recompiles from the supplied bytes but
 * never invokes the loader.
 */
public final class WorkflowDefinitionPublication {
  private WorkflowDefinitionPublication() {}

  public static AdmitWorkflowDefinitionCommand prepare(
      String commandId,
      OksTenantId tenantId,
      String source,
      ActorContext actor,
      Instant requestedAt,
      WorkflowResourceLoader loader) {
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(actor, "actor");
    Objects.requireNonNull(requestedAt, "requestedAt");
    Objects.requireNonNull(loader, "loader");
    if (!tenantId.equals(actor.tenantId())) {
      throw new IllegalArgumentException("Definition and actor tenants must match");
    }
    byte[] bytes = source.getBytes(StandardCharsets.UTF_8);
    var resources = new WorkflowResourceResolver().resolve(bytes, loader);
    var plan = new OpenWorkflowCompiler().compile(bytes, resources);
    return new AdmitWorkflowDefinitionCommand(
        commandId,
        new WorkflowDefinitionKey(tenantId, plan.coordinates()),
        source,
        resources,
        actor,
        requestedAt);
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
