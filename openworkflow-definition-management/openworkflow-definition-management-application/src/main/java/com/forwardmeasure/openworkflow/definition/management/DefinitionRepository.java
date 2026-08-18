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

import com.forwardmeasure.jpa.tenancy.TenantId;
import java.util.List;
import java.util.Optional;

public interface DefinitionRepository {
  boolean exists(TenantId tenantId, String definitionKey);

  int nextRevisionNumber(TenantId tenantId, String definitionKey);

  void save(
      TenantId tenantId,
      ManagedWorkflowRevision revision,
      String actingActorId,
      String correlationId);

  Optional<ManagedWorkflowRevision> find(
      TenantId tenantId, String definitionKey, int revisionNumber);

  List<ManagedWorkflowRevision> list(TenantId tenantId);
}
